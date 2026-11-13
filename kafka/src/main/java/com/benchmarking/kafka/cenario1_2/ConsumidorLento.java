package com.benchmarking.kafka.cenario1_2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Cenário 2 — Consumidor Lento (Kafka).
 *
 * OBJETIVO: medir como o modelo PULL entrega mensagens a um consumidor que
 * não acompanha o produtor. O produtor envia tudo de uma vez, então sempre
 * há mensagens esperando; o que interessa é quanto tempo o consumidor fica
 * PARADO esperando a próxima mensagem chegar até ele.
 *
 * Para cada mensagem são registrados dois instantes:
 *   - chegada: quando o consumidor recebe a mensagem do poll();
 *   - fim:     quando termina o processamento (delay) e o commit.
 *
 * A partir deles:
 *   - tempo ocupado da mensagem i = fim[i] - chegada[i]
 *   - tempo ocioso antes da mensagem i = chegada[i] - fim[i-1]
 *     (quanto o consumidor esperou pela próxima mensagem)
 *   - utilização do consumidor = soma dos tempos ocupados / tempo total
 *     (mesma métrica de "consumer utilisation" usada pelo RabbitMQ)
 *
 * max.poll.records só limita quantos registros cada poll() devolve. Ele NÃO
 * muda como o consumidor busca mensagens no broker: os registros buscados
 * ficam em um buffer local e são entregues aos poucos (documentação do
 * Kafka). Por isso a hipótese é que ele quase não altera o tempo ocioso.
 *
 * Args: bootstrapServers topico numMsgs execucao delayMs maxPollRecords arquivoCSV
 */
public class ConsumidorLento {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorLento <bootstrapServers> <topico> <numMsgs> "
                    + "<execucao> <delayMs> <maxPollRecords> <arquivoCSV>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        int    delayMs          = Integer.parseInt(args[4]);
        int    maxPollRecords   = Integer.parseInt(args[5]);
        String arquivoCSV       = args[6];

        System.out.printf("Consumidor Kafka — delay: %d ms/msg | max.poll.records: %d%n",
                delayMs, maxPollRecords);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // grupo novo a cada execução: nenhum offset antigo é reaproveitado
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-c2-" + execucao + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        // o broker responde assim que houver ao menos 1 byte, sem segurar a resposta
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        // inofensivo: evita registro de MBeans JMX a cada processo novo
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        int[]  seq     = new int[numMsgs];
        long[] tsEnvio = new long[numMsgs];
        long[] chegada = new long[numMsgs];
        long[] fim     = new long[numMsgs];
        int n = 0;

        int[] falhasCommit = {0};
        OffsetCommitCallback onCommit = (offsets, ex) -> {
            if (ex != null) {
                falhasCommit[0]++;
                System.err.println("Falha em commitAsync: " + ex.getMessage());
            }
        };
        Map<TopicPartition, OffsetAndMetadata> ultimoOffset = new HashMap<>();

        // folga: 3x o tempo esperado de processamento + 60 s
        long timeoutNs = ((long) numMsgs * Math.max(delayMs, 1) * 3 + 60_000L) * 1_000_000L;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topico));

            // espera a partição ser atribuída ANTES de liberar o produtor
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            long inicioNs = System.nanoTime();

            while (n < numMsgs) {
                if (System.nanoTime() - inicioNs > timeoutNs) {
                    System.err.printf("Timeout: %d/%d mensagens recebidas%n", n, numMsgs);
                    break;
                }

                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(50));

                for (ConsumerRecord<String, byte[]> record : records) {
                    long tChegada = System.nanoTime();

                    Header hTs  = record.headers().lastHeader(HEADER_TS);
                    Header hSeq = record.headers().lastHeader(HEADER_SEQ);

                    // processamento simulado
                    aguardarPreciso(delayMs);

                    // commit assíncrono por mensagem (análogo ao basicAck do RabbitMQ)
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata om = new OffsetAndMetadata(record.offset() + 1);
                    ultimoOffset.put(tp, om);
                    consumer.commitAsync(Map.of(tp, om), onCommit);

                    long tFim = System.nanoTime();

                    if (hTs == null || hSeq == null) {
                        System.err.println("Mensagem sem cabeçalhos de medição — ignorada.");
                        continue;
                    }

                    seq[n]     = ByteBuffer.wrap(hSeq.value()).getInt();
                    tsEnvio[n] = ByteBuffer.wrap(hTs.value()).getLong();
                    chegada[n] = tChegada;
                    fim[n]     = tFim;
                    n++;

                    if (n >= numMsgs) break;
                }
            }

            // commit final síncrono, fora do caminho medido
            if (!ultimoOffset.isEmpty()) {
                try {
                    consumer.commitSync(ultimoOffset);
                } catch (Exception e) {
                    System.err.println("Falha no commitSync final: " + e.getMessage());
                }
            }
        }

        if (n != numMsgs) {
            throw new IllegalStateException("Mensagens recebidas: " + n + " de " + numMsgs);
        }

        gravarDetalhado(arquivoCSV, execucao, maxPollRecords, seq, tsEnvio, chegada, fim, n);
        imprimirMetricas(seq, tsEnvio, chegada, fim, n);
        System.out.println("FALHAS_COMMIT=" + falhasCommit[0]);
    }

    // ------------------------------------------------------------------
    // CSV detalhado: uma linha por mensagem, na ORDEM DE CHEGADA.
    // ------------------------------------------------------------------
    static void gravarDetalhado(String arquivo, int execucao, int parametro,
                                int[] seq, long[] tsEnvio, long[] chegada, long[] fim, int n)
            throws IOException {
        boolean novo = !new File(arquivo).exists() || new File(arquivo).length() == 0;
        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
            if (novo) {
                w.println("execucao,max_poll_records,ordem_chegada,sequencia,latencia_ms,"
                        + "chegada_rel_ms,ocupado_ms,ocioso_ms");
            }
            for (int i = 0; i < n; i++) {
                double latenciaMs = (chegada[i] - tsEnvio[i]) / 1e6;
                double chegadaRel = (chegada[i] - chegada[0]) / 1e6;
                double ocupadoMs  = (fim[i] - chegada[i]) / 1e6;
                String ocioso     = i == 0 ? "" : String.format(Locale.US, "%.4f",
                        (chegada[i] - fim[i - 1]) / 1e6);
                w.printf(Locale.US, "%d,%d,%d,%d,%.4f,%.4f,%.4f,%s%n",
                        execucao, parametro, i + 1, seq[i], latenciaMs, chegadaRel, ocupadoMs, ocioso);
            }
        }
    }

    // ------------------------------------------------------------------
    // Métricas agregadas da execução, lidas pelo orquestrador (CHAVE=valor).
    // ------------------------------------------------------------------
    static void imprimirMetricas(int[] seq, long[] tsEnvio, long[] chegada, long[] fim, int n) {
        long totalNs = fim[n - 1] - chegada[0];
        long ocupadoNs = 0;
        double somaLatenciaMs = 0;
        double[] ociosoMs = new double[Math.max(n - 1, 0)];

        for (int i = 0; i < n; i++) {
            ocupadoNs += fim[i] - chegada[i];
            somaLatenciaMs += (chegada[i] - tsEnvio[i]) / 1e6;
            if (i > 0) ociosoMs[i - 1] = (chegada[i] - fim[i - 1]) / 1e6;
        }

        double utilizacaoPct = totalNs > 0 ? 100.0 * ocupadoNs / totalNs : 0.0;
        double vazaoConsumidor = totalNs > 0 ? n / (totalNs / 1e9) : 0.0;
        double ociosoMedio = Arrays.stream(ociosoMs).average().orElse(0.0);
        double[] ordenado = ociosoMs.clone();
        Arrays.sort(ordenado);
        double ociosoMediana = ordenado.length == 0 ? 0.0
                : (ordenado.length % 2 == 1
                    ? ordenado[ordenado.length / 2]
                    : (ordenado[ordenado.length / 2 - 1] + ordenado[ordenado.length / 2]) / 2.0);

        System.out.println("RECEBIDAS=" + n);
        System.out.println("LATENCIA_MEDIA=" + String.format(Locale.US, "%.4f", somaLatenciaMs / n));
        System.out.println("VAZAO_CONSUMIDOR=" + String.format(Locale.US, "%.4f", vazaoConsumidor));
        System.out.println("UTILIZACAO=" + String.format(Locale.US, "%.4f", utilizacaoPct));
        System.out.println("OCIOSO_MEDIO=" + String.format(Locale.US, "%.4f", ociosoMedio));
        System.out.println("OCIOSO_MEDIANA=" + String.format(Locale.US, "%.4f", ociosoMediana));
    }

    // ------------------------------------------------------------------
    // Espera precisa, imune à resolução de ~15,6 ms do timer do Windows.
    // Para delays curtos (abaixo de 20 ms) a espera é toda em busy-wait.
    // ------------------------------------------------------------------
    private static final long LIMIAR_SLEEP_NS = 20_000_000L;

    static void aguardarPreciso(long delayMs) {
        if (delayMs <= 0) return;
        long delayNs = delayMs * 1_000_000L;
        long alvoNs = System.nanoTime() + delayNs;

        if (delayNs > LIMIAR_SLEEP_NS) {
            long margemNs = 3_000_000L;
            long faltaNs = alvoNs - System.nanoTime();
            if (faltaNs > margemNs) {
                try {
                    Thread.sleep((faltaNs - margemNs) / 1_000_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        while (System.nanoTime() < alvoNs) {
            Thread.onSpinWait();
        }
    }
}
