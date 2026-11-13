package com.benchmarking.kafka.cenario2_1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

/**
 * Nível intermediário — Cenário 1: Comparação de garantias — CONSUMIDOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - número de mensagens esperado (sem contar duplicatas)
 *   3 - execução (0 = aquecimento)
 *   4 - garantia: at-most-once | at-least-once | exactly-once
 *   5 - arquivo CSV detalhado ("-" = não gravar; Parte B e aquecimento)
 *   6 - max.poll.records
 *
 * Configuração por nível:
 *   at-most-once  : commit síncrono do lote ANTES de processar. Se o
 *                   consumidor cair no meio do lote, o resto se perde.
 *   at-least-once : commit ASSÍNCRONO do lote DEPOIS de processar. Se cair,
 *                   ou se um commit se perder, o lote é relido (pode haver
 *                   duplicata), o que o at least once permite.
 *   exactly-once  : isolation.level=read_committed (só lê transações
 *                   concluídas) + commit assíncrono DEPOIS + deduplicação por
 *                   número de sequência. As transações garantem o
 *                   "exatamente uma vez" entre produtor e broker; no
 *                   consumidor final o efeito vem de ele ser idempotente
 *                   (descarta o que já viu).
 *
 * Por que assíncrono no at-least-once e no exactly-once: o basicAck do
 * RabbitMQ não espera resposta do broker. Com commitSync, o consumidor
 * Kafka pararia a cada lote esperando o broker, um custo que a garantia não
 * exige e que o RabbitMQ não paga. Só o at-most-once continua síncrono,
 * porque ali o commit precisa estar confirmado ANTES de processar. Ao final,
 * um commitSync fecha o último lote (fora da medição).
 *
 * A latência é carimbada uma vez por lote, logo que o poll() devolve os
 * registros. Assim o commit síncrono do at-most-once (feito antes de
 * processar) não entra na latência do próprio lote; o custo dele aparece
 * como atraso dos lotes seguintes.
 *
 * Mesmo padrão do Cenário 3: STATUS=PRONTO só depois da atribuição da
 * partição, fetch.max.wait.ms=0, latências gravadas na ordem de chegada,
 * contagem conferida (o processo falha se não bater).
 */
public class ConsumidorGarantias {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";
    private static final long TIMEOUT_PRONTO_MS = 30_000;
    private static final long TIMEOUT_LOOP_NS   = 300_000_000_000L; // 300 s

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorGarantias <bootstrap> <topico> <numMsgs> <execucao> <garantia> <arquivoCSV|-> <maxPollRecords>");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        String garantia         = args[4];
        String arquivoCSV       = args[5];
        int    maxPollRecords   = Integer.parseInt(args[6]);

        boolean commitAntes = garantia.equals("at-most-once");
        boolean exactlyOnce = garantia.equals("exactly-once");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "bench-g1-" + execucao + "-" + garantia + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         String.valueOf(maxPollRecords));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        "0");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,          "1");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,          exactlyOnce ? "read_committed" : "read_uncommitted");
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        long[] latenciasNs = new long[numMsgs];   // ordem de chegada, sem duplicatas
        boolean[] vistos = new boolean[numMsgs + 1];
        int recebidas = 0;
        int duplicatas = 0;
        int negativas = 0;
        int[] falhasCommit = {0};   // commits assíncronos que falharam (contados no callback)
        long primeiraChegada = 0;
        long ultimaChegada = 0;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topico));

            // só sinaliza PRONTO depois que a partição foi atribuída
            long inicioEspera = System.currentTimeMillis();
            while (consumer.assignment().isEmpty()) {
                if (System.currentTimeMillis() - inicioEspera > TIMEOUT_PRONTO_MS) {
                    System.err.println("Partição não atribuída em " + TIMEOUT_PRONTO_MS / 1000 + " s");
                    System.exit(1);
                }
                consumer.poll(Duration.ofMillis(100));
            }
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            long inicioLoop = System.nanoTime();
            while (recebidas < numMsgs) {
                if (System.nanoTime() - inicioLoop > TIMEOUT_LOOP_NS) {
                    System.err.printf("TIMEOUT: %d/%d mensagens recebidas%n", recebidas, numMsgs);
                    break;
                }

                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) continue;
                long chegadaLote = System.nanoTime();

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, byte[]> r : records) {
                    offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                }

                // at-most-once: confirma ANTES de processar
                if (commitAntes) consumer.commitSync(offsets);

                for (ConsumerRecord<String, byte[]> record : records) {
                    Header ts  = record.headers().lastHeader(HEADER_TS);
                    Header seq = record.headers().lastHeader(HEADER_SEQ);
                    if (ts == null || seq == null || ts.value().length != Long.BYTES
                            || seq.value().length != Integer.BYTES) {
                        System.err.println("Mensagem sem headers ts-envio/sequencia válidos");
                        continue;
                    }
                    int s = ByteBuffer.wrap(seq.value()).getInt();
                    if (s < 1 || s > numMsgs) {
                        System.err.println("Sequência fora do intervalo: " + s);
                        continue;
                    }
                    if (vistos[s]) {
                        // duplicata: no exactly-once é descartada; nos outros também é
                        // contada, para a contagem de mensagens únicas fechar
                        duplicatas++;
                        continue;
                    }
                    vistos[s] = true;

                    long latencia = chegadaLote - ByteBuffer.wrap(ts.value()).getLong();
                    if (latencia < 0) negativas++;
                    if (recebidas == 0) primeiraChegada = chegadaLote;
                    ultimaChegada = chegadaLote;
                    latenciasNs[recebidas++] = latencia;
                }

                // at-least-once e exactly-once: confirma DEPOIS de processar,
                // sem esperar a resposta do broker (como o basicAck do RabbitMQ).
                // O callback roda na própria thread do consumidor, dentro de um
                // poll() seguinte, então o vetor não precisa de sincronização.
                if (!commitAntes) {
                    consumer.commitAsync(offsets, (o, ex) -> {
                        if (ex != null && ++falhasCommit[0] <= 5) {
                            System.err.println("Falha no commit assíncrono: " + ex.getMessage());
                        }
                    });
                }
            }

            // fecha o último lote de forma síncrona; já está fora da medição,
            // pois todas as chegadas foram carimbadas
            if (!commitAntes) {
                try {
                    consumer.commitSync();
                } catch (Exception e) {
                    System.err.println("Falha no commit final: " + e.getMessage());
                }
            }
        }

        if (recebidas != numMsgs) {
            throw new IllegalStateException("Número incorreto de mensagens. Esperado="
                    + numMsgs + ", recebido=" + recebidas);
        }
        if (negativas > 0) {
            System.err.printf("Aviso: %d latências negativas%n", negativas);
        }

        if (!"-".equals(arquivoCSV)) {
            File arquivo = new File(arquivoCSV);
            boolean novo = !arquivo.exists() || arquivo.length() == 0;
            try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
                if (novo) w.println("execucao,garantia,mensagem,latencia_ms");
                for (int i = 0; i < recebidas; i++) {
                    w.printf(Locale.US, "%d,%s,%d,%.4f%n", execucao, garantia, i + 1, latenciasNs[i] / 1e6);
                }
            }
        }

        long[] ord = Arrays.copyOf(latenciasNs, recebidas);
        Arrays.sort(ord);
        double media = Arrays.stream(ord).average().orElse(0) / 1e6;
        double mediana = (ord.length % 2 == 1)
                ? ord[ord.length / 2] / 1e6
                : (ord[ord.length / 2 - 1] + ord[ord.length / 2]) / 2.0 / 1e6;

        System.out.println("MEDIA="      + String.format(Locale.US, "%.4f", media));
        System.out.println("MEDIANA="    + String.format(Locale.US, "%.4f", mediana));
        System.out.println("MIN="        + String.format(Locale.US, "%.4f", ord[0] / 1e6));
        System.out.println("MAX="        + String.format(Locale.US, "%.4f", ord[ord.length - 1] / 1e6));
        System.out.println("PRIMEIRA_CHEGADA=" + primeiraChegada);
        System.out.println("ULTIMA_CHEGADA="   + ultimaChegada);
        System.out.println("NEGATIVAS="  + negativas);
        System.out.println("DUPLICATAS=" + duplicatas);
        System.out.println("FALHAS_COMMIT=" + falhasCommit[0]);
        System.out.println("RECEBIDAS="  + recebidas);
    }
}