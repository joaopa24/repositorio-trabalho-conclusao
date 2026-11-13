package com.benchmarking.kafka.cenario1_3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Cenário 3 — Variação do tamanho da mensagem — CONSUMIDOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - número de mensagens esperado
 *   3 - execução (0 = aquecimento)
 *   4 - tamanho esperado da mensagem em bytes
 *   5 - arquivo CSV detalhado ("-" = não gravar; usado na Parte B e no aquecimento)
 *
 * MUDANÇAS em relação à versão anterior:
 *
 *   - Configuração IGUAL à do Cenário 1: grupo novo a cada execução,
 *     auto.offset.reset=earliest, sem commit de offset (o grupo é
 *     descartável), fetch.min.bytes=1 e fetch.max.wait.ms=0. O commitAsync
 *     por lote foi removido: não tem utilidade com grupo descartável e é
 *     uma requisição a mais no caminho medido.
 *
 *   - Latências gravadas na ORDEM DE CHEGADA (é isso que permite verificar,
 *     no gráfico "latência ao longo da execução", se formou fila).
 *
 *   - Confere o tamanho de cada mensagem recebida. Se alguma vier com
 *     tamanho diferente do esperado, o processo falha.
 *
 *   - Imprime a hora (nanoTime) da primeira e da última chegada. O
 *     orquestrador usa a última chegada para calcular a vazão de ponta a
 *     ponta da Parte B (do primeiro envio até a última entrega).
 *
 *   - CSV em formato padrão (vírgula como separador, ponto decimal), igual
 *     aos Cenários 1 e 2.
 */
public class ConsumidorCenario3 {

    private static final String HEADER_TS = "ts-envio";
    private static final long TIMEOUT_LOOP_NS = 300_000_000_000L; // 300 s

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ConsumidorCenario3 <bootstrap> <topico> <numMsgs> <execucao> <tamanhoBytes> <arquivoCSV|->");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        int    tamanhoBytes     = Integer.parseInt(args[4]);
        String arquivoCSV       = args[5];

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-c3-" + execucao + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        long[] latenciasNs = new long[numMsgs];   // ordem de chegada
        int recebidas = 0;
        int negativas = 0;
        int tamanhoErrado = 0;
        long primeiraChegada = 0;
        long ultimaChegada = 0;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topico));

            // só sinaliza PRONTO depois que a partição foi atribuída
            while (consumer.assignment().isEmpty()) {
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
                for (ConsumerRecord<String, byte[]> record : records) {
                    if (recebidas >= numMsgs) break;
                    long chegada = System.nanoTime();

                    Header h = record.headers().lastHeader(HEADER_TS);
                    if (h == null || h.value() == null || h.value().length != Long.BYTES) {
                        System.err.println("Mensagem sem header de timestamp válido");
                        continue;
                    }
                    if (record.value() == null || record.value().length != tamanhoBytes) {
                        tamanhoErrado++;
                    }

                    long latencia = chegada - ByteBuffer.wrap(h.value()).getLong();
                    if (latencia < 0) negativas++;

                    if (recebidas == 0) primeiraChegada = chegada;
                    ultimaChegada = chegada;
                    latenciasNs[recebidas++] = latencia;
                }
            }
        }

        if (recebidas != numMsgs) {
            throw new IllegalStateException("Número incorreto de mensagens. Esperado="
                    + numMsgs + ", recebido=" + recebidas);
        }
        if (tamanhoErrado > 0) {
            throw new IllegalStateException(tamanhoErrado + " mensagens com tamanho diferente de "
                    + tamanhoBytes + " bytes");
        }
        if (negativas > 0) {
            System.err.printf("Aviso: %d latências negativas%n", negativas);
        }

        if (!"-".equals(arquivoCSV)) {
            gravarDetalhado(arquivoCSV, execucao, tamanhoBytes / 1024, latenciasNs);
        }

        long[] ord = Arrays.copyOf(latenciasNs, recebidas);
        Arrays.sort(ord);
        double media = Arrays.stream(ord).average().orElse(0) / 1e6;
        double mediana = (ord.length % 2 == 1)
                ? ord[ord.length / 2] / 1e6
                : (ord[ord.length / 2 - 1] + ord[ord.length / 2]) / 2.0 / 1e6;

        System.out.println("MEDIA="   + String.format(Locale.US, "%.4f", media));
        System.out.println("MEDIANA=" + String.format(Locale.US, "%.4f", mediana));
        System.out.println("MIN="     + String.format(Locale.US, "%.4f", ord[0] / 1e6));
        System.out.println("MAX="     + String.format(Locale.US, "%.4f", ord[ord.length - 1] / 1e6));
        System.out.println("PRIMEIRA_CHEGADA=" + primeiraChegada);
        System.out.println("ULTIMA_CHEGADA="   + ultimaChegada);
        System.out.println("NEGATIVAS=" + negativas);
        System.out.println("RECEBIDAS=" + recebidas);
    }

    /** execucao,tamanho_kb,mensagem,latencia_ms — acumula entre execuções. */
    static void gravarDetalhado(String arquivoCSV, int execucao, int tamanhoKb, long[] latenciasNs)
            throws Exception {
        File arquivo = new File(arquivoCSV);
        boolean novo = !arquivo.exists() || arquivo.length() == 0;
        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
            if (novo) w.println("execucao,tamanho_kb,mensagem,latencia_ms");
            for (int i = 0; i < latenciasNs.length; i++) {
                w.printf(Locale.US, "%d,%d,%d,%.4f%n", execucao, tamanhoKb, i + 1, latenciasNs[i] / 1e6);
            }
        }
    }
}
