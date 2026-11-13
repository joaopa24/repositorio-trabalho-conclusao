package com.benchmarking.kafka.cenario1_4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cenário 4 — Múltiplos consumidores — CONSUMIDOR (Kafka).
 *
 * Um dos N consumidores do MESMO grupo. O Kafka dá a cada membro uma ou mais
 * partições, e cada partição é lida por um único membro.
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - group.id (gerado pelo orquestrador, único por execução)
 *   3 - execução (0 = aquecimento)
 *   4 - arquivo temporário onde gravar as mensagens tratadas
 *   5 - max.poll.records
 *   6 - id deste consumidor (1..N)
 *   7 - tempo de processamento simulado por mensagem, em ms
 *
 * Processamento: Thread.sleep(ms) por mensagem. Simula um trabalho de I/O
 * (consultar um banco, chamar uma API) sem gastar CPU, então N consumidores
 * não disputam núcleos entre si. O tempo real de cada mensagem é medido
 * (início e fim), porque no Windows o sleep pode durar mais que o pedido.
 *
 * Commit do offset uma vez por lote do poll(), depois de processar o lote
 * (o equivalente ao ack manual do RabbitMQ).
 *
 * Conversa com o orquestrador:
 *   stdout  STATUS=PRONTO      depois do subscribe; o orquestrador então
 *                              confere pelo AdminClient se o grupo está
 *                              estável antes de soltar o produtor
 *   stdin   PRODUTOR_TERMINOU  todas as mensagens já estão no tópico
 *   stdout  STATUS=CONCLUIDO   3 s sem nada para processar depois do sinal
 *   stdin   SAIR               todos concluíram: grava o arquivo e encerra
 *
 * O consumidor só sai do grupo quando TODOS concluíram. Se um consumidor
 * saísse antes (por exemplo, um sem partição na configuração 6c-4p), o grupo
 * faria um rebalanceamento no meio do processamento dos outros.
 *
 * Saída (stdout): RECEBIDAS, PROC_MEDIO_MS, PARTICOES, REVOGACOES.
 * REVOGACOES = partições perdidas por rebalanceamento DURANTE o
 * processamento (deveria ser sempre 0).
 */
public class ConsumidorCenario4 {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    private static final long OCIOSO_MS    = 3_000;
    private static final long MAX_TOTAL_MS = 900_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 8) {
            System.err.println("Uso: ConsumidorCenario4 <bootstrap> <topico> <groupId> <execucao> <arquivo> <maxPollRecords> <consumidorId> <procMs>");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        String groupId          = args[2];
        int    execucao         = Integer.parseInt(args[3]);
        String arquivo          = args[4];
        int    maxPollRecords   = Integer.parseInt(args[5]);
        int    consumidorId     = Integer.parseInt(args[6]);
        long   procMs           = Long.parseLong(args[7]);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,                "c4-consumidor-" + consumidorId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         String.valueOf(maxPollRecords));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        "0");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,          "1");
        // divisão simples e previsível: com N = partições, uma partição por consumidor
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        // sinais vindos do orquestrador pela entrada padrão
        AtomicLong    produtorTerminouNs = new AtomicLong(0);
        AtomicBoolean sair = new AtomicBoolean(false);
        Thread leitorStdin = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                String linha;
                while ((linha = in.readLine()) != null) {
                    if (linha.trim().equals("PRODUTOR_TERMINOU")) produtorTerminouNs.set(System.nanoTime());
                    if (linha.trim().equals("SAIR")) break;
                }
            } catch (IOException ignored) {
            }
            sair.set(true); // SAIR ou o orquestrador fechou a entrada
        });
        leitorStdin.setDaemon(true);
        leitorStdin.start();

        List<long[]> registros = new ArrayList<>(); // seq, envio, inicio, fim
        Set<Integer> particoesRecebidas = new TreeSet<>();
        int[] revogacoes = {0};
        long inicioProcesso = System.nanoTime();
        long ultimaAtividade = 0;
        boolean concluido = false;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topico), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    // só conta perdas depois de começar a processar e antes de sair
                    if (!parts.isEmpty() && !registros.isEmpty() && !sair.get()) {
                        revogacoes[0] += parts.size();
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                    System.out.println("ATRIBUIDAS=" + parts);
                }
            });

            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (!sair.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, byte[]> record : records) {
                    long inicio = System.nanoTime();
                    processar(procMs);
                    long fim = System.nanoTime();

                    Header ts  = record.headers().lastHeader(HEADER_TS);
                    Header seq = record.headers().lastHeader(HEADER_SEQ);
                    if (ts == null || seq == null) {
                        System.err.println("Mensagem sem headers ts-envio/sequencia");
                        continue;
                    }
                    registros.add(new long[]{
                            ByteBuffer.wrap(seq.value()).getInt(),
                            ByteBuffer.wrap(ts.value()).getLong(),
                            inicio, fim});
                    particoesRecebidas.add(record.partition());
                    offsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    ultimaAtividade = fim;
                }
                if (!records.isEmpty()) {
                    consumer.commitAsync(new HashMap<>(offsets), (o, ex) -> {
                        if (ex != null) System.err.println("Falha em commitAsync: " + ex.getMessage());
                    });
                }

                long agora = System.nanoTime();
                long sinal = produtorTerminouNs.get();
                if (!concluido && sinal > 0
                        && (agora - Math.max(sinal, ultimaAtividade)) / 1_000_000 > OCIOSO_MS) {
                    concluido = true;
                    System.out.println("STATUS=CONCLUIDO");
                    System.out.flush();
                }
                if ((agora - inicioProcesso) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido sem o sinal SAIR");
                    System.exit(1);
                }
            }

            if (!offsets.isEmpty()) {
                try {
                    consumer.commitSync(offsets);
                } catch (Exception e) {
                    System.err.println("Falha no commitSync final: " + e.getMessage());
                }
            }
        }

        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, false))) {
            w.println("sequencia,envio_ns,inicio_ns,fim_ns");
            for (long[] r : registros) {
                w.println(r[0] + "," + r[1] + "," + r[2] + "," + r[3]);
            }
        }

        double procMedio = registros.stream().mapToLong(r -> r[3] - r[2]).average().orElse(0) / 1e6;
        System.out.println("CONSUMIDOR_ID=" + consumidorId);
        System.out.println("EXECUCAO="      + execucao);
        System.out.println("PARTICOES="     + particoesRecebidas);
        System.out.println("REVOGACOES="    + revogacoes[0]);
        System.out.println("PROC_MEDIO_MS=" + String.format(Locale.US, "%.4f", procMedio));
        System.out.println("RECEBIDAS="     + registros.size());
    }

    /** Processamento simulado: sleep (trabalho de I/O, sem gastar CPU). */
    static void processar(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
