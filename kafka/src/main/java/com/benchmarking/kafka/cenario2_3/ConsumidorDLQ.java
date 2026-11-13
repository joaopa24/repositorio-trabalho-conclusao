package com.benchmarking.kafka.cenario2_3;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 3: Dead Letter Queue — CONSUMIDOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico principal
 *   2 - tópico da DLQ
 *   3 - group.id (único por execução)
 *   4 - número de mensagens esperado
 *   5 - execução (0 = aquecimento)
 *   6 - max.poll.records
 *   7 - número máximo de tentativas por mensagem
 *
 * O Kafka não tem DLQ no broker: tudo é feito pela aplicação.
 *   - Mensagem válida: processada uma vez.
 *   - Mensagem inválida: o processamento falha; o consumidor tenta de novo,
 *     ali mesmo, até MAX tentativas (a partição fica parada enquanto isso,
 *     porque o offset só avança depois). Esgotadas as tentativas, publica a
 *     mensagem no tópico da DLQ com headers de diagnóstico: motivo,
 *     número de tentativas, tópico, partição e offset de origem.
 *   - O commit do lote só acontece depois que a DLQ confirmou as mensagens
 *     desviadas (flush). Se o consumidor caísse antes, o lote seria relido
 *     e nada se perderia.
 *   - O commit é assíncrono (commitAsync), como nos Cenários 1 e 2: o
 *     consumidor não fica parado esperando o broker a cada lote, assim como
 *     o basicAck do RabbitMQ não espera resposta. Depois do loop, um
 *     commitSync final garante que o último offset foi gravado.
 *
 * Mesma política de tentativas do consumidor RabbitMQ, para a comparação
 * ser direta.
 */
public class ConsumidorDLQ {

    private static final String HEADER_SEQ      = "sequencia";
    private static final String HEADER_INVALIDO = "invalido";
    private static final long TIMEOUT_PRONTO_MS = 30_000;
    private static final long TIMEOUT_LOOP_MS   = 300_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 8) {
            System.err.println("Uso: ConsumidorDLQ <bootstrap> <topico> <topicoDlq> <groupId> <numMsgs> <execucao> <maxPoll> <maxTentativas>");
            System.exit(2);
        }
        String bootstrapServers = args[0];
        String topico           = args[1];
        String topicoDlq        = args[2];
        String groupId          = args[3];
        int    numMsgs          = Integer.parseInt(args[4]);
        int    execucao         = Integer.parseInt(args[5]);
        int    maxPoll          = Integer.parseInt(args[6]);
        int    maxTentativas    = Integer.parseInt(args[7]);

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        cp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         String.valueOf(maxPoll));
        cp.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        "0");
        cp.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,          "1");
        cp.put("auto.include.jmx.reporter", "false");
        cp.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.ACKS_CONFIG,                   "all");
        pp.put(ProducerConfig.LINGER_MS_CONFIG,              0);
        pp.put("auto.include.jmx.reporter", "false");
        pp.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        int processadasOk = 0;
        int enviadasDlq = 0;
        int tentativasInvalidas = 0;
        long ultimoFim = 0;
        AtomicInteger falhasDlq = new AtomicInteger();
        int[] falhasCommit = {0};   // o callback do commitAsync roda na thread do poll

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(cp);
             KafkaProducer<String, byte[]> dlq = new KafkaProducer<>(pp)) {

            dlq.partitionsFor(topicoDlq);   // conexão do produtor da DLQ fora da medição
            consumer.subscribe(List.of(topico));
            long inicio = System.currentTimeMillis();
            while (consumer.assignment().isEmpty()) {
                if (System.currentTimeMillis() - inicio > TIMEOUT_PRONTO_MS) {
                    System.err.println("Partição não atribuída em " + TIMEOUT_PRONTO_MS / 1000 + " s");
                    System.exit(1);
                }
                consumer.poll(Duration.ofMillis(100));
            }
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            inicio = System.currentTimeMillis();
            while (processadasOk + enviadasDlq < numMsgs) {
                if (System.currentTimeMillis() - inicio > TIMEOUT_LOOP_MS) {
                    System.err.printf("TIMEOUT: %d/%d mensagens tratadas%n", processadasOk + enviadasDlq, numMsgs);
                    System.exit(1);
                }
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) continue;

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                boolean desviouAlguma = false;

                for (ConsumerRecord<String, byte[]> record : records) {
                    offsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    int tentativas = 0;
                    boolean ok = false;
                    String motivo = null;
                    while (tentativas < maxTentativas && !ok) {
                        tentativas++;
                        try {
                            processar(record);
                            ok = true;
                        } catch (IllegalArgumentException e) {
                            motivo = e.getMessage();
                        }
                    }

                    if (ok) {
                        processadasOk++;
                        if (tentativas > 1) tentativasInvalidas += tentativas;
                    } else {
                        tentativasInvalidas += tentativas;
                        // desvio para a DLQ, com headers de diagnóstico
                        ProducerRecord<String, byte[]> d = new ProducerRecord<>(topicoDlq, record.key(), record.value());
                        for (Header h : record.headers()) d.headers().add(h);
                        d.headers().add("dlq-motivo",     motivo.getBytes(StandardCharsets.UTF_8));
                        d.headers().add("dlq-tentativas", ByteBuffer.allocate(Integer.BYTES).putInt(tentativas).array());
                        d.headers().add("dlq-topico",     record.topic().getBytes(StandardCharsets.UTF_8));
                        d.headers().add("dlq-particao",   ByteBuffer.allocate(Integer.BYTES).putInt(record.partition()).array());
                        d.headers().add("dlq-offset",     ByteBuffer.allocate(Long.BYTES).putLong(record.offset()).array());
                        dlq.send(d, (m, ex) -> {
                            if (ex != null && falhasDlq.incrementAndGet() <= 5) {
                                System.err.println("Falha ao publicar na DLQ: " + ex.getMessage());
                            }
                        });
                        enviadasDlq++;
                        desviouAlguma = true;
                    }
                }

                // a DLQ confirma ANTES do commit: nada se perde se o consumidor cair aqui
                if (desviouAlguma) dlq.flush();
                if (falhasDlq.get() > 0) {
                    System.err.println("Falhas ao publicar na DLQ: " + falhasDlq.get());
                    System.exit(1);
                }
                // commit assíncrono: só é enviado depois do flush acima, então a ordem se mantém
                consumer.commitAsync(offsets, (o, e) -> {
                    if (e != null && ++falhasCommit[0] <= 5) {
                        System.err.println("Falha no commit assíncrono: " + e.getMessage());
                    }
                });
                ultimoFim = System.nanoTime();
            }

            // fora da medição: garante que o último offset foi gravado
            try {
                consumer.commitSync();
            } catch (Exception e) {
                System.err.println("Falha no commit final: " + e.getMessage());
            }
        }

        System.out.println("PROCESSADAS_OK="       + processadasOk);
        System.out.println("ENVIADAS_PARA_DLQ="    + enviadasDlq);
        System.out.println("TENTATIVAS_INVALIDAS=" + tentativasInvalidas);
        System.out.println("ULTIMO_FIM="           + ultimoFim);
        System.out.println("FALHAS_COMMIT="        + falhasCommit[0]);
        System.out.println("EXECUCAO="             + execucao);
    }

    /** Processamento simulado: a mensagem marcada como inválida sempre falha. */
    static void processar(ConsumerRecord<String, byte[]> record) {
        Header inv = record.headers().lastHeader(HEADER_INVALIDO);
        if (record.headers().lastHeader(HEADER_SEQ) == null) {
            throw new IllegalArgumentException("sem header sequencia");
        }
        if (inv != null && inv.value().length > 0 && inv.value()[0] == 1) {
            throw new IllegalArgumentException("mensagem inválida (campo obrigatório ausente)");
        }
    }
}