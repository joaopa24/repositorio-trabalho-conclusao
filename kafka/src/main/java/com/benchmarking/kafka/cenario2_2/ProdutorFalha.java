package com.benchmarking.kafka.cenario2_2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 2: Falha do consumidor — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens, execução.
 *
 * Envia todas as mensagens sem controle de taxa (a falha é no consumidor,
 * então o produtor só precisa garantir que tudo chegou ao broker). Produtor
 * confiável: acks=all e idempotência (padrão), linger.ms=0. Qualquer falha
 * de envio faz o processo sair com código 1.
 */
public class ProdutorFalha {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: ProdutorFalha <bootstrap> <topico> <numMsgs> <execucao>");
            System.exit(2);
        }
        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                   "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,             65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG,              0);
        props.put("auto.include.jmx.reporter", "false");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        byte[] payload = new byte[1024];
        AtomicInteger falhas = new AtomicInteger();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.partitionsFor(topico);
            long t0 = System.nanoTime();
            for (int i = 0; i < numMsgs; i++) {
                final int seq = i + 1;
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topico, payload);
                record.headers().add(HEADER_TS,  ByteBuffer.allocate(Long.BYTES).putLong(System.nanoTime()).array());
                record.headers().add(HEADER_SEQ, ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                producer.send(record, (m, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) {
                        System.err.println("Falha ao enviar mensagem " + seq + ": " + ex.getMessage());
                    }
                });
            }
            producer.flush();
            System.out.println("T0="       + t0);
            System.out.println("T1="       + System.nanoTime());
            System.out.println("MSGS="     + numMsgs);
            System.out.println("FALHAS="   + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
        }
        if (falhas.get() > 0) System.exit(1);
    }
}
