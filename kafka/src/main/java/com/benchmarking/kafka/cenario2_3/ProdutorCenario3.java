package com.benchmarking.kafka.cenario2_3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 3: Dead Letter Queue — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens, execução,
 * intervalo de inválidas (10 = 1 a cada 10 é inválida; 0 = nenhuma).
 *
 * Cada mensagem leva os headers "sequencia" e "invalido" (1 byte: 1 ou 0).
 * O consumidor trata a mensagem marcada como inválida como uma que sempre
 * falha no processamento (por exemplo, um campo obrigatório faltando).
 *
 * Produtor confiável: acks=all, linger.ms=0; falha de envio = código 1.
 */
public class ProdutorCenario3 {

    private static final String HEADER_SEQ      = "sequencia";
    private static final String HEADER_INVALIDO = "invalido";

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorCenario3 <bootstrap> <topico> <numMsgs> <execucao> <intervaloInvalido>");
            System.exit(2);
        }
        String bootstrapServers  = args[0];
        String topico            = args[1];
        int    numMsgs           = Integer.parseInt(args[2]);
        int    execucao          = Integer.parseInt(args[3]);
        int    intervaloInvalido = Integer.parseInt(args[4]);

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
        int invalidas = 0;
        AtomicInteger falhas = new AtomicInteger();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.partitionsFor(topico);
            long t0 = System.nanoTime();
            for (int i = 0; i < numMsgs; i++) {
                final int seq = i + 1;
                boolean invalida = intervaloInvalido > 0 && seq % intervaloInvalido == 0;
                if (invalida) invalidas++;

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topico, payload);
                record.headers().add(HEADER_SEQ,      ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                record.headers().add(HEADER_INVALIDO, new byte[]{(byte) (invalida ? 1 : 0)});
                producer.send(record, (m, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) {
                        System.err.println("Falha ao enviar mensagem " + seq + ": " + ex.getMessage());
                    }
                });
            }
            producer.flush();

            System.out.println("T0="                 + t0);
            System.out.println("MSGS="               + numMsgs);
            System.out.println("INVALIDAS_ENVIADAS=" + invalidas);
            System.out.println("FALHAS="             + falhas.get());
            System.out.println("EXECUCAO="           + execucao);
        }
        if (falhas.get() > 0) System.exit(1);
    }
}
