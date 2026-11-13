package com.benchmarking.kafka.cenario1_2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cenário 2 — Produtor (Kafka).
 *
 * Envia todas as mensagens na velocidade máxima, para que o consumidor
 * (5 ms por mensagem) tenha SEMPRE mensagens esperando. Assim, qualquer
 * tempo em que o consumidor fique parado é custo da entrega, e não falta
 * de mensagens.
 *
 * Cada mensagem leva dois cabeçalhos: o instante de envio (nanoTime) e o
 * número de sequência.
 *
 * Args: bootstrapServers topico numMsgs execucao
 */
public class ProdutorCenario2 {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: ProdutorCenario2 <bootstrapServers> <topico> <numMsgs> <execucao>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put("auto.include.jmx.reporter", "false");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        byte[] payload = new byte[1024];
        AtomicInteger falhas = new AtomicInteger(0);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            long t0 = System.nanoTime();

            for (int i = 0; i < numMsgs; i++) {
                long ts = System.nanoTime();
                int seq = i + 1;

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topico, payload);
                record.headers().add(HEADER_TS, ByteBuffer.allocate(Long.BYTES).putLong(ts).array());
                record.headers().add(HEADER_SEQ, ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        falhas.incrementAndGet();
                        System.err.println("Falha ao enviar mensagem " + seq + ": " + ex.getMessage());
                    }
                });
            }

            // espera todas as confirmações do broker
            producer.flush();
            long t1 = System.nanoTime();

            double duracaoS = (t1 - t0) / 1e9;
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("VAZAO=" + String.format(Locale.US, "%.2f", numMsgs / duracaoS));
            System.out.println("FALHAS_ENVIO=" + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
        }

        if (falhas.get() > 0) {
            System.exit(2);
        }
    }
}
