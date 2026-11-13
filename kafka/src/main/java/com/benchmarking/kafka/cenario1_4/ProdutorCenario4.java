package com.benchmarking.kafka.cenario1_4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cenário 4 — Múltiplos consumidores — PRODUTOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - número de mensagens
 *   3 - execução (0 = aquecimento)
 *   4 - tamanho da mensagem em bytes
 *   5 - número de partições do tópico
 *
 * Envia todas as mensagens sem controle de taxa: elas entram no tópico logo
 * no início e o que se mede é quanto tempo os consumidores levam para tratar
 * tudo. Com 5 ms de processamento por mensagem, o produtor é muito mais
 * rápido que qualquer configuração de consumidores.
 *
 * A mensagem i vai para a partição (i % partições). Assim cada partição
 * recebe exatamente o mesmo número de mensagens, e a comparação entre as
 * configurações fica só no lado do consumidor (com chave + hash, a divisão
 * não sai exatamente igual).
 *
 * Mesma configuração do Cenário 1: acks=1, linger.ms=0, batch.size de 64 KB,
 * idempotência desligada. Metadados buscados antes do t0, e qualquer falha
 * de envio faz o processo sair com código 1.
 */
public class ProdutorCenario4 {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ProdutorCenario4 <bootstrap> <topico> <numMsgs> <execucao> <tamanhoBytes> <numParticoes>");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        int    tamanhoBytes     = Integer.parseInt(args[4]);
        int    numParticoes     = Integer.parseInt(args[5]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // mesma configuração do Cenário 1
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        props.put("auto.include.jmx.reporter", "false");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        byte[] payload = new byte[tamanhoBytes];
        AtomicInteger falhas = new AtomicInteger(0);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {

            // metadados + conexão FORA da medição, e confere as partições
            List<PartitionInfo> particoes = producer.partitionsFor(topico);
            if (particoes.size() != numParticoes) {
                System.err.printf("Tópico com %d partições; esperado %d%n", particoes.size(), numParticoes);
                System.exit(1);
            }

            long t0 = System.nanoTime();

            for (int i = 0; i < numMsgs; i++) {
                final int seq = i + 1;
                long ts = System.nanoTime();

                ProducerRecord<String, byte[]> record =
                        new ProducerRecord<>(topico, i % numParticoes, null, payload);
                record.headers().add(HEADER_TS,  ByteBuffer.allocate(Long.BYTES).putLong(ts).array());
                record.headers().add(HEADER_SEQ, ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());

                producer.send(record, (metadata, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) {
                        System.err.println("Falha ao enviar mensagem " + seq + ": " + ex.getMessage());
                    }
                });
            }

            producer.flush();
            long t1 = System.nanoTime();

            System.out.println("T0="       + t0);
            System.out.println("T1="       + t1);
            System.out.println("MSGS="     + numMsgs);
            System.out.println("FALHAS="   + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
        }

        if (falhas.get() > 0) {
            System.err.printf("Falhas de envio: %d/%d%n", falhas.get(), numMsgs);
            System.exit(1);
        }
    }
}
