package com.benchmarking.kafka.cenario2_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 1: Comparação de garantias — PRODUTOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - número de mensagens
 *   3 - execução (0 = aquecimento)
 *   4 - garantia: at-most-once | at-least-once | exactly-once
 *   5 - tamanho da mensagem em bytes
 *   6 - taxa alvo em msg/s (0 = sem controle de taxa)
 *
 * Configuração por nível:
 *
 *   at-most-once  : acks=0, retries=0, idempotência desligada. "Envia e
 *                   esquece", o equivalente ao RabbitMQ sem publisher
 *                   confirms. Com acks=1 e reenvios, o produtor poderia
 *                   duplicar uma mensagem, o que não é at most once.
 *
 *   at-least-once : acks=all, reenvios padrão e idempotência DESLIGADA
 *                   explicitamente. Desde o Kafka 3.0 a idempotência vem
 *                   ligada por padrão com acks=all; sem desligar, este nível
 *                   ficaria igual ao exactly-once no produtor.
 *
 *   exactly-once  : produtor TRANSACIONAL (transactional.id + idempotência).
 *                   As mensagens são enviadas dentro de transações fechadas a
 *                   cada 100 ms, o intervalo que o Kafka Streams usa por
 *                   padrão em modo exactly-once. Com read_committed, o
 *                   consumidor só vê a mensagem depois do commit da
 *                   transação: esse é o custo do nível.
 *
 * Nos três: linger.ms=0, batch.size de 64 KB, metadados buscados antes do t0
 * e controle de taxa com aguardarAte() (mesmo método dos Cenários 1 e 3).
 * Qualquer falha de envio faz o processo sair com código 1.
 */
public class ProdutorGarantias {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    /** Intervalo de commit da transação no exactly-once. */
    private static final long INTERVALO_TRANSACAO_NS = 100_000_000L; // 100 ms

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ProdutorGarantias <bootstrap> <topico> <numMsgs> <execucao> <garantia> <tamanhoBytes> <taxaMsgS>");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        String garantia         = args[4];
        int    tamanhoBytes     = Integer.parseInt(args[5]);
        int    taxaMsgS         = Integer.parseInt(args[6]);

        boolean transacional = garantia.equals("exactly-once");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put("auto.include.jmx.reporter", "false");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        switch (garantia) {
            case "at-most-once" -> {
                props.put(ProducerConfig.ACKS_CONFIG, "0");
                props.put(ProducerConfig.RETRIES_CONFIG, 0);
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case "at-least-once" -> {
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case "exactly-once" -> {
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        "bench-g1-" + execucao + "-" + System.nanoTime());
            }
            default -> {
                System.err.println("Garantia desconhecida: " + garantia);
                System.exit(2);
            }
        }

        byte[] payload = new byte[tamanhoBytes];
        AtomicInteger falhas = new AtomicInteger(0);
        int transacoes = 0;

        System.out.printf("Produtor: %d msgs, garantia %s, taxa %s%n", numMsgs, garantia,
                taxaMsgS > 0 ? taxaMsgS + " msg/s" : "máxima (sem controle)");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {

            // conexão, metadados e registro do transactional.id FORA da medição
            producer.partitionsFor(topico);
            if (transacional) producer.initTransactions();

            long t0 = System.nanoTime();
            long intervaloNs = taxaMsgS > 0 ? 1_000_000_000L / taxaMsgS : 0L;
            long inicioTransacao = 0;

            if (transacional) {
                producer.beginTransaction();
                inicioTransacao = System.nanoTime();
            }

            for (int i = 0; i < numMsgs; i++) {
                if (intervaloNs > 0) aguardarAte(t0 + i * intervaloNs);

                long ts = System.nanoTime();
                final int seq = i + 1;
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topico, payload);
                record.headers().add(HEADER_TS,  ByteBuffer.allocate(Long.BYTES).putLong(ts).array());
                record.headers().add(HEADER_SEQ, ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());

                producer.send(record, (metadata, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) {
                        System.err.println("Falha ao enviar mensagem " + seq + ": " + ex.getMessage());
                    }
                });

                // fecha a transação a cada 100 ms e abre a próxima
                if (transacional && System.nanoTime() - inicioTransacao >= INTERVALO_TRANSACAO_NS
                        && i < numMsgs - 1) {
                    producer.commitTransaction();
                    transacoes++;
                    producer.beginTransaction();
                    inicioTransacao = System.nanoTime();
                }
            }

            if (transacional) {
                producer.commitTransaction();   // inclui o flush
                transacoes++;
            } else {
                producer.flush();
            }
            long t1 = System.nanoTime();

            System.out.println("T0="         + t0);
            System.out.println("T1="         + t1);
            System.out.println("MSGS="       + numMsgs);
            System.out.println("FALHAS="     + falhas.get());
            System.out.println("TRANSACOES=" + transacoes);
            System.out.println("EXECUCAO="   + execucao);
        }

        if (falhas.get() > 0) {
            System.err.printf("Falhas de envio: %d/%d%n", falhas.get(), numMsgs);
            System.exit(1);
        }
    }

    /** Mesmo método dos Cenários 1 e 3 (busy-wait no trecho final). */
    static void aguardarAte(long alvoNs) {
        long faltaNs;
        while ((faltaNs = alvoNs - System.nanoTime()) > 0) {
            if (faltaNs > 20_000_000L) {
                try {
                    Thread.sleep((faltaNs - 3_000_000L) / 1_000_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } else {
                Thread.onSpinWait();
            }
        }
    }
}
