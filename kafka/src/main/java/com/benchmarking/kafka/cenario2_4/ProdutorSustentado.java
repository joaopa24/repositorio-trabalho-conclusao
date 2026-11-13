package com.benchmarking.kafka.cenario2_4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 4: Sobrecarga prolongada — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens, taxa alvo
 * (msg/s), execução.
 *
 * Envia a uma taxa FIXA, acima da capacidade do consumidor, durante todo o
 * envio (sobrecarga prolongada, e não uma rajada de 1 s). O que se observa é
 * se o broker freia o produtor: o broker do Kafka não freia o produtor por
 * causa do acúmulo (o log simplesmente cresce, e com limite a retenção apaga
 * os segmentos mais antigos), então a taxa efetiva deve ficar na alvo. O
 * Kafka tem cotas de produtor (quotas), mas elas limitam uma taxa fixa de
 * bytes por segundo, sem olhar quanto o consumidor está atrasado; não são
 * usadas aqui.
 *
 * A cada ~1 s imprime ENVIADAS=<confirmadas>, que o orquestrador usa na
 * série temporal. Produtor confiável: acks=all, linger.ms=0; qualquer falha
 * de envio = código 1.
 *
 * Proteção contra processo órfão: o orquestrador mantém o stdin do produtor
 * aberto até o fim; se ele chegar ao fim (EOF), o orquestrador morreu, e o
 * produtor encerra na hora (código 3) em vez de continuar enviando sozinho.
 */
public class ProdutorSustentado {

    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorSustentado <bootstrap> <topico> <numMsgs> <taxaMsgS> <execucao>");
            System.exit(2);
        }
        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    taxa             = Integer.parseInt(args[3]);
        int    execucao         = Integer.parseInt(args[4]);

        encerrarSeOrquestradorMorrer();

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
        AtomicInteger confirmadas = new AtomicInteger();
        AtomicInteger falhas = new AtomicInteger();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.partitionsFor(topico);
            long t0 = System.nanoTime();
            long intervalo = 1_000_000_000L / taxa;
            long proximoRelato = t0 + 1_000_000_000L;

            for (int i = 0; i < numMsgs; i++) {
                aguardarAte(t0 + i * intervalo);
                final int seq = i + 1;
                ProducerRecord<String, byte[]> r = new ProducerRecord<>(topico, payload);
                r.headers().add(HEADER_SEQ, ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                producer.send(r, (m, ex) -> {
                    if (ex == null) confirmadas.incrementAndGet();
                    else if (falhas.incrementAndGet() <= 5) System.err.println("Falha no envio " + seq + ": " + ex.getMessage());
                });
                if (System.nanoTime() >= proximoRelato) {
                    System.out.println("ENVIADAS=" + confirmadas.get());
                    System.out.flush();
                    proximoRelato += 1_000_000_000L;
                }
            }
            producer.flush();
            long t1 = System.nanoTime();

            System.out.println("ENVIADAS=" + confirmadas.get());
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("ENVIADAS_TOTAL=" + confirmadas.get());
            System.out.println("REJEITADAS=0");
            System.out.println("FALHAS=" + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
            System.out.flush();
        }
        if (falhas.get() > 0) System.exit(1);
    }

    /**
     * Thread daemon que lê o stdin até o fim. O orquestrador nunca fecha o
     * stdin do produtor; o EOF só chega quando ele morre (inclusive à força),
     * e então o produtor encerra em vez de virar órfão.
     */
    static void encerrarSeOrquestradorMorrer() {
        Thread vigia = new Thread(() -> {
            try {
                while (System.in.read() != -1) {
                    // ignora qualquer entrada; só interessa o fim do fluxo
                }
            } catch (IOException ignored) {
            }
            System.err.println("Entrada padrão fechada: o orquestrador encerrou. Saindo para não ficar órfão.");
            Runtime.getRuntime().halt(3);
        }, "vigia-stdin");
        vigia.setDaemon(true);
        vigia.start();
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
