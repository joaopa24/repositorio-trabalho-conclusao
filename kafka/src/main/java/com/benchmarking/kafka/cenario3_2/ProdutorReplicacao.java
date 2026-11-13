package com.benchmarking.kafka.cenario3_2;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível avançado — Cenário 2: Impacto da replicação — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens, execução,
 * taxa alvo em msg/s (0 = sem controle de taxa).
 *
 * acks=all: o líder só confirma depois que TODAS as réplicas em sincronia
 * (ISR) gravaram a mensagem. É com acks=all que o fator de replicação pesa
 * na confirmação. Idempotência desligada e linger.ms=0, como no Cenário 1
 * básico, para que só o fator de replicação mude entre as configurações.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim (EOF), o
 * orquestrador morreu (mesmo à força), e o processo encerra na hora com
 * código 3, em vez de continuar rodando sozinho (Util.vigiarEntrada).
 */
public class ProdutorReplicacao {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorReplicacao <bootstrap> <topico> <numMsgs> <execucao> <taxaMsgS>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topico    = args[1];
        int    numMsgs   = Integer.parseInt(args[2]);
        int    execucao  = Integer.parseInt(args[3]);
        int    taxa      = Integer.parseInt(args[4]);

        Util.vigiarEntrada(l -> { });

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        Util.semMetricas(props);

        byte[] payload = new byte[1024];
        AtomicInteger falhas = new AtomicInteger();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.partitionsFor(topico);   // conexão e metadados fora da medição
            long t0 = System.nanoTime();
            long intervalo = taxa > 0 ? 1_000_000_000L / taxa : 0;
            for (int i = 0; i < numMsgs; i++) {
                if (intervalo > 0) Util.aguardarAte(t0 + i * intervalo);
                final int seq = i + 1;
                ProducerRecord<String, byte[]> r = new ProducerRecord<>(topico, payload);
                r.headers().add("ts-envio", ByteBuffer.allocate(Long.BYTES).putLong(System.nanoTime()).array());
                r.headers().add("sequencia", ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                producer.send(r, (m, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) System.err.println("Falha no envio " + seq + ": " + ex.getMessage());
                });
            }
            producer.flush();
            long t1 = System.nanoTime();
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("MSGS=" + numMsgs);
            System.out.println("FALHAS=" + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
        }
        if (falhas.get() > 0) System.exit(1);
    }
}