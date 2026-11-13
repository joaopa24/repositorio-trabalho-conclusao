package com.benchmarking.kafka.cenario3_1;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível avançado — Cenário 1: Roteamento — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, modo, prefixo do tópico, número de mensagens, execução.
 *
 * Cada mensagem tem uma "chave de roteamento" no formato do RabbitMQ:
 * pedidos.<região>.<evento>, com 4 regiões e 3 eventos (12 combinações),
 * definida pela sequência (ver Cenario1Roteamento.regiao/evento). A chave
 * vai no header "chave" e a sequência no header "sequencia".
 *
 * O Kafka não tem exchanges: o "roteamento" é decidir EM QUE TÓPICO
 * publicar. Os modos são as três formas de fazer isso:
 *   unico      : tudo num tópico só (<prefixo>); quem consome filtra.
 *   por-regiao : um tópico por região (<prefixo>.<região>).
 *   por-chave  : um tópico por chave completa (<prefixo>.<região>.<evento>),
 *                para os consumidores assinarem por padrão (regex) de nome.
 * A regra fica em Cenario1Roteamento.topico, a mesma que o orquestrador usa
 * para calcular o que cada consumidor deve receber.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim, o
 * orquestrador morreu, e o produtor encerra na hora (Util.vigiarEntrada).
 */
public class ProdutorRoteamento {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorRoteamento <bootstrap> <modo> <prefixo> <numMsgs> <execucao>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String modo      = args[1];
        String prefixo   = args[2];
        int    numMsgs   = Integer.parseInt(args[3]);
        int    execucao  = Integer.parseInt(args[4]);

        Util.vigiarEntrada(linha -> { });   // só interessa o fim da entrada

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        Util.semMetricas(props);

        byte[] payload = new byte[1024];
        AtomicInteger falhas = new AtomicInteger();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            for (int seq = 1; seq <= numMsgs; seq++) {
                String regiao = Cenario1Roteamento.regiao(seq);
                String evento = Cenario1Roteamento.evento(seq);
                String chave = "pedidos." + regiao + "." + evento;
                String topico = Cenario1Roteamento.topico(modo, prefixo, seq);
                ProducerRecord<String, byte[]> r = new ProducerRecord<>(topico, chave, payload);
                r.headers().add("chave", chave.getBytes(StandardCharsets.UTF_8));
                r.headers().add("sequencia", ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                final int s = seq;
                producer.send(r, (m, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) System.err.println("Falha no envio " + s + ": " + ex.getMessage());
                });
            }
            producer.flush();
        }
        System.out.println("MSGS=" + numMsgs);
        System.out.println("FALHAS=" + falhas.get());
        System.out.println("EXECUCAO=" + execucao);
        System.out.flush();
        if (falhas.get() > 0) System.exit(1);
    }
}
