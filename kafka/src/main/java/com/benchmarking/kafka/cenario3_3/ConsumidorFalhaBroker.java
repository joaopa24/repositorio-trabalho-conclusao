package com.benchmarking.kafka.cenario3_3;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível avançado — Cenário 3: Falha de broker — CONSUMIDOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, arquivo de saída.
 *
 * Lê a partição 0 desde o início com assign() (sem grupo): assim o resultado
 * não depende do coordenador de grupo, que também poderia estar no broker
 * derrubado. O efeito medido é só o do caminho dos dados (troca de líder).
 * Como lê por offset e não confirma nada, este consumidor nunca recebe uma
 * mensagem duas vezes por conta própria: no Kafka, as duplicadas só podem
 * vir de reenvios do produtor.
 *
 * Para cada mensagem grava "sequencia;envioNs;chegadaNs" na ordem de chegada.
 * O orquestrador calcula a partir desse arquivo: perdidas, duplicadas,
 * maior pausa na entrega e a latência ao longo do tempo.
 *
 * allow.auto.create.topics=false: o consumidor nunca cria tópico.
 *
 * Conversa com o orquestrador: STATUS=PRONTO depois de posicionado;
 * PRODUTOR_TERMINOU na entrada padrão → encerra depois de 10 s sem mensagens
 * (tempo para chegarem os reenvios atrasados); fim da entrada padrão
 * (orquestrador morreu) → encerra na hora.
 */
public class ConsumidorFalhaBroker {

    private static final long OCIOSO_MS = 10_000;
    private static final long MAX_TOTAL_MS = 400_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Uso: ConsumidorFalhaBroker <bootstrap> <topico> <arquivo>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topico    = args[1];
        String arquivo   = args[2];

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        Util.semMetricas(props);

        AtomicLong sinal = new AtomicLong(0);
        Util.vigiarEntrada(l -> {
            if (l.equals("PRODUTOR_TERMINOU")) sinal.set(System.nanoTime());
        });

        int recebidas = 0;
        long inicio = System.nanoTime();
        long ultimaAtividade = inicio;
        TopicPartition tp = new TopicPartition(topico, 0);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
             PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(arquivo)))) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            consumer.position(tp);
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> r : records) {
                    long chegada = System.nanoTime();
                    Header ts = r.headers().lastHeader("ts-envio");
                    Header sq = r.headers().lastHeader("sequencia");
                    if (ts == null || sq == null) continue;
                    w.println(ByteBuffer.wrap(sq.value()).getInt() + ";" + ByteBuffer.wrap(ts.value()).getLong() + ";" + chegada);
                    recebidas++;
                    ultimaAtividade = chegada;
                }
                long agora = System.nanoTime();
                long s = sinal.get();
                if (s > 0 && (agora - Math.max(s, ultimaAtividade)) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
        }
        System.out.println("RECEBIDAS=" + recebidas);
        System.out.flush();
    }
}
