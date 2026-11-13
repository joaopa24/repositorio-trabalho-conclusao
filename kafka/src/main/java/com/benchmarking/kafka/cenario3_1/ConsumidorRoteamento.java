package com.benchmarking.kafka.cenario3_1;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Nível avançado — Cenário 1: Roteamento — CONSUMIDOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - group.id (cada consumidor tem o seu: todos recebem o que assinam)
 *   2 - assinatura: "topicos:<t1>,<t2>..." (nomes exatos) ou "padrao:<regex>"
 *       (assina todo tópico cujo NOME casa com a expressão — o mais perto que
 *       o Kafka chega do curinga do topic exchange)
 *   3 - filtro na aplicação: "todas", "regiao=<r>" ou "evento=<e>"
 *   4 - arquivo onde gravar as sequências ACEITAS
 *   5 - execução
 *
 * O consumidor lê tudo o que chega dos tópicos assinados. As mensagens que
 * não passam no filtro foram transferidas pela rede e lidas à toa: é o custo
 * de rotear na aplicação. RECEBIDAS − ACEITAS mostra esse desperdício.
 *
 * allow.auto.create.topics=false: o consumidor nunca cria tópico (um
 * consumidor de uma execução anterior não recria o tópico apagado).
 *
 * Conversa com o orquestrador: STATUS=PRONTO depois da atribuição;
 * PRODUTOR_TERMINOU na entrada padrão → encerra depois de 3 s sem mensagens;
 * fim da entrada padrão (orquestrador morreu) → encerra na hora.
 * Saída final: TOPICOS (lista dos tópicos lidos, separados por vírgula),
 * TOPICOS_LIDOS, RECEBIDAS e ACEITAS.
 */
public class ConsumidorRoteamento {

    private static final long OCIOSO_MS = 3_000;
    private static final long MAX_TOTAL_MS = 300_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ConsumidorRoteamento <bootstrap> <groupId> <assinatura> <filtro> <arquivo> <execucao>");
            System.exit(2);
        }
        String bootstrap  = args[0];
        String groupId    = args[1];
        String assinatura = args[2];
        String filtro     = args[3];
        String arquivo    = args[4];
        int    execucao   = Integer.parseInt(args[5]);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        Util.semMetricas(props);

        AtomicLong sinal = new AtomicLong(0);
        Util.vigiarEntrada(l -> {
            if (l.equals("PRODUTOR_TERMINOU")) sinal.set(System.nanoTime());
        });

        List<Integer> aceitas = new ArrayList<>();
        int recebidas = 0;
        long ultimaAtividade = 0;
        long inicio = System.nanoTime();
        Set<String> topicosLidos = new TreeSet<>();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            if (assinatura.startsWith("topicos:")) {
                consumer.subscribe(Arrays.asList(assinatura.substring(8).split(",")));
            } else if (assinatura.startsWith("padrao:")) {
                consumer.subscribe(Pattern.compile(assinatura.substring(7)));
            } else {
                throw new IllegalArgumentException("Assinatura inválida: " + assinatura);
            }
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
                if ((System.nanoTime() - inicio) / 1_000_000 > 60_000) {
                    System.err.println("Nenhum tópico atribuído em 60 s (assinatura " + assinatura + ")");
                    System.exit(1);
                }
            }
            for (TopicPartition tp : consumer.assignment()) topicosLidos.add(tp.topic());
            System.out.println("ATRIBUIDOS=" + String.join(",", topicosLidos));
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> r : records) {
                    recebidas++;
                    topicosLidos.add(r.topic());
                    Header ch = r.headers().lastHeader("chave");
                    Header sq = r.headers().lastHeader("sequencia");
                    if (ch == null || sq == null) {
                        System.err.println("Mensagem sem headers chave/sequencia");
                        System.exit(1);
                    }
                    String[] partes = new String(ch.value(), StandardCharsets.UTF_8).split("\\.");
                    if (passa(filtro, partes[1], partes[2])) aceitas.add(ByteBuffer.wrap(sq.value()).getInt());
                    ultimaAtividade = System.nanoTime();
                }
                if (!records.isEmpty()) consumer.commitSync();

                long agora = System.nanoTime();
                long s = sinal.get();
                if (s > 0 && (agora - Math.max(s, ultimaAtividade)) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
            // tópicos atribuídos ao fim (a assinatura por padrão pode mudar a atribuição)
            for (TopicPartition tp : consumer.assignment()) topicosLidos.add(tp.topic());
        }

        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo))) {
            for (int s : aceitas) w.println(s);
        }
        System.out.println("TOPICOS=" + String.join(",", topicosLidos));
        System.out.println("TOPICOS_LIDOS=" + topicosLidos.size());
        System.out.println("RECEBIDAS=" + recebidas);
        System.out.println("ACEITAS=" + aceitas.size());
        System.out.println("EXECUCAO=" + execucao);
        System.out.flush();
    }

    /** Filtro feito na aplicação (o que o broker do Kafka não faz). */
    static boolean passa(String filtro, String regiao, String evento) {
        if (filtro.equals("todas")) return true;
        if (filtro.startsWith("regiao=")) return regiao.equals(filtro.substring(7));
        if (filtro.startsWith("evento=")) return evento.equals(filtro.substring(7));
        throw new IllegalArgumentException("Filtro inválido: " + filtro);
    }
}
