package com.benchmarking.kafka.cenario3_2;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;

/**
 * Nível avançado — Cenário 2: Impacto da replicação — CONSUMIDOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens esperado,
 * execução, fator de replicação (só para o CSV), arquivo CSV detalhado
 * ("-" = não gravar; Parte B e aquecimento).
 *
 * Detalhe importante para o texto: o consumidor do Kafka só enxerga uma
 * mensagem depois que ela foi copiada para todas as réplicas em sincronia
 * (a "high watermark"). Por isso a latência de ponta a ponta inclui o custo
 * da replicação mesmo do lado do consumidor, independentemente do acks.
 *
 * Mesmo padrão do Cenário 3 básico: grupo novo a cada execução, sem commit,
 * fetch.max.wait.ms=0, STATUS=PRONTO depois da atribuição, latências na
 * ordem de chegada e contagem conferida.
 *
 * Consumo sem commit (diferente do RabbitMQ, que confirma cada mensagem com
 * basicAck): no log do Kafka, consumir não altera o log, enquanto numa
 * quorum queue retirar a mensagem é uma operação replicada pelo Raft. Essa
 * diferença é da arquitetura e deve ser registrada no texto (pesa na Parte B).
 *
 * allow.auto.create.topics=false: o consumidor nunca cria tópico, então um
 * consumidor que sobrevivesse a uma execução não recria o tópico dela com o
 * fator de replicação padrão do cluster.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim (EOF), o
 * orquestrador morreu (mesmo à força), e o processo encerra na hora com
 * código 3, em vez de continuar rodando sozinho (Util.vigiarEntrada).
 */
public class ConsumidorReplicacao {

    private static final long TIMEOUT_LOOP_NS = 300_000_000_000L;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ConsumidorReplicacao <bootstrap> <topico> <numMsgs> <execucao> <fatorReplicacao> <arquivoCSV|->");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topico    = args[1];
        int    numMsgs   = Integer.parseInt(args[2]);
        int    execucao  = Integer.parseInt(args[3]);
        int    rf        = Integer.parseInt(args[4]);
        String arquivo   = args[5];

        Util.vigiarEntrada(l -> { });

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-repl-" + execucao + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        Util.semMetricas(props);

        long[] lat = new long[numMsgs];
        int recebidas = 0, negativas = 0;
        long ultimaChegada = 0;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topico));
            while (consumer.assignment().isEmpty()) consumer.poll(Duration.ofMillis(100));
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            long inicio = System.nanoTime();
            while (recebidas < numMsgs) {
                if (System.nanoTime() - inicio > TIMEOUT_LOOP_NS) {
                    System.err.printf("TIMEOUT: %d/%d%n", recebidas, numMsgs);
                    break;
                }
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(100))) {
                    if (recebidas >= numMsgs) break;
                    long chegada = System.nanoTime();
                    Header h = r.headers().lastHeader("ts-envio");
                    if (h == null) continue;
                    long l = chegada - ByteBuffer.wrap(h.value()).getLong();
                    if (l < 0) negativas++;
                    lat[recebidas++] = l;
                    ultimaChegada = chegada;
                }
            }
        }
        if (recebidas != numMsgs) throw new IllegalStateException("Esperado=" + numMsgs + ", recebido=" + recebidas);
        if (negativas > 0) System.err.printf("Aviso: %d latências negativas%n", negativas);

        if (!"-".equals(arquivo)) {
            File f = new File(arquivo);
            boolean novo = !f.exists() || f.length() == 0;
            try (PrintWriter w = new PrintWriter(new FileWriter(f, true))) {
                if (novo) w.println("execucao,fator_replicacao,mensagem,latencia_ms");
                for (int i = 0; i < recebidas; i++) w.printf(Locale.US, "%d,%d,%d,%.4f%n", execucao, rf, i + 1, lat[i] / 1e6);
            }
        }
        double[] r = Util.resumoMs(lat, recebidas);
        System.out.println("MEDIA="   + String.format(Locale.US, "%.4f", r[0]));
        System.out.println("MEDIANA=" + String.format(Locale.US, "%.4f", r[1]));
        System.out.println("MIN="     + String.format(Locale.US, "%.4f", r[2]));
        System.out.println("MAX="     + String.format(Locale.US, "%.4f", r[3]));
        System.out.println("ULTIMA_CHEGADA=" + ultimaChegada);
        System.out.println("RECEBIDAS=" + recebidas);
    }
}