package com.benchmarking.rabbitmq.cenario3_3;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.*;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível avançado — Cenário 3: Falha de broker — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, porta, fila, número de mensagens, taxa (msg/s),
 * modo ("confirms" ou "sem-confirms"), arquivo das sequências CONFIRMADAS.
 *
 * Conectado a um nó que NÃO é o líder da quorum queue: a queda do líder não
 * derruba a conexão do produtor, só a fila troca de líder (mesma situação da
 * versão Kafka, em que o cliente continua ligado aos brokers que sobraram).
 *
 * Configurações comparadas:
 *   confirms     — publisher confirms: o broker só confirma depois que a
 *                  maioria dos membros gravou. Par do Kafka: acks=all.
 *   sem-confirms — publica e segue, sem saber se a mensagem foi aceita.
 *                  Par do Kafka: acks=0.
 * A quorum queue não tem confirmação só pelo líder; o acks=1 do Kafka não
 * tem par aqui.
 *
 * O cliente do RabbitMQ não reenvia mensagens recusadas (nack) nem sem
 * resposta: elas são contadas como FALHAS e SEM_RESPOSTA. O do Kafka reenvia
 * sozinho por até 120 s, então as falhas transitórias não aparecem lá.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim, o
 * orquestrador morreu, e o produtor encerra na hora (Util.vigiarEntrada).
 *
 * Saída: T0 e STATUS=PRONTO no início do envio, ENVIADAS a cada segundo e no
 * fim T1, MSGS, CONFIRMADAS, FALHAS (nacks), SEM_RESPOSTA (nem ack nem nack
 * em 120 s) e MAIOR_INTERVALO_SEM_CONFIRMACAO_MS.
 */
public class ProdutorFalhaBroker {

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ProdutorFalhaBroker <host> <porta> <fila> <numMsgs> <taxa> <confirms|sem-confirms> <arquivo>");
            System.exit(2);
        }
        String host    = args[0];
        int    porta   = Integer.parseInt(args[1]);
        String fila    = args[2];
        int    numMsgs = Integer.parseInt(args[3]);
        int    taxa    = Integer.parseInt(args[4]);
        boolean comConfirms = args[5].equals("confirms");
        String arquivo = args[6];

        Util.vigiarEntrada(linha -> { });   // só interessa o fim da entrada

        byte[] payload = new byte[1024];
        ConcurrentNavigableMap<Long, Integer> pendentes = new ConcurrentSkipListMap<>();
        ConcurrentLinkedQueue<Integer> confirmadas = new ConcurrentLinkedQueue<>();
        AtomicInteger falhas = new AtomicInteger();
        AtomicLong ultimaConfirmacao = new AtomicLong();
        AtomicLong maiorIntervalo = new AtomicLong();

        try (Connection conn = Util.conectar(host, porta, 60);
             Channel ch = conn.createChannel()) {
            ch.queueDeclarePassive(fila);
            if (comConfirms) {
                ch.confirmSelect();
                ch.addConfirmListener(new ConfirmListener() {
                    @Override public void handleAck(long tag, boolean multiple) {
                        long agora = System.nanoTime();
                        if (multiple) {
                            ConcurrentNavigableMap<Long, Integer> h = pendentes.headMap(tag, true);
                            confirmadas.addAll(h.values());
                            h.clear();
                        } else {
                            Integer s = pendentes.remove(tag);
                            if (s != null) confirmadas.add(s);
                        }
                        long antes = ultimaConfirmacao.getAndSet(agora);
                        maiorIntervalo.accumulateAndGet(agora - antes, Math::max);
                    }
                    @Override public void handleNack(long tag, boolean multiple) {
                        if (multiple) {
                            ConcurrentNavigableMap<Long, Integer> h = pendentes.headMap(tag, true);
                            falhas.addAndGet(h.size());
                            h.clear();
                        } else if (pendentes.remove(tag) != null) {
                            falhas.incrementAndGet();
                        }
                    }
                });
            }

            long t0 = System.nanoTime();
            ultimaConfirmacao.set(t0);
            System.out.println("T0=" + t0);
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            long intervalo = 1_000_000_000L / taxa;
            long proximoRelato = t0 + 1_000_000_000L;
            for (int i = 0; i < numMsgs; i++) {
                Util.aguardarAte(t0 + i * intervalo);
                Map<String, Object> h = new HashMap<>();
                h.put("sequencia", i + 1);
                h.put("ts-envio", System.nanoTime());
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(h).deliveryMode(2).build();
                if (comConfirms) pendentes.put(ch.getNextPublishSeqNo(), i + 1);
                ch.basicPublish("", fila, props, payload);
                if (System.nanoTime() >= proximoRelato) {
                    System.out.println("ENVIADAS=" + (i + 1) + ";CONFIRMADAS=" + confirmadas.size());
                    proximoRelato += 1_000_000_000L;
                }
            }
            long limite = System.nanoTime() + 120_000_000_000L;
            while (comConfirms && !pendentes.isEmpty() && System.nanoTime() < limite) Thread.sleep(5);
            long t1 = System.nanoTime();

            try (PrintWriter w = new PrintWriter(new FileWriter(arquivo))) {
                for (int s : confirmadas) w.println(s);
            }
            System.out.println("T1=" + t1);
            System.out.println("MSGS=" + numMsgs);
            System.out.println("CONFIRMADAS=" + (comConfirms ? confirmadas.size() : -1));
            System.out.println("FALHAS=" + falhas.get());
            System.out.println("SEM_RESPOSTA=" + pendentes.size());
            System.out.println("MAIOR_INTERVALO_SEM_CONFIRMACAO_MS="
                    + (comConfirms ? String.format(Locale.US, "%.1f", maiorIntervalo.get() / 1e6) : "-1"));
            System.out.flush();
        }
    }
}
