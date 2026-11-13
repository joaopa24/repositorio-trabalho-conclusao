package com.benchmarking.rabbitmq.cenario3_2;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível avançado — Cenário 2: Impacto da replicação — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, porta, fila, número de mensagens, execução,
 * taxa alvo em msg/s (0 = sem controle de taxa).
 *
 * Mensagens persistentes com publisher confirms ASSÍNCRONOS (ConfirmListener):
 * o envio não para a cada mensagem, como no Kafka com acks=all. Numa quorum
 * queue o broker só confirma depois que a MAIORIA dos membros gravou a
 * mensagem; numa fila clássica, depois de gravar no disco do único nó.
 * No fim espera todas as confirmações. FALHAS = nacks.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim (EOF), o
 * orquestrador morreu (mesmo à força), e o processo encerra na hora com
 * código 3, em vez de continuar rodando sozinho (Util.vigiarEntrada).
 */
public class ProdutorReplicacao {

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ProdutorReplicacao <host> <porta> <fila> <numMsgs> <execucao> <taxaMsgS>");
            System.exit(2);
        }
        String host     = args[0];
        int    porta    = Integer.parseInt(args[1]);
        String fila     = args[2];
        int    numMsgs  = Integer.parseInt(args[3]);
        int    execucao = Integer.parseInt(args[4]);
        int    taxa     = Integer.parseInt(args[5]);

        Util.vigiarEntrada(l -> { });

        byte[] payload = new byte[1024];
        ConcurrentNavigableMap<Long, Integer> pendentes = new ConcurrentSkipListMap<>();
        AtomicInteger falhas = new AtomicInteger();

        try (Connection conn = Util.conectar(host, porta, 60);
             Channel ch = conn.createChannel()) {
            ch.confirmSelect();
            ch.addConfirmListener(new ConfirmListener() {
                @Override public void handleAck(long tag, boolean multiple) {
                    if (multiple) pendentes.headMap(tag, true).clear(); else pendentes.remove(tag);
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
            ch.queueDeclarePassive(fila);

            long t0 = System.nanoTime();
            long intervalo = taxa > 0 ? 1_000_000_000L / taxa : 0;
            for (int i = 0; i < numMsgs; i++) {
                if (intervalo > 0) Util.aguardarAte(t0 + i * intervalo);
                Map<String, Object> h = new HashMap<>();
                h.put("sequencia", i + 1);
                h.put("ts-envio", System.nanoTime());
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(h).deliveryMode(2).build();
                pendentes.put(ch.getNextPublishSeqNo(), i + 1);
                ch.basicPublish("", fila, props, payload);
            }
            long limite = System.nanoTime() + 120_000_000_000L;
            while (!pendentes.isEmpty() && System.nanoTime() < limite) Thread.sleep(1);
            long t1 = System.nanoTime();
            if (!pendentes.isEmpty()) {
                System.err.println("Sem confirmação para " + pendentes.size() + " mensagens após 120 s");
                falhas.addAndGet(pendentes.size());
            }
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("MSGS=" + numMsgs);
            System.out.println("FALHAS=" + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
        }
        if (falhas.get() > 0) System.exit(1);
    }
}