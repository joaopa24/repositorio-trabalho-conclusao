package com.benchmarking.rabbitmq.cenario3_1;

import com.rabbitmq.client.*;
import com.benchmarking.rabbitmq.cenario3_comum.Util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível avançado — Cenário 1: Roteamento — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos: host, porta, fila, arquivo onde gravar as sequências recebidas, execução.
 *
 * Diferente da versão Kafka, aqui NÃO há filtro na aplicação: o consumidor
 * lê a sua fila, e a fila só tem o que os bindings deixaram passar. O
 * orquestrador confere se o que chegou é exatamente o esperado.
 *
 * Conversa com o orquestrador: STATUS=PRONTO depois de registrado na fila;
 * PRODUTOR_TERMINOU na entrada padrão → encerra depois de 3 s sem mensagens;
 * fim da entrada padrão (orquestrador morreu) → encerra na hora.
 */
public class ConsumidorRoteamento {

    private static final long OCIOSO_MS = 3_000;
    private static final long MAX_TOTAL_MS = 300_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ConsumidorRoteamento <host> <porta> <fila> <arquivo> <execucao>");
            System.exit(2);
        }
        String host     = args[0];
        int    porta    = Integer.parseInt(args[1]);
        String fila     = args[2];
        String arquivo  = args[3];
        int    execucao = Integer.parseInt(args[4]);

        AtomicLong sinal = new AtomicLong(0);
        Util.vigiarEntrada(l -> {
            if (l.equals("PRODUTOR_TERMINOU")) sinal.set(System.nanoTime());
        });

        List<Integer> recebidas = new ArrayList<>();   // callbacks de um canal rodam um de cada vez
        AtomicLong ultimaAtividade = new AtomicLong(0);
        long inicio = System.nanoTime();

        try (Connection conn = Util.fabrica(host, porta).newConnection();
             Channel ch = conn.createChannel()) {
            ch.queueDeclarePassive(fila);
            ch.basicQos(100);
            ch.basicConsume(fila, false, (tag, d) -> {
                Map<String, Object> h = d.getProperties().getHeaders();
                Object s = h == null ? null : h.get("sequencia");
                if (s == null) {
                    System.err.println("Mensagem sem header sequencia");
                    System.exit(1);
                }
                synchronized (recebidas) {
                    recebidas.add(((Number) s).intValue());
                }
                ch.basicAck(d.getEnvelope().getDeliveryTag(), false);
                ultimaAtividade.set(System.nanoTime());
            }, tag -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                Thread.sleep(50);
                long agora = System.nanoTime();
                long s = sinal.get();
                if (s > 0 && (agora - Math.max(s, ultimaAtividade.get())) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
        }
        synchronized (recebidas) {
            try (PrintWriter w = new PrintWriter(new FileWriter(arquivo))) {
                for (int s : recebidas) w.println(s);
            }
            System.out.println("RECEBIDAS=" + recebidas.size());
        }
        System.out.println("EXECUCAO=" + execucao);
        System.out.flush();
    }
}
