package com.benchmarking.rabbitmq.cenario1_4;

import com.rabbitmq.client.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cenário 4 — Múltiplos consumidores — CONSUMIDOR (RabbitMQ).
 *
 * Um dos N consumidores da MESMA fila. O broker entrega as mensagens aos
 * consumidores da fila em rodízio, com no máximo PREFETCH mensagens sem
 * confirmação por consumidor.
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - execução (0 = aquecimento)
 *   3 - arquivo temporário onde gravar as mensagens tratadas
 *   4 - prefetch (basicQos)
 *   5 - id deste consumidor (1..N)
 *   6 - tempo de processamento simulado por mensagem, em ms
 *
 * Processamento: Thread.sleep(ms) por mensagem, com início e fim medidos
 * (mesmo método do consumidor Kafka). Ack manual de cada mensagem DEPOIS do
 * processamento; sem ack manual o prefetch não teria efeito.
 *
 * Conversa com o orquestrador (igual ao Kafka):
 *   stdout  STATUS=PRONTO      depois do basicConsume (o broker já registrou o consumidor)
 *   stdin   PRODUTOR_TERMINOU  todas as mensagens já estão na fila
 *   stdout  STATUS=CONCLUIDO   3 s sem nada para processar depois do sinal
 *   stdin   SAIR               todos concluíram: grava o arquivo e encerra
 *
 * Saída (stdout): RECEBIDAS, PROC_MEDIO_MS.
 */
public class ConsumidorCenario4 {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    private static final long OCIOSO_MS    = 3_000;
    private static final long MAX_TOTAL_MS = 900_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorCenario4 <host> <fila> <execucao> <arquivo> <prefetch> <consumidorId> <procMs>");
            System.exit(2);
        }

        String host         = args[0];
        String fila         = args[1];
        int    execucao     = Integer.parseInt(args[2]);
        String arquivo      = args[3];
        int    prefetch     = Integer.parseInt(args[4]);
        int    consumidorId = Integer.parseInt(args[5]);
        long   procMs       = Long.parseLong(args[6]);

        AtomicLong    produtorTerminouNs = new AtomicLong(0);
        AtomicBoolean sair = new AtomicBoolean(false);
        Thread leitorStdin = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                String linha;
                while ((linha = in.readLine()) != null) {
                    if (linha.trim().equals("PRODUTOR_TERMINOU")) produtorTerminouNs.set(System.nanoTime());
                    if (linha.trim().equals("SAIR")) break;
                }
            } catch (IOException ignored) {
            }
            sair.set(true);
        });
        leitorStdin.setDaemon(true);
        leitorStdin.start();

        List<long[]> registros = Collections.synchronizedList(new ArrayList<>()); // seq, envio, inicio, fim
        AtomicLong ultimaAtividade = new AtomicLong(0);
        int[] semHeader = {0};
        long inicioProcesso = System.nanoTime();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);
            channel.basicQos(prefetch);

            // os callbacks de um canal rodam um de cada vez
            DeliverCallback onEntrega = (tag, delivery) -> {
                long inicio = System.nanoTime();
                processar(procMs);
                long fim = System.nanoTime();

                Map<String, Object> h = delivery.getProperties().getHeaders();
                Object ts  = h == null ? null : h.get(HEADER_TS);
                Object seq = h == null ? null : h.get(HEADER_SEQ);
                if (ts == null || seq == null) {
                    semHeader[0]++;
                } else {
                    registros.add(new long[]{((Number) seq).longValue(), ((Number) ts).longValue(), inicio, fim});
                }
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                ultimaAtividade.set(fim);
            };

            // basicConsume só retorna depois que o broker confirma o consumidor
            String consumerTag = channel.basicConsume(fila, false, onEntrega, tag -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            boolean concluido = false;
            while (!sair.get()) {
                Thread.sleep(100);
                long agora = System.nanoTime();
                long sinal = produtorTerminouNs.get();
                if (!concluido && sinal > 0
                        && (agora - Math.max(sinal, ultimaAtividade.get())) / 1_000_000 > OCIOSO_MS) {
                    concluido = true;
                    System.out.println("STATUS=CONCLUIDO");
                    System.out.flush();
                }
                if ((agora - inicioProcesso) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido sem o sinal SAIR");
                    System.exit(1);
                }
            }
            try {
                channel.basicCancel(consumerTag);
            } catch (Exception ignored) {
            }
        }

        if (semHeader[0] > 0) {
            System.err.printf("%d mensagens sem headers ts-envio/sequencia%n", semHeader[0]);
            System.exit(1);
        }

        List<long[]> copia;
        synchronized (registros) {
            copia = new ArrayList<>(registros);
        }
        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, false))) {
            w.println("sequencia,envio_ns,inicio_ns,fim_ns");
            for (long[] r : copia) {
                w.println(r[0] + "," + r[1] + "," + r[2] + "," + r[3]);
            }
        }

        double procMedio = copia.stream().mapToLong(r -> r[3] - r[2]).average().orElse(0) / 1e6;
        System.out.println("CONSUMIDOR_ID=" + consumidorId);
        System.out.println("EXECUCAO="      + execucao);
        System.out.println("PROC_MEDIO_MS=" + String.format(Locale.US, "%.4f", procMedio));
        System.out.println("RECEBIDAS="     + copia.size());
    }

    /** Processamento simulado: sleep (trabalho de I/O, sem gastar CPU). */
    static void processar(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
