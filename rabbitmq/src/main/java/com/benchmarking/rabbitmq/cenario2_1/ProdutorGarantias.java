package com.benchmarking.rabbitmq.cenario2_1;

import com.rabbitmq.client.*;

import java.util.Map;

/**
 * Nível intermediário — Cenário 1: Comparação de garantias — PRODUTOR (RabbitMQ).
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - número de mensagens
 *   3 - execução (0 = aquecimento)
 *   4 - garantia: at-most-once | at-least-once | exactly-once
 *   5 - tamanho da mensagem em bytes
 *   6 - taxa alvo em msg/s (0 = sem controle de taxa)
 *   7 - (opcional) publisher confirms: sim | nao | "-". Sem ele (ou com "-"),
 *       vale o padrão do nível. Só o teste de confirmações (Parte C do
 *       orquestrador) passa este argumento, para ligar e desligar os
 *       confirms independentemente do modo de ACK do consumidor.
 *
 * Configuração por nível:
 *   at-most-once  : sem publisher confirms ("envia e esquece").
 *   at-least-once : publisher confirms, aguardados uma vez no fim
 *                   (waitForConfirmsOrDie; nack ou timeout = erro).
 *   exactly-once  : igual ao at-least-once. O RabbitMQ não tem exactly once
 *                   nativo: o efeito vem do consumidor idempotente, que
 *                   descarta duplicatas pelo messageId.
 *
 * Nos três: mensagens PERSISTENTES (deliveryMode=2). Antes o at-most-once
 * usava deliveryMode=1, mas o Kafka sempre grava no log; com a persistência
 * igual, só o mecanismo de confirmação muda entre os níveis.
 *
 * messageId = número de sequência em todos os níveis (é o que a
 * deduplicação usa). Controle de taxa com aguardarAte(), o mesmo método dos
 * Cenários 1 e 3.
 */
public class ProdutorGarantias {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";
    private static final long TIMEOUT_CONFIRMS_MS = 120_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ProdutorGarantias <host> <fila> <numMsgs> <execucao> <garantia> <tamanhoBytes> <taxaMsgS>");
            System.exit(2);
        }

        String host         = args[0];
        String fila         = args[1];
        int    numMsgs      = Integer.parseInt(args[2]);
        int    execucao     = Integer.parseInt(args[3]);
        String garantia     = args[4];
        int    tamanhoBytes = Integer.parseInt(args[5]);
        int    taxaMsgS     = Integer.parseInt(args[6]);

        boolean usarConfirms = !garantia.equals("at-most-once");
        if (args.length >= 8 && !"-".equals(args[7])) {
            usarConfirms = switch (args[7]) {
                case "sim" -> true;
                case "nao" -> false;
                default -> throw new IllegalArgumentException("Confirms inválido: " + args[7] + " (use sim | nao)");
            };
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        byte[] payload = new byte[tamanhoBytes];

        System.out.printf("Produtor: %d msgs, garantia %s, confirms %s, taxa %s%n", numMsgs, garantia,
                usarConfirms ? "sim" : "nao",
                taxaMsgS > 0 ? taxaMsgS + " msg/s" : "máxima (sem controle)");
        System.out.println("CONFIRMS=" + (usarConfirms ? "sim" : "nao"));

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);
            if (usarConfirms) channel.confirmSelect();

            long t0 = System.nanoTime();
            long intervaloNs = taxaMsgS > 0 ? 1_000_000_000L / taxaMsgS : 0L;

            for (int i = 0; i < numMsgs; i++) {
                if (intervaloNs > 0) aguardarAte(t0 + i * intervaloNs);

                long ts = System.nanoTime();
                int seq = i + 1;
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_TS, ts, HEADER_SEQ, seq))
                        .messageId(String.valueOf(seq))
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", fila, props, payload);
            }

            // lança exceção (e o processo sai com código 1) se houver nack ou timeout
            if (usarConfirms) channel.waitForConfirmsOrDie(TIMEOUT_CONFIRMS_MS);
            long t1 = System.nanoTime();

            System.out.println("T0="         + t0);
            System.out.println("T1="         + t1);
            System.out.println("MSGS="       + numMsgs);
            System.out.println("TRANSACOES=0");   // não se aplica; mantém o formato igual ao Kafka
            System.out.println("EXECUCAO="   + execucao);
        }
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