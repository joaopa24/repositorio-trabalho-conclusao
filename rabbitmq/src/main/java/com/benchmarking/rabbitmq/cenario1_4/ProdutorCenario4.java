package com.benchmarking.rabbitmq.cenario1_4;

import com.rabbitmq.client.*;

import java.util.Map;

/**
 * Cenário 4 — Múltiplos consumidores — PRODUTOR (RabbitMQ).
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - número de mensagens
 *   3 - execução (0 = aquecimento)
 *   4 - tamanho da mensagem em bytes
 *
 * Envia todas as mensagens sem controle de taxa: elas entram na fila logo no
 * início e o que se mede é quanto tempo os consumidores levam para tratar
 * tudo.
 *
 * Mesma configuração do Cenário 1: mensagens persistentes (deliveryMode=2),
 * publisher confirms aguardados uma vez no fim, conexão e canal abertos
 * antes de t0. Nack ou timeout de confirmação faz o processo sair com erro.
 */
public class ProdutorCenario4 {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";
    private static final long TIMEOUT_CONFIRMS_MS = 120_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorCenario4 <host> <fila> <numMsgs> <execucao> <tamanhoBytes>");
            System.exit(2);
        }

        String host         = args[0];
        String fila         = args[1];
        int    numMsgs      = Integer.parseInt(args[2]);
        int    execucao     = Integer.parseInt(args[3]);
        int    tamanhoBytes = Integer.parseInt(args[4]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        byte[] payload = new byte[tamanhoBytes];

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);
            channel.confirmSelect();

            long t0 = System.nanoTime();

            for (int i = 0; i < numMsgs; i++) {
                long ts = System.nanoTime();
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_TS, ts, HEADER_SEQ, i + 1))
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", fila, props, payload);
            }

            // lança exceção (e o processo sai com código 1) se houver nack ou timeout
            channel.waitForConfirmsOrDie(TIMEOUT_CONFIRMS_MS);
            long t1 = System.nanoTime();

            System.out.println("T0="       + t0);
            System.out.println("T1="       + t1);
            System.out.println("MSGS="     + numMsgs);
            System.out.println("EXECUCAO=" + execucao);
        }
    }
}
