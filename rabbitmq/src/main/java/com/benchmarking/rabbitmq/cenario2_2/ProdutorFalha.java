package com.benchmarking.rabbitmq.cenario2_2;

import com.rabbitmq.client.*;

import java.util.Map;

/**
 * Nível intermediário — Cenário 2: Falha do consumidor — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, fila, número de mensagens, execução.
 *
 * Envia todas as mensagens sem controle de taxa. Produtor confiável:
 * mensagens persistentes (deliveryMode=2) e publisher confirms aguardados no
 * fim (nack ou timeout = erro). A fila (com single active consumer) é criada
 * pelo orquestrador; aqui só se confere que ela existe.
 */
public class ProdutorFalha {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: ProdutorFalha <host> <fila> <numMsgs> <execucao>");
            System.exit(2);
        }
        String host     = args[0];
        String fila     = args[1];
        int    numMsgs  = Integer.parseInt(args[2]);
        int    execucao = Integer.parseInt(args[3]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        byte[] payload = new byte[1024];

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclarePassive(fila);
            channel.confirmSelect();

            long t0 = System.nanoTime();
            for (int i = 0; i < numMsgs; i++) {
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_TS, System.nanoTime(), HEADER_SEQ, i + 1))
                        .messageId(String.valueOf(i + 1))
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", fila, props, payload);
            }
            channel.waitForConfirmsOrDie(120_000);

            System.out.println("T0="       + t0);
            System.out.println("T1="       + System.nanoTime());
            System.out.println("MSGS="     + numMsgs);
            System.out.println("EXECUCAO=" + execucao);
        }
    }
}
