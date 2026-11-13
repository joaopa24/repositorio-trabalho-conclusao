package com.benchmarking.rabbitmq.cenario2_3;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Nível intermediário — Cenário 3: Dead Letter Queue — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, fila, número de mensagens, execução, intervalo de
 * inválidas (10 = 1 a cada 10 é inválida; 0 = nenhuma).
 *
 * Headers "sequencia" e "invalido" (booleano). Mensagens persistentes e
 * publisher confirms aguardados no fim (nack ou timeout = erro). A fila (com
 * a dead letter exchange) é criada pelo orquestrador; aqui só se confere que
 * ela existe.
 */
public class ProdutorCenario3 {

    private static final String HEADER_SEQ      = "sequencia";
    private static final String HEADER_INVALIDO = "invalido";

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorCenario3 <host> <fila> <numMsgs> <execucao> <intervaloInvalido>");
            System.exit(2);
        }
        String host              = args[0];
        String fila              = args[1];
        int    numMsgs           = Integer.parseInt(args[2]);
        int    execucao          = Integer.parseInt(args[3]);
        int    intervaloInvalido = Integer.parseInt(args[4]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        byte[] payload = new byte[1024];
        int invalidas = 0;

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclarePassive(fila);
            channel.confirmSelect();

            long t0 = System.nanoTime();
            for (int i = 0; i < numMsgs; i++) {
                int seq = i + 1;
                boolean invalida = intervaloInvalido > 0 && seq % intervaloInvalido == 0;
                if (invalida) invalidas++;

                Map<String, Object> headers = new HashMap<>();
                headers.put(HEADER_SEQ, seq);
                headers.put(HEADER_INVALIDO, invalida);
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(headers)
                        .messageId(String.valueOf(seq))
                        .deliveryMode(2)
                        .build();
                channel.basicPublish("", fila, props, payload);
            }
            channel.waitForConfirmsOrDie(120_000);

            System.out.println("T0="                 + t0);
            System.out.println("MSGS="               + numMsgs);
            System.out.println("INVALIDAS_ENVIADAS=" + invalidas);
            System.out.println("EXECUCAO="           + execucao);
        }
    }
}
