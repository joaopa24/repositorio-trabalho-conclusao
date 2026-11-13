package com.benchmarking.rabbitmq.cenario1_2;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Locale;
import java.util.Map;

/**
 * Cenário 2 — Produtor (RabbitMQ).
 *
 * Envia todas as mensagens na velocidade máxima, para que o consumidor
 * (5 ms por mensagem) tenha SEMPRE mensagens esperando na fila. Assim,
 * qualquer tempo em que o consumidor fique parado é custo da entrega, e não
 * falta de mensagens.
 *
 * Mensagens persistentes (deliveryMode=2) com Publisher Confirms, aguardados
 * uma única vez, no fim do lote.
 *
 * Args: host fila numMsgs execucao
 */
public class ProdutorCenario2 {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: ProdutorCenario2 <host> <fila> <numMsgs> <execucao>");
            System.exit(1);
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

            channel.queueDeclare(fila, true, false, false, null);
            channel.confirmSelect();

            long t0 = System.nanoTime();

            for (int i = 0; i < numMsgs; i++) {
                long ts = System.nanoTime();
                int seq = i + 1;

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_TS, ts, HEADER_SEQ, seq))
                        .deliveryMode(2)
                        .build();

                channel.basicPublish("", fila, props, payload);
            }

            // folga grande: com prefetch baixo, a fila fica ocupada atendendo
            // o consumidor e as confirmações podem demorar mais a sair
            channel.waitForConfirmsOrDie(60_000);
            long t1 = System.nanoTime();

            double duracaoS = (t1 - t0) / 1e9;
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("VAZAO=" + String.format(Locale.US, "%.2f", numMsgs / duracaoS));
            System.out.println("EXECUCAO=" + execucao);
        }
    }
}
