package com.benchmarking.rabbitmq.cenario2_3;

import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Nível intermediário — Cenário 3: Dead Letter Queue — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos: host, fila, número de mensagens esperado, execução, prefetch,
 * número máximo de tentativas por mensagem.
 *
 * O RabbitMQ tem dead lettering NATIVO: a fila principal é declarada (pelo
 * orquestrador) com x-dead-letter-exchange. Quando o consumidor rejeita uma
 * mensagem sem devolvê-la à fila (basicNack com requeue=false), o próprio
 * broker a encaminha para a DLX, que a entrega à DLQ, e acrescenta o header
 * x-death (motivo, fila de origem, horário, contagem).
 *
 *   - Mensagem válida: processada e confirmada (basicAck).
 *   - Mensagem inválida: o processamento falha; enquanto houver tentativas,
 *     ela é devolvida à fila (basicNack com requeue=true) e chega de novo;
 *     na última tentativa, é rejeitada sem requeue e o broker a desvia.
 *
 * As tentativas são contadas pelo consumidor (mesma política do consumidor
 * Kafka). Em quorum queues, o RabbitMQ também conta sozinho
 * (x-delivery-limit); aqui a fila é clássica, como nos outros cenários.
 */
public class ConsumidorDLQ {

    private static final String HEADER_SEQ      = "sequencia";
    private static final String HEADER_INVALIDO = "invalido";
    private static final long TIMEOUT_S = 300;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ConsumidorDLQ <host> <fila> <numMsgs> <execucao> <prefetch> <maxTentativas>");
            System.exit(2);
        }
        String host          = args[0];
        String fila          = args[1];
        int    numMsgs       = Integer.parseInt(args[2]);
        int    execucao      = Integer.parseInt(args[3]);
        int    prefetch      = Integer.parseInt(args[4]);
        int    maxTentativas = Integer.parseInt(args[5]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        // os callbacks de um canal rodam um de cada vez
        int[] processadasOk = {0};
        int[] enviadasDlq = {0};
        int[] tentativasInvalidas = {0};
        long[] ultimoFim = {0};
        Map<Integer, Integer> tentativas = new HashMap<>();
        CountDownLatch latch = new CountDownLatch(numMsgs);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclarePassive(fila);
            channel.basicQos(prefetch);

            DeliverCallback onEntrega = (tag, delivery) -> {
                long dt = delivery.getEnvelope().getDeliveryTag();
                Map<String, Object> h = delivery.getProperties().getHeaders();
                Object seqObj = h == null ? null : h.get(HEADER_SEQ);
                Object invObj = h == null ? null : h.get(HEADER_INVALIDO);
                if (seqObj == null) {
                    System.err.println("Mensagem sem header sequencia");
                    Runtime.getRuntime().halt(1);
                }
                int seq = ((Number) seqObj).intValue();
                boolean invalida = Boolean.TRUE.equals(invObj);

                if (!invalida) {                       // processamento OK
                    channel.basicAck(dt, false);
                    processadasOk[0]++;
                    ultimoFim[0] = System.nanoTime();
                    latch.countDown();
                    return;
                }

                int t = tentativas.merge(seq, 1, Integer::sum);
                tentativasInvalidas[0]++;
                if (t < maxTentativas) {
                    channel.basicNack(dt, false, true);     // devolve à fila: nova tentativa
                } else {
                    channel.basicNack(dt, false, false);    // rejeição definitiva: o broker desvia para a DLX
                    enviadasDlq[0]++;
                    ultimoFim[0] = System.nanoTime();
                    latch.countDown();
                }
            };

            channel.basicConsume(fila, false, onEntrega, t -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            if (!latch.await(TIMEOUT_S, TimeUnit.SECONDS)) {
                System.err.printf("TIMEOUT: %d/%d mensagens tratadas%n", processadasOk[0] + enviadasDlq[0], numMsgs);
                System.exit(1);
            }
        }

        System.out.println("PROCESSADAS_OK="       + processadasOk[0]);
        System.out.println("ENVIADAS_PARA_DLQ="    + enviadasDlq[0]);
        System.out.println("TENTATIVAS_INVALIDAS=" + tentativasInvalidas[0]);
        System.out.println("ULTIMO_FIM="           + ultimoFim[0]);
        System.out.println("EXECUCAO="             + execucao);
    }
}
