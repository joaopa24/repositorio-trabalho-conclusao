package com.benchmarking.rabbitmq.cenario3_4;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Nível avançado — Cenário 4: Replay — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, porta, exchange, número de mensagens, pausa no meio (ms).
 *
 * Publica numa exchange fanout ligada a DUAS filas: uma fila clássica e um
 * stream. Assim as duas recebem exatamente as mesmas mensagens e o
 * orquestrador compara o que cada uma permite reler.
 *
 * Publica a 1ª metade, espera as confirmações, imprime MEIO_MS (relógio de
 * parede), espera a pausa e publica a 2ª metade. A pausa existe por causa do
 * replay por tempo: no stream o timestamp é a hora em que o BROKER gravou o
 * bloco (chunk), com precisão de segundos — o produtor não escolhe o
 * timestamp como no Kafka. Com uma pausa de 5 s, há um intervalo de tempo
 * sem nenhuma mensagem, e o orquestrador pede o replay a partir do meio dele.
 * Util.vigiarEntrada encerra o processo se o orquestrador morrer.
 */
public class ProdutorReplay {

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorReplay <host> <porta> <exchange> <numMsgs> <pausaMs>");
            System.exit(2);
        }
        String host     = args[0];
        int    porta    = Integer.parseInt(args[1]);
        String exchange = args[2];
        int    numMsgs  = Integer.parseInt(args[3]);
        long   pausa    = Long.parseLong(args[4]);
        Util.vigiarEntrada(l -> { });

        byte[] payload = new byte[1024];
        try (Connection conn = Util.fabrica(host, porta).newConnection();
             Channel ch = conn.createChannel()) {
            ch.confirmSelect();
            long t0 = System.nanoTime();
            for (int seq = 1; seq <= numMsgs; seq++) {
                if (seq == numMsgs / 2 + 1) {
                    ch.waitForConfirmsOrDie(60_000);
                    System.out.println("MEIO_MS=" + System.currentTimeMillis());
                    Thread.sleep(pausa);
                }
                Map<String, Object> h = new HashMap<>();
                h.put("sequencia", seq);
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(h).deliveryMode(2).build();
                ch.basicPublish(exchange, "", props, payload);
            }
            ch.waitForConfirmsOrDie(60_000);
            System.out.println("T0=" + t0);
            System.out.println("T1=" + System.nanoTime());
            System.out.println("MSGS=" + numMsgs);
        }
    }
}