package com.benchmarking.rabbitmq.cenario1_1;

import com.rabbitmq.client.*;

import java.util.Map;

/**
 * Produtor independente do Cenário 1.1 (baseline) — RabbitMQ.
 *
 * Envia numMsgs mensagens de 1 KB a 200 msg/s, persistentes
 * (deliveryMode=2), com Publisher Confirms aguardados uma vez, no fim.
 * A conexão e o canal são abertos ANTES de t0, fora da medição.
 *
 * FIX (controle de taxa): o laço com várias chamadas a
 * LockSupport.parkNanos(10 µs) foi trocado por aguardarAte(), o MESMO
 * método usado no produtor do Kafka. Assim as duas tecnologias usam
 * exatamente o mesmo controle de taxa. Para esperas curtas, ele faz
 * busy-wait, imune à resolução do timer do Windows.
 *
 * NOVO (atraso do gerador): além do "ts-envio" (instante efetivo, gravado
 * imediatamente antes do basicPublish), cada mensagem leva o header
 * "ts-planejado", com o instante em que ela DEVERIA ter saído
 * (t0 + i * INTERVALO_NS). O consumidor calcula a diferença e grava a
 * coluna atraso_gerador_ms no CSV detalhado. Mesma mudança do produtor do
 * Kafka (8 bytes a mais de header por mensagem nas duas tecnologias).
 */
public class Produtor {

    private static final String HEADER_TS           = "ts-envio";
    private static final String HEADER_TS_PLANEJADO = "ts-planejado";
    private static final long   INTERVALO_NS = 5_000_000L; // 200 msg/s

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: Produtor <host> <fila> <numMsgs> <execucao>");
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
                long tsAlvo = t0 + i * INTERVALO_NS;
                aguardarAte(tsAlvo);

                long ts = System.nanoTime();

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_TS, ts, HEADER_TS_PLANEJADO, tsAlvo))
                        .deliveryMode(2)
                        .build();

                channel.basicPublish("", fila, props, payload);
            }

            channel.waitForConfirmsOrDie(15_000);
            long t1 = System.nanoTime();

            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("MSGS=" + numMsgs);
            System.out.println("EXECUCAO=" + execucao);
        }
    }

    /**
     * Espera até o instante alvo (nanoTime). Esperas longas (> 20 ms) dormem
     * a maior parte do tempo; o trecho final, e as esperas curtas como os
     * 5 ms deste cenário, são feitos em busy-wait. Mesmo método do Kafka.
     */
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