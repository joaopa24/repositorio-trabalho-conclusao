package com.benchmarking.rabbitmq.cenario1_3;

import com.rabbitmq.client.*;

import java.util.Map;

/**
 * Cenário 3 — Variação do tamanho da mensagem — PRODUTOR (RabbitMQ).
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - número de mensagens
 *   3 - execução (0 = aquecimento)
 *   4 - tamanho da mensagem em bytes
 *   5 - taxa alvo em msg/s (0 = sem controle de taxa)
 *
 * Mesma configuração do Cenário 1: mensagens persistentes (deliveryMode=2),
 * publisher confirms aguardados uma vez no fim, conexão e canal abertos
 * antes de t0.
 *
 * MUDANÇAS em relação à versão anterior:
 *
 *   - Removido o semáforo de 300 mensagens em voo + ConfirmListener. O
 *     confirm do RabbitMQ chega quando a mensagem é GRAVADA NA FILA, não
 *     quando é entregue ao consumidor. Então o semáforo limitava só as
 *     mensagens não confirmadas, e não o que estava parado na fila: quando
 *     o broker confirmava rápido, o produtor enchia a fila e a latência
 *     explodia (execuções 3, 5 e 8 de 100 KB, com média de 725-990 ms).
 *     Na Parte A, quem impede a fila agora é o controle de taxa.
 *
 *   - Controle de taxa com aguardarAte(), o mesmo método do Cenário 1 e do
 *     produtor Kafka deste cenário.
 *
 *   - Nack ou timeout de confirmação faz o processo terminar com erro
 *     (waitForConfirmsOrDie), e o orquestrador interrompe a campanha.
 */
public class ProdutorCenario3 {

    private static final String HEADER_TS = "ts-envio";
    private static final long TIMEOUT_CONFIRMS_MS = 300_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ProdutorCenario3 <host> <fila> <numMsgs> <execucao> <tamanhoBytes> <taxaMsgS>");
            System.exit(2);
        }

        String host         = args[0];
        String fila         = args[1];
        int    numMsgs      = Integer.parseInt(args[2]);
        int    execucao     = Integer.parseInt(args[3]);
        int    tamanhoBytes = Integer.parseInt(args[4]);
        int    taxaMsgS     = Integer.parseInt(args[5]);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        byte[] payload = new byte[tamanhoBytes];

        System.out.printf("Produtor: %d msgs de %d bytes, taxa %s%n", numMsgs, tamanhoBytes,
                taxaMsgS > 0 ? taxaMsgS + " msg/s" : "máxima (sem controle)");

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);
            channel.confirmSelect();

            long t0 = System.nanoTime();
            long intervaloNs = taxaMsgS > 0 ? 1_000_000_000L / taxaMsgS : 0L;

            for (int i = 0; i < numMsgs; i++) {
                if (intervaloNs > 0) {
                    aguardarAte(t0 + i * intervaloNs);
                }

                long ts = System.nanoTime();
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_TS, ts))
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

    /** Mesmo método do Cenário 1 (busy-wait no trecho final). */
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
