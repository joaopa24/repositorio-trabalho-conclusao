package com.benchmarking.rabbitmq.cenario1_2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Cenário 2 — Consumidor Lento (RabbitMQ).
 *
 * OBJETIVO: medir como o modelo PUSH entrega mensagens a um consumidor que
 * não acompanha o produtor, e como o prefetch (basicQos) altera isso.
 *
 * Com prefetch = N, o broker só envia uma nova mensagem quando o consumidor
 * tem menos de N mensagens sem confirmação. Com prefetch = 1, depois de cada
 * ack o consumidor precisa esperar a próxima mensagem atravessar a rede; com
 * prefetch maior, a próxima mensagem já está esperando no cliente.
 *
 * Para cada mensagem são registrados dois instantes:
 *   - chegada: início do DeliverCallback;
 *   - fim:     depois do processamento (delay) e do basicAck.
 *
 * A partir deles:
 *   - tempo ocupado da mensagem i = fim[i] - chegada[i]
 *   - tempo ocioso antes da mensagem i = chegada[i] - fim[i-1]
 *   - utilização do consumidor = soma dos tempos ocupados / tempo total
 *     (mesma ideia de "consumer utilisation" de MacMullen, 2014)
 *
 * Os callbacks de um mesmo canal são executados um de cada vez, na ordem de
 * entrega, então os vetores abaixo são preenchidos sem concorrência.
 *
 * Args: host fila numMsgs execucao delayMs prefetch arquivoCSV
 */
public class ConsumidorLento {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorLento <host> <fila> <numMsgs> <execucao> "
                    + "<delayMs> <prefetch> <arquivoCSV>");
            System.exit(1);
        }

        String host       = args[0];
        String fila       = args[1];
        int    numMsgs    = Integer.parseInt(args[2]);
        int    execucao   = Integer.parseInt(args[3]);
        int    delayMs    = Integer.parseInt(args[4]);
        int    prefetch   = Integer.parseInt(args[5]);
        String arquivoCSV = args[6];

        System.out.printf("Consumidor RabbitMQ — delay: %d ms/msg | prefetch: %d%n", delayMs, prefetch);

        int[]  seq     = new int[numMsgs];
        long[] tsEnvio = new long[numMsgs];
        long[] chegada = new long[numMsgs];
        long[] fim     = new long[numMsgs];
        int[]  n       = {0};
        CountDownLatch latch = new CountDownLatch(numMsgs);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);
            channel.basicQos(prefetch);

            DeliverCallback onEntrega = (tag, delivery) -> {
                long tChegada = System.nanoTime();
                Map<String, Object> headers = delivery.getProperties().getHeaders();

                // processamento simulado
                aguardarPreciso(delayMs);

                // confirmação manual por mensagem
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                long tFim = System.nanoTime();

                int i = n[0];
                if (headers == null || headers.get(HEADER_TS) == null
                        || headers.get(HEADER_SEQ) == null || i >= numMsgs) {
                    System.err.println("Mensagem sem cabeçalhos de medição — ignorada.");
                    return;
                }

                seq[i]     = ((Number) headers.get(HEADER_SEQ)).intValue();
                tsEnvio[i] = ((Number) headers.get(HEADER_TS)).longValue();
                chegada[i] = tChegada;
                fim[i]     = tFim;
                n[0] = i + 1;
                latch.countDown();
            };

            channel.basicConsume(fila, false, onEntrega, tag -> { });

            // o consumidor já está registrado na fila: o produtor pode começar
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            long timeoutMs = (long) numMsgs * Math.max(delayMs, 1) * 3 + 60_000L;
            if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                System.err.printf("Timeout: %d/%d mensagens recebidas%n",
                        numMsgs - (int) latch.getCount(), numMsgs);
            }
        }

        int total = n[0];
        if (total != numMsgs) {
            throw new IllegalStateException("Mensagens recebidas: " + total + " de " + numMsgs);
        }

        gravarDetalhado(arquivoCSV, execucao, prefetch, seq, tsEnvio, chegada, fim, total);
        imprimirMetricas(tsEnvio, chegada, fim, total);
    }

    // ------------------------------------------------------------------
    // CSV detalhado: uma linha por mensagem, na ORDEM DE CHEGADA.
    // ------------------------------------------------------------------
    static void gravarDetalhado(String arquivo, int execucao, int parametro,
                                int[] seq, long[] tsEnvio, long[] chegada, long[] fim, int n)
            throws IOException {
        boolean novo = !new File(arquivo).exists() || new File(arquivo).length() == 0;
        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
            if (novo) {
                w.println("execucao,prefetch,ordem_chegada,sequencia,latencia_ms,"
                        + "chegada_rel_ms,ocupado_ms,ocioso_ms");
            }
            for (int i = 0; i < n; i++) {
                double latenciaMs = (chegada[i] - tsEnvio[i]) / 1e6;
                double chegadaRel = (chegada[i] - chegada[0]) / 1e6;
                double ocupadoMs  = (fim[i] - chegada[i]) / 1e6;
                String ocioso     = i == 0 ? "" : String.format(Locale.US, "%.4f",
                        (chegada[i] - fim[i - 1]) / 1e6);
                w.printf(Locale.US, "%d,%d,%d,%d,%.4f,%.4f,%.4f,%s%n",
                        execucao, parametro, i + 1, seq[i], latenciaMs, chegadaRel, ocupadoMs, ocioso);
            }
        }
    }

    // ------------------------------------------------------------------
    // Métricas agregadas da execução, lidas pelo orquestrador (CHAVE=valor).
    // ------------------------------------------------------------------
    static void imprimirMetricas(long[] tsEnvio, long[] chegada, long[] fim, int n) {
        long totalNs = fim[n - 1] - chegada[0];
        long ocupadoNs = 0;
        double somaLatenciaMs = 0;
        double[] ociosoMs = new double[Math.max(n - 1, 0)];

        for (int i = 0; i < n; i++) {
            ocupadoNs += fim[i] - chegada[i];
            somaLatenciaMs += (chegada[i] - tsEnvio[i]) / 1e6;
            if (i > 0) ociosoMs[i - 1] = (chegada[i] - fim[i - 1]) / 1e6;
        }

        double utilizacaoPct = totalNs > 0 ? 100.0 * ocupadoNs / totalNs : 0.0;
        double vazaoConsumidor = totalNs > 0 ? n / (totalNs / 1e9) : 0.0;
        double ociosoMedio = Arrays.stream(ociosoMs).average().orElse(0.0);
        double[] ordenado = ociosoMs.clone();
        Arrays.sort(ordenado);
        double ociosoMediana = ordenado.length == 0 ? 0.0
                : (ordenado.length % 2 == 1
                    ? ordenado[ordenado.length / 2]
                    : (ordenado[ordenado.length / 2 - 1] + ordenado[ordenado.length / 2]) / 2.0);

        System.out.println("RECEBIDAS=" + n);
        System.out.println("LATENCIA_MEDIA=" + String.format(Locale.US, "%.4f", somaLatenciaMs / n));
        System.out.println("VAZAO_CONSUMIDOR=" + String.format(Locale.US, "%.4f", vazaoConsumidor));
        System.out.println("UTILIZACAO=" + String.format(Locale.US, "%.4f", utilizacaoPct));
        System.out.println("OCIOSO_MEDIO=" + String.format(Locale.US, "%.4f", ociosoMedio));
        System.out.println("OCIOSO_MEDIANA=" + String.format(Locale.US, "%.4f", ociosoMediana));
    }

    // ------------------------------------------------------------------
    // Espera precisa, imune à resolução de ~15,6 ms do timer do Windows.
    // ------------------------------------------------------------------
    private static final long LIMIAR_SLEEP_NS = 20_000_000L;

    static void aguardarPreciso(long delayMs) {
        if (delayMs <= 0) return;
        long delayNs = delayMs * 1_000_000L;
        long alvoNs = System.nanoTime() + delayNs;

        if (delayNs > LIMIAR_SLEEP_NS) {
            long margemNs = 3_000_000L;
            long faltaNs = alvoNs - System.nanoTime();
            if (faltaNs > margemNs) {
                try {
                    Thread.sleep((faltaNs - margemNs) / 1_000_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        while (System.nanoTime() < alvoNs) {
            Thread.onSpinWait();
        }
    }
}
