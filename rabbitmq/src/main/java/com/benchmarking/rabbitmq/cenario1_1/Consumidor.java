package com.benchmarking.rabbitmq.cenario1_1;

import com.rabbitmq.client.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Consumidor independente do Cenário 1.1 (baseline) — RabbitMQ.
 *
 * FIX (ordem de chegada): antes, as latências eram ORDENADAS antes de ir
 * para o CSV detalhado, e a coluna "mensagem" virava a posição no ranking.
 * Isso impedia ver QUANDO as mensagens lentas aconteceram. Agora o CSV é
 * gravado na ordem de chegada, igual ao consumidor do Kafka; a cópia
 * ordenada é usada só para as estatísticas.
 *
 * FIX (latências negativas): antes, Math.max(0, latência) escondia
 * qualquer valor negativo. Agora o valor é gravado como medido e os
 * negativos são contados e avisados (NEGATIVAS=...), como no Kafka.
 *
 * FIX (prontidão): o consumidor imprime STATUS=PRONTO assim que se
 * registra na fila, e o orquestrador só inicia o produtor depois disso
 * (antes era uma espera fixa de 1,2 s).
 *
 * REMOVIDO: basicQos(500). Com autoAck=true o prefetch não tem efeito, e a
 * linha só gerava dúvida sobre a configuração.
 *
 * Percentis calculados pelo mesmo método do Kafka (nearest-rank).
 *
 * NOVO (atraso do gerador): lê também o header "ts-planejado", enviado
 * pelo Produtor, e grava no CSV detalhado a coluna atraso_gerador_ms
 * (ts-envio - ts-planejado). A latência continua sendo chegada - ts-envio;
 * a latência contada a partir do instante planejado pode ser obtida no
 * Python como latencia_ms + atraso_gerador_ms. O resumo ganha
 * ATRASO_MEDIANO e ATRASO_MAX (ms), só informativos. Igual ao Kafka.
 */
public class Consumidor {

    private static final String HEADER_TS           = "ts-envio";
    private static final String HEADER_TS_PLANEJADO = "ts-planejado";

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: Consumidor <host> <fila> <numMsgs> <execucao> <arquivoCSV>");
            System.exit(1);
        }

        String host       = args[0];
        String fila       = args[1];
        int    numMsgs    = Integer.parseInt(args[2]);
        int    execucao   = Integer.parseInt(args[3]);
        String arquivoCSV = args[4];

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        // Latências em NANOSSEGUNDOS, na ORDEM DE CHEGADA. Os callbacks de um
        // canal rodam um de cada vez, então o vetor é preenchido sem disputa.
        long[] latenciasNs = new long[numMsgs];
        // NOVO: atraso do gerador (ts-envio - ts-planejado), em NANOSSEGUNDOS,
        // com o mesmo índice de latenciasNs.
        long[] atrasosNs   = new long[numMsgs];
        int[]  recebidas   = {0};
        int[]  negativas   = {0};
        CountDownLatch latch = new CountDownLatch(numMsgs);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);

            DeliverCallback onEntrega = (tag, delivery) -> {
                long chegadaNs = System.nanoTime();
                Map<String, Object> headers = delivery.getProperties().getHeaders();
                Object tsHeader = headers == null ? null : headers.get(HEADER_TS);

                Object tsPlanejadoHeader = headers == null ? null : headers.get(HEADER_TS_PLANEJADO);

                if (tsHeader == null) {
                    System.err.println("Mensagem sem header " + HEADER_TS);
                    return;
                }
                if (tsPlanejadoHeader == null) {
                    System.err.println("Mensagem sem header " + HEADER_TS_PLANEJADO);
                    return;
                }
                int i = recebidas[0];
                if (i >= numMsgs) return; // segurança

                long tsEnvio     = ((Number) tsHeader).longValue();
                long tsPlanejado = ((Number) tsPlanejadoHeader).longValue();

                long latenciaNs = chegadaNs - tsEnvio;
                if (latenciaNs < 0) negativas[0]++;

                latenciasNs[i] = latenciaNs;
                atrasosNs[i]   = tsEnvio - tsPlanejado;
                recebidas[0] = i + 1;
                latch.countDown();
            };

            channel.basicConsume(fila, true, onEntrega, tag -> { });

            // registrado na fila: o orquestrador pode iniciar o produtor
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            if (!latch.await(120, TimeUnit.SECONDS)) {
                System.err.printf("Timeout: %d/%d mensagens recebidas%n", recebidas[0], numMsgs);
            }
        }

        int n = recebidas[0];
        if (n != numMsgs) {
            throw new IllegalStateException("Número incorreto de mensagens. Esperado="
                    + numMsgs + ", recebido=" + n);
        }
        if (negativas[0] > 0) {
            System.err.printf("Aviso: %d latências negativas (relógios dos processos?)%n", negativas[0]);
        }

        // CSV DETALHADO — execucao,mensagem,latencia_ms,atraso_gerador_ms, na ordem de chegada
        File arquivo = new File(arquivoCSV);
        boolean novo = !arquivo.exists() || arquivo.length() == 0;
        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
            if (novo) {
                w.println("execucao,mensagem,latencia_ms,atraso_gerador_ms");
            }
            for (int i = 0; i < n; i++) {
                w.printf(Locale.US, "%d,%d,%.4f,%.4f%n", execucao, i + 1,
                        latenciasNs[i] / 1_000_000.0, atrasosNs[i] / 1_000_000.0);
            }
        }

        // cópia ordenada SOMENTE para as estatísticas
        long[] ordenado = Arrays.copyOf(latenciasNs, n);
        Arrays.sort(ordenado);

        double mediaNs = Arrays.stream(ordenado).average().orElse(0.0);

        System.out.println("MEDIA=" + String.format(Locale.US, "%.4f", mediaNs / 1_000_000.0));
        System.out.println("MIN="   + String.format(Locale.US, "%.4f", ordenado[0] / 1_000_000.0));
        System.out.println("MAX="   + String.format(Locale.US, "%.4f", ordenado[n - 1] / 1_000_000.0));
        System.out.println("P95="   + String.format(Locale.US, "%.4f", percentil(ordenado, 0.95) / 1_000_000.0));
        System.out.println("P99="   + String.format(Locale.US, "%.4f", percentil(ordenado, 0.99) / 1_000_000.0));
        // NOVO: resumo do atraso do gerador (informativo; o orquestrador
        // ignora chaves que não usa).
        long[] atrasosOrdenados = Arrays.copyOf(atrasosNs, n);
        Arrays.sort(atrasosOrdenados);
        System.out.println("ATRASO_MEDIANO=" + String.format(Locale.US, "%.4f",
                percentil(atrasosOrdenados, 0.50) / 1_000_000.0));
        System.out.println("ATRASO_MAX=" + String.format(Locale.US, "%.4f",
                atrasosOrdenados[n - 1] / 1_000_000.0));

        System.out.println("NEGATIVAS=" + negativas[0]);
        System.out.println("RECEBIDAS=" + n);
    }

    /** Percentil nearest-rank (posição = ceil(p * n), 1-based) — igual ao Kafka. */
    private static long percentil(long[] ordenado, double p) {
        int n = ordenado.length;
        int indice = (int) Math.ceil(p * n) - 1;
        indice = Math.max(0, Math.min(indice, n - 1));
        return ordenado[indice];
    }
}