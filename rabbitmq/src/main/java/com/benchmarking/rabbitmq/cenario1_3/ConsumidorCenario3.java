package com.benchmarking.rabbitmq.cenario1_3;

import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Cenário 3 — Variação do tamanho da mensagem — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - número de mensagens esperado
 *   3 - execução (0 = aquecimento)
 *   4 - tamanho esperado da mensagem em bytes
 *   5 - arquivo CSV detalhado ("-" = não gravar; usado na Parte B e no aquecimento)
 *   6 - (opcional) modo de confirmação: "auto" (padrão) ou "manual"
 *
 * Configuração do cenário (modo "auto"): a mesma do Cenário 1 — autoAck=true,
 * sem basicQos. As duas tecnologias seguem o mesmo "melhor esforço" do
 * baseline: o Kafka também não faz commit de offset.
 *
 * Modo "manual" (só no teste complementar): autoAck=false e um basicAck por
 * mensagem, enviado DEPOIS de registrar o instante de chegada (a latência
 * medida não inclui o ACK). O objetivo é criar tráfego de volta na conexão do
 * consumidor e verificar se os atrasos em degraus de 20 ms desaparecem, o que
 * indicaria a interação entre Nagle e ACK atrasado do TCP. Sem basicQos, o
 * número de mensagens não confirmadas não é limitado, como no modo automático.
 *
 * Outros pontos:
 *   - Latências gravadas na ORDEM DE CHEGADA.
 *   - Confere o tamanho de cada mensagem recebida.
 *   - Imprime a hora da primeira e da última chegada (a última é usada na
 *     vazão de ponta a ponta da Parte B).
 *   - CSV em formato padrão (vírgula, ponto decimal), igual ao Kafka.
 */
public class ConsumidorCenario3 {

    private static final String HEADER_TS = "ts-envio";
    private static final long TIMEOUT_S = 300;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ConsumidorCenario3 <host> <fila> <numMsgs> <execucao> <tamanhoBytes> "
                    + "<arquivoCSV|-> [auto|manual]");
            System.exit(2);
        }

        String host         = args[0];
        String fila         = args[1];
        int    numMsgs      = Integer.parseInt(args[2]);
        int    execucao     = Integer.parseInt(args[3]);
        int    tamanhoBytes = Integer.parseInt(args[4]);
        String arquivoCSV   = args[5];
        String modoAck      = args.length >= 7 ? args[6] : "auto";
        if (!"auto".equals(modoAck) && !"manual".equals(modoAck)) {
            System.err.println("Modo de confirmação inválido: " + modoAck + " (use auto ou manual)");
            System.exit(2);
        }
        boolean ackManual = "manual".equals(modoAck);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        // os callbacks de um canal rodam um de cada vez: sem disputa nos vetores
        long[] latenciasNs = new long[numMsgs];
        int[]  recebidas = {0};
        int[]  negativas = {0};
        int[]  tamanhoErrado = {0};
        long[] chegadas = {0, 0}; // primeira, última
        CountDownLatch latch = new CountDownLatch(numMsgs);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);

            DeliverCallback onEntrega = (tag, delivery) -> {
                long chegada = System.nanoTime();

                // modo manual: confirma logo após registrar a chegada
                if (ackManual) {
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }

                Map<String, Object> headers = delivery.getProperties().getHeaders();
                Object ts = headers == null ? null : headers.get(HEADER_TS);
                if (ts == null) {
                    System.err.println("Mensagem sem header " + HEADER_TS);
                    return;
                }
                int i = recebidas[0];
                if (i >= numMsgs) return;

                if (delivery.getBody() == null || delivery.getBody().length != tamanhoBytes) {
                    tamanhoErrado[0]++;
                }

                long latencia = chegada - ((Number) ts).longValue();
                if (latencia < 0) negativas[0]++;

                if (i == 0) chegadas[0] = chegada;
                chegadas[1] = chegada;
                latenciasNs[i] = latencia;
                recebidas[0] = i + 1;
                latch.countDown();
            };

            channel.basicConsume(fila, !ackManual, onEntrega, tag -> { });

            System.out.println("STATUS=PRONTO");
            System.out.flush();

            if (!latch.await(TIMEOUT_S, TimeUnit.SECONDS)) {
                System.err.printf("TIMEOUT: %d/%d mensagens recebidas%n", recebidas[0], numMsgs);
            }
        }

        int n = recebidas[0];
        if (n != numMsgs) {
            throw new IllegalStateException("Número incorreto de mensagens. Esperado="
                    + numMsgs + ", recebido=" + n);
        }
        if (tamanhoErrado[0] > 0) {
            throw new IllegalStateException(tamanhoErrado[0] + " mensagens com tamanho diferente de "
                    + tamanhoBytes + " bytes");
        }
        if (negativas[0] > 0) {
            System.err.printf("Aviso: %d latências negativas%n", negativas[0]);
        }

        if (!"-".equals(arquivoCSV)) {
            File arquivo = new File(arquivoCSV);
            boolean novo = !arquivo.exists() || arquivo.length() == 0;
            try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
                if (novo) w.println("execucao,tamanho_kb,mensagem,latencia_ms");
                for (int i = 0; i < n; i++) {
                    w.printf(Locale.US, "%d,%d,%d,%.4f%n", execucao, tamanhoBytes / 1024, i + 1,
                            latenciasNs[i] / 1e6);
                }
            }
        }

        long[] ord = Arrays.copyOf(latenciasNs, n);
        Arrays.sort(ord);
        double media = Arrays.stream(ord).average().orElse(0) / 1e6;
        double mediana = (n % 2 == 1)
                ? ord[n / 2] / 1e6
                : (ord[n / 2 - 1] + ord[n / 2]) / 2.0 / 1e6;

        System.out.println("MODO_ACK=" + modoAck);
        System.out.println("MEDIA="   + String.format(Locale.US, "%.4f", media));
        System.out.println("MEDIANA=" + String.format(Locale.US, "%.4f", mediana));
        System.out.println("MIN="     + String.format(Locale.US, "%.4f", ord[0] / 1e6));
        System.out.println("MAX="     + String.format(Locale.US, "%.4f", ord[n - 1] / 1e6));
        System.out.println("PRIMEIRA_CHEGADA=" + chegadas[0]);
        System.out.println("ULTIMA_CHEGADA="   + chegadas[1]);
        System.out.println("NEGATIVAS=" + negativas[0]);
        System.out.println("RECEBIDAS=" + n);
    }
}