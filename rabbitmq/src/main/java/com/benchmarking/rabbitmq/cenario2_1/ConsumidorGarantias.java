package com.benchmarking.rabbitmq.cenario2_1;

import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Nível intermediário — Cenário 1: Comparação de garantias — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - número de mensagens esperado (sem contar duplicatas)
 *   3 - execução (0 = aquecimento)
 *   4 - garantia: at-most-once | at-least-once | exactly-once
 *   5 - arquivo CSV detalhado ("-" = não gravar; Parte B e aquecimento)
 *   6 - prefetch (basicQos; só tem efeito com ack manual)
 *   7 - (opcional) modo de ACK: auto | manual | lote | "-". Sem ele (ou com
 *       "-"), vale o padrão do nível (auto no at-most-once, manual nos
 *       outros). Só o teste de confirmações (Parte C do orquestrador) passa
 *       este argumento.
 *         auto   : autoAck (o broker considera entregue ao enviar)
 *         manual : basicAck(tag, false) de cada mensagem, depois de processar
 *         lote   : basicAck(tag, true) a cada ACK_LOTE mensagens, que
 *                  confirma de uma vez todas as anteriores ainda sem ACK
 *
 * Configuração por nível:
 *   at-most-once  : autoAck — o broker considera a mensagem entregue ao
 *                   enviá-la. Se o consumidor cair, o que estava em trânsito
 *                   se perde.
 *   at-least-once : ack manual de cada mensagem DEPOIS de processar, com
 *                   prefetch. Se cair, as mensagens sem ack são reentregues.
 *   exactly-once  : at-least-once + deduplicação pelo messageId
 *                   (consumidor idempotente). É o único caminho no RabbitMQ.
 *
 * Mesmo padrão do Cenário 3: STATUS=PRONTO depois do basicConsume, latências
 * na ordem de chegada, contagem conferida (o processo falha se não bater).
 */
public class ConsumidorGarantias {

    private static final String HEADER_TS = "ts-envio";
    private static final long TIMEOUT_S = 300;

    /**
     * Tamanho do lote no modo "lote". É METADE do prefetch (100), e não 100:
     * o prefetch limita quantas mensagens sem ACK o broker deixa com o
     * consumidor. Com lote = prefetch, o broker para de entregar ao chegar
     * em 100 sem ACK e só volta depois do ACK da centésima, e o fluxo vira
     * "para e anda", um custo que vem do lote, não do ACK. Com metade, o ACK
     * sai enquanto ainda há 50 mensagens liberadas, e o fluxo não para.
     * No Kafka o commit não limita a entrega, então lá a questão não existe.
     */
    private static final int ACK_LOTE = 50;

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorGarantias <host> <fila> <numMsgs> <execucao> <garantia> <arquivoCSV|-> <prefetch>");
            System.exit(2);
        }

        String host       = args[0];
        String fila       = args[1];
        int    numMsgs    = Integer.parseInt(args[2]);
        int    execucao   = Integer.parseInt(args[3]);
        String garantia   = args[4];
        String arquivoCSV = args[5];
        int    prefetch   = Integer.parseInt(args[6]);

        String modoAck = garantia.equals("at-most-once") ? "auto" : "manual";
        if (args.length >= 8 && !"-".equals(args[7])) modoAck = args[7];
        if (!Set.of("auto", "manual", "lote").contains(modoAck)) {
            throw new IllegalArgumentException("Modo de ACK inválido: " + modoAck + " (use auto | manual | lote)");
        }
        boolean autoAck = modoAck.equals("auto");
        boolean ackLote = modoAck.equals("lote");
        System.out.println("MODO_ACK=" + modoAck);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        // os callbacks de um canal rodam um de cada vez: sem disputa nos vetores
        long[] latenciasNs = new long[numMsgs];
        Set<String> vistos = new HashSet<>(numMsgs * 2);
        int[] recebidas = {0};
        int[] duplicatas = {0};
        int[] negativas = {0};
        int[] invalidas = {0};
        int[] semAck = {0};        // modo lote: entregas desde o último ACK
        long[] chegadas = {0, 0}; // primeira, última
        CountDownLatch latch = new CountDownLatch(numMsgs);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.queueDeclare(fila, true, false, false, null);
            if (!autoAck) channel.basicQos(prefetch);

            DeliverCallback onEntrega = (consumerTag, delivery) -> {
                long chegada = System.nanoTime();
                boolean contou = false;
                Map<String, Object> headers = delivery.getProperties().getHeaders();
                Object ts = headers == null ? null : headers.get(HEADER_TS);
                String id = delivery.getProperties().getMessageId();

                if (ts == null || id == null) {
                    invalidas[0]++;
                } else if (!vistos.add(id)) {
                    // duplicata: no exactly-once é descartada; nos outros também é
                    // contada, para a contagem de mensagens únicas fechar
                    duplicatas[0]++;
                } else if (recebidas[0] < numMsgs) {
                    long latencia = chegada - ((Number) ts).longValue();
                    if (latencia < 0) negativas[0]++;
                    int i = recebidas[0];
                    if (i == 0) chegadas[0] = chegada;
                    chegadas[1] = chegada;
                    latenciasNs[i] = latencia;
                    recebidas[0] = i + 1;
                    contou = true;
                }

                // at-least-once e exactly-once: ack DEPOIS de processar
                if (!autoAck) {
                    long tagEntrega = delivery.getEnvelope().getDeliveryTag();
                    if (!ackLote) {
                        channel.basicAck(tagEntrega, false);
                    } else if (++semAck[0] >= ACK_LOTE || recebidas[0] >= numMsgs) {
                        // multiple=true: confirma esta e todas as anteriores sem ACK;
                        // a última mensagem fecha o lote incompleto
                        channel.basicAck(tagEntrega, true);
                        semAck[0] = 0;
                    }
                }

                // só libera o main DEPOIS do ACK, para a conexão não fechar com
                // ACK pendente (o carimbo de chegada já foi feito no início)
                if (contou) latch.countDown();
            };

            channel.basicConsume(fila, autoAck, onEntrega, tag -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            if (!latch.await(TIMEOUT_S, TimeUnit.SECONDS)) {
                System.err.printf("TIMEOUT: %d/%d mensagens recebidas%n", recebidas[0], numMsgs);
            }
        }

        int n = recebidas[0];
        if (invalidas[0] > 0) {
            throw new IllegalStateException(invalidas[0] + " mensagens sem ts-envio ou messageId");
        }
        if (n != numMsgs) {
            throw new IllegalStateException("Número incorreto de mensagens. Esperado=" + numMsgs + ", recebido=" + n);
        }
        if (negativas[0] > 0) {
            System.err.printf("Aviso: %d latências negativas%n", negativas[0]);
        }

        if (!"-".equals(arquivoCSV)) {
            File arquivo = new File(arquivoCSV);
            boolean novo = !arquivo.exists() || arquivo.length() == 0;
            try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
                if (novo) w.println("execucao,garantia,mensagem,latencia_ms");
                for (int i = 0; i < n; i++) {
                    w.printf(Locale.US, "%d,%s,%d,%.4f%n", execucao, garantia, i + 1, latenciasNs[i] / 1e6);
                }
            }
        }

        long[] ord = Arrays.copyOf(latenciasNs, n);
        Arrays.sort(ord);
        double media = Arrays.stream(ord).average().orElse(0) / 1e6;
        double mediana = (n % 2 == 1) ? ord[n / 2] / 1e6 : (ord[n / 2 - 1] + ord[n / 2]) / 2.0 / 1e6;

        System.out.println("MEDIA="      + String.format(Locale.US, "%.4f", media));
        System.out.println("MEDIANA="    + String.format(Locale.US, "%.4f", mediana));
        System.out.println("MIN="        + String.format(Locale.US, "%.4f", ord[0] / 1e6));
        System.out.println("MAX="        + String.format(Locale.US, "%.4f", ord[n - 1] / 1e6));
        System.out.println("PRIMEIRA_CHEGADA=" + chegadas[0]);
        System.out.println("ULTIMA_CHEGADA="   + chegadas[1]);
        System.out.println("NEGATIVAS="  + negativas[0]);
        System.out.println("DUPLICATAS=" + duplicatas[0]);
        System.out.println("RECEBIDAS="  + n);
    }
}