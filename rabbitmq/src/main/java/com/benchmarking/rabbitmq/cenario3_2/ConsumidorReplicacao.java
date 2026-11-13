package com.benchmarking.rabbitmq.cenario3_2;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Nível avançado — Cenário 2: Impacto da replicação — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos: host, porta, fila, número esperado, execução, configuração
 * (só para o CSV), arquivo CSV detalhado ("-" = não gravar).
 *
 * Numa quorum queue o líder só entrega a mensagem ao consumidor depois que
 * ela foi aceita pela maioria (é o equivalente da high watermark do Kafka).
 * Por isso a latência de ponta a ponta já inclui o custo da replicação.
 *
 * STATUS=PRONTO depois do basicConsume, latências na ordem de chegada e
 * contagem conferida, como no Cenário 3 básico. Diferença em relação a ele:
 * aqui o ACK é manual (basicAck por mensagem, prefetch 100), e não autoAck.
 *
 * Numa quorum queue, cada ACK também é replicado pelo Raft, porque retirar a
 * mensagem da fila é uma operação do log. O consumidor do Kafka lê sem commit
 * (consumir não altera o log). A diferença é da arquitetura, foi mantida de
 * propósito e deve ser registrada no texto: pesa sobretudo na Parte B (vazão).
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim (EOF), o
 * orquestrador morreu (mesmo à força), e o processo encerra na hora com
 * código 3, em vez de continuar rodando sozinho (Util.vigiarEntrada).
 */
public class ConsumidorReplicacao {

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorReplicacao <host> <porta> <fila> <numMsgs> <execucao> <config> <arquivoCSV|->");
            System.exit(2);
        }
        String host     = args[0];
        int    porta    = Integer.parseInt(args[1]);
        String fila     = args[2];
        int    numMsgs  = Integer.parseInt(args[3]);
        int    execucao = Integer.parseInt(args[4]);
        String config   = args[5];
        String arquivo  = args[6];

        Util.vigiarEntrada(l -> { });

        long[] lat = new long[numMsgs];
        int[] recebidas = {0}, negativas = {0};
        long[] ultimaChegada = {0};
        CountDownLatch fim = new CountDownLatch(1);

        try (Connection conn = Util.conectar(host, porta, 60);
             Channel ch = conn.createChannel()) {
            ch.queueDeclarePassive(fila);
            ch.basicQos(100);
            ch.basicConsume(fila, false, (tag, d) -> {
                long chegada = System.nanoTime();
                Map<String, Object> h = d.getProperties().getHeaders();
                Object ts = h == null ? null : h.get("ts-envio");
                if (ts != null && recebidas[0] < numMsgs) {
                    long l = chegada - ((Number) ts).longValue();
                    if (l < 0) negativas[0]++;
                    lat[recebidas[0]++] = l;
                    ultimaChegada[0] = chegada;
                    if (recebidas[0] == numMsgs) fim.countDown();
                }
                ch.basicAck(d.getEnvelope().getDeliveryTag(), false);
            }, tag -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();
            if (!fim.await(300, TimeUnit.SECONDS)) System.err.printf("TIMEOUT: %d/%d%n", recebidas[0], numMsgs);
        }
        if (recebidas[0] != numMsgs) throw new IllegalStateException("Esperado=" + numMsgs + ", recebido=" + recebidas[0]);
        if (negativas[0] > 0) System.err.printf("Aviso: %d latências negativas%n", negativas[0]);

        if (!"-".equals(arquivo)) {
            File f = new File(arquivo);
            boolean novo = !f.exists() || f.length() == 0;
            try (PrintWriter w = new PrintWriter(new FileWriter(f, true))) {
                if (novo) w.println("execucao,configuracao,mensagem,latencia_ms");
                for (int i = 0; i < numMsgs; i++) w.printf(Locale.US, "%d,%s,%d,%.4f%n", execucao, config, i + 1, lat[i] / 1e6);
            }
        }
        double[] r = Util.resumoMs(lat, numMsgs);
        System.out.println("MEDIA="   + String.format(Locale.US, "%.4f", r[0]));
        System.out.println("MEDIANA=" + String.format(Locale.US, "%.4f", r[1]));
        System.out.println("MIN="     + String.format(Locale.US, "%.4f", r[2]));
        System.out.println("MAX="     + String.format(Locale.US, "%.4f", r[3]));
        System.out.println("ULTIMA_CHEGADA=" + ultimaChegada[0]);
        System.out.println("RECEBIDAS=" + recebidas[0]);
    }
}