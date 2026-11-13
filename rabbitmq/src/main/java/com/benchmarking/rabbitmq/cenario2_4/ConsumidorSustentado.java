package com.benchmarking.rabbitmq.cenario2_4;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível intermediário — Cenário 4: Sobrecarga prolongada — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos: host, fila, número de mensagens enviadas, tempo de
 * processamento por mensagem (ms), prefetch.
 *
 * Consumidor lento: Thread.sleep(procMs) por mensagem e ack manual depois
 * de processar, com prefetch (o mesmo valor do max.poll.records do Kafka).
 *
 * A conexão leva o nome "bench-c4-consumidor": o orquestrador procura
 * conexões com esse prefixo na management API para detectar processos de
 * execuções anteriores ainda vivos.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim (EOF), o
 * orquestrador morreu (mesmo à força), e o consumidor encerra na hora.
 *
 * Conversa com o orquestrador (igual ao Kafka):
 *   stdout STATUS=PRONTO, CONSUMIDAS=<n> a cada ~1 s
 *   stdin  PRODUTOR_TERMINOU → encerra depois de 5 s sem mensagens
 *   stdin  EOF → o orquestrador morreu: encerra (código 3)
 */
public class ConsumidorSustentado {

    private static final String HEADER_SEQ = "sequencia";
    private static final String NOME_CONEXAO = "bench-c4-consumidor";
    private static final long OCIOSO_MS = 5_000;
    private static final long MAX_TOTAL_MS = 1_800_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ConsumidorSustentado <host> <fila> <numMsgs> <procMs> <prefetch>");
            System.exit(2);
        }
        String host     = args[0];
        String fila     = args[1];
        int    numMsgs  = Integer.parseInt(args[2]);
        long   procMs   = Long.parseLong(args[3]);
        int    prefetch = Integer.parseInt(args[4]);

        AtomicLong sinal = new AtomicLong(0);
        Thread stdin = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                String l;
                while ((l = in.readLine()) != null) {
                    if (l.trim().equals("PRODUTOR_TERMINOU")) sinal.set(System.nanoTime());
                }
            } catch (IOException ignored) {
            }
            // Fim da entrada padrão: o orquestrador encerrou (normalmente ou à força).
            // Numa execução normal isso nunca acontece antes de o consumidor terminar,
            // porque o orquestrador espera por ele com o stdin ainda aberto.
            System.err.println("Entrada padrão fechada: o orquestrador encerrou. Saindo para não ficar órfão.");
            Runtime.getRuntime().halt(3);
        }, "vigia-stdin");
        stdin.setDaemon(true);
        stdin.start();

        // os callbacks de um canal rodam um de cada vez
        boolean[] vista = new boolean[numMsgs + 1];
        int[] recebidas = {0}, unicas = {0}, saltos = {0}, ultimaSeq = {0};
        AtomicLong ultimaAtividade = new AtomicLong(0);
        long[] ultimoFim = {0};
        long inicio = System.nanoTime();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        try (Connection conn = factory.newConnection(NOME_CONEXAO);
             Channel channel = conn.createChannel()) {

            channel.queueDeclarePassive(fila);
            channel.basicQos(prefetch);

            DeliverCallback onEntrega = (tag, d) -> {
                try {
                    Thread.sleep(procMs);   // processamento lento
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                Map<String, Object> h = d.getProperties().getHeaders();
                Object s = h == null ? null : h.get(HEADER_SEQ);
                if (s != null) {
                    int seq = ((Number) s).intValue();
                    if (seq > ultimaSeq[0] + 1) saltos[0]++;
                    ultimaSeq[0] = Math.max(ultimaSeq[0], seq);
                    if (seq >= 1 && seq <= numMsgs && !vista[seq]) {
                        vista[seq] = true;
                        unicas[0]++;
                    }
                }
                channel.basicAck(d.getEnvelope().getDeliveryTag(), false);
                recebidas[0]++;
                long agora = System.nanoTime();
                ultimaAtividade.set(agora);
                ultimoFim[0] = agora;
            };

            String tagConsumo = channel.basicConsume(fila, false, onEntrega, t -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                Thread.sleep(1000);
                System.out.println("CONSUMIDAS=" + recebidas[0]);
                System.out.flush();
                long agora = System.nanoTime();
                long s = sinal.get();
                if (s > 0 && (agora - Math.max(s, ultimaAtividade.get())) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
            try {
                channel.basicCancel(tagConsumo);
            } catch (Exception ignored) {
            }
        }

        System.out.println("CONSUMIDAS=" + recebidas[0]);
        System.out.println("RECEBIDAS=" + recebidas[0]);
        System.out.println("UNICAS=" + unicas[0]);
        System.out.println("SALTOS=" + saltos[0]);
        System.out.println("ULTIMO_FIM=" + ultimoFim[0]);
        System.out.println("STATUS=CONCLUIDO");
        System.out.flush();
    }
}
