package com.benchmarking.rabbitmq.cenario2_4;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 4: Sobrecarga prolongada — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, fila, número de mensagens, taxa alvo (msg/s), execução.
 *
 * Envia a uma taxa FIXA, acima da capacidade do consumidor. Mensagens
 * persistentes, com publisher confirms ASSÍNCRONOS: cada envio é anotado e
 * retirado quando o broker confirma.
 *
 * O comportamento com a fila cheia depende da política (definida pelo
 * orquestrador na declaração da fila); o código do produtor é o mesmo:
 *   - com-limite (x-overflow = drop-head): o broker ACEITA a publicação
 *     (ack) e apaga a mensagem mais antiga da fila. O produtor não fica
 *     sabendo do descarte: REJEITADAS fica em 0 e a taxa fica na alvo.
 *   - com-limite-recusa (x-overflow = reject-publish): o broker RECUSA a
 *     publicação e responde com nack. O produtor reenvia a mensagem (nada se
 *     perde) e, na prática, passa a andar no ritmo do consumidor. É isso que
 *     a taxa de produção efetiva e o contador REJEITADAS mostram. Os
 *     reenvios chegam fora de ordem, então o consumidor conta "saltos" que
 *     não são perdas.
 *
 * A cada ~1 s imprime ENVIADAS=<confirmadas> para a série temporal.
 * No máximo 2.000 mensagens sem confirmação ao mesmo tempo.
 *
 * A conexão leva o nome "bench-c4-produtor" (o orquestrador usa o prefixo
 * para detectar processos de execuções anteriores ainda vivos).
 *
 * Proteção contra processo órfão: o orquestrador mantém o stdin do produtor
 * aberto até o fim; se ele chegar ao fim (EOF), o orquestrador morreu, e o
 * produtor encerra na hora (código 3) em vez de continuar publicando.
 */
public class ProdutorSustentado {

    private static final String HEADER_SEQ = "sequencia";
    private static final String NOME_CONEXAO = "bench-c4-produtor";
    private static final int MAX_PENDENTES = 2_000;
    private static final long TIMEOUT_TOTAL_MS = 1_800_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ProdutorSustentado <host> <fila> <numMsgs> <taxaMsgS> <execucao>");
            System.exit(2);
        }
        String host     = args[0];
        String fila     = args[1];
        int    numMsgs  = Integer.parseInt(args[2]);
        int    taxa     = Integer.parseInt(args[3]);
        int    execucao = Integer.parseInt(args[4]);

        encerrarSeOrquestradorMorrer();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        byte[] payload = new byte[1024];

        ConcurrentNavigableMap<Long, Integer> pendentes = new ConcurrentSkipListMap<>();
        ConcurrentLinkedDeque<Integer> reenviar = new ConcurrentLinkedDeque<>();
        AtomicInteger confirmadas = new AtomicInteger();
        AtomicInteger rejeitadas = new AtomicInteger();

        try (Connection conn = factory.newConnection(NOME_CONEXAO);
             Channel channel = conn.createChannel()) {

            channel.queueDeclarePassive(fila);
            channel.confirmSelect();
            channel.addConfirmListener(new ConfirmListener() {
                @Override
                public void handleAck(long tag, boolean multiple) {
                    if (multiple) {
                        ConcurrentNavigableMap<Long, Integer> h = pendentes.headMap(tag, true);
                        confirmadas.addAndGet(h.size());
                        h.clear();
                    } else if (pendentes.remove(tag) != null) {
                        confirmadas.incrementAndGet();
                    }
                }

                @Override
                public void handleNack(long tag, boolean multiple) {
                    if (multiple) {
                        ConcurrentNavigableMap<Long, Integer> h = pendentes.headMap(tag, true);
                        rejeitadas.addAndGet(h.size());
                        reenviar.addAll(h.values());
                        h.clear();
                    } else {
                        Integer seq = pendentes.remove(tag);
                        if (seq != null) {
                            rejeitadas.incrementAndGet();
                            reenviar.add(seq);
                        }
                    }
                }
            });

            long t0 = System.nanoTime();
            long intervalo = 1_000_000_000L / taxa;
            long proximoRelato = t0 + 1_000_000_000L;
            int proximaNova = 1;
            long envio = 0;   // número do "horário" de envio (novas + reenvios)

            while (confirmadas.get() < numMsgs) {
                if ((System.nanoTime() - t0) / 1_000_000 > TIMEOUT_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
                if (System.nanoTime() >= proximoRelato) {
                    System.out.println("ENVIADAS=" + confirmadas.get());
                    System.out.flush();
                    proximoRelato += 1_000_000_000L;
                }

                if (pendentes.size() >= MAX_PENDENTES) {
                    Thread.sleep(1);   // muitas sem confirmação: espera o broker
                    continue;
                }
                Integer seq = reenviar.poll();          // reenvios têm prioridade
                if (seq == null) {
                    if (proximaNova > numMsgs) {
                        Thread.sleep(1);               // tudo enviado; esperando confirmações
                        continue;
                    }
                    seq = proximaNova++;
                }

                // ritmo fixo; se ficou para trás (esperando o broker), não tenta "compensar" em rajada
                long alvo = t0 + envio * intervalo;
                long agora = System.nanoTime();
                if (agora - alvo > 50_000_000L) {
                    envio = (agora - t0) / intervalo;
                    alvo = t0 + envio * intervalo;
                }
                aguardarAte(alvo);
                envio++;
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .headers(Map.of(HEADER_SEQ, seq))
                        .deliveryMode(2)
                        .build();
                pendentes.put(channel.getNextPublishSeqNo(), seq);
                channel.basicPublish("", fila, props, payload);
            }
            long t1 = System.nanoTime();

            System.out.println("ENVIADAS=" + confirmadas.get());
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("ENVIADAS_TOTAL=" + confirmadas.get());
            System.out.println("REJEITADAS=" + rejeitadas.get());
            System.out.println("EXECUCAO=" + execucao);
            System.out.flush();
        }
    }

    /**
     * Thread daemon que lê o stdin até o fim. O orquestrador nunca fecha o
     * stdin do produtor; o EOF só chega quando ele morre (inclusive à força),
     * e então o produtor encerra em vez de virar órfão.
     */
    static void encerrarSeOrquestradorMorrer() {
        Thread vigia = new Thread(() -> {
            try {
                while (System.in.read() != -1) {
                    // ignora qualquer entrada; só interessa o fim do fluxo
                }
            } catch (IOException ignored) {
            }
            System.err.println("Entrada padrão fechada: o orquestrador encerrou. Saindo para não ficar órfão.");
            Runtime.getRuntime().halt(3);
        }, "vigia-stdin");
        vigia.setDaemon(true);
        vigia.start();
    }

    /** Mesmo método dos Cenários 1 e 3. Se já passou do horário, envia na hora. */
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
