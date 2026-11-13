package com.benchmarking.rabbitmq.cenario3_3;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.*;

import java.io.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível avançado — Cenário 3: Falha de broker — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos: host, porta, fila, arquivo de saída.
 *
 * Conectado a um nó que não é o líder (como o produtor). Ack manual,
 * prefetch 100. Para cada entrega grava "sequencia;envioNs;chegadaNs;redelivered"
 * na ordem de chegada. Mensagens entregues e ainda sem ack quando o líder cai
 * podem ser reentregues pelo novo líder (redelivered = true): são as duplicatas.
 * No RabbitMQ, as duplicatas vêm dessas reentregas; o produtor não reenvia.
 *
 * Se o broker cancelar a assinatura (CancelCallback), o consumidor assina de
 * novo e conta CANCELAMENTOS. Uma assinatura que falha fecha o canal (é a
 * regra do AMQP), então cada nova tentativa usa um canal novo; tentar de novo
 * no canal fechado lançaria AlreadyClosedException e derrubaria o consumidor.
 * Cada canal confirma as próprias entregas.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim, o
 * orquestrador morreu, e o consumidor encerra na hora (Util.vigiarEntrada).
 * Sem isso, a reassinatura automática faria um consumidor órfão se inscrever
 * na fila da execução seguinte e dividir as mensagens com o consumidor dela.
 *
 * Conversa com o orquestrador: STATUS=PRONTO depois do basicConsume;
 * PRODUTOR_TERMINOU na entrada padrão → encerra depois de 10 s sem mensagens.
 */
public class ConsumidorFalhaBroker {

    private static final long OCIOSO_MS = 10_000;
    private static final long MAX_TOTAL_MS = 400_000;
    private static final int PREFETCH = 100;

    private static final AtomicInteger recebidas = new AtomicInteger();
    private static final AtomicInteger cancelamentos = new AtomicInteger();
    private static final AtomicBoolean cancelado = new AtomicBoolean(false);
    private static final AtomicLong ultimaAtividade = new AtomicLong();

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: ConsumidorFalhaBroker <host> <porta> <fila> <arquivo>");
            System.exit(2);
        }
        String host    = args[0];
        int    porta   = Integer.parseInt(args[1]);
        String fila    = args[2];
        String arquivo = args[3];

        AtomicLong sinal = new AtomicLong(0);
        Util.vigiarEntrada(l -> {
            if (l.equals("PRODUTOR_TERMINOU")) sinal.set(System.nanoTime());
        });

        long inicio = System.nanoTime();
        ultimaAtividade.set(inicio);

        try (Connection conn = Util.conectar(host, porta, 60);
             PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(arquivo)))) {
            Channel canal = assinar(conn, fila, w, true);
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                Thread.sleep(50);
                if (cancelado.getAndSet(false)) {
                    System.out.println("AVISO=assinatura cancelada pelo broker; assinando de novo");
                    fecharEmSilencio(canal);
                    try {
                        canal = assinar(conn, fila, w, false);
                    } catch (Exception e) {
                        // fila ainda sem líder (o canal da tentativa já foi fechado pelo broker):
                        // tenta de novo, com canal novo, na próxima volta
                        canal = null;
                        cancelado.set(true);
                        Thread.sleep(500);
                    }
                }
                long agora = System.nanoTime();
                long s = sinal.get();
                if (s > 0 && (agora - Math.max(s, ultimaAtividade.get())) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
            fecharEmSilencio(canal);
        }
        System.out.println("RECEBIDAS=" + recebidas.get());
        System.out.println("CANCELAMENTOS=" + cancelamentos.get());
        System.out.flush();
    }

    /**
     * Abre um canal novo e assina a fila nele. Cada entrega é confirmada no
     * canal em que chegou. Na primeira assinatura confere que a fila existe.
     */
    private static Channel assinar(Connection conn, String fila, PrintWriter w, boolean conferirFila) throws IOException {
        Channel c = conn.createChannel();
        try {
            if (conferirFila) c.queueDeclarePassive(fila);
            c.basicQos(PREFETCH);
            DeliverCallback entrega = (tag, d) -> {
                long chegada = System.nanoTime();
                Map<String, Object> h = d.getProperties().getHeaders();
                Object sq = h == null ? null : h.get("sequencia");
                Object ts = h == null ? null : h.get("ts-envio");
                if (sq != null && ts != null) {
                    synchronized (w) {
                        w.println(((Number) sq).intValue() + ";" + ((Number) ts).longValue() + ";" + chegada + ";"
                                + d.getEnvelope().isRedeliver());
                    }
                    recebidas.incrementAndGet();
                    ultimaAtividade.set(chegada);
                }
                c.basicAck(d.getEnvelope().getDeliveryTag(), false);
            };
            CancelCallback cancel = tag -> {
                cancelamentos.incrementAndGet();
                cancelado.set(true);
            };
            c.basicConsume(fila, false, entrega, cancel);
            return c;
        } catch (IOException | RuntimeException e) {
            fecharEmSilencio(c);
            throw e;
        }
    }

    private static void fecharEmSilencio(Channel c) {
        if (c == null) return;
        try {
            if (c.isOpen()) c.close();
        } catch (Exception ignored) {
            // já fechado pelo broker
        }
    }
}
