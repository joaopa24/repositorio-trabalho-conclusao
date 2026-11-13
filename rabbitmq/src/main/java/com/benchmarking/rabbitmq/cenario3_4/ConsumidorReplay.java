package com.benchmarking.rabbitmq.cenario3_4;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível avançado — Cenário 4: Replay — CONSUMIDOR (RabbitMQ).
 *
 * Argumentos: host, porta, fila, modo, parâmetro, sequência mínima, número esperado.
 *
 * Modos:
 *   classica         — lê a fila clássica e dá ack (o consumo "normal").
 *   classica-de-novo — lê a fila clássica outra vez: deve receber 0, porque o
 *                      ack APAGOU as mensagens (não há o que reler). Controle.
 *   normal-stream    — consumo "normal" do stream, desde o início. No fim,
 *                      grava no arquivo (parâmetro) o último offset processado:
 *                      é o "commit" feito pela APLICAÇÃO, porque pelo AMQP 0.9.1
 *                      o broker não guarda a posição do consumidor no stream.
 *   posicao-salva    — volta ao stream a partir do offset seguinte ao gravado
 *                      pelo normal-stream: deve receber 0 (já leu tudo).
 *                      Par do "grupo-existente" do Kafka; lá quem guarda a
 *                      posição é o broker, aqui é a aplicação.
 *   inicio           — stream, x-stream-offset = "first": relê tudo.
 *   timestamp        — stream, x-stream-offset = data (parâmetro em segundos).
 *   offset           — stream, x-stream-offset = número (parâmetro).
 *
 * O stream começa a entregar no início do BLOCO (chunk) que contém a posição
 * pedida, então pode mandar algumas mensagens anteriores. Por isso o consumidor
 * filtra pela sequência mínima (e, no posicao-salva, pelo offset mínimo, como
 * faria uma aplicação real) e conta as DESCARTADAS. No Kafka a posição é exata.
 *
 * ACK em lote (multiple = true a cada 100), como no Cenário 3 básico. No fim,
 * confirma o restante: sem isso, na fila clássica as últimas mensagens sem ack
 * voltariam para a fila e o modo classica-de-novo receberia mensagens.
 *
 * Medição: conecta antes, imprime STATUS=PRONTO e espera INICIAR. Tempo até a
 * 1ª mensagem aceita e duração até a última, a partir do INICIAR.
 *
 * Saída: RECEBIDAS, ACEITAS, DESCARTADAS, PRIMEIRA_SEQ, ULTIMA_SEQ, EM_ORDEM,
 * T_PRIMEIRA_MS, DURACAO_MS.
 *
 * Proteção contra órfãos: a entrada padrão é lida por Util.vigiarEntrada, que
 * libera o INICIAR e encerra o processo se o orquestrador morrer.
 */
public class ConsumidorReplay {

    private static final long OCIOSO_MS = 5_000;
    private static final long MAX_TOTAL_MS = 300_000;
    private static final int PREFETCH = 500;   // obrigatório para consumir um stream
    private static final int ACK_A_CADA = 100;
    private static final long SEM_VALOR = Long.MIN_VALUE;

    public static void main(String[] args) throws Exception {
        if (args.length < 7) {
            System.err.println("Uso: ConsumidorReplay <host> <porta> <fila> <modo> <parametro> <seqMinima> <numEsperado>");
            System.exit(2);
        }
        String host      = args[0];
        int    porta     = Integer.parseInt(args[1]);
        String fila      = args[2];
        String modo      = args[3];
        String parametro = args[4];
        int    seqMinima = Integer.parseInt(args[5]);
        int    esperado  = Integer.parseInt(args[6]);

        CountDownLatch iniciar = new CountDownLatch(1);
        Util.vigiarEntrada(l -> { if (l.equals("INICIAR")) iniciar.countDown(); });

        long offsetMinimo = -1;   // só no modo posicao-salva
        Map<String, Object> argumentos = new HashMap<>();
        switch (modo) {
            case "classica", "classica-de-novo" -> { }
            case "normal-stream", "inicio" -> argumentos.put("x-stream-offset", "first");
            case "posicao-salva" -> {
                // a aplicação guardou o último offset processado; volta a partir do seguinte
                offsetMinimo = Long.parseLong(Files.readString(Path.of(parametro)).trim()) + 1;
                argumentos.put("x-stream-offset", offsetMinimo);
            }
            case "timestamp" -> argumentos.put("x-stream-offset", new Date(Long.parseLong(parametro) * 1000L));
            case "offset"    -> argumentos.put("x-stream-offset", Long.parseLong(parametro));
            default -> throw new IllegalArgumentException("Modo inválido: " + modo);
        }
        final long offMin = offsetMinimo;

        AtomicInteger recebidas = new AtomicInteger(), aceitas = new AtomicInteger();
        int[] primeira = {-1}, ultima = {-1};
        boolean[] emOrdem = {true};
        AtomicLong tPrimeira = new AtomicLong(SEM_VALOR), tUltima = new AtomicLong(SEM_VALOR);
        AtomicLong ultimoOffset = new AtomicLong(-1);
        AtomicLong ultimaTag = new AtomicLong(0), ultimaConfirmada = new AtomicLong(0);

        try (Connection conn = Util.fabrica(host, porta).newConnection();
             Channel ch = conn.createChannel()) {
            ch.queueDeclarePassive(fila);
            ch.basicQos(PREFETCH);
            System.out.println("STATUS=PRONTO");
            System.out.flush();
            iniciar.await();   // se a entrada fechar antes, vigiarEntrada encerra o processo

            long inicio = System.nanoTime();
            AtomicLong ultimaAtividade = new AtomicLong(inicio);
            String tagConsumo = ch.basicConsume(fila, false, argumentos, (tag, d) -> {
                long agora = System.nanoTime();
                Map<String, Object> h = d.getProperties().getHeaders();
                Object s   = h == null ? null : h.get("sequencia");
                Object off = h == null ? null : h.get("x-stream-offset");   // o broker acrescenta em cada entrega do stream
                long offset = off == null ? -1 : ((Number) off).longValue();
                recebidas.incrementAndGet();
                // o stream entrega a partir do início do bloco: a aplicação descarta o que já processou
                boolean depoisDaPosicao = offMin < 0 || offset >= offMin;
                if (s != null && depoisDaPosicao && ((Number) s).intValue() >= seqMinima) {
                    int seq = ((Number) s).intValue();
                    if (primeira[0] < 0) {
                        primeira[0] = seq;
                        tPrimeira.set(agora);
                    } else if (seq != ultima[0] + 1) {
                        emOrdem[0] = false;
                    }
                    ultima[0] = seq;
                    aceitas.incrementAndGet();
                    tUltima.set(agora);
                }
                if (offset >= 0) ultimoOffset.set(offset);
                ultimaAtividade.set(agora);
                long dt = d.getEnvelope().getDeliveryTag();
                ultimaTag.set(dt);
                if (dt % ACK_A_CADA == 0) {
                    ch.basicAck(dt, true);   // ACK em lote
                    ultimaConfirmada.set(dt);
                }
            }, tag -> { });

            while (true) {
                Thread.sleep(20);
                long agora = System.nanoTime();
                if (esperado > 0 && aceitas.get() >= esperado) {
                    Thread.sleep(500);   // confere se não chega nada além do esperado
                    break;
                }
                if (esperado == 0 && (agora - ultimaAtividade.get()) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.printf("Tempo máximo atingido: %d/%d%n", aceitas.get(), esperado);
                    break;
                }
            }

            ch.basicCancel(tagConsumo);
            // confirma o restante (as que ficaram depois do último múltiplo de 100)
            if (ultimaTag.get() > ultimaConfirmada.get()) ch.basicAck(ultimaTag.get(), true);
            if (modo.equals("normal-stream")) {
                // o "commit" da aplicação: a posição fica num arquivo, não no broker
                Files.writeString(Path.of(parametro), String.valueOf(ultimoOffset.get()));
            }

            System.out.println("RECEBIDAS=" + recebidas.get());
            System.out.println("ACEITAS=" + aceitas.get());
            System.out.println("DESCARTADAS=" + (recebidas.get() - aceitas.get()));
            System.out.println("PRIMEIRA_SEQ=" + primeira[0]);
            System.out.println("ULTIMA_SEQ=" + ultima[0]);
            System.out.println("EM_ORDEM=" + emOrdem[0]);
            System.out.println("T_PRIMEIRA_MS=" + String.format(Locale.US, "%.2f",
                    tPrimeira.get() != SEM_VALOR ? (tPrimeira.get() - inicio) / 1e6 : -1.0));
            System.out.println("DURACAO_MS=" + String.format(Locale.US, "%.2f",
                    tUltima.get() != SEM_VALOR ? (tUltima.get() - inicio) / 1e6 : -1.0));
        }
    }
}