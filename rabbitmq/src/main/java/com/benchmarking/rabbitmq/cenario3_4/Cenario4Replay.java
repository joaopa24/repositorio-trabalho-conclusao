package com.benchmarking.rabbitmq.cenario3_4;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível avançado — Cenário 4: Replay / reprocessamento (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: mostrar e medir a capacidade de RELER mensagens já consumidas.
 * Na fila clássica, o ack apaga a mensagem: não há releitura. O RabbitMQ
 * oferece releitura com STREAMS (x-queue-type=stream, desde a 3.9): um log
 * só de acréscimo, como a partição do Kafka, lido a partir de uma posição
 * (x-stream-offset). O cenário mostra as duas coisas.
 *
 * Diferença de projeto em relação ao Kafka: para poder reler, a mensagem
 * precisa ter sido publicada num stream desde o início (aqui, pela exchange
 * fanout ligada às duas filas). No Kafka, todo tópico é um log e pode ser
 * relido dentro da retenção, sem nenhuma decisão prévia.
 *
 * Roda no broker único (5672). Cada execução:
 *   1. recria a exchange fanout, a fila clássica e o stream, ligados juntos;
 *      publica N = 50.000 mensagens (as duas filas recebem as mesmas), com uma
 *      pausa de 5 s no meio (ver ProdutorReplay);
 *   2. consumo normal da fila clássica (lê tudo e dá ack);
 *   3. consumo normal do stream (lê tudo e GRAVA o último offset num arquivo:
 *      pelo AMQP 0.9.1, quem guarda a posição é a aplicação);
 *   4. os modos, em ordem rotativa:
 *        classica-de-novo — fila clássica outra vez             → espera 0 (apagadas)
 *        posicao-salva    — stream a partir do offset gravado+1 → espera 0
 *                           (par do "grupo-existente" do Kafka)
 *        inicio           — stream "first"                      → espera seq 1..N
 *        timestamp        — stream a partir do meio da pausa    → espera N/2+1..N
 *        offset           — stream offset N − 1000              → espera as últimas 1.000
 *
 * O "novo-grupo" do Kafka não tem par aqui: um consumidor novo do stream com
 * "first" é o próprio modo inicio.
 *
 * Métricas: recebidas, aceitas, descartadas (entregues antes da posição pedida,
 * por causa dos blocos do stream), primeira/última sequência, em ordem,
 * correto, tempo até a 1ª, duração e vazão de releitura.
 *
 * Proteção contra órfãos: matarFilhosAoEncerrar no orquestrador e
 * vigiarEntrada em cada produtor e consumidor.
 *
 * Arquivo gerado em PASTA_RESULTADOS:
 *   resultados-rabbitmq-replay.csv (1 linha por execução e modo)
 */
@Component
public class Cenario4Replay {

    private static final String EXCHANGE = "replay.fanout";
    private static final String CLASSICA = "replay-classica";
    private static final String STREAM   = "replay-stream";
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_3_4";
    private static final String CSV = "resultados-rabbitmq-replay.csv";
    /** Arquivo onde o consumo normal do stream grava a posição (o "commit" da aplicação). */
    private static final String ARQ_POSICAO = PASTA_RESULTADOS + File.separator + "posicao-stream.txt";
    private static final int N = 50_000;
    private static final int ULTIMAS = 1_000;
    private static final long PAUSA_MS = 5_000;
    private static final long TIMEOUT_S = 300;
    private static final String[] MODOS = {"classica-de-novo", "posicao-salva", "inicio", "timestamp", "offset"};

    private static final String PRODUTOR   = "com.benchmarking.rabbitmq.cenario3_4.ProdutorReplay";
    private static final String CONSUMIDOR = "com.benchmarking.rabbitmq.cenario3_4.ConsumidorReplay";

    @Value("${benchmarking.execucoes}") private int totalExecucoes;
    @Value("${benchmarking.host:localhost}") private String host;
    @Value("${benchmarking.port:5672}")      private int porta;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private record Leitura(String modo, String fila, String parametro, int esperadas, int primeira, int ultima) {}

    private record Saida(Leitura l, int recebidas, int aceitas, int primeira, int ultima, boolean emOrdem,
                         double tPrimeira, double duracao) {
        boolean correto() {
            return aceitas == l.esperadas && primeira == l.primeira && ultima == l.ultima && emOrdem;
        }
    }

    public void executar() throws Exception {
        // Ctrl+C ou fim normal da JVM: mata produtor e consumidor ainda vivos
        Thread gancho = Util.matarFilhosAoEncerrar("replay-rabbitmq-filhos");
        try {
            executarCampanha();
        } finally {
            Util.removerGancho(gancho);
        }
    }

    private void executarCampanha() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 4 RabbitMQ — Replay");
        System.out.println("====================================================================");

        Util.pasta(PASTA_RESULTADOS);
        String csv = PASTA_RESULTADOS + File.separator + CSV;
        Util.apagarSeExistir(csv);

        try (PrintWriter w = new PrintWriter(new FileWriter(csv))) {
            w.println("execucao,ordem,modo,fila,parametro,esperadas,recebidas,aceitas,descartadas,primeira_seq,ultima_seq," +
                    "em_ordem,correto,t_primeira_ms,duracao_ms,vazao_msg_s");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                System.out.printf("%nExecução %d/%d%n", exec, totalExecucoes);
                preparar();
                Util.apagarSeExistir(ARQ_POSICAO);   // a posição de uma execução não vale para a seguinte
                Thread.sleep(1_000);
                Process p = Util.iniciar(javaCmd, classpath, PRODUTOR, host, String.valueOf(porta), EXCHANGE,
                        String.valueOf(N), String.valueOf(PAUSA_MS));
                Map<String, String> sp = new HashMap<>();
                Util.ler(p, "PRODUTOR", sp, null, null);
                Util.esperar(p, "produtor", 0, TIMEOUT_S);
                long meio = Long.parseLong(Util.obrigatorio(sp, "MEIO_MS"));
                // meio da pausa, em segundos: tolera até ~2 s de diferença entre o relógio do host e o do broker
                long tsSegundos = Math.round((meio + PAUSA_MS / 2.0) / 1000.0);

                // consumo "de produção": fila clássica e stream
                registrar(w, exec, 0, ler(new Leitura("classica", CLASSICA, "-", N, 1, N)));
                registrar(w, exec, 0, ler(new Leitura("normal-stream", STREAM, ARQ_POSICAO, N, 1, N)));

                List<Leitura> modos = new ArrayList<>();
                for (String m : MODOS) {
                    modos.add(switch (m) {
                        case "classica-de-novo" -> new Leitura(m, CLASSICA, "-", 0, -1, -1);
                        case "posicao-salva"    -> new Leitura(m, STREAM, ARQ_POSICAO, 0, -1, -1);
                        case "inicio"           -> new Leitura(m, STREAM, "first", N, 1, N);
                        case "timestamp"        -> new Leitura(m, STREAM, String.valueOf(tsSegundos), N / 2, N / 2 + 1, N);
                        case "offset"           -> new Leitura(m, STREAM, String.valueOf(N - ULTIMAS), ULTIMAS, N - ULTIMAS + 1, N);
                        default -> throw new IllegalArgumentException(m);
                    });
                }
                for (int pos = 0; pos < modos.size(); pos++) {
                    registrar(w, exec, pos + 1, ler(modos.get((pos + exec - 1) % modos.size())));
                    Thread.sleep(1_000);
                }
                w.flush();
            }
        }
        System.out.println();
        System.out.println("[SUCESSO] Avançado — Cenário 4 RabbitMQ concluído.");
        System.out.println("  " + new File(csv).getAbsolutePath());
    }

    private void preparar() throws Exception {
        try (Connection conn = Util.fabrica(host, porta).newConnection(); Channel ch = conn.createChannel()) {
            ch.exchangeDelete(EXCHANGE);
            ch.queueDelete(CLASSICA);
            ch.queueDelete(STREAM);
            ch.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT, true);
            ch.queueDeclare(CLASSICA, true, false, false, null);
            ch.queueDeclare(STREAM, true, false, false, Map.of("x-queue-type", "stream"));
            ch.queueBind(CLASSICA, EXCHANGE, "");
            ch.queueBind(STREAM, EXCHANGE, "");
        }
    }

    private Saida ler(Leitura l) throws Exception {
        System.out.printf("  Modo %s (%s, parâmetro %s)%n", l.modo, l.fila, l.parametro);
        int seqMinima = l.primeira > 0 ? l.primeira : 1;
        ExecutorService ex = Executors.newSingleThreadExecutor();
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> s = new ConcurrentHashMap<>();
        Process c = Util.iniciar(javaCmd, classpath, CONSUMIDOR, host, String.valueOf(porta), l.fila, l.modo, l.parametro,
                String.valueOf(seqMinima), String.valueOf(l.esperadas));
        try {
            Future<?> leitor = Util.lerEmSegundoPlano(ex, c, "CONSUMIDOR", s, pronto, null);
            if (!pronto.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");
            Util.enviar(c, "INICIAR");
            Util.esperar(c, "consumidor", 0, TIMEOUT_S);
            leitor.get();
            return new Saida(l, Integer.parseInt(Util.obrigatorio(s, "RECEBIDAS")), Integer.parseInt(Util.obrigatorio(s, "ACEITAS")),
                    Integer.parseInt(Util.obrigatorio(s, "PRIMEIRA_SEQ")), Integer.parseInt(Util.obrigatorio(s, "ULTIMA_SEQ")),
                    Boolean.parseBoolean(Util.obrigatorio(s, "EM_ORDEM")),
                    Double.parseDouble(Util.obrigatorio(s, "T_PRIMEIRA_MS")), Double.parseDouble(Util.obrigatorio(s, "DURACAO_MS")));
        } finally {
            Util.matar(c);
            ex.shutdownNow();
        }
    }

    private void registrar(PrintWriter w, int exec, int ordem, Saida r) {
        double janela = (r.duracao - r.tPrimeira) / 1000.0;
        double vazao = r.aceitas > 1 && janela > 0 ? (r.aceitas - 1) / janela : 0;
        w.printf(Locale.US, "%d,%d,%s,%s,%s,%d,%d,%d,%d,%d,%d,%s,%s,%.2f,%.2f,%.0f%n", exec, ordem, r.l.modo, r.l.fila,
                r.l.parametro, r.l.esperadas, r.recebidas, r.aceitas, r.recebidas - r.aceitas, r.primeira, r.ultima,
                r.emOrdem, r.correto(), r.tPrimeira, r.duracao, vazao);
        System.out.printf(Locale.US, "  → %s: %d/%d (seq %d..%d, descartadas %d) | 1ª em %.0f ms | %.0f msg/s | correto: %s%n",
                r.l.modo, r.aceitas, r.l.esperadas, r.primeira, r.ultima, r.recebidas - r.aceitas, r.tPrimeira, vazao, r.correto());
        if (!r.correto()) throw new IllegalStateException("Releitura no modo " + r.l.modo + " não devolveu o esperado.");
    }
}