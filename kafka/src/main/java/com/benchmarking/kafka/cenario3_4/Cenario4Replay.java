package com.benchmarking.kafka.cenario3_4;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível avançado — Cenário 4: Replay / reprocessamento (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: mostrar e medir a capacidade de RELER mensagens já consumidas.
 * No Kafka a mensagem continua no log depois de lida (até a retenção); o
 * consumidor só guarda uma posição (offset). Reler é mudar essa posição, em
 * qualquer tópico, sem decisão prévia. Numa fila clássica do RabbitMQ a
 * mensagem é apagada após o ack, então não há o que reler (o RabbitMQ só
 * oferece isso com Streams, declarados antes da publicação).
 *
 * Roda no broker único (bootstrap padrão), tópico com 1 partição e
 * message.timestamp.type = LogAppendTime (o broker carimba a hora, como no
 * stream do RabbitMQ).
 *
 * Cada execução:
 *   1. recria o tópico e publica N = 50.000 mensagens, com uma pausa de 5 s
 *      no meio (ver ProdutorReplay), como na versão RabbitMQ;
 *   2. consumo normal com o grupo G, que lê tudo e faz commit;
 *   3. os modos de releitura, em ordem rotativa:
 *        grupo-existente  — mesmo grupo G       → espera 0 (offset guardado pelo broker)
 *                           (par do "posicao-salva" do RabbitMQ)
 *        inicio           — seekToBeginning     → espera seq 1..N
 *        timestamp        — offsetsForTimes(meio da pausa) → espera seq N/2+1..N
 *        offset           — seek(N − 1000)      → espera as últimas 1.000
 *        novo-grupo       — grupo novo, earliest → espera seq 1..N (sem par no RabbitMQ:
 *                           lá, um consumidor novo com "first" é o próprio modo inicio)
 *
 * Na comparação do tempo até a 1ª mensagem, use só inicio, timestamp e offset
 * (assign, sem grupo): nos modos com grupo, esse tempo inclui a entrada no grupo.
 *
 * Métricas: recebidas, primeira e última sequência, em ordem, correto, tempo até
 * a primeira mensagem, duração e vazão de releitura.
 *
 * Proteção contra órfãos: matarFilhosAoEncerrar no orquestrador e
 * vigiarEntrada em cada produtor e consumidor.
 *
 * Arquivo gerado em PASTA_RESULTADOS:
 *   resultados-kafka-replay.csv (1 linha por execução e modo)
 */
@Component
public class Cenario4Replay {

    private static final String TOPICO = "avancado-replay";
    private static final String PASTA_RESULTADOS = "resultado_cenario_3_4";
    private static final String CSV = "resultados-kafka-replay.csv";
    private static final int N = 50_000;
    private static final int ULTIMAS = 1_000;
    private static final long PAUSA_MS = 5_000;
    private static final long TIMEOUT_S = 300;
    private static final String TIPO_TIMESTAMP = "LogAppendTime";
    private static final String[] MODOS = {"grupo-existente", "inicio", "timestamp", "offset", "novo-grupo"};

    private static final String PRODUTOR   = "com.benchmarking.kafka.cenario3_4.ProdutorReplay";
    private static final String CONSUMIDOR = "com.benchmarking.kafka.cenario3_4.ConsumidorReplay";

    @Value("${benchmarking.execucoes}") private int totalExecucoes;
    @Value("${benchmarking.bootstrap-servers}") private String bootstrap;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    /** Um modo de leitura: parâmetro passado ao consumidor e o intervalo de sequências esperado. */
    private record Leitura(String modo, String parametro, int esperadas, int primeira, int ultima) {}

    public void executar() throws Exception {
        // Ctrl+C ou fim normal da JVM: mata produtor e consumidor ainda vivos
        Thread gancho = Util.matarFilhosAoEncerrar("replay-kafka-filhos");
        try {
            executarCampanha();
        } finally {
            Util.removerGancho(gancho);
        }
    }

    private void executarCampanha() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 4 Kafka — Replay");
        System.out.println("====================================================================");

        Util.pasta(PASTA_RESULTADOS);
        String csv = PASTA_RESULTADOS + File.separator + CSV;
        Util.apagarSeExistir(csv);

        try (PrintWriter w = new PrintWriter(new FileWriter(csv))) {
            w.println("execucao,ordem,modo,parametro,esperadas,recebidas,primeira_seq,ultima_seq,em_ordem,correto," +
                    "t_primeira_ms,duracao_ms,vazao_msg_s");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                System.out.printf("%nExecução %d/%d%n", exec, totalExecucoes);
                String grupo = "replay-G-" + exec + "-" + System.nanoTime();

                try (AdminClient admin = Util.admin(bootstrap)) {
                    Util.recriarTopicos(admin, List.of(new NewTopic(TOPICO, 1, (short) 1)
                            .configs(Map.of("message.timestamp.type", TIPO_TIMESTAMP))));
                    conferirTipoTimestamp(admin);
                }
                Thread.sleep(1_000);
                Process p = Util.iniciar(javaCmd, classpath, PRODUTOR, bootstrap, TOPICO, String.valueOf(N),
                        String.valueOf(PAUSA_MS));
                Map<String, String> sp = new HashMap<>();
                Util.ler(p, "PRODUTOR", sp, null, null);
                Util.esperar(p, "produtor", 0, TIMEOUT_S);
                long meio = Long.parseLong(Util.obrigatorio(sp, "MEIO_MS"));
                // meio da pausa, em ms: tolera até ~2,5 s de diferença entre o relógio do host e o do broker
                long tsMs = meio + PAUSA_MS / 2;

                registrar(w, exec, 0, ler(new Leitura("normal", grupo, N, 1, N)));

                List<Leitura> modos = new ArrayList<>();
                for (String m : MODOS) {
                    modos.add(switch (m) {
                        case "grupo-existente" -> new Leitura(m, grupo, 0, -1, -1);
                        case "inicio"          -> new Leitura(m, "-", N, 1, N);
                        case "timestamp"       -> new Leitura(m, String.valueOf(tsMs), N / 2, N / 2 + 1, N);
                        case "offset"          -> new Leitura(m, String.valueOf(N - ULTIMAS), ULTIMAS, N - ULTIMAS + 1, N);
                        case "novo-grupo"      -> new Leitura(m, "replay-novo-" + exec + "-" + System.nanoTime(), N, 1, N);
                        default -> throw new IllegalArgumentException(m);
                    });
                }
                for (int pos = 0; pos < modos.size(); pos++) {
                    Leitura l = modos.get((pos + exec - 1) % modos.size());
                    registrar(w, exec, pos + 1, ler(l));
                    Thread.sleep(1_000);
                }
                w.flush();
            }
        }
        System.out.println();
        System.out.println("[SUCESSO] Avançado — Cenário 4 Kafka concluído.");
        System.out.println("  " + new File(csv).getAbsolutePath());
    }

    /**
     * Confere que o tópico ficou com message.timestamp.type = LogAppendTime.
     * Sem isso, a releitura por tempo usaria a hora do produtor, e o método
     * deixaria de ser o mesmo da versão RabbitMQ. Tenta por até 10 s, porque
     * logo depois da criação o broker pode ainda não responder pelo tópico.
     */
    private void conferirTipoTimestamp(AdminClient admin) throws Exception {
        ConfigResource rt = new ConfigResource(ConfigResource.Type.TOPIC, TOPICO);
        long limite = System.currentTimeMillis() + 10_000;
        String atual = null;
        while (System.currentTimeMillis() < limite) {
            try {
                Config cfg = admin.describeConfigs(List.of(rt)).all().get(5, TimeUnit.SECONDS).get(rt);
                ConfigEntry e = cfg.get("message.timestamp.type");
                atual = e == null ? null : e.value();
                if (TIPO_TIMESTAMP.equals(atual)) return;
            } catch (ExecutionException | TimeoutException ignorada) {
                // tópico ainda não propagado
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("O tópico " + TOPICO + " ficou com message.timestamp.type=" + atual
                + " (esperado " + TIPO_TIMESTAMP + ").");
    }

    private record Saida(Leitura l, int recebidas, int primeira, int ultima, boolean emOrdem, double tPrimeira, double duracao) {
        boolean correto() {
            return recebidas == l.esperadas && primeira == l.primeira && ultima == l.ultima && emOrdem;
        }
    }

    private Saida ler(Leitura l) throws Exception {
        System.out.printf("  Modo %s (parâmetro %s)%n", l.modo, l.parametro);
        ExecutorService ex = Executors.newSingleThreadExecutor();
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> s = new ConcurrentHashMap<>();
        Process c = Util.iniciar(javaCmd, classpath, CONSUMIDOR, bootstrap, TOPICO, l.modo, l.parametro,
                String.valueOf(l.esperadas));
        try {
            Future<?> leitor = Util.lerEmSegundoPlano(ex, c, "CONSUMIDOR", s, pronto, null);
            if (!pronto.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");
            Util.enviar(c, "INICIAR");
            Util.esperar(c, "consumidor", 0, TIMEOUT_S);
            leitor.get();
            return new Saida(l, Integer.parseInt(Util.obrigatorio(s, "RECEBIDAS")),
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
        double vazao = r.recebidas > 1 && janela > 0 ? (r.recebidas - 1) / janela : 0;
        w.printf(Locale.US, "%d,%d,%s,%s,%d,%d,%d,%d,%s,%s,%.2f,%.2f,%.0f%n", exec, ordem, r.l.modo, r.l.parametro,
                r.l.esperadas, r.recebidas, r.primeira, r.ultima, r.emOrdem, r.correto(), r.tPrimeira, r.duracao, vazao);
        System.out.printf(Locale.US, "  → %s: %d/%d (seq %d..%d) | 1ª em %.0f ms | %.0f msg/s | correto: %s%n",
                r.l.modo, r.recebidas, r.l.esperadas, r.primeira, r.ultima, r.tPrimeira, vazao, r.correto());
        if (!r.correto()) throw new IllegalStateException("Releitura no modo " + r.l.modo + " não devolveu o esperado.");
    }
}