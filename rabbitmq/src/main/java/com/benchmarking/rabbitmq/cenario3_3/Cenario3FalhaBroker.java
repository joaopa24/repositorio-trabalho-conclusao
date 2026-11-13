package com.benchmarking.rabbitmq.cenario3_3;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Nível avançado — Cenário 3: Falha de broker (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: ver o que acontece com as mensagens quando o nó LÍDER da quorum
 * queue cai no meio do envio: quantas confirmadas se perdem, quantas chegam
 * duplicadas e por quanto tempo o fluxo para até um novo líder assumir.
 *
 * Precisa do CLUSTER de 3 nós (docker-compose-cluster-rabbitmq.yml) e do
 * comando "docker" no PATH.
 *
 * Fila: quorum queue com 3 membros e nome NOVO a cada execução
 * (avancado-falha-broker-<modo>-e<n>-<ms>), criada por uma conexão com o nó 1
 * (x-queue-leader-locator=client-local → o líder fica no nó 1). Um produtor ou
 * consumidor de uma execução anterior não enxerga a fila nova: sem isso, um
 * produtor órfão misturaria sequências repetidas (contadas como duplicadas) e
 * um consumidor órfão, que reassina sozinho, dividiria as mensagens com o
 * consumidor da execução. A fila de cada execução é apagada no fim dela; as
 * que sobrarem de sessões interrompidas são apagadas no início.
 * Produtor conectado ao nó 2 e consumidor ao nó 3: a queda do líder não
 * derruba a conexão deles.
 *
 * Configurações (ordem alternada a cada execução):
 *   confirms     — publisher confirms              — par do Kafka "acks-all"
 *   sem-confirms — sem confirmação                 — par do Kafka "acks-0"
 * O "acks-1" do Kafka não tem par: a quorum queue não confirma só pelo líder.
 * A coluna "garantia" do CSV (replicas, nenhuma) indica os pares.
 *
 * Linha do tempo de cada execução (contada do início do envio), igual à do Kafka:
 *   0 s   produtor começa: 200 msg/s durante 60 s (12.000 mensagens)
 *   20 s  docker kill no container do líder (rabbit-1)
 *   40 s  docker start no mesmo container (volta como seguidor)
 *   60 s  fim do envio; o consumidor espera mais 10 s
 * Depois espera o cluster voltar a 3 nós e a fila a 3 membros online, e mais 20 s.
 *
 * Métricas por execução (CSV no MESMO formato da versão Kafka):
 *   confirmadas, perdidas (confirmadas que nunca chegaram), nao_entregues
 *   (enviadas que nunca chegaram — a única medida de perda sem confirms),
 *   duplicadas (entregas repetidas; aqui vêm das reentregas ao consumidor),
 *   redeliveries, lider_final (líder ao fim do envio), eleicao_ms, maior
 *   intervalo sem confirmação e maior pausa de entrega.
 *
 * Cenário de comportamento: 1 execução de aquecimento por configuração
 * (com queda e volta, conferida, mas fora dos CSVs) e 3 execuções válidas.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-rabbitmq-falha-broker.csv
 *   resultados-rabbitmq-falha-broker-serie.csv
 */
@Component
public class Cenario3FalhaBroker {

    private static final String PREFIXO_FILA = "avancado-falha-broker";
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_3_3";
    private static final String CSV       = "resultados-rabbitmq-falha-broker.csv";
    private static final String CSV_SERIE = "resultados-rabbitmq-falha-broker-serie.csv";

    /** Mesmo cabeçalho da versão Kafka. */
    static final String CABECALHO = "execucao,ordem,configuracao,garantia,idempotencia,enviadas,confirmadas," +
            "falhas_envio,sem_resposta,recebidas_unicas,entregas,duplicadas,redeliveries,perdidas,nao_entregues," +
            "cancelamentos,lider_derrubado,era_controlador,novo_lider,lider_final,t_queda_s,t_volta_s,eleicao_ms," +
            "maior_intervalo_sem_confirmacao_ms,maior_pausa_entrega_ms";
    static final String CABECALHO_SERIE = "execucao,configuracao,sequencia,envio_s,chegada_s,latencia_ms,duplicada,redelivered";

    /** nome no CSV (também é o argumento do produtor) e nível de garantia (para parear com o Kafka). */
    private record Config(String nome, String garantia) {
        boolean comConfirms() { return nome.equals("confirms"); }
    }
    private static final List<Config> CONFIGS = List.of(
            new Config("confirms", "replicas"),
            new Config("sem-confirms", "nenhuma"));

    private static final int TAXA = 200;
    private static final int DURACAO_S = 60;
    private static final int MSGS = TAXA * DURACAO_S;
    private static final long QUEDA_S = 20;
    private static final long VOLTA_S = 40;
    private static final int EXECUCOES = 3;
    private static final long TIMEOUT_S = 400;
    private static final long ESPERA_ESTAVEL_MS = 20_000;

    private static final String PRODUTOR   = "com.benchmarking.rabbitmq.cenario3_3.ProdutorFalhaBroker";
    private static final String CONSUMIDOR = "com.benchmarking.rabbitmq.cenario3_3.ConsumidorFalhaBroker";

    @Value("${benchmarking.cluster-host:localhost}")      private String host;
    @Value("${benchmarking.cluster-porta-1:5681}")        private int porta1;
    @Value("${benchmarking.cluster-porta-2:5682}")        private int porta2;
    @Value("${benchmarking.cluster-porta-3:5683}")        private int porta3;
    /** Management API de um nó que nunca é derrubado (nó 2). */
    @Value("${benchmarking.cluster-management-2:15682}")  private int portaManagement;
    @Value("${benchmarking.management-user:guest}")       private String usuario;
    @Value("${benchmarking.management-pass:guest}")       private String senha;
    /** Containers rabbit-1, rabbit-2, rabbit-3; nós rabbit@rabbit-1, ... */
    @Value("${benchmarking.prefixo-container-rabbit:rabbit-}") private String prefixoContainer;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 3 RabbitMQ — Falha de broker");
        System.out.println("====================================================================");

        Thread gancho = Util.matarFilhosAoEncerrar("mata-subprocessos-falha-broker");
        try {
            int nos = Util.nosRodando(host, portaManagement, usuario, senha);
            System.out.println("Nós rodando no cluster: " + nos);
            if (nos < 3) throw new IllegalStateException("Precisa do cluster de 3 nós (docker-compose-cluster-rabbitmq.yml). Encontrados: " + nos);
            System.out.println("Docker: " + Util.rodarComando(30, "docker", "--version"));
            apagarFilasAntigas();

            File pasta = Util.pasta(PASTA_RESULTADOS);
            String csv = PASTA_RESULTADOS + File.separator + CSV;
            String csvSerie = PASTA_RESULTADOS + File.separator + CSV_SERIE;
            Util.apagarSeExistir(csv);
            Util.apagarSeExistir(csvSerie);

            try (PrintWriter w = new PrintWriter(new FileWriter(csv));
                 PrintWriter ws = new PrintWriter(new BufferedWriter(new FileWriter(csvSerie)))) {
                w.println(CABECALHO);
                ws.println(CABECALHO_SERIE);
                for (Config c : CONFIGS) {
                    System.out.printf("%nAquecimento (descartado) — %s%n", c.nome);
                    rodar(c, 0, 0, pasta, null, null);
                }
                for (int exec = 1; exec <= EXECUCOES; exec++) {
                    for (int pos = 0; pos < CONFIGS.size(); pos++) {
                        Config c = CONFIGS.get((pos + exec - 1) % CONFIGS.size());
                        System.out.printf("%nExecução %d/%d — %s (posição %d)%n", exec, EXECUCOES, c.nome, pos + 1);
                        rodar(c, exec, pos + 1, pasta, w, ws);
                        w.flush();
                        ws.flush();
                    }
                }
            }
            System.out.println();
            System.out.println("[SUCESSO] Avançado — Cenário 3 RabbitMQ concluído.");
            System.out.println("  " + new File(csv).getAbsolutePath());
            System.out.println("  " + new File(csvSerie).getAbsolutePath());
        } finally {
            Util.removerGancho(gancho);
        }
    }

    private String queueJson(String fila) {
        return Util.http(host, portaManagement, usuario, senha, "/api/queues/%2F/" + URLEncoder.encode(fila, StandardCharsets.UTF_8));
    }

    /** Cria a fila da execução pelo nó 1 e espera 3 membros online com o líder no nó 1. */
    private String prepararFila(String fila) throws Exception {
        String esperado = "rabbit@" + prefixoContainer + "1";
        for (int tentativa = 1; tentativa <= 5; tentativa++) {
            try (Connection conn = Util.conectar(host, porta1, 120); Channel ch = conn.createChannel()) {
                ch.queueDelete(fila);
                ch.queueDeclare(fila, true, false, false, Map.of("x-queue-type", "quorum",
                        "x-quorum-initial-group-size", 3, "x-queue-leader-locator", "client-local"));
            }
            long limite = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < limite) {
                String j = queueJson(fila);
                String lider = Util.textoJson(j, "leader");
                if (Util.listaJson(j, "online").size() == 3 && lider != null) {
                    if (lider.equals(esperado)) return lider;
                    break;   // líder em outro nó: recria
                }
                Thread.sleep(250);
            }
            System.out.println("  líder fora do nó 1; recriando a fila (tentativa " + tentativa + ")");
        }
        throw new IllegalStateException("Não consegui criar a fila com o líder em " + esperado + " e 3 membros online.");
    }

    private static boolean ehFilaDoCenario(String nome) {
        return nome.equals(PREFIXO_FILA) || nome.startsWith(PREFIXO_FILA + "-");
    }

    /**
     * Apaga as filas deste cenário que sobraram de sessões anteriores (inclusive
     * a de nome fixo das versões antigas), listando pela management API do nó 2.
     */
    private void apagarFilasAntigas() {
        Set<String> antigas = new LinkedHashSet<>();
        antigas.add(PREFIXO_FILA);
        String j = Util.http(host, portaManagement, usuario, senha, "/api/queues/%2F?columns=name");
        if (j != null) {
            Matcher m = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]*)\"").matcher(j);
            while (m.find()) if (ehFilaDoCenario(m.group(1))) antigas.add(m.group(1));
        } else {
            System.out.println("  [AVISO] management API sem resposta: só a fila de nome fixo das versões antigas será apagada.");
        }
        if (antigas.size() > 1) System.out.println("Apagando filas de sessões anteriores: " + antigas);
        for (String f : antigas) apagarFila(f);
    }

    /** Apaga uma fila pelo nó 2 (que nunca cai). Falha vira aviso; a fila é apagada no próximo início. */
    private void apagarFila(String fila) {
        try (Connection conn = Util.conectar(host, porta2, 30); Channel ch = conn.createChannel()) {
            ch.queueDelete(fila);
        } catch (Exception e) {
            String msg = String.valueOf(e.getMessage());
            if (!msg.contains("NOT_FOUND") && !msg.contains("404")) {
                System.out.println("  [AVISO] não foi possível apagar a fila " + fila + ": " + msg);
            }
        }
    }

    private void procurarNovoLider(String fila, String antigo, long limiteNs, String[] novo, long[] t) throws InterruptedException {
        while (System.nanoTime() < limiteNs) {
            String l = Util.textoJson(queueJson(fila), "leader");
            if (l != null && !l.isBlank() && !l.equals(antigo)) {
                novo[0] = l;
                t[0] = System.nanoTime();
                return;
            }
            Thread.sleep(100);
        }
    }

    /** Os 3 nós aceitam conexão AMQP (o que voltou da queda pode demorar mais que a management API). */
    private boolean aceitaConexoes() {
        for (int p : new int[]{porta1, porta2, porta3}) {
            try {
                Util.fabrica(host, p).newConnection().close();
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private void esperarClusterSaudavel(String fila) throws Exception {
        long limite = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < limite) {
            if (Util.nosRodando(host, portaManagement, usuario, senha) == 3
                    && Util.listaJson(queueJson(fila), "online").size() == 3
                    && aceitaConexoes()) {
                System.out.println("  cluster estável; aguardando " + ESPERA_ESTAVEL_MS / 1000 + " s antes da próxima execução");
                Thread.sleep(ESPERA_ESTAVEL_MS);
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("O cluster não voltou a 3 nós / 3 membros online em 3 min.");
    }

    /** w == null: aquecimento (executa e confere tudo, mas não grava). */
    private void rodar(Config c, int exec, int ordem, File pasta, PrintWriter w, PrintWriter ws) throws Exception {
        File arqConf = new File(pasta, "temp-confirmadas.txt");
        File arqRec  = new File(pasta, "temp-recebidas.txt");
        Util.apagarSeExistir(arqConf.getPath());
        Util.apagarSeExistir(arqRec.getPath());

        String fila = PREFIXO_FILA + "-" + c.nome + "-e" + exec + "-" + System.currentTimeMillis();
        System.out.println("  fila: " + fila);
        String lider = prepararFila(fila);
        String container = lider.substring(lider.indexOf('@') + 1);

        ExecutorService ex = Executors.newFixedThreadPool(2);
        CountDownLatch prontoC = new CountDownLatch(1);
        CountDownLatch prontoP = new CountDownLatch(1);
        Map<String, String> sc = new ConcurrentHashMap<>();
        Map<String, String> sp = new ConcurrentHashMap<>();
        Process consumidor = null, produtor = null;
        String containerDerrubado = null;
        boolean filaApagada = false;
        try {
            consumidor = Util.iniciar(javaCmd, classpath, CONSUMIDOR, host, String.valueOf(porta3), fila, arqRec.getAbsolutePath());
            Future<?> lc = Util.lerEmSegundoPlano(ex, consumidor, "CONSUMIDOR", sc, prontoC, null);
            if (!prontoC.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");

            produtor = Util.iniciar(javaCmd, classpath, PRODUTOR, host, String.valueOf(porta2), fila, String.valueOf(MSGS),
                    String.valueOf(TAXA), c.nome, arqConf.getAbsolutePath());
            Future<?> lp = Util.lerEmSegundoPlano(ex, produtor, "PRODUTOR", sp, prontoP, Set.of("ENVIADAS"));
            if (!prontoP.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Produtor não ficou pronto.");
            long t0 = Long.parseLong(Util.obrigatorio(sp, "T0"));

            // --- queda do líder ---
            Util.aguardarAte(t0 + QUEDA_S * 1_000_000_000L);
            long tQueda = System.nanoTime();
            containerDerrubado = container;
            Util.rodarComando(60, "docker", "kill", container);
            System.out.println("  >>> docker kill " + container + " (líder " + lider + ")");

            // procura o novo líder só até a hora da volta: a volta acontece SEMPRE aos 40 s
            long tVoltaAlvo = t0 + VOLTA_S * 1_000_000_000L;
            String[] novo = {""};
            long[] tEl = {-1};
            procurarNovoLider(fila, lider, tVoltaAlvo, novo, tEl);

            // --- volta do nó ---
            Util.aguardarAte(tVoltaAlvo);
            long tVolta = System.nanoTime();
            Util.rodarComando(60, "docker", "start", container);
            containerDerrubado = null;
            System.out.println("  >>> docker start " + container);
            if (tEl[0] < 0) procurarNovoLider(fila, lider, tQueda + 120_000_000_000L, novo, tEl);
            String novoLider = novo[0];
            long tEleicao = tEl[0];
            System.out.printf(Locale.US, "  >>> novo líder: %s (%.0f ms após a queda)%n", novoLider,
                    tEleicao > 0 ? (tEleicao - tQueda) / 1e6 : -1.0);

            Util.esperar(produtor, "produtor", 0, TIMEOUT_S);
            lp.get();
            String liderFinal = Util.textoJson(queueJson(fila), "leader");
            Util.enviar(consumidor, "PRODUTOR_TERMINOU");
            Util.esperar(consumidor, "consumidor", 0, TIMEOUT_S);
            lc.get();

            // --- auditoria ---
            boolean comConfirms = c.comConfirms();
            Set<Integer> confirmadas = new HashSet<>();
            for (String l : Files.readAllLines(arqConf.toPath())) if (!l.isBlank()) confirmadas.add(Integer.parseInt(l.trim()));
            if (comConfirms && confirmadas.size() != Integer.parseInt(Util.obrigatorio(sp, "CONFIRMADAS"))) {
                throw new IllegalStateException("Arquivo de confirmadas não bate com o produtor");
            }
            Set<Integer> unicas = new HashSet<>();
            int entregas = 0, duplicadas = 0, redeliveries = 0;
            long chegadaAnterior = 0, maiorPausa = 0;
            List<String> serie = new ArrayList<>();
            for (String l : Files.readAllLines(arqRec.toPath())) {
                if (l.isBlank()) continue;
                String[] p = l.split(";");
                int seq = Integer.parseInt(p[0]);
                long envio = Long.parseLong(p[1]);
                long chegada = Long.parseLong(p[2]);
                boolean redelivered = Boolean.parseBoolean(p[3]);
                boolean dup = !unicas.add(seq);
                if (dup) duplicadas++;
                if (redelivered) redeliveries++;
                entregas++;
                if (chegadaAnterior > 0) maiorPausa = Math.max(maiorPausa, chegada - chegadaAnterior);
                chegadaAnterior = chegada;
                serie.add(String.format(Locale.US, "%d,%s,%d,%.4f,%.4f,%.3f,%s,%s", exec, c.nome, seq, (envio - t0) / 1e9,
                        (chegada - t0) / 1e9, (chegada - envio) / 1e6, dup, redelivered));
            }
            if (entregas != Integer.parseInt(Util.obrigatorio(sc, "RECEBIDAS"))) {
                throw new IllegalStateException("Arquivo de recebidas não bate com o consumidor");
            }
            for (int s : unicas) {
                if (s < 1 || s > MSGS) throw new IllegalStateException("Sequência fora do intervalo na fila: " + s);
            }
            int perdidas = -1;
            if (comConfirms) {
                perdidas = 0;
                for (int s : confirmadas) if (!unicas.contains(s)) perdidas++;
            }
            int naoEntregues = MSGS - unicas.size();

            if (w != null) {
                w.printf(Locale.US, "%d,%d,%s,%s,%s,%d,%d,%s,%s,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%.3f,%.3f,%.1f,%s,%.1f%n",
                        exec, ordem, c.nome, c.garantia, false, MSGS, comConfirms ? confirmadas.size() : -1,
                        Util.obrigatorio(sp, "FALHAS"), Util.obrigatorio(sp, "SEM_RESPOSTA"), unicas.size(), entregas,
                        duplicadas, redeliveries, perdidas, naoEntregues, Util.obrigatorio(sc, "CANCELAMENTOS"),
                        lider, "na", novoLider, liderFinal == null ? "" : liderFinal,
                        (tQueda - t0) / 1e9, (tVolta - t0) / 1e9, tEleicao > 0 ? (tEleicao - tQueda) / 1e6 : -1.0,
                        Util.obrigatorio(sp, "MAIOR_INTERVALO_SEM_CONFIRMACAO_MS"), maiorPausa / 1e6);
                for (String l : serie) ws.println(l);
            }
            System.out.printf(Locale.US, "  → recebidas %d/%d | perdidas %s | não entregues %d | duplicadas %d | pausa %.0f ms%n",
                    unicas.size(), MSGS, perdidas < 0 ? "-" : String.valueOf(perdidas), naoEntregues, duplicadas, maiorPausa / 1e6);

            Util.apagarSeExistir(arqConf.getPath());
            Util.apagarSeExistir(arqRec.getPath());
            esperarClusterSaudavel(fila);
            apagarFila(fila);
            filaApagada = true;
        } finally {
            Util.matar(consumidor, produtor);
            ex.shutdownNow();
            if (containerDerrubado != null) {
                try {
                    Util.rodarComando(60, "docker", "start", containerDerrubado);
                } catch (Exception e) {
                    System.err.println("Não consegui religar " + containerDerrubado + ": " + e.getMessage());
                }
            }
            if (!filaApagada) apagarFila(fila);
        }
    }
}
