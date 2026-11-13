package com.benchmarking.kafka.cenario2_1;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível intermediário — Cenário 1: Comparação de garantias (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: medir quanto cada nível de garantia de entrega custa em latência
 * e em vazão. Nenhuma falha é provocada: o comportamento sob falha é
 * avaliado no Cenário 2 (Falha do Consumidor).
 *
 * Níveis (ver ProdutorGarantias e ConsumidorGarantias):
 *   at-most-once  : acks=0, retries=0 | commit ANTES de processar
 *   at-least-once : acks=all, idempotência desligada | commit DEPOIS
 *   exactly-once  : produtor transacional (commit a cada 100 ms) |
 *                   read_committed + commit DEPOIS + deduplicação
 *
 * Duas partes, cada uma medindo uma coisa só (mesmo formato do Cenário 3):
 *   Parte A — LATÊNCIA: 5.000 mensagens a 200 msg/s (taxa do Cenário 1
 *             básico), sem formar fila.
 *   Parte B — VAZÃO MÁXIMA: 20.000 mensagens sem controle de taxa; vazão de
 *             ponta a ponta (do primeiro envio até a última chegada).
 *
 * Os três níveis rodam intercalados, em ordem rotativa, depois de uma
 * execução de aquecimento de cada nível em cada parte. Métrica ausente,
 * código de saída diferente de 0 ou contagem errada interrompem a campanha.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-kafka-garantias-latencia.csv            (1 linha por execução e nível)
 *   resultados-kafka-garantias-latencia-detalhado.csv  (1 linha por mensagem)
 *   resultados-kafka-garantias-vazao.csv               (1 linha por execução e nível)
 */
@Component
public class Cenario1ComparacaoGarantias {

    private static final String TOPICO = "garantias-cenario1";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_cenario_2_1";

    private static final String CSV_LATENCIA  = "resultados-kafka-garantias-latencia.csv";
    private static final String CSV_DETALHADO = "resultados-kafka-garantias-latencia-detalhado.csv";
    private static final String CSV_VAZAO     = "resultados-kafka-garantias-vazao.csv";

    static final String[] GARANTIAS = {"at-most-once", "at-least-once", "exactly-once"};

    private static final int TAMANHO_BYTES = 1_024;
    private static final int MAX_POLL      = 100;   // igual ao prefetch do RabbitMQ

    // Parte A — latência
    private static final int MSGS_LATENCIA       = 5_000;
    private static final int TAXA_LATENCIA_MSG_S = 200;

    // Parte B — vazão máxima
    private static final int MSGS_VAZAO = 20_000;

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 3_000;
    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;
    private static final long TIMEOUT_PROCESSO_S = 360;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.kafka.cenario2_1.ConsumidorGarantias";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.kafka.cenario2_1.ProdutorGarantias";

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.bootstrap-servers}") private String bootstrapServers;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    /** Resultado de um par produtor/consumidor. */
    private record Resultado(long t0, long t1, long ultimaChegada, int duplicatas, int transacoes,
                             double media, double mediana, double min, double max) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Intermediário — Cenário 1 Kafka — Comparação de garantias");
        System.out.println("====================================================================");
        System.out.printf("Níveis: %s | Parte A: %d msgs a %d msg/s | Parte B: %d msgs sem controle%n",
                Arrays.toString(GARANTIAS), MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, MSGS_VAZAO);

        File pasta = new File(PASTA_RESULTADOS);
        if (!pasta.exists() && !pasta.mkdirs()) {
            throw new IOException("Não foi possível criar a pasta: " + pasta.getAbsolutePath());
        }
        System.out.println("Gravando em: " + pasta.getAbsolutePath());

        String csvLatencia  = PASTA_RESULTADOS + File.separator + CSV_LATENCIA;
        String csvDetalhado = PASTA_RESULTADOS + File.separator + CSV_DETALHADO;
        String csvVazao     = PASTA_RESULTADOS + File.separator + CSV_VAZAO;
        apagarSeExistir(csvLatencia);
        apagarSeExistir(csvDetalhado);
        apagarSeExistir(csvVazao);

        parteLatencia(csvLatencia, csvDetalhado);
        parteVazao(csvVazao);

        System.out.println();
        System.out.println("[SUCESSO] Intermediário — Cenário 1 Kafka concluído.");
        System.out.println("  " + new File(csvLatencia).getAbsolutePath());
        System.out.println("  " + new File(csvDetalhado).getAbsolutePath());
        System.out.println("  " + new File(csvVazao).getAbsolutePath());
    }

    // ------------------------------------------------------------------
    // Parte A — latência a taxa fixa
    // ------------------------------------------------------------------
    private void parteLatencia(String csvLatencia, String csvDetalhado) throws Exception {
        cabecalhoParte("PARTE A — LATÊNCIA (" + TAXA_LATENCIA_MSG_S + " msg/s, " + MSGS_LATENCIA + " mensagens)");

        for (String g : GARANTIAS) {
            System.out.printf("%nAquecimento (descartado) — %s%n", g);
            rodar(g, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, 0, "-");
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter csv = new PrintWriter(new FileWriter(csvLatencia))) {
            csv.println("execucao,ordem,garantia,num_msgs,taxa_alvo_msg_s,taxa_envio_msg_s," +
                    "latencia_media_ms,latencia_mediana_ms,latencia_min_ms,latencia_max_ms,duplicatas,transacoes");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                String[] ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.length; pos++) {
                    String g = ordem[pos];
                    System.out.printf("%n[A] Execução %d/%d — %s (posição %d)%n", exec, totalExecucoes, g, pos + 1);

                    Resultado r = rodar(g, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, exec, csvDetalhado);
                    double taxaEnvio = MSGS_LATENCIA / ((r.t1 - r.t0) / 1e9);

                    csv.printf(Locale.US, "%d,%d,%s,%d,%d,%.2f,%.4f,%.4f,%.4f,%.4f,%d,%d%n",
                            exec, pos + 1, g, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, taxaEnvio,
                            r.media, r.mediana, r.min, r.max, r.duplicatas, r.transacoes);
                    csv.flush();

                    System.out.printf(Locale.US, "  → média %.2f ms | mediana %.2f ms | envio %.1f msg/s%n",
                            r.media, r.mediana, taxaEnvio);
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Parte B — vazão máxima
    // ------------------------------------------------------------------
    private void parteVazao(String csvVazao) throws Exception {
        cabecalhoParte("PARTE B — VAZÃO MÁXIMA (" + MSGS_VAZAO + " mensagens, sem controle de taxa)");

        for (String g : GARANTIAS) {
            System.out.printf("%nAquecimento (descartado) — %s%n", g);
            rodar(g, MSGS_VAZAO, 0, 0, "-");
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter csv = new PrintWriter(new FileWriter(csvVazao))) {
            csv.println("execucao,ordem,garantia,num_msgs,duracao_s,vazao_msg_s,vazao_produtor_msg_s,duplicatas,transacoes");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                String[] ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.length; pos++) {
                    String g = ordem[pos];
                    System.out.printf("%n[B] Execução %d/%d — %s (posição %d)%n", exec, totalExecucoes, g, pos + 1);

                    Resultado r = rodar(g, MSGS_VAZAO, 0, exec, "-");

                    // ponta a ponta: do primeiro envio até a última chegada
                    double duracaoS = (r.ultimaChegada - r.t0) / 1e9;
                    if (duracaoS <= 0) {
                        throw new IllegalStateException("Duração não positiva na Parte B: " + duracaoS);
                    }
                    double vazao = MSGS_VAZAO / duracaoS;
                    double vazaoProdutor = MSGS_VAZAO / ((r.t1 - r.t0) / 1e9);

                    csv.printf(Locale.US, "%d,%d,%s,%d,%.4f,%.2f,%.2f,%d,%d%n",
                            exec, pos + 1, g, MSGS_VAZAO, duracaoS, vazao, vazaoProdutor, r.duplicatas, r.transacoes);
                    csv.flush();

                    System.out.printf(Locale.US, "  → %.0f msg/s (ponta a ponta)%n", vazao);
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }
    }

    /** Ordem rotativa: exec 1 → AMO, ALO, EO; exec 2 → ALO, EO, AMO; exec 3 → EO, AMO, ALO; ... */
    static String[] ordemDaExecucao(int exec) {
        int n = GARANTIAS.length;
        String[] ordem = new String[n];
        for (int i = 0; i < n; i++) ordem[i] = GARANTIAS[(i + exec - 1) % n];
        return ordem;
    }

    // ------------------------------------------------------------------
    // Um par produtor/consumidor
    // ------------------------------------------------------------------
    private Resultado rodar(String garantia, int numMsgs, int taxaMsgS, int exec, String csvDetalhado)
            throws Exception {
        recriarTopico();
        Thread.sleep(1000);

        Process procConsumidor = new ProcessBuilder(comando(CONSUMIDOR_CLASS,
                bootstrapServers, TOPICO, String.valueOf(numMsgs), String.valueOf(exec), garantia,
                csvDetalhado, String.valueOf(MAX_POLL)
        )).redirectErrorStream(true).start();

        // stdout do consumidor lido em thread própria desde o início
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> saidaConsumidor = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> leitor = executor.submit(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(procConsumidor.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    System.out.println("  [CONSUMIDOR] " + linha);
                    if (linha.contains("STATUS=PRONTO")) pronto.countDown();
                    guardarChaveValor(linha, saidaConsumidor);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        try {
            if (!pronto.await(TIMEOUT_CONSUMIDOR_PRONTO_S, TimeUnit.SECONDS)) {
                procConsumidor.destroyForcibly();
                throw new IllegalStateException("Timeout aguardando STATUS=PRONTO do consumidor.");
            }

            Process procProdutor = new ProcessBuilder(comando(PRODUTOR_CLASS,
                    bootstrapServers, TOPICO, String.valueOf(numMsgs), String.valueOf(exec), garantia,
                    String.valueOf(TAMANHO_BYTES), String.valueOf(taxaMsgS)
            )).redirectErrorStream(true).start();

            Map<String, String> saidaProdutor = new HashMap<>();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(procProdutor.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    System.out.println("  [PRODUTOR] " + linha);
                    guardarChaveValor(linha, saidaProdutor);
                }
            }

            esperar(procProdutor, "produtor", procConsumidor);
            esperar(procConsumidor, "consumidor", null);
            leitor.get();

            int recebidas = Integer.parseInt(obrigatorio(saidaConsumidor, "RECEBIDAS"));
            if (recebidas != numMsgs) {
                throw new IllegalStateException("Esperado=" + numMsgs + ", recebido=" + recebidas);
            }

            long t0 = Long.parseLong(obrigatorio(saidaProdutor, "T0"));
            long t1 = Long.parseLong(obrigatorio(saidaProdutor, "T1"));
            if (t1 <= t0) {
                throw new IllegalStateException("Duração não positiva: T0=" + t0 + ", T1=" + t1);
            }

            return new Resultado(t0, t1,
                    Long.parseLong(obrigatorio(saidaConsumidor, "ULTIMA_CHEGADA")),
                    Integer.parseInt(obrigatorio(saidaConsumidor, "DUPLICATAS")),
                    Integer.parseInt(obrigatorio(saidaProdutor, "TRANSACOES")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MEDIA")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MEDIANA")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MIN")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MAX")));
        } finally {
            if (procConsumidor.isAlive()) procConsumidor.destroyForcibly();
            executor.shutdownNow();
        }
    }

    /** Espera o processo terminar e exige código de saída 0. */
    private static void esperar(Process proc, String nome, Process outro) throws InterruptedException {
        if (!proc.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
            proc.destroyForcibly();
            if (outro != null) outro.destroyForcibly();
            throw new IllegalStateException("Timeout aguardando o " + nome + " terminar.");
        }
        if (proc.exitValue() != 0) {
            if (outro != null) outro.destroyForcibly();
            throw new IllegalStateException("O " + nome + " terminou com código " + proc.exitValue()
                    + " — veja as linhas [" + nome.toUpperCase() + "] acima.");
        }
    }

    private List<String> comando(String classeMain, String... argumentos) {
        List<String> c = new ArrayList<>();
        c.add(javaCmd);
        c.add("-cp");
        c.add(classpath);
        c.add(classeMain);
        c.addAll(Arrays.asList(argumentos));
        return c;
    }

    private static void cabecalhoParte(String texto) {
        System.out.println();
        System.out.println("--------------------------------------------------------------------");
        System.out.println("  " + texto);
        System.out.println("--------------------------------------------------------------------");
    }

    private static void guardarChaveValor(String linha, Map<String, String> destino) {
        int i = linha.indexOf('=');
        if (i > 0) destino.put(linha.substring(0, i).trim(), linha.substring(i + 1).trim());
    }

    private static String obrigatorio(Map<String, String> saida, String chave) {
        String v = saida.get(chave);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Métrica não retornada pelo subprocesso: " + chave);
        }
        return v;
    }

    private static void apagarSeExistir(String caminho) throws IOException {
        File f = new File(caminho);
        if (f.exists() && !f.delete()) {
            throw new IOException("Não foi possível apagar: " + f.getAbsolutePath()
                    + " (o arquivo está aberto em outro programa?)");
        }
    }

    // ------------------------------------------------------------------
    // Tópico
    // ------------------------------------------------------------------
    /** Apaga e recria o tópico (1 partição); para a campanha se não conseguir. */
    private void recriarTopico() throws Exception {
        try (AdminClient admin = adminClient()) {
            Set<String> existentes = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            if (existentes.contains(TOPICO)) {
                admin.deleteTopics(List.of(TOPICO)).all().get(30, TimeUnit.SECONDS);
            }

            // a exclusão no Kafka é assíncrona: tenta criar até o nome ficar livre
            for (int tentativa = 1; ; tentativa++) {
                try {
                    admin.createTopics(List.of(new NewTopic(TOPICO, 1, (short) 1)))
                            .all().get(30, TimeUnit.SECONDS);
                    return;
                } catch (ExecutionException e) {
                    if (tentativa >= 20) {
                        throw new IllegalStateException("Não foi possível recriar o tópico " + TOPICO, e);
                    }
                    Thread.sleep(500);
                }
            }
        }
    }

    private AdminClient adminClient() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put("auto.include.jmx.reporter", "false");
        config.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        return AdminClient.create(config);
    }
}
