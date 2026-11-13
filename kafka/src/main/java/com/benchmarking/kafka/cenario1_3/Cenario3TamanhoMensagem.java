package com.benchmarking.kafka.cenario1_3;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cenário 3 — Variação do tamanho da mensagem (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: medir quanto o tamanho da mensagem pesa na latência (Parte A) e
 * na vazão máxima (Parte B).
 *
 * TAMANHOS: 1, 2, 5, 10, 20, 50, 100, 200 e 500 KB — série 1-2-5, três pontos
 * por década, espaçamento uniforme em escala log. 500 KB fica abaixo do limite
 * padrão de ~1 MB por mensagem do Kafka (sem mudar configuração).
 *
 * Observação sobre o batch.size (64 KB): o produtor só junta mensagens num
 * mesmo lote quando há acúmulo (Parte B). Um lote de 64 KB comporta três
 * mensagens de 20 KB, duas de ~30 KB e apenas UMA de 50 KB ou mais; a
 * transição fica, portanto, entre 20 e 50 KB. Na Parte A, a taxa baixa faz
 * cada mensagem sair sozinha em qualquer tamanho, mesmo com linger.ms=0.
 *
 * Parte A — LATÊNCIA POR TAMANHO
 *   1.000 mensagens por execução, a TAXA ÚNICA de 10 msg/s para todos os
 *   tamanhos. Com 500 KB isso dá ~5 MB/s, bem abaixo do que o ambiente
 *   entrega, então não se forma fila em nenhum tamanho. Usar a mesma taxa
 *   para todos evita que a taxa de envio se confunda com o efeito do tamanho
 *   (na versão anterior, 200 e 500 KB iam a 25 e 10 msg/s e os demais a 50).
 *
 * Parte B — VAZÃO MÁXIMA POR TAMANHO
 *   ~50 MB por execução (51.200 x 1 KB ... 512 x 100 KB ... 102 x 500 KB),
 *   enviados sem controle de taxa. Mede a vazão de ponta a ponta: do
 *   primeiro envio até a última mensagem chegar no consumidor. Aqui a fila é
 *   esperada, por isso a latência NÃO é usada. Pode ser desligada com
 *   RODAR_PARTE_B = false (nesse caso o CSV de vazão anterior é preservado).
 *
 * Organização das execuções:
 *   - Tamanhos INTERCALADOS em rodadas: em cada rodada os 9 tamanhos rodam,
 *     e a ordem começa um tamanho adiante a cada rodada (rodada 1: 1, 2, ...,
 *     500; rodada 2: 2, 5, ..., 500, 1; ...). Com 9 tamanhos e 10 rodadas,
 *     cada tamanho ocupa cada posição pelo menos uma vez.
 *   - Aquecimento: uma execução descartada de cada tamanho no início de cada
 *     parte.
 *   - Tópico recriado antes de cada execução; falha ao recriar, métrica
 *     ausente, código de saída diferente de 0 ou número errado de mensagens
 *     interrompem a campanha.
 *   - Mesma configuração de produtor e consumidor do Cenário 1.
 *
 * Duração aproximada da Parte A: 9 tamanhos x 11 execuções x ~108 s ≈ 3 h.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-kafka-cenario3-latencia.csv            (1 linha por execução e tamanho)
 *   resultados-kafka-cenario3-latencia-detalhado.csv  (1 linha por mensagem)
 *   resultados-kafka-cenario3-vazao.csv               (1 linha por execução e tamanho)
 */
@Component
public class Cenario3TamanhoMensagem {

    private static final String TOPICO = "cenario3-tamanho-mensagem";
    // mesmo padrão dos Cenários 1 e 2: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_cenario_1_3";

    private static final String CSV_LATENCIA  = "resultados-kafka-cenario3-latencia.csv";
    private static final String CSV_DETALHADO = "resultados-kafka-cenario3-latencia-detalhado.csv";
    private static final String CSV_VAZAO     = "resultados-kafka-cenario3-vazao.csv";

    private static final int[] TAMANHOS_KB = {1, 2, 5, 10, 20, 50, 100, 200, 500};

    // Parte A — latência: taxa ÚNICA para todos os tamanhos
    private static final int MSGS_LATENCIA = 1_000;
    private static final int TAXA_LATENCIA_MSG_S = 10;

    // Parte B — vazão máxima (mesmo volume para todos os tamanhos)
    /** false = não roda a Parte B e preserva o CSV de vazão da campanha anterior. */
    private static final boolean RODAR_PARTE_B = true;
    private static final int VOLUME_VAZAO_MB = 50;

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 5_000;
    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;
    private static final long TIMEOUT_PROCESSO_S = 360;

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.bootstrap-servers}") private String bootstrapServers;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    /** Resultado de um par produtor/consumidor. */
    private record Resultado(long t0, long t1, long ultimaChegada,
                             double media, double mediana, double min, double max) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Cenário 3 Kafka — Variação do tamanho da mensagem");
        System.out.println("====================================================================");
        System.out.printf("Tamanhos: %s KB | Parte A: %d msgs a %d msg/s | Parte B: %s%n",
                Arrays.toString(TAMANHOS_KB), MSGS_LATENCIA, TAXA_LATENCIA_MSG_S,
                RODAR_PARTE_B ? VOLUME_VAZAO_MB + " MB sem controle" : "desligada");

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
        if (RODAR_PARTE_B) {
            apagarSeExistir(csvVazao);
        }

        parteLatencia(csvLatencia, csvDetalhado);
        if (RODAR_PARTE_B) {
            parteVazao(csvVazao);
        } else {
            System.out.println("\nParte B desligada (RODAR_PARTE_B = false): CSV de vazão preservado.");
        }

        System.out.println();
        System.out.println("[SUCESSO] Cenário 3 Kafka concluído.");
        System.out.println("  " + new File(csvLatencia).getAbsolutePath());
        System.out.println("  " + new File(csvDetalhado).getAbsolutePath());
        System.out.println("  " + new File(csvVazao).getAbsolutePath());
    }

    // ------------------------------------------------------------------
    // Parte A — latência a taxa fixa
    // ------------------------------------------------------------------
    private void parteLatencia(String csvLatencia, String csvDetalhado) throws Exception {
        cabecalhoParte("PARTE A — LATÊNCIA (" + TAXA_LATENCIA_MSG_S + " msg/s em todos os tamanhos; "
                + MSGS_LATENCIA + " mensagens)");

        for (int kb : TAMANHOS_KB) {
            System.out.printf("%nAquecimento (descartado) — %d KB%n", kb);
            rodar(kb * 1024, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, 0, "-");
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter csv = new PrintWriter(new FileWriter(csvLatencia))) {
            csv.println("execucao,ordem,tamanho_kb,num_msgs,taxa_alvo_msg_s,taxa_envio_msg_s," +
                    "latencia_media_ms,latencia_mediana_ms,latencia_min_ms,latencia_max_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                int[] ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.length; pos++) {
                    int kb = ordem[pos];
                    System.out.printf("%n[A] Rodada %d/%d — %d KB (posição %d de %d na rodada)%n",
                            exec, totalExecucoes, kb, pos + 1, ordem.length);

                    Resultado r = rodar(kb * 1024, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, exec, csvDetalhado);
                    double taxaEnvio = MSGS_LATENCIA / ((r.t1 - r.t0) / 1e9);

                    csv.printf(Locale.US, "%d,%d,%d,%d,%d,%.2f,%.4f,%.4f,%.4f,%.4f%n",
                            exec, pos + 1, kb, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, taxaEnvio,
                            r.media, r.mediana, r.min, r.max);
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
        cabecalhoParte("PARTE B — VAZÃO MÁXIMA (" + VOLUME_VAZAO_MB + " MB por execução, sem controle de taxa)");

        for (int kb : TAMANHOS_KB) {
            System.out.printf("%nAquecimento (descartado) — %d KB%n", kb);
            rodar(kb * 1024, msgsVazao(kb), 0, 0, "-");
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter csv = new PrintWriter(new FileWriter(csvVazao))) {
            csv.println("execucao,ordem,tamanho_kb,num_msgs,volume_mb,duracao_s," +
                    "vazao_msg_s,vazao_mb_s,vazao_produtor_msg_s");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                int[] ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.length; pos++) {
                    int kb = ordem[pos];
                    int n = msgsVazao(kb);
                    System.out.printf("%n[B] Rodada %d/%d — %d KB (posição %d de %d na rodada)%n",
                            exec, totalExecucoes, kb, pos + 1, ordem.length);

                    Resultado r = rodar(kb * 1024, n, 0, exec, "-");

                    // ponta a ponta: do primeiro envio até a última chegada
                    double duracaoS = (r.ultimaChegada - r.t0) / 1e9;
                    if (duracaoS <= 0) {
                        throw new IllegalStateException("Duração não positiva na Parte B: " + duracaoS);
                    }
                    double volumeMb = n * (double) kb / 1024.0;
                    double vazaoMsgS = n / duracaoS;
                    double vazaoMbS = volumeMb / duracaoS;
                    double vazaoProdutor = n / ((r.t1 - r.t0) / 1e9);

                    csv.printf(Locale.US, "%d,%d,%d,%d,%.2f,%.4f,%.2f,%.2f,%.2f%n",
                            exec, pos + 1, kb, n, volumeMb, duracaoS, vazaoMsgS, vazaoMbS, vazaoProdutor);
                    csv.flush();

                    System.out.printf(Locale.US, "  → %.1f MB/s | %.0f msg/s (ponta a ponta)%n",
                            vazaoMbS, vazaoMsgS);
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }
    }

    private static int msgsVazao(int kb) {
        return VOLUME_VAZAO_MB * 1024 / kb;
    }

    /** Ordem rotativa: rodada 1 → 1,2,5,...,500; rodada 2 → 2,5,...,500,1; ... */
    static int[] ordemDaExecucao(int exec) {
        int n = TAMANHOS_KB.length;
        int[] ordem = new int[n];
        for (int i = 0; i < n; i++) {
            ordem[i] = TAMANHOS_KB[(i + exec - 1) % n];
        }
        return ordem;
    }

    // ------------------------------------------------------------------
    // Um par produtor/consumidor
    // ------------------------------------------------------------------
    private Resultado rodar(int tamanhoBytes, int numMsgs, int taxaMsgS, int exec, String csvDetalhado)
            throws Exception {
        recriarTopico();
        Thread.sleep(1000);

        Process procConsumidor = new ProcessBuilder(comando(
                "com.benchmarking.kafka.cenario1_3.ConsumidorCenario3",
                bootstrapServers, TOPICO, String.valueOf(numMsgs), String.valueOf(exec),
                String.valueOf(tamanhoBytes), csvDetalhado
        )).redirectErrorStream(true).start();

        // stdout do consumidor lido em thread própria desde o início
        // (evita o bloqueio do pipe corrigido nos Cenários 1 e 2)
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

            Process procProdutor = new ProcessBuilder(comando(
                    "com.benchmarking.kafka.cenario1_3.ProdutorCenario3",
                    bootstrapServers, TOPICO, String.valueOf(numMsgs), String.valueOf(exec),
                    String.valueOf(tamanhoBytes), String.valueOf(taxaMsgS)
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
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MEDIA")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MEDIANA")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MIN")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MAX")));
        } finally {
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