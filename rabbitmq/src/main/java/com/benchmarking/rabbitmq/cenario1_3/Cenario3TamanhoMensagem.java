package com.benchmarking.rabbitmq.cenario1_3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cenário 3 — Variação do tamanho da mensagem (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: medir quanto o tamanho da mensagem pesa na latência (Parte A) e
 * na vazão máxima (Parte B).
 *
 * TAMANHOS: 1, 2, 5, 10, 20, 50, 100, 200 e 500 KB — série 1-2-5, três pontos
 * por década, espaçamento uniforme em escala log. Os pontos ficam dos dois
 * lados do limite de 4 KB abaixo do qual a fila clássica guarda a mensagem no
 * próprio índice.
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
 *   primeiro envio até a última mensagem chegar no consumidor. Pode ser
 *   desligada com RODAR_PARTE_B = false (o CSV de vazão anterior é preservado).
 *
 * TESTE COMPLEMENTAR (desligado por padrão: TESTE_ACK = false)
 *   Verifica a explicação para as execuções lentas de 10 e 50 KB (interação
 *   entre o algoritmo de Nagle e o ACK atrasado do TCP no repasse de portas
 *   do Docker Desktop). Roda só 10 e 50 KB, a 50 msg/s (a taxa em que o
 *   efeito foi observado), alternando o consumidor com confirmação
 *   AUTOMÁTICA (configuração do cenário) e MANUAL (o consumidor envia um ACK
 *   por mensagem, criando tráfego de volta na conexão). Se a explicação
 *   estiver certa, os degraus de 20 ms aparecem só no modo automático.
 *   Grava em arquivos separados e não altera os resultados das Partes A e B.
 *
 * Organização das execuções:
 *   - Tamanhos INTERCALADOS em rodadas: em cada rodada os 9 tamanhos rodam,
 *     e a ordem começa um tamanho adiante a cada rodada (rodada 1: 1, 2, ...,
 *     500; rodada 2: 2, 5, ..., 500, 1; ...). Com 9 tamanhos e 10 rodadas,
 *     cada tamanho ocupa cada posição pelo menos uma vez.
 *   - Aquecimento: uma execução descartada de cada tamanho no início de cada
 *     parte.
 *   - Fila esvaziada (purge) antes de cada execução; métrica ausente, código
 *     de saída diferente de 0 ou número errado de mensagens interrompem a
 *     campanha.
 *   - Mesma configuração de produtor e consumidor do Cenário 1
 *     (persistente + confirms aguardados no fim; autoAck, sem prefetch).
 *
 * Duração aproximada da Parte A: 9 tamanhos x 11 execuções x ~108 s ≈ 3 h.
 * Teste complementar: 2 tamanhos x 2 modos x 11 execuções x ~27 s ≈ 20 min.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-rabbitmq-cenario3-latencia.csv              (1 linha por execução e tamanho)
 *   resultados-rabbitmq-cenario3-latencia-detalhado.csv    (1 linha por mensagem)
 *   resultados-rabbitmq-cenario3-vazao.csv                 (1 linha por execução e tamanho)
 *   resultados-rabbitmq-cenario3-teste-ack.csv             (teste complementar, 1 linha por execução)
 *   resultados-rabbitmq-cenario3-teste-ack-detalhado.csv   (teste complementar, 1 linha por mensagem)
 */
@Component
public class Cenario3TamanhoMensagem {

    private static final String FILA = "cenario3-tamanho-mensagem";
    // mesmo padrão dos Cenários 1 e 2: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_1_3";

    private static final String CSV_LATENCIA       = "resultados-rabbitmq-cenario3-latencia.csv";
    private static final String CSV_DETALHADO      = "resultados-rabbitmq-cenario3-latencia-detalhado.csv";
    private static final String CSV_VAZAO          = "resultados-rabbitmq-cenario3-vazao.csv";
    private static final String CSV_TESTE_ACK      = "resultados-rabbitmq-cenario3-teste-ack.csv";
    private static final String CSV_TESTE_ACK_DET  = "resultados-rabbitmq-cenario3-teste-ack-detalhado.csv";

    private static final int[] TAMANHOS_KB = {1, 2, 5, 10, 20, 50, 100, 200, 500};

    // Parte A — latência: taxa ÚNICA para todos os tamanhos
    private static final int MSGS_LATENCIA = 1_000;
    private static final int TAXA_LATENCIA_MSG_S = 10;

    // Parte B — vazão máxima (mesmo volume para todos os tamanhos)
    /** false = não roda a Parte B e preserva o CSV de vazão da campanha anterior. */
    private static final boolean RODAR_PARTE_B = true;
    private static final int VOLUME_VAZAO_MB = 50;

    // Teste complementar do Nagle (ver javadoc)
    /** true = roda também o teste complementar de confirmação automática × manual. */
    private static final boolean TESTE_ACK = false;
    /** true = roda SÓ o teste complementar (pula as Partes A e B e não apaga os CSVs delas). */
    private static final boolean SOMENTE_TESTE_ACK = false;
    private static final int[] TAMANHOS_TESTE_ACK_KB = {10, 50};
    private static final int TAXA_TESTE_ACK_MSG_S = 50;

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 5_000;
    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;
    private static final long TIMEOUT_PROCESSO_S = 360;

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.host:localhost}")    private String host;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    /** Resultado de um par produtor/consumidor. */
    private record Resultado(long t0, long t1, long ultimaChegada,
                             double media, double mediana, double min, double max) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Cenário 3 RabbitMQ — Variação do tamanho da mensagem");
        System.out.println("====================================================================");

        File pasta = new File(PASTA_RESULTADOS);
        if (!pasta.exists() && !pasta.mkdirs()) {
            throw new IOException("Não foi possível criar a pasta: " + pasta.getAbsolutePath());
        }
        System.out.println("Gravando em: " + pasta.getAbsolutePath());

        String csvLatencia  = PASTA_RESULTADOS + File.separator + CSV_LATENCIA;
        String csvDetalhado = PASTA_RESULTADOS + File.separator + CSV_DETALHADO;
        String csvVazao     = PASTA_RESULTADOS + File.separator + CSV_VAZAO;

        if (!SOMENTE_TESTE_ACK) {
            System.out.printf("Tamanhos: %s KB | Parte A: %d msgs a %d msg/s | Parte B: %s%n",
                    Arrays.toString(TAMANHOS_KB), MSGS_LATENCIA, TAXA_LATENCIA_MSG_S,
                    RODAR_PARTE_B ? VOLUME_VAZAO_MB + " MB sem controle" : "desligada");

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
        }

        if (TESTE_ACK || SOMENTE_TESTE_ACK) {
            testeAck(PASTA_RESULTADOS + File.separator + CSV_TESTE_ACK,
                     PASTA_RESULTADOS + File.separator + CSV_TESTE_ACK_DET);
        }

        System.out.println();
        System.out.println("[SUCESSO] Cenário 3 RabbitMQ concluído.");
        System.out.println("  Pasta: " + pasta.getAbsolutePath());
    }

    // ------------------------------------------------------------------
    // Parte A — latência a taxa fixa
    // ------------------------------------------------------------------
    private void parteLatencia(String csvLatencia, String csvDetalhado) throws Exception {
        cabecalhoParte("PARTE A — LATÊNCIA (" + TAXA_LATENCIA_MSG_S + " msg/s em todos os tamanhos; "
                + MSGS_LATENCIA + " mensagens)");

        for (int kb : TAMANHOS_KB) {
            System.out.printf("%nAquecimento (descartado) — %d KB%n", kb);
            rodar(kb * 1024, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, 0, "-", "auto");
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

                    Resultado r = rodar(kb * 1024, MSGS_LATENCIA, TAXA_LATENCIA_MSG_S, exec, csvDetalhado, "auto");
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
            rodar(kb * 1024, msgsVazao(kb), 0, 0, "-", "auto");
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

                    Resultado r = rodar(kb * 1024, n, 0, exec, "-", "auto");

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

    // ------------------------------------------------------------------
    // Teste complementar — confirmação automática × manual (Nagle)
    // ------------------------------------------------------------------
    private void testeAck(String csvTeste, String csvTesteDet) throws Exception {
        cabecalhoParte("TESTE COMPLEMENTAR — confirmação automática × manual ("
                + Arrays.toString(TAMANHOS_TESTE_ACK_KB) + " KB, " + TAXA_TESTE_ACK_MSG_S + " msg/s)");
        apagarSeExistir(csvTeste);
        apagarSeExistir(csvTesteDet);

        String[] modos = {"auto", "manual"};

        for (int kb : TAMANHOS_TESTE_ACK_KB) {
            for (String modo : modos) {
                System.out.printf("%nAquecimento (descartado) — %d KB, ack %s%n", kb, modo);
                rodar(kb * 1024, MSGS_LATENCIA, TAXA_TESTE_ACK_MSG_S, 0, "-", modo);
                Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
            }
        }

        try (PrintWriter csv = new PrintWriter(new FileWriter(csvTeste))) {
            csv.println("execucao,ordem,modo_ack,tamanho_kb,num_msgs,taxa_alvo_msg_s," +
                    "latencia_media_ms,latencia_mediana_ms,latencia_min_ms,latencia_max_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                // alterna qual modo vem primeiro a cada rodada
                String[] ordemModos = (exec % 2 == 1) ? modos : new String[]{"manual", "auto"};
                int pos = 0;
                for (int kb : TAMANHOS_TESTE_ACK_KB) {
                    for (String modo : ordemModos) {
                        pos++;
                        System.out.printf("%n[T] Rodada %d/%d — %d KB, ack %s%n", exec, totalExecucoes, kb, modo);

                        // o CSV detalhado do teste recebe execução e tamanho; o modo vai
                        // codificado no número da execução: +1000 para o modo manual
                        int execGravada = "manual".equals(modo) ? exec + 1000 : exec;
                        Resultado r = rodar(kb * 1024, MSGS_LATENCIA, TAXA_TESTE_ACK_MSG_S,
                                execGravada, csvTesteDet, modo);

                        csv.printf(Locale.US, "%d,%d,%s,%d,%d,%d,%.4f,%.4f,%.4f,%.4f%n",
                                exec, pos, modo, kb, MSGS_LATENCIA, TAXA_TESTE_ACK_MSG_S,
                                r.media, r.mediana, r.min, r.max);
                        csv.flush();

                        System.out.printf(Locale.US, "  → mediana %.2f ms (ack %s)%n", r.mediana, modo);
                        Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                    }
                }
            }
        }
        System.out.println("\nTeste complementar gravado em: " + new File(csvTeste).getAbsolutePath());
        System.out.println("No CSV detalhado do teste, execuções acima de 1000 são do modo manual (execução - 1000).");
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
    private Resultado rodar(int tamanhoBytes, int numMsgs, int taxaMsgS, int exec, String csvDetalhado,
                            String modoAck) throws Exception {
        limparFila();
        Thread.sleep(1000);

        Process procConsumidor = new ProcessBuilder(comando(
                "com.benchmarking.rabbitmq.cenario1_3.ConsumidorCenario3",
                host, FILA, String.valueOf(numMsgs), String.valueOf(exec),
                String.valueOf(tamanhoBytes), csvDetalhado, modoAck
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
                    "com.benchmarking.rabbitmq.cenario1_3.ProdutorCenario3",
                    host, FILA, String.valueOf(numMsgs), String.valueOf(exec),
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
    // Fila
    // ------------------------------------------------------------------
    private void limparFila() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {
            channel.queueDeclare(FILA, true, false, false, null);
            channel.queuePurge(FILA);
        }
    }
}