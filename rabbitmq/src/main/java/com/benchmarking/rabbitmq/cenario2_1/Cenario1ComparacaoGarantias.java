package com.benchmarking.rabbitmq.cenario2_1;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível intermediário — Cenário 1: Comparação de garantias (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: medir quanto cada nível de garantia de entrega custa em latência
 * e em vazão. Nenhuma falha é provocada: o comportamento sob falha é
 * avaliado no Cenário 2 (Falha do Consumidor).
 *
 * Níveis (ver ProdutorGarantias e ConsumidorGarantias):
 *   at-most-once  : sem publisher confirms | autoAck
 *   at-least-once : publisher confirms | ack manual DEPOIS de processar
 *   exactly-once  : publisher confirms | ack manual + deduplicação pelo
 *                   messageId (o RabbitMQ não tem exactly once nativo)
 *   Nos três: mensagens persistentes (deliveryMode=2) e fila durável.
 *
 * Duas partes, cada uma medindo uma coisa só (mesmo formato do Cenário 3):
 *   Parte A — LATÊNCIA: 5.000 mensagens a 200 msg/s (taxa do Cenário 1
 *             básico), sem formar fila.
 *   Parte B — VAZÃO MÁXIMA: 20.000 mensagens sem controle de taxa; vazão de
 *             ponta a ponta (do primeiro envio até a última chegada).
 *
 * Mudanças em relação à versão anterior: aquecimento descartado, níveis
 * intercalados (antes eram 10 execuções seguidas de cada nível), sinal
 * STATUS=PRONTO no lugar do sleep(800), validação de contagem e de código de
 * saída, e taxa fixa na medição da latência.
 *
 * Os três níveis rodam intercalados, em ordem rotativa, depois de uma
 * execução de aquecimento de cada nível em cada parte. Métrica ausente,
 * código de saída diferente de 0 ou contagem errada interrompem a campanha.
 *
 * Parte C — TESTE DE CONFIRMAÇÕES (opcional, flag TESTE_CONFIRMACOES):
 *   do at-most-once para o at-least-once mudam DUAS coisas no RabbitMQ
 *   (publisher confirms e ACK manual por mensagem). A Parte C repete a
 *   Parte B (20.000 mensagens, sem controle de taxa) com 5 variações, para
 *   separar o efeito de cada uma e ver se o ACK em lote recupera a vazão:
 *     v1  sem confirms | autoAck              (= at-most-once)
 *     v2  com confirms | autoAck              (só os confirms)
 *     v3  sem confirms | ACK por mensagem     (só o ACK manual)
 *     v4  com confirms | ACK por mensagem     (= at-least-once)
 *     v5  com confirms | ACK em lote          (multiple=true a cada 50)
 *   Mesmo método das outras partes: 1 aquecimento de cada variação,
 *   depois totalExecucoes rodadas com ordem rotativa.
 *   Com SOMENTE_TESTE_CONFIRMACOES = true, as Partes A e B NÃO rodam e os
 *   CSVs delas NÃO são apagados.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-rabbitmq-garantias-latencia.csv            (1 linha por execução e nível)
 *   resultados-rabbitmq-garantias-latencia-detalhado.csv  (1 linha por mensagem)
 *   resultados-rabbitmq-garantias-vazao.csv               (1 linha por execução e nível)
 *   resultados-rabbitmq-garantias-teste-confirmacoes.csv  (Parte C, 1 linha por execução e variação)
 */
@Component
public class Cenario1ComparacaoGarantias {

    private static final String FILA = "garantias-cenario1";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_2_1";

    private static final String CSV_LATENCIA  = "resultados-rabbitmq-garantias-latencia.csv";
    private static final String CSV_DETALHADO = "resultados-rabbitmq-garantias-latencia-detalhado.csv";
    private static final String CSV_VAZAO     = "resultados-rabbitmq-garantias-vazao.csv";
    private static final String CSV_TESTE     = "resultados-rabbitmq-garantias-teste-confirmacoes.csv";

    // Parte C — teste de confirmações (ver cabeçalho)
    private static final boolean TESTE_CONFIRMACOES         = true;
    private static final boolean SOMENTE_TESTE_CONFIRMACOES = false;   // true = não roda A e B

    /** Uma variação da Parte C: confirms do produtor e modo de ACK do consumidor. */
    private record Variacao(String id, String confirms, String modoAck) {}

    static final Variacao[] VARIACOES = {
            new Variacao("v1-semconfirms-auto",   "nao", "auto"),
            new Variacao("v2-confirms-auto",      "sim", "auto"),
            new Variacao("v3-semconfirms-manual", "nao", "manual"),
            new Variacao("v4-confirms-manual",    "sim", "manual"),
            new Variacao("v5-confirms-lote",      "sim", "lote"),
    };

    static final String[] GARANTIAS = {"at-most-once", "at-least-once", "exactly-once"};

    private static final int TAMANHO_BYTES = 1_024;
    private static final int PREFETCH      = 100;   // igual ao max.poll.records do Kafka

    // Parte A — latência
    private static final int MSGS_LATENCIA       = 5_000;
    private static final int TAXA_LATENCIA_MSG_S = 200;

    // Parte B — vazão máxima
    private static final int MSGS_VAZAO = 20_000;

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 3_000;
    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;
    private static final long TIMEOUT_PROCESSO_S = 360;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.rabbitmq.cenario2_1.ConsumidorGarantias";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.rabbitmq.cenario2_1.ProdutorGarantias";

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.host:localhost}")    private String host;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    /** Resultado de um par produtor/consumidor. */
    private record Resultado(long t0, long t1, long ultimaChegada, int duplicatas, int transacoes,
                             double media, double mediana, double min, double max) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Intermediário — Cenário 1 RabbitMQ — Comparação de garantias");
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
        String csvTeste     = PASTA_RESULTADOS + File.separator + CSV_TESTE;

        if (!SOMENTE_TESTE_CONFIRMACOES) {
            apagarSeExistir(csvLatencia);
            apagarSeExistir(csvDetalhado);
            apagarSeExistir(csvVazao);
            parteLatencia(csvLatencia, csvDetalhado);
            parteVazao(csvVazao);
        } else {
            System.out.println("SOMENTE_TESTE_CONFIRMACOES = true: Partes A e B puladas, CSVs delas preservados.");
        }

        if (TESTE_CONFIRMACOES) {
            apagarSeExistir(csvTeste);
            parteTesteConfirmacoes(csvTeste);
        }

        System.out.println();
        System.out.println("[SUCESSO] Intermediário — Cenário 1 RabbitMQ concluído.");
        if (!SOMENTE_TESTE_CONFIRMACOES) {
            System.out.println("  " + new File(csvLatencia).getAbsolutePath());
            System.out.println("  " + new File(csvDetalhado).getAbsolutePath());
            System.out.println("  " + new File(csvVazao).getAbsolutePath());
        }
        if (TESTE_CONFIRMACOES) System.out.println("  " + new File(csvTeste).getAbsolutePath());
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

    // ------------------------------------------------------------------
    // Parte C — teste de confirmações (vazão máxima, 5 variações)
    // ------------------------------------------------------------------
    private void parteTesteConfirmacoes(String csvTeste) throws Exception {
        cabecalhoParte("PARTE C — TESTE DE CONFIRMAÇÕES (" + MSGS_VAZAO + " mensagens, sem controle de taxa, "
                + VARIACOES.length + " variações)");

        for (Variacao v : VARIACOES) {
            System.out.printf("%nAquecimento (descartado) — %s%n", v.id());
            rodar(v.id(), MSGS_VAZAO, 0, 0, "-", v.confirms(), v.modoAck());
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter csv = new PrintWriter(new FileWriter(csvTeste))) {
            csv.println("execucao,ordem,variacao,confirms,modo_ack,num_msgs,duracao_s,vazao_msg_s,vazao_produtor_msg_s,duplicatas");

            int n = VARIACOES.length;
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int pos = 0; pos < n; pos++) {
                    // ordem rotativa, como nas Partes A e B
                    Variacao v = VARIACOES[(pos + exec - 1) % n];
                    System.out.printf("%n[C] Execução %d/%d — %s (posição %d de %d)%n",
                            exec, totalExecucoes, v.id(), pos + 1, n);

                    Resultado r = rodar(v.id(), MSGS_VAZAO, 0, exec, "-", v.confirms(), v.modoAck());

                    double duracaoS = (r.ultimaChegada - r.t0) / 1e9;
                    if (duracaoS <= 0) {
                        throw new IllegalStateException("Duração não positiva na Parte C: " + duracaoS);
                    }
                    double vazao = MSGS_VAZAO / duracaoS;
                    double vazaoProdutor = MSGS_VAZAO / ((r.t1 - r.t0) / 1e9);

                    csv.printf(Locale.US, "%d,%d,%s,%s,%s,%d,%.4f,%.2f,%.2f,%d%n",
                            exec, pos + 1, v.id(), v.confirms(), v.modoAck(), MSGS_VAZAO,
                            duracaoS, vazao, vazaoProdutor, r.duplicatas);
                    csv.flush();

                    System.out.printf(Locale.US, "  → %.0f msg/s (ponta a ponta) | produtor %.0f msg/s%n",
                            vazao, vazaoProdutor);
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
        // "-" = cada processo usa o padrão do nível (Partes A e B não mudam)
        return rodar(garantia, numMsgs, taxaMsgS, exec, csvDetalhado, "-", "-");
    }

    /**
     * confirms ("sim" | "nao" | "-") vai para o produtor e modoAck
     * ("auto" | "manual" | "lote" | "-") para o consumidor. Quando não é "-",
     * o valor que cada processo informa (CONFIRMS= / MODO_ACK=) é conferido,
     * para pegar uma classe antiga compilada que ignore o argumento.
     */
    private Resultado rodar(String garantia, int numMsgs, int taxaMsgS, int exec, String csvDetalhado,
                            String confirms, String modoAck) throws Exception {
        limparFila();
        Thread.sleep(500);

        Process procConsumidor = new ProcessBuilder(comando(CONSUMIDOR_CLASS,
                host, FILA, String.valueOf(numMsgs), String.valueOf(exec), garantia,
                csvDetalhado, String.valueOf(PREFETCH), modoAck
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
                    host, FILA, String.valueOf(numMsgs), String.valueOf(exec), garantia,
                    String.valueOf(TAMANHO_BYTES), String.valueOf(taxaMsgS), confirms
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

            if (!"-".equals(confirms) && !confirms.equals(obrigatorio(saidaProdutor, "CONFIRMS"))) {
                throw new IllegalStateException("Produtor informou CONFIRMS=" + saidaProdutor.get("CONFIRMS")
                        + ", esperado " + confirms + " (classe ProdutorGarantias desatualizada?)");
            }
            if (!"-".equals(modoAck) && !modoAck.equals(obrigatorio(saidaConsumidor, "MODO_ACK"))) {
                throw new IllegalStateException("Consumidor informou MODO_ACK=" + saidaConsumidor.get("MODO_ACK")
                        + ", esperado " + modoAck + " (classe ConsumidorGarantias desatualizada?)");
            }

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
    // Fila
    // ------------------------------------------------------------------
    /** Declara a fila (durável) e remove qualquer mensagem que tenha sobrado. */
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