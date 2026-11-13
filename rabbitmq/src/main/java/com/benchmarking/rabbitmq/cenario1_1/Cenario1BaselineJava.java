package com.benchmarking.rabbitmq.cenario1_1;

import com.rabbitmq.client.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cenário 1.1 — Baseline RabbitMQ — orquestrador.
 *
 * Mesma estrutura do orquestrador do Kafka:
 *   - 1 execução de aquecimento, descartada;
 *   - 10 execuções válidas de 10.000 mensagens;
 *   - a cada execução: esvazia a fila -> inicia o consumidor -> lê a saída
 *     dele em uma thread própria desde o início -> espera STATUS=PRONTO ->
 *     inicia o produtor.
 *
 * FIX (prontidão): a espera fixa de 1,2 s foi trocada pelo sinal
 * STATUS=PRONTO do consumidor, como no Kafka.
 *
 * FIX (erros visíveis): antes, se um processo falhasse, as métricas viravam
 * 0 sem aviso (getOrDefault(..., "0")) e o código de saída não era
 * verificado. Agora qualquer falha interrompe o benchmark com uma mensagem
 * clara: código de saída diferente de 0, métrica ausente, número errado de
 * mensagens ou timeout.
 *
 * DIAGNÓSTICO (coleta de lixo): com LOG_GC = true, produtor e consumidor
 * rodam com -Xlog:gc, um log por processo e execução, em
 * PASTA_RESULTADOS/gc-*.log. Mesmo diagnóstico do Kafka.
 */
@Component
public class Cenario1BaselineJava {

    private static final String FILA = "cenario1-baseline-java";
    private static final int    MSGS = 10_000;

    private static final String PASTA_RESULTADOS          = "resultado_rabbitmq_cenario_1_1";
    private static final String CSV_DETALHADO_NOME        = "resultados-rabbitmq-cenario1-java-detalhado.csv";
    private static final String CSV_AGREGADO_NOME         = "resultados-rabbitmq-cenario1-java.csv";
    private static final String CSV_DETALHADO_WARMUP_NOME = "warmup-detalhado-temp.csv";

    /** Grava logs de coleta de lixo (-Xlog:gc) de cada processo. */
    private static final boolean LOG_GC = false;

    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;
    private static final long TIMEOUT_PROCESSO_S          = 180;

    @Value("${benchmarking.execucoes}")      private int    totalExecucoes;
    @Value("${benchmarking.host:localhost}") private String host;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private ConnectionFactory factory;

    /** Resultado agregado de uma execução. */
    private record ResultadoExecucao(double vazao, double media, double min,
                                     double max, double p95, double p99) {}

    public void executar() throws Exception {
        System.out.println("\n====================================================================");
        System.out.println("   Cenário 1.1 RabbitMQ — Baseline (Processos Separados)");
        System.out.println("====================================================================");

        File pasta = new File(PASTA_RESULTADOS);
        pasta.mkdirs();
        if (!pasta.isDirectory()) {
            throw new IOException("Não foi possível criar a pasta de resultados: " + pasta.getAbsolutePath());
        }
        System.out.println("Gravando em: " + pasta.getAbsolutePath());

        String csvDetalhado = PASTA_RESULTADOS + File.separator + CSV_DETALHADO_NOME;
        String csvAgregado  = PASTA_RESULTADOS + File.separator + CSV_AGREGADO_NOME;
        String csvWarmup    = PASTA_RESULTADOS + File.separator + CSV_DETALHADO_WARMUP_NOME;

        apagarSeExistir(csvDetalhado);
        apagarSeExistir(csvAgregado);
        apagarSeExistir(csvWarmup);
        apagarLogsGc();

        factory = new ConnectionFactory();
        factory.setHost(host);

        // ---- AQUECIMENTO (descartado) ----
        System.out.println("\nExecução de aquecimento (warmup) — resultado será descartado...");
        ResultadoExecucao warmup = rodarParProdutorConsumidor(0, csvWarmup);
        System.out.printf(Locale.US,
                "  → [warmup, descartado] Vazão: %.2f msg/s | Média: %.2f ms%n",
                warmup.vazao(), warmup.media());
        apagarSeExistir(csvWarmup);
        Thread.sleep(3000);

        // ---- EXECUÇÕES VÁLIDAS ----
        try (PrintWriter agregado = new PrintWriter(new FileWriter(csvAgregado, false))) {
            agregado.println("cenario1-baseline-java-rabbitmq");
            agregado.println("execucao,vazao_msg_s,latencia_media_ms,latencia_min_ms," +
                    "latencia_max_ms,latencia_p95_ms,latencia_p99_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                System.out.printf("%nExecução %d/%d...%n", exec, totalExecucoes);

                ResultadoExecucao r = rodarParProdutorConsumidor(exec, csvDetalhado);

                agregado.printf(Locale.US, "%d,%.2f,%.4f,%.4f,%.4f,%.4f,%.4f%n",
                        exec, r.vazao(), r.media(), r.min(), r.max(), r.p95(), r.p99());
                agregado.flush();

                System.out.printf(Locale.US, "  → Vazão: %.2f msg/s | Média: %.4f ms | Máx: %.4f ms%n",
                        r.vazao(), r.media(), r.max());

                if (exec < totalExecucoes) Thread.sleep(5000); // mesma pausa do Kafka
            }
        }

        System.out.println("\n[SUCESSO] Resultados consolidados em: " + new File(csvAgregado).getAbsolutePath());
    }

    /**
     * Esvazia a fila, sobe o consumidor, espera STATUS=PRONTO, sobe o
     * produtor, lê as saídas e devolve o resultado agregado.
     */
    private ResultadoExecucao rodarParProdutorConsumidor(int exec, String csvDetalhadoDestino) throws Exception {
        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {
            channel.queueDeclare(FILA, true, false, false, null);
            channel.queuePurge(FILA);
        }
        Thread.sleep(1000);

        Process procConsumidor = new ProcessBuilder(montarComando(
                "com.benchmarking.rabbitmq.cenario1_1.Consumidor",
                "gc-consumidor-exec" + exec,
                host, FILA, String.valueOf(MSGS), String.valueOf(exec), csvDetalhadoDestino
        )).redirectErrorStream(true).start();

        // leitura da saída do consumidor em thread própria, desde o início
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> saidaConsumidor = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> leitorConsumidor = executor.submit(() -> {
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

            Process procProdutor = new ProcessBuilder(montarComando(
                    "com.benchmarking.rabbitmq.cenario1_1.Produtor",
                    "gc-produtor-exec" + exec,
                    host, FILA, String.valueOf(MSGS), String.valueOf(exec)
            )).redirectErrorStream(true).start();

            Map<String, String> saidaProdutor = new HashMap<>();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(procProdutor.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    System.out.println("  [PRODUTOR] " + linha);
                    guardarChaveValor(linha, saidaProdutor);
                }
            }

            if (!procProdutor.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
                procProdutor.destroyForcibly();
                procConsumidor.destroyForcibly();
                throw new IllegalStateException("Timeout aguardando o produtor terminar.");
            }
            if (procProdutor.exitValue() != 0) {
                procConsumidor.destroyForcibly();
                throw new IllegalStateException("Produtor terminou com código " + procProdutor.exitValue());
            }

            if (!procConsumidor.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
                procConsumidor.destroyForcibly();
                throw new IllegalStateException("Timeout aguardando o consumidor terminar.");
            }
            leitorConsumidor.get();
            if (procConsumidor.exitValue() != 0) {
                throw new IllegalStateException("Consumidor terminou com código " + procConsumidor.exitValue());
            }

            int recebidas = Integer.parseInt(obrigatorio(saidaConsumidor, "RECEBIDAS"));
            if (recebidas != MSGS) {
                throw new IllegalStateException("Esperado=" + MSGS + ", recebido=" + recebidas);
            }

            long t0 = Long.parseLong(obrigatorio(saidaProdutor, "T0"));
            long t1 = Long.parseLong(obrigatorio(saidaProdutor, "T1"));
            if (t1 <= t0) {
                throw new IllegalStateException("Duração não positiva: T0=" + t0 + ", T1=" + t1);
            }
            double vazao = MSGS / ((t1 - t0) / 1_000_000_000.0);

            return new ResultadoExecucao(
                    vazao,
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MEDIA")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MIN")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "MAX")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "P95")),
                    Double.parseDouble(obrigatorio(saidaConsumidor, "P99")));
        } finally {
            executor.shutdownNow();
        }
    }

    private List<String> montarComando(String classeMain, String nomeLogGc, String... argumentos) {
        List<String> comando = new ArrayList<>();
        comando.add(javaCmd);
        if (LOG_GC) {
            // caminho relativo e com "/" de propósito: o ":" de um caminho
            // absoluto do Windows (D:\...) confundiria a sintaxe do -Xlog
            comando.add("-Xlog:gc:file=" + PASTA_RESULTADOS + "/" + nomeLogGc + ".log:uptime");
        }
        comando.add("-cp");
        comando.add(classpath);
        comando.add(classeMain);
        comando.addAll(Arrays.asList(argumentos));
        return comando;
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

    /** Apaga os logs de coleta de lixo de campanhas anteriores. */
    private void apagarLogsGc() {
        File[] logs = new File(PASTA_RESULTADOS).listFiles(
                (dir, nome) -> nome.startsWith("gc-") && nome.contains(".log"));
        if (logs == null) return;
        for (File f : logs) {
            if (!f.delete()) {
                System.out.println("Aviso: não foi possível apagar " + f.getAbsolutePath());
            }
        }
    }
}
