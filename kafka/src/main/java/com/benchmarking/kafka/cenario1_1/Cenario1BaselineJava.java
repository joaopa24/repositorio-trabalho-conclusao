package com.benchmarking.kafka.cenario1_1;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cenário 1 — Baseline Kafka.
 *
 * TESTE DE ISOLAMENTO DE CAUSA: o Consumidor voltou a usar subscribe() em
 * vez de assign()/seekToBeginning() (ver javadoc de Consumidor.java). O que
 * permanece deste arquivo — e é a peça que resolveu de fato o atraso de
 * ~51-54 s observado antes — é a leitura assíncrona do stdout do
 * Consumidor em thread dedicada, com CountDownLatch aguardando o sinal
 * STATUS=PRONTO antes de liberar o Produtor.
 *
 * Causa raiz original: o Consumidor escreve um volume grande de log logo
 * na inicialização (dump do ConsumerConfig, eventos do ConsumerCoordinator).
 * O buffer do pipe de stdout do SO é limitado; se ninguém lê enquanto ele
 * enche, a próxima escrita do processo filho BLOQUEIA até haver leitura.
 * O código anterior só lia o stdout do Consumidor depois que o Produtor
 * terminava (procProdutor.waitFor() antes de lerSaida(procConsumidor)) —
 * por isso o Consumidor ficava "travado" pelo tempo exato que o Produtor
 * levava para rodar (50 s a 200 msg/s), e não por DNS, JMX ou protocolo
 * de consumer group.
 *
 * FIX (aquecimento): a EXECUÇÃO 1 paga um custo de "primeira conexão" que
 * não se repete nas execuções seguintes. Uma execução extra de AQUECIMENTO
 * roda ANTES do loop principal, com resultado DESCARTADO.
 *
 * FIX (delete com retry): apagarArquivoAntigo() tenta algumas vezes com
 * pequena espera antes de desistir — locks de arquivo no Windows (OneDrive
 * sincronizando, Excel aberto, antivírus escaneando) costumam ser
 * transitórios.
 *
 * FIX (CSV de ingestão): o CSV de ingestão, gravado pelo Produtor, agora
 * também é apagado no início. Antes ele acumulava as campanhas anteriores.
 *
 * DIAGNÓSTICO (coleta de lixo): com LOG_GC = true, produtor e consumidor
 * rodam com -Xlog:gc, gravando um log por processo e execução em
 * PASTA_RESULTADOS/gc-*.log. Serve para verificar se as pausas observadas
 * no trecho produtor -> broker coincidem com pausas de coleta de lixo da
 * JVM. Não altera a medição.
 */
@Component
public class Cenario1BaselineJava {

    private static final String TOPICO = "cenario1-baseline-java";
    private static final int MSGS = 10_000;

    private static final String PASTA_RESULTADOS = "resultado_cenario_1_1";
    private static final String CSV_DETALHADO_NOME = "resultados-kafka-cenario1-java-detalhado.csv";
    private static final String CSV_AGREGADO_NOME  = "resultados-kafka-cenario1-java.csv";
    private static final String CSV_INGESTAO_NOME  = "resultados-kafka-cenario1-ingestao-detalhado.csv";

    /** Grava logs de coleta de lixo (-Xlog:gc) de cada processo. */
    private static final boolean LOG_GC = false;

    private static final int TENTATIVAS_DELETE = 5;
    private static final long ESPERA_DELETE_MS = 300;

    /**
     * Tempo máximo de espera pelo STATUS=PRONTO do Consumidor. Com
     * subscribe(), esse tempo cobre construção do cliente + JoinGroup +
     * SyncGroup — folgado o bastante para não estourar em condições
     * normais, mas curto o bastante para falhar rápido se o pipe travar
     * de novo por algum outro motivo.
     */
    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;

    @Value("${benchmarking.execucoes}")
    private int totalExecucoes;

    @Value("${benchmarking.bootstrap-servers}")
    private String bootstrapServers;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd = ProcessHandle.current().info().command().orElse("java");

    private static class Resultado {
        double vazao, media, min, max, p95, p99;
        int recebidas;
    }

    public void executar() throws Exception {

        System.out.println();
        System.out.println("===============================================");
        System.out.println("=== Cenário 1 Kafka - Baseline              ===");
        System.out.println("===============================================");
        System.out.println();

        File pasta = new File(PASTA_RESULTADOS);
        if (pasta.mkdirs()) {
            System.out.println("Pasta de resultados criada: " + pasta.getAbsolutePath());
        } else {
            System.out.println("Gravando em: " + pasta.getAbsolutePath());
        }

        String csvDetalhado = PASTA_RESULTADOS + File.separator + CSV_DETALHADO_NOME;
        String csvAgregado  = PASTA_RESULTADOS + File.separator + CSV_AGREGADO_NOME;

        apagarArquivoAntigo(csvDetalhado);
        apagarArquivoAntigo(csvAgregado);
        apagarArquivoAntigo(PASTA_RESULTADOS + File.separator + CSV_INGESTAO_NOME);
        apagarLogsGc();

        criarTopico();

        // ---- AQUECIMENTO (descartado) ----
        System.out.println();
        System.out.println("-----------------------------------------------");
        System.out.println("Execução de AQUECIMENTO (não gravada, não contabilizada)");
        System.out.println("-----------------------------------------------");

        recriarTopico();
        Thread.sleep(1000);

        String csvAquecimentoTemp = PASTA_RESULTADOS + File.separator + "temp-aquecimento.csv";
        try {
            Resultado aquecimento = rodarParProdutorConsumidor(0, csvAquecimentoTemp);
            System.out.printf(Locale.US,
                    "Aquecimento concluído — Vazão: %.0f msg/s | Média: %.4f ms | Recebidas: %d%n",
                    aquecimento.vazao, aquecimento.media, aquecimento.recebidas);
        } finally {
            new File(csvAquecimentoTemp).delete();
        }

        System.out.println();
        System.out.println("Aguardando antes de iniciar as execuções contabilizadas...");
        Thread.sleep(3000);

        // ---- EXECUÇÕES REAIS ----
        try (PrintWriter agregado = new PrintWriter(new FileWriter(csvAgregado))) {

            agregado.println("cenario1-baseline-java");
            agregado.println("execucao,vazao_msg_s,latencia_media_ms,latencia_min_ms," +
                    "latencia_max_ms,latencia_p95_ms,latencia_p99_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {

                System.out.println();
                System.out.println("-----------------------------------------------");
                System.out.printf(Locale.US, "Execução %d/%d%n", exec, totalExecucoes);
                System.out.println("-----------------------------------------------");

                recriarTopico();
                Thread.sleep(1000);

                Resultado r = rodarParProdutorConsumidor(exec, csvDetalhado);

                if (r.recebidas != MSGS) {
                    throw new IllegalStateException(
                            "Quantidade incorreta de mensagens recebidas. " +
                                    "Esperado=" + MSGS + ", recebido=" + r.recebidas);
                }

                agregado.printf(Locale.US, "%d,%.2f,%.4f,%.4f,%.4f,%.4f,%.4f%n",
                        exec, r.vazao, r.media, r.min, r.max, r.p95, r.p99);
                agregado.flush();

                System.out.printf(Locale.US,
                        "%nResultado execução %d:%n" +
                                "  Vazão:   %.0f msg/s%n" +
                                "  Média:   %.4f ms%n" +
                                "  Mínimo:  %.4f ms%n" +
                                "  Máximo:  %.4f ms%n" +
                                "  P95:     %.4f ms%n" +
                                "  P99:     %.4f ms%n" +
                                "  Mensagens recebidas: %d%n",
                        exec, r.vazao, r.media, r.min, r.max, r.p95, r.p99, r.recebidas);

                if (exec < totalExecucoes) {
                    System.out.println();
                    System.out.println("Aguardando antes da próxima execução...");
                    Thread.sleep(5000);
                }
            }
        }

        System.out.println();
        System.out.println("===============================================");
        System.out.println("Benchmark Kafka cenário 1 concluído.");
        System.out.println("CSV agregado: " + csvAgregado);
        System.out.println("CSV detalhado: " + csvDetalhado);
        System.out.println("===============================================");
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

    /**
     * Sobe o Consumidor primeiro e espera o sinal STATUS=PRONTO — lido por
     * uma thread dedicada que drena o stdout dele desde o início, evitando
     * o bloqueio de pipe que causava o atraso original. Só depois disso o
     * Produtor é iniciado, garantindo que ele nunca "atropele" o Consumidor
     * enviando mensagens antes de haver alguém para recebê-las.
     */
    private Resultado rodarParProdutorConsumidor(int execLabel, String csvSaida) throws Exception {
        Process procConsumidor = new ProcessBuilder(montarComando(
                "com.benchmarking.kafka.cenario1_1.Consumidor",
                "gc-consumidor-exec" + execLabel,
                bootstrapServers, TOPICO,
                String.valueOf(MSGS), String.valueOf(execLabel),
                csvSaida
        )).redirectErrorStream(true).start();

        // Leitura assíncrona do stdout do Consumidor: uma thread dedicada
        // drena o stream continuamente, desde o início do processo — é
        // isso que impede o pipe de encher e bloquear a escrita do
        // Consumidor. O CountDownLatch libera o Produtor assim que a
        // linha STATUS=PRONTO aparece.
        CountDownLatch latchConsumidorPronto = new CountDownLatch(1);
        Map<String, String> saidaConsumidor = new ConcurrentHashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(2);

        Future<?> futureConsumidor = executor.submit(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(procConsumidor.getInputStream()))) {
                String linha;
                while ((linha = reader.readLine()) != null) {
                    System.out.println("  [CONSUMIDOR] " + linha);
                    if (linha.contains("STATUS=PRONTO")) {
                        latchConsumidorPronto.countDown();
                    }
                    if (linha.contains("=")) {
                        String[] partes = linha.split("=", 2);
                        saidaConsumidor.put(partes[0].trim(), partes[1].trim());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        if (!latchConsumidorPronto.await(TIMEOUT_CONSUMIDOR_PRONTO_S, TimeUnit.SECONDS)) {
            procConsumidor.destroyForcibly();
            executor.shutdownNow();
            throw new IllegalStateException(
                    "Timeout de " + TIMEOUT_CONSUMIDOR_PRONTO_S +
                            "s aguardando STATUS=PRONTO do Consumidor.");
        }

        // Agora que o consumidor já está pronto e no poll, o produtor é disparado.
        Process procProdutor = new ProcessBuilder(montarComando(
                "com.benchmarking.kafka.cenario1_1.Produtor",
                "gc-produtor-exec" + execLabel,
                bootstrapServers, TOPICO,
                String.valueOf(MSGS), String.valueOf(execLabel)
        )).redirectErrorStream(true).start();

        Map<String, String> saidaProdutor = lerSaida(procProdutor, "PRODUTOR");
        int codigoProdutor = procProdutor.waitFor();
        if (codigoProdutor != 0) {
            procConsumidor.destroyForcibly();
            executor.shutdownNow();
            throw new IllegalStateException("Produtor terminou com código " + codigoProdutor);
        }

        int codigoConsumidor = procConsumidor.waitFor();
        futureConsumidor.get(); // garante leitura completa do stream antes de ler saidaConsumidor
        executor.shutdown();

        if (codigoConsumidor != 0) {
            throw new IllegalStateException("Consumidor terminou com código " + codigoConsumidor);
        }

        long t0 = Long.parseLong(saidaProdutor.getOrDefault("T0", "0"));
        long t1 = Long.parseLong(saidaProdutor.getOrDefault("T1", "0"));

        // FIX: sem checagem de "t0 <= 0" — nanoTime() pode ser negativo
        // legitimamente. Só a duração (t1 > t0) importa.
        if (t1 <= t0) {
            throw new IllegalStateException(
                    "Timestamps inválidos retornados pelo produtor (duração não positiva)." +
                            " T0=" + t0 + ", T1=" + t1);
        }

        double tempoSegundos = (t1 - t0) / 1_000_000_000.0;

        Resultado r = new Resultado();
        r.vazao      = MSGS / tempoSegundos;
        r.media      = parseDouble(saidaConsumidor, "MEDIA");
        r.min        = parseDouble(saidaConsumidor, "MIN");
        r.max        = parseDouble(saidaConsumidor, "MAX");
        r.p95        = parseDouble(saidaConsumidor, "P95");
        r.p99        = parseDouble(saidaConsumidor, "P99");
        r.recebidas  = Integer.parseInt(saidaConsumidor.getOrDefault("RECEBIDAS", "0"));
        return r;
    }

    private Map<String, String> lerSaida(Process proc, String nomeProcesso) throws IOException {
        Map<String, String> mapa = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String linha;
            while ((linha = reader.readLine()) != null) {
                System.out.println("  [" + nomeProcesso + "] " + linha);
                if (linha.contains("=")) {
                    String[] partes = linha.split("=", 2);
                    mapa.put(partes[0].trim(), partes[1].trim());
                }
            }
        }
        return mapa;
    }

    private double parseDouble(Map<String, String> mapa, String chave) {
        String valor = mapa.get(chave);
        if (valor == null) {
            throw new IllegalStateException("Métrica não retornada pelo consumidor: " + chave);
        }
        return Double.parseDouble(valor);
    }

    /**
     * Tenta apagar algumas vezes antes de desistir. Locks de arquivo no
     * Windows (OneDrive sincronizando, Excel com o CSV aberto, antivírus
     * escaneando, handle não liberado por um subprocesso anterior) costumam
     * durar centenas de milissegundos, não indefinidamente.
     */
    private void apagarArquivoAntigo(String caminhoArquivo) throws IOException {
        File arquivo = new File(caminhoArquivo);
        if (!arquivo.exists()) return;

        for (int tentativa = 1; tentativa <= TENTATIVAS_DELETE; tentativa++) {
            if (arquivo.delete()) {
                System.out.println("Arquivo antigo removido: " + caminhoArquivo);
                return;
            }
            if (tentativa < TENTATIVAS_DELETE) {
                System.out.printf("Falha ao apagar (tentativa %d/%d), aguardando %d ms...%n",
                        tentativa, TENTATIVAS_DELETE, ESPERA_DELETE_MS);
                try {
                    Thread.sleep(ESPERA_DELETE_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrompido ao aguardar liberação do arquivo: "
                            + arquivo.getAbsolutePath(), e);
                }
            }
        }

        throw new IOException("Não foi possível apagar arquivo antigo após " + TENTATIVAS_DELETE
                + " tentativas: " + arquivo.getAbsolutePath()
                + " — verifique se ele está aberto em outro programa (Excel, editor),"
                + " se o OneDrive está sincronizando, ou se há um java.exe de execução anterior ainda ativo.");
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

    private void criarTopico() throws Exception {
        try (AdminClient admin = adminClient()) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPICO, 1, (short) 1))).all().get();
                System.out.println("Tópico criado: " + TOPICO);
            } catch (Exception e) {
                System.out.println("Tópico já existe ou não precisou ser criado: " + TOPICO);
            }
        }
    }

    private void recriarTopico() throws Exception {
        try (AdminClient admin = adminClient()) {
            try {
                admin.deleteTopics(List.of(TOPICO)).all().get();
            } catch (Exception ignored) {}

            Thread.sleep(1000);

            int tentativas = 0;
            while (true) {
                try {
                    admin.createTopics(List.of(new NewTopic(TOPICO, 1, (short) 1))).all().get();
                    break;
                } catch (Exception e) {
                    tentativas++;
                    if (tentativas >= 10) {
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