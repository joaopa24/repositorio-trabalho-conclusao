package com.benchmarking.kafka.cenario1_2;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Cenário 2 — Consumidor Lento (Kafka) — orquestrador.
 *
 * Pergunta do cenário: como o modelo PULL entrega mensagens a um consumidor
 * mais lento que o produtor, e se max.poll.records altera esse custo.
 *
 * Configuração:
 *   - 1.000 mensagens de 1 KB por execução, produtor na velocidade máxima;
 *   - consumidor com 5 ms de processamento por mensagem (200 msg/s teóricos);
 *   - max.poll.records = 1, 3, 10, 30 e 100 (mesmos valores usados para o
 *     prefetch no RabbitMQ, seguindo os valores de MacMullen, 2014);
 *   - 1 execução de aquecimento descartada por valor;
 *   - 10 execuções válidas por valor, INTERCALADAS: em cada rodada todos os
 *     valores são executados, e a ordem gira a cada rodada, para que
 *     nenhuma configuração seja sempre a primeira ou a última.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   - resultados-kafka-cenario2.csv           (1 linha por execução)
 *   - resultados-kafka-cenario2-detalhado.csv (1 linha por mensagem)
 *
 * Cada execução: recria o tópico (1 partição, RF 1) -> inicia o consumidor
 * -> espera STATUS=PRONTO (partição atribuída) -> inicia o produtor. A saída
 * do consumidor é lida em uma thread própria desde o início, para o pipe de
 * stdout nunca encher e travar o processo.
 */
@Component
public class Cenario2ConsumidorLento {

    private static final String TOPICO   = "cenario2-consumidor-lento";
    private static final int    MSGS     = 1_000;
    private static final int    DELAY_MS = 5;
    private static final int[]  VALORES  = {1, 3, 10, 30, 100};

    private static final String PASTA_RESULTADOS = "resultado_cenario_1_2";
    private static final String CSV_AGREGADO     = "resultados-kafka-cenario2.csv";
    private static final String CSV_DETALHADO    = "resultados-kafka-cenario2-detalhado.csv";

    private static final long PAUSA_ENTRE_EXECUCOES_MS   = 2_000;
    private static final long TIMEOUT_CONSUMIDOR_PRONTO_S = 60;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.kafka.cenario1_2.ConsumidorLento";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.kafka.cenario1_2.ProdutorCenario2";

    @Value("${benchmarking.execucoes}")
    private int totalExecucoes;

    @Value("${benchmarking.bootstrap-servers}")
    private String bootstrapServers;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd = ProcessHandle.current().info().command().orElse("java");

    public void executar() throws Exception {
        System.out.println("\n=== Cenário 2 Kafka — Consumidor Lento ===");
        System.out.printf("Mensagens: %d | Delay: %d ms/msg | max.poll.records: %s | %d execuções por valor%n",
                MSGS, DELAY_MS, Arrays.toString(VALORES), totalExecucoes);

        File pasta = new File(PASTA_RESULTADOS);
        pasta.mkdirs();
        if (!pasta.isDirectory()) {
            throw new IOException("Não foi possível criar a pasta: " + pasta.getAbsolutePath());
        }

        String csvAgregado  = PASTA_RESULTADOS + File.separator + CSV_AGREGADO;
        String csvDetalhado = PASTA_RESULTADOS + File.separator + CSV_DETALHADO;
        apagarSeExistir(csvAgregado);
        apagarSeExistir(csvDetalhado);

        // ---- aquecimento: 1 execução descartada por valor ----
        System.out.println("\n--- Aquecimento (descartado) ---");
        for (int valor : VALORES) {
            String csvTemp = PASTA_RESULTADOS + File.separator + "temp-aquecimento-" + valor + ".csv";
            recriarTopico();
            Thread.sleep(1_000);
            Map<String, String> cons = new ConcurrentHashMap<>();
            Map<String, String> prod = new HashMap<>();
            rodarPar(valor, 0, csvTemp, prod, cons);
            System.out.printf(Locale.US, "  max.poll.records=%d → utilização %s%% (descartado)%n",
                    valor, cons.get("UTILIZACAO"));
            apagarSeExistir(csvTemp);
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        // ---- execuções válidas, intercaladas e com ordem rotativa ----
        try (PrintWriter agregado = new PrintWriter(new FileWriter(csvAgregado, false))) {
            agregado.println("execucao,max_poll_records,vazao_produtor_msg_s,vazao_consumidor_msg_s,"
                    + "utilizacao_consumidor_pct,ocioso_medio_ms,ocioso_mediana_ms,latencia_media_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int k = 0; k < VALORES.length; k++) {
                    int valor = VALORES[(k + exec - 1) % VALORES.length];

                    System.out.printf("%nExecução %d/%d — max.poll.records=%d%n", exec, totalExecucoes, valor);
                    recriarTopico();
                    Thread.sleep(1_000);

                    Map<String, String> cons = new ConcurrentHashMap<>();
                    Map<String, String> prod = new HashMap<>();
                    rodarPar(valor, exec, csvDetalhado, prod, cons);

                    agregado.printf(Locale.US, "%d,%d,%s,%s,%s,%s,%s,%s%n",
                            exec, valor,
                            obrigatorio(prod, "VAZAO"),
                            obrigatorio(cons, "VAZAO_CONSUMIDOR"),
                            obrigatorio(cons, "UTILIZACAO"),
                            obrigatorio(cons, "OCIOSO_MEDIO"),
                            obrigatorio(cons, "OCIOSO_MEDIANA"),
                            obrigatorio(cons, "LATENCIA_MEDIA"));
                    agregado.flush();

                    System.out.printf("  → utilização %s%% | ocioso médio %s ms | consumidor %s msg/s%n",
                            cons.get("UTILIZACAO"), cons.get("OCIOSO_MEDIO"), cons.get("VAZAO_CONSUMIDOR"));

                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }

        System.out.println("\nCenário 2 Kafka concluído.");
        System.out.println("  Agregado:  " + csvAgregado);
        System.out.println("  Detalhado: " + csvDetalhado);
    }

    /** Sobe o consumidor, espera STATUS=PRONTO, sobe o produtor e coleta as saídas. */
    private void rodarPar(int maxPollRecords, int exec, String csvDetalhado,
                          Map<String, String> saidaProdutor,
                          Map<String, String> saidaConsumidor) throws Exception {

        Process procConsumidor = new ProcessBuilder(montarComando(CONSUMIDOR_CLASS,
                bootstrapServers, TOPICO, String.valueOf(MSGS), String.valueOf(exec),
                String.valueOf(DELAY_MS), String.valueOf(maxPollRecords), csvDetalhado))
                .redirectErrorStream(true).start();

        CountDownLatch pronto = new CountDownLatch(1);
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

            Process procProdutor = new ProcessBuilder(montarComando(PRODUTOR_CLASS,
                    bootstrapServers, TOPICO, String.valueOf(MSGS), String.valueOf(exec)))
                    .redirectErrorStream(true).start();

            try (BufferedReader r = new BufferedReader(new InputStreamReader(procProdutor.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    System.out.println("  [PRODUTOR] " + linha);
                    guardarChaveValor(linha, saidaProdutor);
                }
            }

            int exitProdutor = procProdutor.waitFor();
            if (exitProdutor != 0) {
                procConsumidor.destroyForcibly();
                throw new IllegalStateException("Produtor terminou com código " + exitProdutor);
            }

            int exitConsumidor = procConsumidor.waitFor();
            leitorConsumidor.get();
            if (exitConsumidor != 0) {
                throw new IllegalStateException("Consumidor terminou com código " + exitConsumidor);
            }
        } finally {
            executor.shutdownNow();
        }
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

    private List<String> montarComando(String classe, String... argumentos) {
        List<String> cmd = new ArrayList<>(List.of(javaCmd, "-cp", classpath, classe));
        cmd.addAll(Arrays.asList(argumentos));
        return cmd;
    }

    private static void apagarSeExistir(String caminho) throws IOException {
        File f = new File(caminho);
        if (f.exists() && !f.delete()) {
            throw new IOException("Não foi possível apagar: " + f.getAbsolutePath()
                    + " (o arquivo está aberto em outro programa?)");
        }
    }

    private void recriarTopico() throws Exception {
        try (AdminClient admin = adminClient()) {
            try {
                admin.deleteTopics(List.of(TOPICO)).all().get();
            } catch (Exception ignorado) {
                // o tópico pode não existir ainda
            }
            Thread.sleep(1_000); // a exclusão é assíncrona

            Exception ultimoErro = null;
            for (int tentativa = 1; tentativa <= 10; tentativa++) {
                try {
                    admin.createTopics(List.of(new NewTopic(TOPICO, 1, (short) 1))).all().get();
                    return;
                } catch (Exception e) {
                    ultimoErro = e;
                    Thread.sleep(500);
                }
            }
            throw new IllegalStateException("Não foi possível recriar o tópico " + TOPICO, ultimoErro);
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
