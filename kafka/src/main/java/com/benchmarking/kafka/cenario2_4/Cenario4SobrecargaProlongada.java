package com.benchmarking.kafka.cenario2_4;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível intermediário — Cenário 4: Sobrecarga prolongada (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: observar o que cada tecnologia faz quando o produtor envia, de
 * forma contínua, mais rápido do que o consumidor consegue processar
 * (KLEPPMANN; RICCOMINI, 2026: descartar, armazenar em buffer ou aplicar
 * backpressure).
 *
 * Desenho:
 *   - Produtor a 1.000 msg/s durante 20 s (20.000 mensagens de 1 KB).
 *   - Consumidor com 5 ms por mensagem (~175–200 msg/s): a fila cresce
 *     ~800 msg/s enquanto o produtor envia, e depois é drenada.
 *   - Buffer do consumidor: max.poll.records = 100 e
 *     max.partition.fetch.bytes = 112640 (~100 mensagens), para que o cliente
 *     guarde em memória o mesmo que o prefetch de 100 do RabbitMQ.
 *   - Duas políticas, intercaladas:
 *       sem-limite : tópico com retenção padrão. O log simplesmente cresce.
 *       com-limite : tópico com retention.bytes = 2 MB (segment.bytes = 1 MB).
 *                    Ao passar do limite, o broker apaga os segmentos mais
 *                    antigos, mesmo que o consumidor ainda não os tenha lido.
 *   - A cada 1 s, uma linha da série temporal: enviadas, consumidas, fila
 *     no broker (log end offset − max(commit, início do log)) e bytes do
 *     log em disco (describeLogDirs).
 *
 * A política com-limite exige que o broker verifique a retenção com
 * frequência. O padrão (log.retention.check.interval.ms = 5 min) é maior
 * que a execução inteira; por isso o orquestrador confere essa configuração
 * e para com uma mensagem explicativa se ela estiver alta.
 *
 * Um tópico NOVO por execução (garantias-cenario4-<política>-e<n>-<ms>):
 *   - a criação não depende do tempo que o broker leva para apagar o tópico
 *     anterior (a exclusão no Kafka é assíncrona);
 *   - um cliente órfão de uma execução anterior só enxerga o tópico antigo,
 *     que não é mais medido; ele não recria nem contamina o tópico novo.
 *   O tópico de cada execução é apagado no fim dela; os que sobrarem de
 *   sessões interrompidas são apagados no início.
 *
 * Proteção contra processos órfãos (que gastam CPU e carga do broker durante
 * a medição):
 *   - os subprocessos encerram sozinhos quando o stdin fecha (o orquestrador
 *     morreu) e o orquestrador mata os filhos num shutdown hook;
 *   - antes de começar, procura grupos bench-sobrecarga-* com membros ativos
 *     e para se encontrar algum;
 *   - depois de criar o tópico com-limite, confere retention.bytes.
 *
 * É um cenário de COMPORTAMENTO, com séries temporais longas (~2 min cada):
 * 1 execução de aquecimento por política (descartada, fora dos CSVs) + 10
 * válidas por política, em ordem rotativa, como nos outros cenários do
 * nível intermediário.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-kafka-sobrecarga.csv        (1 linha por execução e política)
 *   resultados-kafka-sobrecarga-serie.csv  (1 linha por segundo)
 */
@Component
public class Cenario4SobrecargaProlongada {

    // cada execução usa <PREFIXO_TOPICO>-<política>-e<exec>-<ms>
    private static final String PREFIXO_TOPICO = "garantias-cenario4";
    private static final String PREFIXO_GRUPO = "bench-sobrecarga-";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_cenario_2_4";
    private static final String CSV_RESUMO = "resultados-kafka-sobrecarga.csv";
    private static final String CSV_SERIE  = "resultados-kafka-sobrecarga-serie.csv";

    private static final int  TAXA_PRODUCAO = 1_000;      // msg/s
    private static final int  DURACAO_PRODUCAO_S = 20;
    private static final int  MSGS = TAXA_PRODUCAO * DURACAO_PRODUCAO_S;
    private static final long PROC_MS = 5;               // consumidor lento
    private static final int  MAX_POLL = 100;            // igual ao prefetch do RabbitMQ
    // ~100 mensagens de 1 KB com cabeçalhos e overhead de lote: o buffer do
    // cliente fica do tamanho do prefetch do RabbitMQ (ver ConsumidorSustentado)
    private static final int  MAX_PARTITION_FETCH_BYTES = 112_640;

    private static final String[] POLITICAS = {"sem-limite", "com-limite"};
    private static final long RETENCAO_BYTES = 2L * 1024 * 1024;   // 2 MB
    private static final long SEGMENTO_BYTES = 1024 * 1024;        // 1 MB
    private static final long MAX_INTERVALO_CHECAGEM_MS = 10_000;

    private static final long TIMEOUT_PROCESSO_S = 1_800;
    // session.timeout.ms padrão do consumidor (45 s) + folga: um consumidor que
    // morreu sem sair do grupo continua listado como membro até esse prazo.
    private static final long ESPERA_ORFAOS_MS = 60_000;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.kafka.cenario2_4.ConsumidorSustentado";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.kafka.cenario2_4.ProdutorSustentado";

    @Value("${benchmarking.execucoes:10}")    private int totalExecucoes;
    @Value("${benchmarking.bootstrap-servers}") private String bootstrapServers;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Intermediário — Cenário 4 Kafka — Sobrecarga prolongada");
        System.out.println("====================================================================");
        System.out.printf("Produtor: %d msg/s por %d s | consumidor: %d ms/msg | políticas: %s%n",
                TAXA_PRODUCAO, DURACAO_PRODUCAO_S, PROC_MS, Arrays.toString(POLITICAS));

        // Ctrl+C / encerramento normal da JVM: mata os subprocessos ainda vivos.
        // (Kill forçado não executa hooks; para esse caso os filhos vigiam o stdin.)
        Thread gancho = new Thread(() -> ProcessHandle.current().descendants()
                .forEach(ProcessHandle::destroyForcibly), "mata-subprocessos-cenario4");
        Runtime.getRuntime().addShutdownHook(gancho);

        try {
            conferirIntervaloRetencao();
            conferirConsumidoresOrfaos();
            apagarTopicosAntigos();

            File pasta = new File(PASTA_RESULTADOS);
            if (!pasta.exists() && !pasta.mkdirs()) throw new IOException("Não foi possível criar " + pasta.getAbsolutePath());
            System.out.println("Gravando em: " + pasta.getAbsolutePath());
            String csvResumo = PASTA_RESULTADOS + File.separator + CSV_RESUMO;
            String csvSerie  = PASTA_RESULTADOS + File.separator + CSV_SERIE;
            apagarSeExistir(csvResumo);
            apagarSeExistir(csvSerie);

            try (PrintWriter resumo = new PrintWriter(new FileWriter(csvResumo));
                 PrintWriter serie = new PrintWriter(new FileWriter(csvSerie))) {
                resumo.println("execucao,ordem,politica,enviadas,rejeicoes,recebidas,unicas,perdidas,saltos," +
                        "duracao_producao_s,taxa_producao_efetiva_msg_s,pico_fila,pico_armazenamento_bytes,tempo_total_s");
                serie.println("execucao,politica,tempo_s,enviadas,consumidas,fila,armazenamento_bytes");

                for (String politica : POLITICAS) {
                    System.out.printf("%nAquecimento (descartado) — %s%n", politica);
                    rodar(politica, 0, 0, null, null);
                    Thread.sleep(5_000);
                }

                for (int exec = 1; exec <= totalExecucoes; exec++) {
                    for (int pos = 0; pos < POLITICAS.length; pos++) {
                        String politica = POLITICAS[(pos + exec - 1) % POLITICAS.length];
                        System.out.printf("%nExecução %d/%d — %s (posição %d)%n", exec, totalExecucoes, politica, pos + 1);
                        rodar(politica, exec, pos + 1, resumo, serie);
                        Thread.sleep(5_000);
                    }
                }
            }

            System.out.println();
            System.out.println("[SUCESSO] Intermediário — Cenário 4 Kafka concluído.");
            System.out.println("  " + new File(csvResumo).getAbsolutePath());
            System.out.println("  " + new File(csvSerie).getAbsolutePath());
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(gancho);
            } catch (IllegalStateException ignored) {
                // a JVM já está encerrando: o hook vai rodar de qualquer forma
            }
        }
    }

    // ------------------------------------------------------------------
    // Uma execução
    // ------------------------------------------------------------------
    private void rodar(String politica, int exec, int ordem, PrintWriter resumo, PrintWriter serie) throws Exception {
        String sufixo  = politica + "-e" + exec + "-" + System.currentTimeMillis();
        String topico  = PREFIXO_TOPICO + "-" + sufixo;
        String groupId = PREFIXO_GRUPO + sufixo;
        System.out.println("  tópico: " + topico);
        criarTopico(topico, politica.equals("com-limite"));

        AtomicInteger enviadas = new AtomicInteger();
        AtomicInteger consumidas = new AtomicInteger();
        Map<String, String> saidaP = new ConcurrentHashMap<>();
        Map<String, String> saidaC = new ConcurrentHashMap<>();
        CountDownLatch pronto = new CountDownLatch(1);
        ExecutorService ex = Executors.newFixedThreadPool(2);
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        Process consumidor = null;
        Process produtor = null;

        long[] picoFila = {0}, picoBytes = {0};
        try (AdminClient admin = adminClient()) {
            // O stdin dos filhos fica aberto (pipe) durante toda a execução: o EOF
            // só chega se este processo morrer, e aí os filhos encerram sozinhos.
            consumidor = new ProcessBuilder(comando(CONSUMIDOR_CLASS, bootstrapServers, topico, groupId,
                    String.valueOf(MSGS), String.valueOf(PROC_MS), String.valueOf(MAX_POLL),
                    String.valueOf(MAX_PARTITION_FETCH_BYTES)))
                    .redirectErrorStream(true).start();
            final Process c = consumidor;
            Future<?> leitorC = ex.submit(() -> ler(c, "CONSUMIDOR", saidaC, pronto, "CONSUMIDAS", consumidas));

            if (!pronto.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");

            long inicio = System.nanoTime();
            monitor.scheduleAtFixedRate(() -> {
                try {
                    long[] f = filaEArmazenamento(admin, topico, groupId);
                    picoFila[0] = Math.max(picoFila[0], f[0]);
                    picoBytes[0] = Math.max(picoBytes[0], f[1]);
                    if (serie != null) synchronized (serie) {   // null = aquecimento: não grava
                        serie.printf(Locale.US, "%d,%s,%.1f,%d,%d,%d,%d%n", exec, politica,
                                (System.nanoTime() - inicio) / 1e9, enviadas.get(), consumidas.get(), f[0], f[1]);
                        serie.flush();
                    }
                } catch (Exception e) {
                    System.err.println("  [AVISO] falha no monitor: " + e.getMessage());
                }
            }, 0, 1, TimeUnit.SECONDS);

            produtor = new ProcessBuilder(comando(PRODUTOR_CLASS, bootstrapServers, topico,
                    String.valueOf(MSGS), String.valueOf(TAXA_PRODUCAO), String.valueOf(exec)))
                    .redirectErrorStream(true).start();
            ler(produtor, "PRODUTOR", saidaP, null, "ENVIADAS", enviadas);
            esperar(produtor, "produtor");

            enviar(consumidor, "PRODUTOR_TERMINOU");
            esperar(consumidor, "consumidor");
            leitorC.get();
            monitor.shutdown();
            monitor.awaitTermination(5, TimeUnit.SECONDS);

            long t0 = Long.parseLong(obrigatorio(saidaP, "T0"));
            long t1 = Long.parseLong(obrigatorio(saidaP, "T1"));
            int env = Integer.parseInt(obrigatorio(saidaP, "ENVIADAS_TOTAL"));
            int rej = Integer.parseInt(obrigatorio(saidaP, "REJEITADAS"));
            int rec = Integer.parseInt(obrigatorio(saidaC, "RECEBIDAS"));
            int uni = Integer.parseInt(obrigatorio(saidaC, "UNICAS"));
            int sal = Integer.parseInt(obrigatorio(saidaC, "SALTOS"));
            long fim = Long.parseLong(obrigatorio(saidaC, "ULTIMO_FIM"));
            if (env != MSGS) throw new IllegalStateException("O produtor confirmou " + env + " de " + MSGS);
            if (politica.equals("sem-limite") && uni != MSGS) {
                throw new IllegalStateException("Sem limite, o consumidor deveria receber tudo: " + uni + " de " + MSGS);
            }
            double durProd = (t1 - t0) / 1e9;
            if (resumo != null) synchronized (resumo) {
                resumo.printf(Locale.US, "%d,%d,%s,%d,%d,%d,%d,%d,%d,%.3f,%.2f,%d,%d,%.3f%n", exec, ordem, politica,
                        env, rej, rec, uni, MSGS - uni, sal, durProd, env / durProd, picoFila[0], picoBytes[0],
                        (fim - t0) / 1e9);
                resumo.flush();
            }
            System.out.printf(Locale.US, "  → produção: %.0f msg/s | pico da fila: %d | perdidas: %d%n",
                    env / durProd, picoFila[0], MSGS - uni);
        } finally {
            monitor.shutdownNow();
            if (consumidor != null && consumidor.isAlive()) consumidor.destroyForcibly();
            if (produtor != null && produtor.isAlive()) produtor.destroyForcibly();
            ex.shutdownNow();
            apagarTopico(topico);
        }
    }

    /** {fila no broker, bytes do log em disco}. */
    private long[] filaEArmazenamento(AdminClient admin, String topico, String groupId) throws Exception {
        TopicPartition tp = new TopicPartition(topico, 0);
        long fim = admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get(5, TimeUnit.SECONDS).get(tp).offset();
        long ini = admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get(5, TimeUnit.SECONDS).get(tp).offset();
        Map<TopicPartition, OffsetAndMetadata> c = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        long commit = (c != null && c.get(tp) != null) ? c.get(tp).offset() : 0;
        long fila = Math.max(0, fim - Math.max(commit, ini));

        long bytes = 0;
        List<Integer> ids = new ArrayList<>();
        for (Node n : admin.describeCluster().nodes().get(5, TimeUnit.SECONDS)) ids.add(n.id());
        for (Map<String, LogDirDescription> dirs : admin.describeLogDirs(ids).allDescriptions().get(5, TimeUnit.SECONDS).values()) {
            for (LogDirDescription d : dirs.values()) {
                ReplicaInfo ri = d.replicaInfos().get(tp);
                if (ri != null) bytes += ri.size();
            }
        }
        return new long[]{fila, bytes};
    }

    /** A política com-limite só funciona se o broker checar a retenção com frequência. */
    private void conferirIntervaloRetencao() throws Exception {
        try (AdminClient admin = adminClient()) {
            Node n = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).iterator().next();
            ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(n.id()));
            Config cfg = admin.describeConfigs(List.of(broker)).all().get(10, TimeUnit.SECONDS).get(broker);
            ConfigEntry e = cfg.get("log.retention.check.interval.ms");
            long v = e == null ? 300_000 : Long.parseLong(e.value());
            System.out.println("log.retention.check.interval.ms do broker = " + v + " ms");
            if (v > MAX_INTERVALO_CHECAGEM_MS) {
                throw new IllegalStateException("O broker checa a retenção a cada " + v / 1000 + " s, mais do que a"
                        + " execução inteira. Acrescente no docker-compose do Kafka a variável"
                        + " KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 5000, recrie o container"
                        + " (docker compose up -d --force-recreate kafka) e rode de novo.");
            }
        }
    }

    /**
     * Procura consumidores de execuções anteriores ainda ativos (grupos
     * bench-sobrecarga-* com membros). Com um tópico novo por execução, eles
     * não contaminam os dados, mas consomem CPU e carga do broker durante a
     * medição. Um consumidor que acabou de morrer continua listado até o
     * session.timeout.ms expirar, por isso espera até ESPERA_ORFAOS_MS antes
     * de desistir.
     */
    private void conferirConsumidoresOrfaos() throws Exception {
        long ate = System.currentTimeMillis() + ESPERA_ORFAOS_MS;
        boolean avisou = false;
        try (AdminClient admin = adminClient()) {
            while (true) {
                List<String> grupos = new ArrayList<>();
                for (ConsumerGroupListing g : admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS)) {
                    if (g.groupId().startsWith(PREFIXO_GRUPO)) grupos.add(g.groupId());
                }
                List<String> ativos = new ArrayList<>();
                if (!grupos.isEmpty()) {
                    for (ConsumerGroupDescription d : admin.describeConsumerGroups(grupos).all()
                            .get(10, TimeUnit.SECONDS).values()) {
                        if (!d.members().isEmpty()) ativos.add(d.groupId() + " (" + d.members().size() + " membro(s))");
                    }
                }
                if (ativos.isEmpty()) return;

                if (System.currentTimeMillis() > ate) {
                    throw new IllegalStateException("Há consumidores de execuções anteriores ainda conectados: " + ativos
                            + ". Eles disputariam CPU e broker com a medição. Rode 'jps -l', encerre os processos"
                            + " ConsumidorSustentado/ProdutorSustentado ('taskkill /F /PID <pid>' no Windows,"
                            + " 'kill -9 <pid>' no Linux) e rode de novo.");
                }
                if (!avisou) {
                    System.out.println("  [AVISO] consumidores de execuções anteriores ainda ativos: " + ativos
                            + " — aguardando até " + ESPERA_ORFAOS_MS / 1000 + " s para saírem do grupo...");
                    avisou = true;
                }
                Thread.sleep(2_000);
            }
        }
    }

    private static boolean ehTopicoDoCenario(String nome) {
        return nome.equals(PREFIXO_TOPICO) || nome.startsWith(PREFIXO_TOPICO + "-");
    }

    /**
     * Apaga os tópicos deste cenário que sobraram de sessões interrompidas
     * (inclusive o garantias-cenario4 de nome fixo das versões anteriores).
     * Não espera a exclusão terminar: os tópicos novos têm outros nomes.
     */
    private void apagarTopicosAntigos() throws Exception {
        try (AdminClient admin = adminClient()) {
            List<String> antigos = new ArrayList<>();
            for (String nome : admin.listTopics().names().get(30, TimeUnit.SECONDS)) {
                if (ehTopicoDoCenario(nome)) antigos.add(nome);
            }
            if (antigos.isEmpty()) return;
            System.out.println("Apagando " + antigos.size() + " tópico(s) de sessões anteriores: " + antigos);
            try {
                admin.deleteTopics(antigos).all().get(30, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                System.out.println("  [AVISO] nem todos os tópicos antigos foram apagados: " + e.getCause().getMessage());
            }
        }
    }

    /**
     * Cria o tópico desta execução. O nome é novo, então não há espera pela
     * exclusão de um tópico anterior nem TopicExistsException. Na política
     * com-limite, confere a configuração.
     */
    private void criarTopico(String nome, boolean comLimite) throws Exception {
        try (AdminClient admin = adminClient()) {
            NewTopic t = new NewTopic(nome, 1, (short) 1);
            if (comLimite) {
                t.configs(Map.of("retention.bytes", String.valueOf(RETENCAO_BYTES),
                        "segment.bytes", String.valueOf(SEGMENTO_BYTES)));
            }
            try {
                admin.createTopics(List.of(t)).all().get(30, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Não foi possível criar o tópico " + nome, e);
            }
            if (comLimite) conferirConfigTopico(admin, nome);
        }
    }

    /**
     * Confere que o tópico com-limite ficou com retention.bytes e segment.bytes
     * esperados (protege os dados contra um tópico com a configuração padrão).
     * Logo depois da criação o broker pode ainda não conhecer o tópico, por isso
     * tenta por até 10 s.
     */
    private void conferirConfigTopico(AdminClient admin, String nome) throws Exception {
        ConfigResource rt = new ConfigResource(ConfigResource.Type.TOPIC, nome);
        long ate = System.currentTimeMillis() + 10_000;
        Config cfg;
        while (true) {
            try {
                cfg = admin.describeConfigs(List.of(rt)).all().get(10, TimeUnit.SECONDS).get(rt);
                break;
            } catch (ExecutionException e) {
                if (System.currentTimeMillis() > ate) throw e;
                Thread.sleep(500);
            }
        }
        String retencao = valor(cfg, "retention.bytes");
        String segmento = valor(cfg, "segment.bytes");
        if (!String.valueOf(RETENCAO_BYTES).equals(retencao) || !String.valueOf(SEGMENTO_BYTES).equals(segmento)) {
            throw new IllegalStateException("O tópico " + nome + " ficou com retention.bytes=" + retencao
                    + " e segment.bytes=" + segmento + " (esperado " + RETENCAO_BYTES + " e " + SEGMENTO_BYTES + ").");
        }
    }

    /** Apaga o tópico da execução, sem esperar o broker terminar. Falha vira aviso. */
    private void apagarTopico(String nome) {
        try (AdminClient admin = adminClient()) {
            admin.deleteTopics(List.of(nome)).all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("  [AVISO] não foi possível apagar o tópico " + nome + " (será apagado no próximo início): "
                    + e.getMessage());
        }
    }

    private static String valor(Config cfg, String chave) {
        ConfigEntry e = cfg.get(chave);
        return e == null ? null : e.value();
    }

    private AdminClient adminClient() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put("auto.include.jmx.reporter", "false");
        config.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        return AdminClient.create(config);
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------
    /** Lê a saída; guarda chave=valor e atualiza o contador da série temporal. */
    private static Void ler(Process p, String nome, Map<String, String> destino, CountDownLatch pronto,
                            String chaveContador, AtomicInteger contador) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String linha;
            while ((linha = r.readLine()) != null) {
                int i = linha.indexOf('=');
                String chave = i > 0 ? linha.substring(0, i).trim() : "";
                if (chave.equals(chaveContador)) {
                    contador.set(Integer.parseInt(linha.substring(i + 1).trim()));
                    continue;   // não polui o console com o relato de cada segundo
                }
                System.out.println("  [" + nome + "] " + linha);
                if (pronto != null && linha.contains("STATUS=PRONTO")) pronto.countDown();
                if (i > 0) destino.put(chave, linha.substring(i + 1).trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void enviar(Process p, String comando) throws IOException {
        OutputStream os = p.getOutputStream();
        os.write((comando + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    private static void esperar(Process proc, String nome) throws InterruptedException {
        if (!proc.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) throw new IllegalStateException("Timeout aguardando o " + nome);
        if (proc.exitValue() != 0) {
            throw new IllegalStateException("O " + nome + " terminou com código " + proc.exitValue() + " — veja as linhas acima.");
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

    private static String obrigatorio(Map<String, String> saida, String chave) {
        String v = saida.get(chave);
        if (v == null || v.isBlank()) throw new IllegalStateException("Métrica não retornada pelo subprocesso: " + chave);
        return v;
    }

    private static void apagarSeExistir(String caminho) throws IOException {
        File f = new File(caminho);
        if (f.exists() && !f.delete()) {
            throw new IOException("Não foi possível apagar: " + f.getAbsolutePath() + " (o arquivo está aberto em outro programa?)");
        }
    }
}
