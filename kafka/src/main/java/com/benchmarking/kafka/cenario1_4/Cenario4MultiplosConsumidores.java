package com.benchmarking.kafka.cenario1_4;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cenário 4 — Múltiplos consumidores (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: medir como as mensagens são divididas entre consumidores
 * concorrentes e quanto a vazão de consumo aumenta de 1 para 2, 4 e 6
 * consumidores, quando o PROCESSAMENTO de cada mensagem é o gargalo.
 *
 * Desenho:
 *   - 1 produtor envia 6.000 mensagens de 1 KB, sem controle de taxa: tudo
 *     entra no tópico logo no início e mede-se quanto tempo os N
 *     consumidores levam para tratar tudo.
 *   - Cada consumidor processa cada mensagem em 5 ms (Thread.sleep).
 *   - Tópico recriado em cada execução com uma partição por consumidor, e a
 *     mensagem i vai para a partição i % N (divisão exatamente igual).
 *   - Configuração extra 6c-4p: 6 consumidores e só 4 partições. Mostra o
 *     limite do Kafka: 2 consumidores ficam sem partição e sem mensagens.
 *   - max.poll.records = 100, o mesmo valor do prefetch do RabbitMQ.
 *   - 1 rodada de aquecimento (todas as configurações, descartada) e depois
 *     as configurações intercaladas em ordem rotativa.
 *
 * Métricas por execução:
 *   vazão  = mensagens ÷ (último fim de processamento − primeiro envio)
 *   espera = início do processamento − envio  (fila no broker + buffer do cliente)
 *   divisão = quantas mensagens cada consumidor tratou
 *
 * Antes de soltar o produtor, o orquestrador espera os N STATUS=PRONTO e
 * confere pelo AdminClient que o grupo tem N membros e que todas as
 * partições estão atribuídas, em duas leituras seguidas iguais. Assim
 * nenhum rebalanceamento acontece durante a medição.
 *
 * Qualquer problema (contagem errada, mensagem duplicada, processo com
 * código de saída diferente de 0, tópico que não pôde ser recriado) para a
 * campanha. Nada vira zero no CSV.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-kafka-cenario4.csv             (1 linha por execução e configuração)
 *   resultados-kafka-cenario4-detalhado.csv   (1 linha por mensagem)
 */
@Component
public class Cenario4MultiplosConsumidores {

    private static final String TOPICO = "cenario4-multiplos-consumidores";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_cenario_1_4";
    private static final String CSV_AGREGADO  = "resultados-kafka-cenario4.csv";
    private static final String CSV_DETALHADO = "resultados-kafka-cenario4-detalhado.csv";

    private static final int  TOTAL_MSGS       = 6_000;   // divide por 1, 2, 4 e 6
    private static final int  TAMANHO_BYTES    = 1_024;
    private static final long PROC_MS          = 5;       // processamento por mensagem
    private static final int  MAX_POLL_RECORDS = 100;     // igual ao prefetch do RabbitMQ

    /** consumidores, partições */
    private record Config(int consumidores, int particoes) {
        String rotulo() { return consumidores + "c-" + particoes + "p"; }
    }

    private static final boolean EXECUTAR_6C_4P = true;   // configuração extra
    private static final List<Config> CONFIGS = configs();

    private static List<Config> configs() {
        List<Config> c = new ArrayList<>(List.of(
                new Config(1, 1), new Config(2, 2), new Config(4, 4), new Config(6, 6)));
        if (EXECUTAR_6C_4P) c.add(new Config(6, 4));
        return List.copyOf(c);
    }

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 3_000;
    private static final long TIMEOUT_PRONTO_S   = 60;
    private static final long TIMEOUT_GRUPO_S    = 60;
    private static final long TIMEOUT_PROCESSO_S = 900;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.kafka.cenario1_4.ConsumidorCenario4";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.kafka.cenario1_4.ProdutorCenario4";

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.bootstrap-servers}") private String bootstrapServers;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    /** Um consumidor em execução. */
    private static final class Consumidor {
        final int id;
        final Process proc;
        final String arquivo;
        final Map<String, String> saida = new ConcurrentHashMap<>();
        Future<?> leitor;
        Consumidor(int id, Process proc, String arquivo) { this.id = id; this.proc = proc; this.arquivo = arquivo; }
    }

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Cenário 4 Kafka — Múltiplos consumidores");
        System.out.println("====================================================================");
        System.out.printf("%d msgs de %d bytes | processamento %d ms/msg | max.poll.records %d | configurações: %s%n",
                TOTAL_MSGS, TAMANHO_BYTES, PROC_MS, MAX_POLL_RECORDS,
                CONFIGS.stream().map(Config::rotulo).toList());

        File pasta = new File(PASTA_RESULTADOS);
        if (!pasta.exists() && !pasta.mkdirs()) {
            throw new IOException("Não foi possível criar a pasta: " + pasta.getAbsolutePath());
        }
        System.out.println("Gravando em: " + pasta.getAbsolutePath());

        String csvAgregado  = PASTA_RESULTADOS + File.separator + CSV_AGREGADO;
        String csvDetalhado = PASTA_RESULTADOS + File.separator + CSV_DETALHADO;
        apagarSeExistir(csvAgregado);
        apagarSeExistir(csvDetalhado);

        // aquecimento: uma rodada de todas as configurações, descartada
        for (Config c : CONFIGS) {
            System.out.printf("%nAquecimento (descartado) — %s%n", c.rotulo());
            rodar(c, 0, null);
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter agregado = new PrintWriter(new FileWriter(csvAgregado));
             PrintWriter detalhado = new PrintWriter(new BufferedWriter(new FileWriter(csvDetalhado)))) {

            agregado.println("execucao,ordem,config,num_consumidores,num_particoes,mensagens,duracao_s," +
                    "vazao_msg_s,vazao_produtor_msg_s,tempo_proc_medio_ms,espera_mediana_ms,espera_media_ms," +
                    "espera_max_ms,consumidores_ativos,menor_parcela,maior_parcela,msgs_por_consumidor,revogacoes");
            detalhado.println("execucao,config,consumidor,sequencia,envio_ms,inicio_ms,fim_ms,espera_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                List<Config> ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.size(); pos++) {
                    Config c = ordem.get(pos);
                    System.out.printf("%nExecução %d/%d — %s (posição %d)%n", exec, totalExecucoes, c.rotulo(), pos + 1);

                    Resultado r = rodar(c, exec, detalhado);
                    detalhado.flush();

                    agregado.printf(Locale.US, "%d,%d,%s,%d,%d,%d,%.4f,%.2f,%.2f,%.4f,%.4f,%.4f,%.4f,%d,%d,%d,%s,%d%n",
                            exec, pos + 1, c.rotulo(), c.consumidores(), c.particoes(), TOTAL_MSGS,
                            r.duracaoS, r.vazao, r.vazaoProdutor, r.procMedioMs,
                            r.esperaMediana, r.esperaMedia, r.esperaMax,
                            r.ativos, r.menor, r.maior, r.parcelas, r.revogacoes);
                    agregado.flush();

                    System.out.printf(Locale.US, "  → %.1f msg/s | proc. %.2f ms | espera mediana %.0f ms | divisão %s%n",
                            r.vazao, r.procMedioMs, r.esperaMediana, r.parcelas);
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }

        System.out.println();
        System.out.println("[SUCESSO] Cenário 4 Kafka concluído.");
        System.out.println("  " + new File(csvAgregado).getAbsolutePath());
        System.out.println("  " + new File(csvDetalhado).getAbsolutePath());
    }

    /** Ordem rotativa: exec 1 → c1,c2,c3...; exec 2 → c2,c3,...,c1; ... */
    static List<Config> ordemDaExecucao(int exec) {
        List<Config> o = new ArrayList<>();
        for (int i = 0; i < CONFIGS.size(); i++) {
            o.add(CONFIGS.get((i + exec - 1) % CONFIGS.size()));
        }
        return o;
    }

    // ------------------------------------------------------------------
    // Uma execução de uma configuração
    // ------------------------------------------------------------------
    private record Resultado(double duracaoS, double vazao, double vazaoProdutor, double procMedioMs,
                             double esperaMediana, double esperaMedia, double esperaMax,
                             int ativos, int menor, int maior, String parcelas, int revogacoes) {}

    private Resultado rodar(Config c, int exec, PrintWriter detalhado) throws Exception {
        int n = c.consumidores();
        recriarTopico(c.particoes());

        // nome único: um offset de outra rodada nunca é reaproveitado
        String groupId = "bench-c4-" + c.rotulo() + "-exec" + exec + "-" + System.currentTimeMillis();

        ExecutorService leitores = Executors.newFixedThreadPool(n);
        CountDownLatch prontos    = new CountDownLatch(n);
        CountDownLatch concluidos = new CountDownLatch(n);
        List<Consumidor> consumidores = new ArrayList<>();
        Process procProdutor = null;

        try {
            for (int id = 1; id <= n; id++) {
                String arquivo = PASTA_RESULTADOS + File.separator + "temp-kafka-c4-c" + id + ".csv";
                apagarSeExistir(arquivo);
                Process p = new ProcessBuilder(comando(CONSUMIDOR_CLASS,
                        bootstrapServers, TOPICO, groupId, String.valueOf(exec), arquivo,
                        String.valueOf(MAX_POLL_RECORDS), String.valueOf(id), String.valueOf(PROC_MS)))
                        .redirectErrorStream(true).start();
                Consumidor con = new Consumidor(id, p, arquivo);
                con.leitor = leitores.submit(() -> lerConsumidor(con, prontos, concluidos));
                consumidores.add(con);
            }

            if (!prontos.await(TIMEOUT_PRONTO_S, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout aguardando STATUS=PRONTO dos " + n + " consumidores.");
            }
            aguardarGrupoEstavel(groupId, n, c.particoes());

            // produtor
            procProdutor = new ProcessBuilder(comando(PRODUTOR_CLASS,
                    bootstrapServers, TOPICO, String.valueOf(TOTAL_MSGS), String.valueOf(exec),
                    String.valueOf(TAMANHO_BYTES), String.valueOf(c.particoes())))
                    .redirectErrorStream(true).start();
            Map<String, String> saidaProdutor = new HashMap<>();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(procProdutor.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    System.out.println("  [PRODUTOR] " + linha);
                    guardarChaveValor(linha, saidaProdutor);
                }
            }
            esperar(procProdutor, "produtor");

            // todas as mensagens estão no tópico: consumidores podem concluir
            for (Consumidor con : consumidores) enviar(con.proc, "PRODUTOR_TERMINOU");
            if (!concluidos.await(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout aguardando STATUS=CONCLUIDO dos consumidores.");
            }
            // todos terminaram: saem do grupo juntos
            for (Consumidor con : consumidores) enviar(con.proc, "SAIR");
            for (Consumidor con : consumidores) {
                esperar(con.proc, "consumidor " + con.id);
                con.leitor.get();
            }

            long t0 = Long.parseLong(obrigatorio(saidaProdutor, "T0"));
            long t1 = Long.parseLong(obrigatorio(saidaProdutor, "T1"));
            int revogacoes = 0;
            for (Consumidor con : consumidores) {
                revogacoes += Integer.parseInt(obrigatorio(con.saida, "REVOGACOES"));
            }
            if (revogacoes > 0) {
                System.out.printf("  [AVISO] %d partição(ões) revogada(s) durante o processamento%n", revogacoes);
            }
            return calcular(c, exec, consumidores, t0, t1, revogacoes, detalhado);
        } finally {
            for (Consumidor con : consumidores) {
                if (con.proc.isAlive()) con.proc.destroyForcibly();
            }
            if (procProdutor != null && procProdutor.isAlive()) procProdutor.destroyForcibly();
            leitores.shutdownNow();
        }
    }

    /** Lê os arquivos dos consumidores, valida e calcula as métricas. */
    private Resultado calcular(Config c, int exec, List<Consumidor> consumidores, long t0, long t1,
                               int revogacoes, PrintWriter detalhado) throws IOException {
        boolean[] visto = new boolean[TOTAL_MSGS + 1];
        List<Double> esperas = new ArrayList<>(TOTAL_MSGS);
        long ultimoFim = Long.MIN_VALUE;
        double somaProc = 0;
        int[] parcelas = new int[consumidores.size()];

        for (Consumidor con : consumidores) {
            List<String> linhas = Files.readAllLines(Paths.get(con.arquivo), StandardCharsets.UTF_8);
            int recebidas = Integer.parseInt(obrigatorio(con.saida, "RECEBIDAS"));
            if (linhas.size() - 1 != recebidas) {
                throw new IllegalStateException("Consumidor " + con.id + ": arquivo com " + (linhas.size() - 1)
                        + " linhas, mas informou RECEBIDAS=" + recebidas);
            }
            for (int i = 1; i < linhas.size(); i++) {
                String[] f = linhas.get(i).split(",");
                int  seq    = Integer.parseInt(f[0]);
                long envio  = Long.parseLong(f[1]);
                long inicio = Long.parseLong(f[2]);
                long fim    = Long.parseLong(f[3]);
                if (seq < 1 || seq > TOTAL_MSGS || visto[seq]) {
                    throw new IllegalStateException("Mensagem " + seq + " fora do intervalo ou duplicada");
                }
                visto[seq] = true;
                double espera = (inicio - envio) / 1e6;
                esperas.add(espera);
                somaProc += (fim - inicio) / 1e6;
                ultimoFim = Math.max(ultimoFim, fim);
                parcelas[con.id - 1]++;
                if (detalhado != null) {
                    detalhado.printf(Locale.US, "%d,%s,%d,%d,%.3f,%.3f,%.3f,%.3f%n",
                            exec, c.rotulo(), con.id, seq, (envio - t0) / 1e6, (inicio - t0) / 1e6,
                            (fim - t0) / 1e6, espera);
                }
            }
            apagarSeExistir(con.arquivo);
        }

        int total = Arrays.stream(parcelas).sum();
        if (total != TOTAL_MSGS) {
            throw new IllegalStateException("Esperado=" + TOTAL_MSGS + ", recebido=" + total
                    + " (soma dos consumidores)");
        }

        double duracaoS = (ultimoFim - t0) / 1e9;
        if (duracaoS <= 0 || t1 <= t0) {
            throw new IllegalStateException("Duração não positiva: " + duracaoS);
        }
        Collections.sort(esperas);
        double mediana = esperas.size() % 2 == 1
                ? esperas.get(esperas.size() / 2)
                : (esperas.get(esperas.size() / 2 - 1) + esperas.get(esperas.size() / 2)) / 2;
        double media = esperas.stream().mapToDouble(Double::doubleValue).average().orElse(0);

        StringJoiner sj = new StringJoiner("|");
        int ativos = 0, menor = Integer.MAX_VALUE, maior = 0;
        for (int p : parcelas) {
            sj.add(String.valueOf(p));
            if (p > 0) ativos++;
            menor = Math.min(menor, p);
            maior = Math.max(maior, p);
        }

        return new Resultado(duracaoS, TOTAL_MSGS / duracaoS, TOTAL_MSGS / ((t1 - t0) / 1e9),
                somaProc / TOTAL_MSGS, mediana, media, esperas.get(esperas.size() - 1),
                ativos, menor, maior, sj.toString(), revogacoes);
    }

    private void lerConsumidor(Consumidor con, CountDownLatch prontos, CountDownLatch concluidos) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(con.proc.getInputStream()))) {
            String linha;
            while ((linha = r.readLine()) != null) {
                System.out.println("  [CONSUMIDOR " + con.id + "] " + linha);
                if (linha.contains("STATUS=PRONTO"))    prontos.countDown();
                if (linha.contains("STATUS=CONCLUIDO")) concluidos.countDown();
                guardarChaveValor(linha, con.saida);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // ------------------------------------------------------------------
    // Grupo de consumidores
    // ------------------------------------------------------------------
    /**
     * Espera o grupo ter N membros e todas as partições atribuídas (cada uma a
     * um único membro), em duas leituras seguidas iguais. Com N ≤ partições,
     * todo membro precisa ter pelo menos uma partição.
     */
    private void aguardarGrupoEstavel(String groupId, int membros, int particoes) throws Exception {
        long limite = System.currentTimeMillis() + TIMEOUT_GRUPO_S * 1000;
        String anterior = null;
        String ultimoEstado = "(sem leitura)";
        try (AdminClient admin = adminClient()) {
            while (System.currentTimeMillis() < limite) {
                String atual = null;
                try {
                    ConsumerGroupDescription d = admin.describeConsumerGroups(List.of(groupId))
                            .describedGroups().get(groupId).get(10, TimeUnit.SECONDS);
                    atual = assinaturaSeCompleto(d, membros, particoes);
                    ultimoEstado = d.members().size() + " membro(s)";
                } catch (ExecutionException e) {
                    ultimoEstado = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
                }
                if (atual != null && atual.equals(anterior)) {
                    System.out.println("  [GRUPO] estável: " + atual);
                    return;
                }
                anterior = atual;
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("O grupo " + groupId + " não ficou estável em " + TIMEOUT_GRUPO_S
                + " s (última leitura: " + ultimoEstado + ").");
    }

    /** Descrição da atribuição se ela estiver completa; null caso contrário. */
    private static String assinaturaSeCompleto(ConsumerGroupDescription d, int membros, int particoes) {
        if (d.members().size() != membros) return null;
        Set<Integer> atribuidas = new HashSet<>();
        List<String> partes = new ArrayList<>();
        for (MemberDescription m : d.members()) {
            List<Integer> minhas = new ArrayList<>();
            for (TopicPartition tp : m.assignment().topicPartitions()) {
                if (!tp.topic().equals(TOPICO)) continue;
                if (!atribuidas.add(tp.partition())) return null; // partição em dois membros
                minhas.add(tp.partition());
            }
            if (membros <= particoes && minhas.isEmpty()) return null;
            Collections.sort(minhas);
            partes.add(m.clientId() + "=" + minhas);
        }
        if (atribuidas.size() != particoes) return null;
        Collections.sort(partes);
        return String.join(" ", partes);
    }

    // ------------------------------------------------------------------
    // Tópico
    // ------------------------------------------------------------------
    /** Apaga e recria o tópico com o número de partições pedido; para a campanha se falhar. */
    private void recriarTopico(int particoes) throws Exception {
        try (AdminClient admin = adminClient()) {
            if (admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(TOPICO)) {
                admin.deleteTopics(List.of(TOPICO)).all().get(30, TimeUnit.SECONDS);
            }

            // a exclusão é assíncrona: tenta criar até o nome ficar livre
            for (int tentativa = 1; ; tentativa++) {
                try {
                    admin.createTopics(List.of(new NewTopic(TOPICO, particoes, (short) 1)))
                            .all().get(30, TimeUnit.SECONDS);
                    break;
                } catch (ExecutionException e) {
                    if (tentativa >= 40) {
                        throw new IllegalStateException("Não foi possível recriar o tópico " + TOPICO, e);
                    }
                    Thread.sleep(500);
                }
            }

            // confere que o tópico existe com as N partições
            long limite = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < limite) {
                try {
                    TopicDescription d = admin.describeTopics(List.of(TOPICO)).allTopicNames()
                            .get(10, TimeUnit.SECONDS).get(TOPICO);
                    if (d != null && d.partitions().size() == particoes) return;
                } catch (ExecutionException ignored) {
                    // ainda não visível nos metadados
                }
                Thread.sleep(200);
            }
            throw new IllegalStateException("Tópico " + TOPICO + " não confirmado com " + particoes + " partições.");
        }
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
    private static void enviar(Process p, String comando) throws IOException {
        OutputStream os = p.getOutputStream();
        os.write((comando + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    /** Espera o processo terminar e exige código de saída 0. */
    private static void esperar(Process proc, String nome) throws InterruptedException {
        if (!proc.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timeout aguardando o " + nome + " terminar.");
        }
        if (proc.exitValue() != 0) {
            throw new IllegalStateException("O " + nome + " terminou com código " + proc.exitValue()
                    + " — veja as linhas acima.");
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
}
