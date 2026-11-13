package com.benchmarking.rabbitmq.cenario1_4;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cenário 4 — Múltiplos consumidores (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: medir como as mensagens são divididas entre consumidores
 * concorrentes e quanto a vazão de consumo aumenta de 1 para 2, 4 e 6
 * consumidores, quando o PROCESSAMENTO de cada mensagem é o gargalo.
 *
 * Desenho (igual ao Kafka, para a comparação ser direta):
 *   - 1 produtor envia 6.000 mensagens de 1 KB, persistentes, sem controle
 *     de taxa: tudo entra na fila logo no início e mede-se quanto tempo os N
 *     consumidores levam para tratar tudo.
 *   - Cada consumidor processa cada mensagem em 5 ms (Thread.sleep), com ack
 *     manual depois do processamento.
 *   - Prefetch 100, o mesmo valor do max.poll.records do Kafka. No Cenário 2,
 *     a partir de 30 as duas tecnologias já ficaram acima de 99% de utilização.
 *   - Uma fila só, com os N consumidores: o broker divide as mensagens em
 *     rodízio (não há partições).
 *   - 1 rodada de aquecimento (descartada) e depois as configurações
 *     intercaladas em ordem rotativa.
 *
 * Métricas por execução:
 *   vazão  = mensagens ÷ (último fim de processamento − primeiro envio)
 *   espera = início do processamento − envio  (fila no broker + buffer do cliente)
 *   divisão = quantas mensagens cada consumidor tratou
 *
 * Antes de soltar o produtor, o orquestrador espera os N STATUS=PRONTO e
 * confere no broker que a fila tem N consumidores registrados. Antes havia
 * só um sleep(1200): um consumidor que subisse atrasado receberia menos
 * mensagens, distorcendo justamente a divisão que o cenário mede.
 *
 * Qualquer problema (contagem errada, mensagem duplicada, processo com
 * código de saída diferente de 0) para a campanha. Nada vira zero no CSV.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-rabbitmq-cenario4.csv             (1 linha por execução e configuração)
 *   resultados-rabbitmq-cenario4-detalhado.csv   (1 linha por mensagem)
 */
@Component
public class Cenario4MultiplosConsumidores {

    private static final String FILA = "cenario4-multiplos-consumidores";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_1_4";
    private static final String CSV_AGREGADO  = "resultados-rabbitmq-cenario4.csv";
    private static final String CSV_DETALHADO = "resultados-rabbitmq-cenario4-detalhado.csv";

    private static final int  TOTAL_MSGS    = 6_000;   // divide por 1, 2, 4 e 6
    private static final int  TAMANHO_BYTES = 1_024;
    private static final long PROC_MS       = 5;       // processamento por mensagem
    private static final int  PREFETCH      = 100;     // igual ao max.poll.records do Kafka

    private static final int[] NUM_CONSUMIDORES = {1, 2, 4, 6};

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 3_000;
    private static final long TIMEOUT_PRONTO_S   = 60;
    private static final long TIMEOUT_REGISTRO_S = 30;
    private static final long TIMEOUT_PROCESSO_S = 900;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.rabbitmq.cenario1_4.ConsumidorCenario4";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.rabbitmq.cenario1_4.ProdutorCenario4";

    @Value("${benchmarking.execucoes}")      private int totalExecucoes;
    @Value("${benchmarking.host:localhost}") private String host;

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
        System.out.println("   Cenário 4 RabbitMQ — Múltiplos consumidores");
        System.out.println("====================================================================");
        System.out.printf("%d msgs de %d bytes | processamento %d ms/msg | prefetch %d | consumidores: %s%n",
                TOTAL_MSGS, TAMANHO_BYTES, PROC_MS, PREFETCH, Arrays.toString(NUM_CONSUMIDORES));

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
        for (int n : NUM_CONSUMIDORES) {
            System.out.printf("%nAquecimento (descartado) — %d consumidor(es)%n", n);
            rodar(n, 0, null);
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter agregado = new PrintWriter(new FileWriter(csvAgregado));
             PrintWriter detalhado = new PrintWriter(new BufferedWriter(new FileWriter(csvDetalhado)))) {

            agregado.println("execucao,ordem,config,num_consumidores,mensagens,duracao_s," +
                    "vazao_msg_s,vazao_produtor_msg_s,tempo_proc_medio_ms,espera_mediana_ms,espera_media_ms," +
                    "espera_max_ms,consumidores_ativos,menor_parcela,maior_parcela,msgs_por_consumidor");
            detalhado.println("execucao,config,consumidor,sequencia,envio_ms,inicio_ms,fim_ms,espera_ms");

            for (int exec = 1; exec <= totalExecucoes; exec++) {
                int[] ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.length; pos++) {
                    int n = ordem[pos];
                    System.out.printf("%nExecução %d/%d — %d consumidor(es) (posição %d)%n",
                            exec, totalExecucoes, n, pos + 1);

                    Resultado r = rodar(n, exec, detalhado);
                    detalhado.flush();

                    agregado.printf(Locale.US, "%d,%d,%s,%d,%d,%.4f,%.2f,%.2f,%.4f,%.4f,%.4f,%.4f,%d,%d,%d,%s%n",
                            exec, pos + 1, rotulo(n), n, TOTAL_MSGS,
                            r.duracaoS, r.vazao, r.vazaoProdutor, r.procMedioMs,
                            r.esperaMediana, r.esperaMedia, r.esperaMax,
                            r.ativos, r.menor, r.maior, r.parcelas);
                    agregado.flush();

                    System.out.printf(Locale.US, "  → %.1f msg/s | proc. %.2f ms | espera mediana %.0f ms | divisão %s%n",
                            r.vazao, r.procMedioMs, r.esperaMediana, r.parcelas);
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }

        System.out.println();
        System.out.println("[SUCESSO] Cenário 4 RabbitMQ concluído.");
        System.out.println("  " + new File(csvAgregado).getAbsolutePath());
        System.out.println("  " + new File(csvDetalhado).getAbsolutePath());
    }

    private static String rotulo(int n) { return n + "c"; }

    /** Ordem rotativa: exec 1 → 1,2,4,6; exec 2 → 2,4,6,1; exec 3 → 4,6,1,2; ... */
    static int[] ordemDaExecucao(int exec) {
        int k = NUM_CONSUMIDORES.length;
        int[] o = new int[k];
        for (int i = 0; i < k; i++) o[i] = NUM_CONSUMIDORES[(i + exec - 1) % k];
        return o;
    }

    // ------------------------------------------------------------------
    // Uma execução de uma configuração
    // ------------------------------------------------------------------
    private record Resultado(double duracaoS, double vazao, double vazaoProdutor, double procMedioMs,
                             double esperaMediana, double esperaMedia, double esperaMax,
                             int ativos, int menor, int maior, String parcelas) {}

    private Resultado rodar(int n, int exec, PrintWriter detalhado) throws Exception {
        limparFila();

        ExecutorService leitores = Executors.newFixedThreadPool(n);
        CountDownLatch prontos    = new CountDownLatch(n);
        CountDownLatch concluidos = new CountDownLatch(n);
        List<Consumidor> consumidores = new ArrayList<>();
        Process procProdutor = null;

        try {
            for (int id = 1; id <= n; id++) {
                String arquivo = PASTA_RESULTADOS + File.separator + "temp-rabbitmq-c4-c" + id + ".csv";
                apagarSeExistir(arquivo);
                Process p = new ProcessBuilder(comando(CONSUMIDOR_CLASS,
                        host, FILA, String.valueOf(exec), arquivo,
                        String.valueOf(PREFETCH), String.valueOf(id), String.valueOf(PROC_MS)))
                        .redirectErrorStream(true).start();
                Consumidor con = new Consumidor(id, p, arquivo);
                con.leitor = leitores.submit(() -> lerConsumidor(con, prontos, concluidos));
                consumidores.add(con);
            }

            if (!prontos.await(TIMEOUT_PRONTO_S, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout aguardando STATUS=PRONTO dos " + n + " consumidores.");
            }
            aguardarConsumidoresNaFila(n);

            // produtor
            procProdutor = new ProcessBuilder(comando(PRODUTOR_CLASS,
                    host, FILA, String.valueOf(TOTAL_MSGS), String.valueOf(exec), String.valueOf(TAMANHO_BYTES)))
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

            // todas as mensagens estão na fila: consumidores podem concluir
            for (Consumidor con : consumidores) enviar(con.proc, "PRODUTOR_TERMINOU");
            if (!concluidos.await(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout aguardando STATUS=CONCLUIDO dos consumidores.");
            }
            for (Consumidor con : consumidores) enviar(con.proc, "SAIR");
            for (Consumidor con : consumidores) {
                esperar(con.proc, "consumidor " + con.id);
                con.leitor.get();
            }

            long t0 = Long.parseLong(obrigatorio(saidaProdutor, "T0"));
            long t1 = Long.parseLong(obrigatorio(saidaProdutor, "T1"));
            return calcular(n, exec, consumidores, t0, t1, detalhado);
        } finally {
            for (Consumidor con : consumidores) {
                if (con.proc.isAlive()) con.proc.destroyForcibly();
            }
            if (procProdutor != null && procProdutor.isAlive()) procProdutor.destroyForcibly();
            leitores.shutdownNow();
        }
    }

    /** Lê os arquivos dos consumidores, valida e calcula as métricas. */
    private Resultado calcular(int n, int exec, List<Consumidor> consumidores, long t0, long t1,
                               PrintWriter detalhado) throws IOException {
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
                            exec, rotulo(n), con.id, seq, (envio - t0) / 1e6, (inicio - t0) / 1e6,
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
                ativos, menor, maior, sj.toString());
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
    // Fila
    // ------------------------------------------------------------------
    /** Declara a fila (durável) e remove qualquer mensagem que tenha sobrado. */
    private void limparFila() throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        try (Connection conn = f.newConnection(); Channel ch = conn.createChannel()) {
            ch.queueDeclare(FILA, true, false, false, null);
            ch.queuePurge(FILA);
            // os consumidores da execução anterior já saíram; espera o broker registrar
            long limite = System.currentTimeMillis() + 10_000;
            int consumidores;
            while ((consumidores = ch.queueDeclarePassive(FILA).getConsumerCount()) != 0) {
                if (System.currentTimeMillis() > limite) {
                    throw new IllegalStateException("A fila ainda tem " + consumidores
                            + " consumidor(es) de uma execução anterior.");
                }
                Thread.sleep(200);
            }
        }
    }

    /** Confere no broker que a fila tem exatamente N consumidores registrados. */
    private void aguardarConsumidoresNaFila(int n) throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        long limite = System.currentTimeMillis() + TIMEOUT_REGISTRO_S * 1000;
        int atual = -1;
        try (Connection conn = f.newConnection(); Channel ch = conn.createChannel()) {
            while (System.currentTimeMillis() < limite) {
                atual = ch.queueDeclarePassive(FILA).getConsumerCount();
                if (atual == n) {
                    System.out.println("  [FILA] " + n + " consumidor(es) registrados");
                    return;
                }
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("A fila tem " + atual + " consumidor(es); esperado " + n);
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
