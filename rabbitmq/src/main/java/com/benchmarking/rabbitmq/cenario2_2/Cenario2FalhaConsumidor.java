package com.benchmarking.rabbitmq.cenario2_2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível intermediário — Cenário 2: Falha do consumidor (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: observar o que acontece com as mensagens quando o consumidor cai
 * no meio do trabalho, em cada nível de garantia, e quanto tempo o broker
 * leva para entregar o trabalho a outro consumidor.
 *
 * Desenho de cada execução:
 *   1. Fila apagada e recriada (durável) com x-single-active-consumer:
 *      só um consumidor recebe por vez; os outros ficam de reserva.
 *   2. Consumidor #1 se registra primeiro e vira o consumidor ativo.
 *   3. Consumidor #2 se registra depois e fica de reserva.
 *   4. O produtor envia 5.000 mensagens persistentes, com publisher confirms.
 *   5. O #1 cai (Runtime.halt) depois de 1.000 processadas, antes do ack.
 *   6. O broker percebe a conexão fechada, devolve à fila o que não teve
 *      ack e passa a entregar ao #2.
 *   7. O #2 processa o resto e encerra 10 s depois de ficar sem trabalho.
 *
 * Mudanças em relação à versão anterior: o #2 já está registrado antes da
 * queda (antes ele só era iniciado depois, e o tempo de recuperação incluía a
 * subida da JVM); os três níveis de garantia; aquecimento e ordem rotativa;
 * STATUS=PRONTO no lugar dos sleeps; validação dos códigos de saída.
 *
 * Os dois consumidores gravam cada mensagem processada num arquivo
 * compartilhado ("id;sequência"). Dele saem:
 *   duplicadas = processamentos a mais (mensagem processada mais de uma vez)
 *   perdidas   = mensagens nunca processadas
 * e da saída dos consumidores:
 *   recuperacao_ms = 1ª mensagem recebida pelo #2 − instante da queda do #1
 *
 * Os três níveis de garantia (os mesmos do Cenário 1 intermediário) rodam
 * intercalados em ordem rotativa, depois de uma rodada de aquecimento.
 * Qualquer problema (o #1 não cair, o #2 terminar com erro, fila que não
 * pode ser recriada, consumidor que não se registra) interrompe a campanha.
 *
 * Arquivo gerado em PASTA_RESULTADOS:
 *   resultados-rabbitmq-falha-consumidor.csv (1 linha por execução e nível)
 */
@Component
public class Cenario2FalhaConsumidor {

    private static final String FILA = "garantias-cenario2";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_2_2";
    private static final String CSV = "resultados-rabbitmq-falha-consumidor.csv";

    static final String[] GARANTIAS = {"at-most-once", "at-least-once", "exactly-once"};

    private static final int MSGS        = 5_000;
    private static final int FALHAR_APOS = 1_000;
    private static final int PREFETCH    = 100;   // igual ao max.poll.records do Kafka

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 3_000;
    private static final long TIMEOUT_PRONTO_S   = 60;
    private static final long TIMEOUT_REGISTRO_S = 30;
    private static final long TIMEOUT_PROCESSO_S = 300;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.rabbitmq.cenario2_2.ConsumidorFalhavel";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.rabbitmq.cenario2_2.ProdutorFalha";

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.host:localhost}")    private String host;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private record Resultado(int processadasC1, int processadasC2, int unicas, int duplicadas,
                             int perdidas, int descartadas, double recuperacaoMs) {}

    /** Um consumidor em execução, com a saída lida em thread própria. */
    private static final class Consumidor {
        final Process proc;
        final Map<String, String> saida = new ConcurrentHashMap<>();
        final CountDownLatch pronto = new CountDownLatch(1);
        Future<?> leitor;
        Consumidor(Process proc) { this.proc = proc; }
    }

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Intermediário — Cenário 2 RabbitMQ — Falha do consumidor");
        System.out.println("====================================================================");
        System.out.printf("%d mensagens | consumidor #1 cai depois de ~%d | níveis: %s%n",
                MSGS, FALHAR_APOS, Arrays.toString(GARANTIAS));

        File pasta = new File(PASTA_RESULTADOS);
        if (!pasta.exists() && !pasta.mkdirs()) {
            throw new IOException("Não foi possível criar a pasta: " + pasta.getAbsolutePath());
        }
        System.out.println("Gravando em: " + pasta.getAbsolutePath());
        String csv = PASTA_RESULTADOS + File.separator + CSV;
        apagarSeExistir(csv);

        for (String g : GARANTIAS) {
            System.out.printf("%nAquecimento (descartado) — %s%n", g);
            rodar(g, 0);
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter w = new PrintWriter(new FileWriter(csv))) {
            w.println("execucao,ordem,garantia,enviadas,processadas_c1,processadas_c2,processadas_unicas," +
                    "duplicadas,perdidas,descartadas_dedup,recuperacao_ms");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                String[] ordem = ordemDaExecucao(exec);
                for (int pos = 0; pos < ordem.length; pos++) {
                    String g = ordem[pos];
                    System.out.printf("%nExecução %d/%d — %s (posição %d)%n", exec, totalExecucoes, g, pos + 1);
                    Resultado r = rodar(g, exec);
                    w.printf(Locale.US, "%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%s%n",
                            exec, pos + 1, g, MSGS, r.processadasC1, r.processadasC2, r.unicas,
                            r.duplicadas, r.perdidas, r.descartadas,
                            r.recuperacaoMs < 0 ? "" : String.format(Locale.US, "%.2f", r.recuperacaoMs));
                    w.flush();
                    System.out.printf(Locale.US, "  → #1: %d | #2: %d | duplicadas: %d | perdidas: %d | recuperação: %s ms%n",
                            r.processadasC1, r.processadasC2, r.duplicadas, r.perdidas,
                            r.recuperacaoMs < 0 ? "—" : String.format(Locale.US, "%.0f", r.recuperacaoMs));
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }

        System.out.println();
        System.out.println("[SUCESSO] Intermediário — Cenário 2 RabbitMQ concluído.");
        System.out.println("  " + new File(csv).getAbsolutePath());
    }

    /** Ordem rotativa: exec 1 → AMO, ALO, EO; exec 2 → ALO, EO, AMO; ... */
    static String[] ordemDaExecucao(int exec) {
        int n = GARANTIAS.length;
        String[] o = new String[n];
        for (int i = 0; i < n; i++) o[i] = GARANTIAS[(i + exec - 1) % n];
        return o;
    }

    // ------------------------------------------------------------------
    // Uma execução
    // ------------------------------------------------------------------
    private Resultado rodar(String garantia, int exec) throws Exception {
        prepararFila();
        File arquivo = new File(PASTA_RESULTADOS, "temp-rabbitmq-processadas.txt");
        apagarSeExistir(arquivo.getPath());

        ExecutorService leitores = Executors.newFixedThreadPool(2);
        Consumidor c1 = null, c2 = null;
        Process produtor = null;
        try {
            // o primeiro consumidor registrado vira o ativo
            c1 = iniciarConsumidor(1, garantia, FALHAR_APOS, exec, arquivo, leitores);
            aguardarPronto(c1, "#1");
            aguardarConsumidores(1);
            c2 = iniciarConsumidor(2, garantia, -1, exec, arquivo, leitores);
            aguardarPronto(c2, "#2");   // basicConsume só retorna depois que o broker registrou

            produtor = new ProcessBuilder(comando(PRODUTOR_CLASS,
                    host, FILA, String.valueOf(MSGS), String.valueOf(exec)))
                    .redirectErrorStream(true).start();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(produtor.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) System.out.println("  [PRODUTOR] " + linha);
            }
            esperar(produtor, "produtor", 0);

            // o #1 tem de cair com o código do halt (137)
            esperar(c1.proc, "consumidor #1", 137);
            c1.leitor.get();

            enviar(c2.proc, "QUEDA_CONFIRMADA");
            esperar(c2.proc, "consumidor #2", 0);
            c2.leitor.get();

            return calcular(arquivo, c1, c2);
        } finally {
            for (Consumidor c : new Consumidor[]{c1, c2}) {
                if (c != null && c.proc.isAlive()) c.proc.destroyForcibly();
            }
            if (produtor != null && produtor.isAlive()) produtor.destroyForcibly();
            leitores.shutdownNow();
        }
    }

    private Resultado calcular(File arquivo, Consumidor c1, Consumidor c2) throws IOException {
        int[] vezes = new int[MSGS + 1];
        int p1 = 0, p2 = 0;
        for (String linha : Files.readAllLines(arquivo.toPath(), StandardCharsets.US_ASCII)) {
            String[] f = linha.trim().split(";");
            if (f.length != 2) continue;
            int seq = Integer.parseInt(f[1]);
            if (seq < 1 || seq > MSGS) throw new IllegalStateException("Sequência fora do intervalo: " + seq);
            vezes[seq]++;
            if (f[0].equals("1")) p1++; else p2++;
        }
        apagarSeExistir(arquivo.getPath());

        int unicas = 0, duplicadas = 0;
        for (int s = 1; s <= MSGS; s++) {
            if (vezes[s] > 0) unicas++;
            if (vezes[s] > 1) duplicadas += vezes[s] - 1;
        }
        int informadas2 = Integer.parseInt(obrigatorio(c2.saida, "PROCESSADAS"));
        if (informadas2 != p2) {
            throw new IllegalStateException("O #2 informou " + informadas2 + " processadas, mas o arquivo tem " + p2);
        }
        int descartadas = Integer.parseInt(obrigatorio(c2.saida, "DESCARTADAS"));

        double recuperacao = -1;
        String primeira = c2.saida.get("PRIMEIRA_RECEBIDA");
        if (primeira != null) {
            long queda = Long.parseLong(obrigatorio(c1.saida, "QUEDA"));
            recuperacao = (Long.parseLong(primeira) - queda) / 1e6;
        }
        return new Resultado(p1, p2, unicas, duplicadas, MSGS - unicas, descartadas, recuperacao);
    }

    private Consumidor iniciarConsumidor(int id, String garantia, int falharApos, int exec,
                                         File arquivo, ExecutorService leitores) throws IOException {
        Process p = new ProcessBuilder(comando(CONSUMIDOR_CLASS,
                host, FILA, String.valueOf(exec), String.valueOf(id), garantia,
                String.valueOf(falharApos), arquivo.getAbsolutePath(), String.valueOf(PREFETCH)))
                .redirectErrorStream(true).start();
        Consumidor c = new Consumidor(p);
        c.leitor = leitores.submit(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                String linha;
                while ((linha = r.readLine()) != null) {
                    System.out.println("  [CONSUMIDOR #" + id + "] " + linha);
                    if (linha.contains("STATUS=PRONTO")) c.pronto.countDown();
                    guardarChaveValor(linha, c.saida);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return c;
    }

    private static void aguardarPronto(Consumidor c, String nome) throws InterruptedException {
        if (!c.pronto.await(TIMEOUT_PRONTO_S, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timeout aguardando STATUS=PRONTO do consumidor " + nome);
        }
    }

    /** Confere no broker que a fila tem pelo menos n consumidores registrados. */
    private void aguardarConsumidores(int n) throws Exception {
        long limite = System.currentTimeMillis() + TIMEOUT_REGISTRO_S * 1000;
        try (Connection conn = fabrica().newConnection(); Channel ch = conn.createChannel()) {
            while (System.currentTimeMillis() < limite) {
                if (ch.queueDeclarePassive(FILA).getConsumerCount() >= n) return;
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("A fila não chegou a " + n + " consumidor(es) registrados.");
    }

    // ------------------------------------------------------------------
    // Fila e auxiliares
    // ------------------------------------------------------------------
    /**
     * Apaga e recria a fila com x-single-active-consumer. Recriar (e não só
     * esvaziar) garante que não sobra consumidor nem mensagem de outra execução.
     */
    private void prepararFila() throws Exception {
        try (Connection conn = fabrica().newConnection(); Channel ch = conn.createChannel()) {
            ch.queueDelete(FILA);
            ch.queueDeclare(FILA, true, false, false, Map.of("x-single-active-consumer", true));
        }
    }

    private ConnectionFactory fabrica() {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        return f;
    }

    private static void enviar(Process p, String comando) throws IOException {
        OutputStream os = p.getOutputStream();
        os.write((comando + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    /** Espera o processo terminar e exige o código de saída esperado. */
    private static void esperar(Process proc, String nome, int codigoEsperado) throws InterruptedException {
        if (!proc.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timeout aguardando o " + nome + " terminar.");
        }
        if (proc.exitValue() != codigoEsperado) {
            throw new IllegalStateException("O " + nome + " terminou com código " + proc.exitValue()
                    + " (esperado " + codigoEsperado + ") — veja as linhas acima.");
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
        if (v == null || v.isBlank()) throw new IllegalStateException("Métrica não retornada pelo subprocesso: " + chave);
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
