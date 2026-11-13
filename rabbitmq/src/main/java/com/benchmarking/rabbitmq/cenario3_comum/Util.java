package com.benchmarking.rabbitmq.cenario3_comum;

import com.rabbitmq.client.ConnectionFactory;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Nível avançado (RabbitMQ) — auxiliares comuns aos quatro orquestradores e
 * aos processos de produtor e consumidor. Mesmos métodos da versão Kafka
 * (processos, arquivos, ritmo e estatística) mais a conexão AMQP e a
 * management API (HTTP), usada para achar o líder das quorum queues e
 * conferir a saúde do cluster.
 */
public final class Util {

    private Util() { }

    // ------------------------------------------------------------------
    // Processos
    // ------------------------------------------------------------------
    public static Process iniciar(String javaCmd, String classpath, String classeMain, String... argumentos)
            throws IOException {
        List<String> c = new ArrayList<>();
        c.add(javaCmd);
        c.add("-cp");
        c.add(classpath);
        c.add(classeMain);
        c.addAll(Arrays.asList(argumentos));
        return new ProcessBuilder(c).redirectErrorStream(true).start();
    }

    /**
     * Lê a saída de um processo até ele terminar: imprime cada linha com o nome
     * do processo, guarda as linhas chave=valor e conta STATUS=PRONTO no latch.
     * Linhas cuja chave está em "silenciosas" são guardadas mas não impressas
     * (relatos de progresso a cada segundo).
     */
    public static Void ler(Process p, String nome, Map<String, String> destino, CountDownLatch pronto,
                           Set<String> silenciosas) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String linha;
            while ((linha = r.readLine()) != null) {
                int i = linha.indexOf('=');
                String chave = i > 0 ? linha.substring(0, i).trim() : "";
                if (i > 0) destino.put(chave, linha.substring(i + 1).trim());
                if (silenciosas == null || !silenciosas.contains(chave)) {
                    System.out.println("  [" + nome + "] " + linha);
                }
                if (pronto != null && linha.contains("STATUS=PRONTO")) pronto.countDown();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Future<?> lerEmSegundoPlano(ExecutorService ex, Process p, String nome, Map<String, String> destino,
                                              CountDownLatch pronto, Set<String> silenciosas) {
        return ex.submit(() -> ler(p, nome, destino, pronto, silenciosas));
    }

    /** Escreve uma linha na entrada padrão do processo. */
    public static void enviar(Process p, String comando) throws IOException {
        OutputStream os = p.getOutputStream();
        os.write((comando + System.lineSeparator()).getBytes(StandardCharsets.UTF_8));
        os.flush();
    }

    /** Espera o processo terminar e exige o código de saída esperado. */
    public static void esperar(Process proc, String nome, int codigoEsperado, long timeoutS) throws InterruptedException {
        if (!proc.waitFor(timeoutS, TimeUnit.SECONDS)) {
            proc.destroyForcibly();
            throw new IllegalStateException("Timeout aguardando o " + nome + " terminar.");
        }
        if (proc.exitValue() != codigoEsperado) {
            throw new IllegalStateException("O " + nome + " terminou com código " + proc.exitValue()
                    + " (esperado " + codigoEsperado + ") — veja as linhas acima.");
        }
    }

    public static void matar(Process... ps) {
        for (Process p : ps) if (p != null && p.isAlive()) p.destroyForcibly();
    }

    public static String obrigatorio(Map<String, String> saida, String chave) {
        String v = saida.get(chave);
        if (v == null || v.isBlank()) throw new IllegalStateException("Métrica não retornada pelo subprocesso: " + chave);
        return v;
    }

    /** Roda um comando externo (ex.: docker) e devolve a saída; falha se o código não for 0. */
    public static String rodarComando(long timeoutS, String... comando) throws Exception {
        Process p = new ProcessBuilder(comando).redirectErrorStream(true).start();
        String saida;
        try (InputStream in = p.getInputStream()) {
            saida = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        if (!p.waitFor(timeoutS, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("Timeout em: " + String.join(" ", comando));
        }
        if (p.exitValue() != 0) {
            throw new IllegalStateException("Falhou (" + p.exitValue() + "): " + String.join(" ", comando) + "\n" + saida);
        }
        return saida;
    }

    // ------------------------------------------------------------------
    // Arquivos
    // ------------------------------------------------------------------
    public static File pasta(String nome) throws IOException {
        File pasta = new File(nome);
        if (!pasta.exists() && !pasta.mkdirs()) throw new IOException("Não foi possível criar " + pasta.getAbsolutePath());
        System.out.println("Gravando em: " + pasta.getAbsolutePath());
        return pasta;
    }

    public static void apagarSeExistir(String caminho) throws IOException {
        File f = new File(caminho);
        if (f.exists() && !f.delete()) {
            throw new IOException("Não foi possível apagar: " + f.getAbsolutePath() + " (o arquivo está aberto em outro programa?)");
        }
    }

    // ------------------------------------------------------------------
    // RabbitMQ
    // ------------------------------------------------------------------
    public static ConnectionFactory fabrica(String host, int porta) {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        f.setPort(porta);
        // sem recuperação automática: uma queda de conexão deve aparecer no resultado, não ser escondida
        f.setAutomaticRecoveryEnabled(false);
        return f;
    }

    /**
     * Abre uma conexão AMQP tentando de novo por até timeoutS segundos. Um nó que
     * acabou de voltar de uma queda já aparece como "running" na management API dos
     * outros nós, mas ainda pode fechar conexões novas enquanto termina de iniciar
     * (EOFException / "connection error" no handshake).
     */
    public static com.rabbitmq.client.Connection conectar(String host, int porta, long timeoutS) throws Exception {
        long limite = System.currentTimeMillis() + timeoutS * 1000;
        Exception ultimo = null;
        while (System.currentTimeMillis() < limite) {
            try {
                return fabrica(host, porta).newConnection();
            } catch (Exception e) {
                ultimo = e;
                Thread.sleep(1_000);
            }
        }
        throw new IllegalStateException("Não consegui conectar em " + host + ":" + porta + " em " + timeoutS + " s", ultimo);
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    /** GET na management API; devolve o corpo ou null se não respondeu 200. */
    public static String http(String host, int portaManagement, String usuario, String senha, String caminho) {
        try {
            String auth = Base64.getEncoder().encodeToString((usuario + ":" + senha).getBytes(StandardCharsets.UTF_8));
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create("http://" + host + ":" + portaManagement + caminho))
                    .header("Authorization", "Basic " + auth).timeout(Duration.ofSeconds(3)).GET().build();
            HttpResponse<String> r = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            return r.statusCode() == 200 ? r.body() : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** Valor texto de "chave":"valor" no JSON (primeira ocorrência) ou null. */
    public static String textoJson(String json, String chave) {
        if (json == null) return null;
        Matcher m = Pattern.compile("\"" + Pattern.quote(chave) + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    /** Itens da lista "chave":["a","b"] no JSON (primeira ocorrência); lista vazia se não houver. */
    public static List<String> listaJson(String json, String chave) {
        List<String> r = new ArrayList<>();
        if (json == null) return r;
        Matcher m = Pattern.compile("\"" + Pattern.quote(chave) + "\"\\s*:\\s*\\[([^\\]]*)\\]").matcher(json);
        if (!m.find()) return r;
        Matcher it = Pattern.compile("\"([^\"]*)\"").matcher(m.group(1));
        while (it.find()) r.add(it.group(1));
        return r;
    }

    /** Quantos nós aparecem como "running":true em /api/nodes (−1 se a API não respondeu). */
    public static int nosRodando(String host, int portaManagement, String usuario, String senha) {
        String j = http(host, portaManagement, usuario, senha, "/api/nodes");
        if (j == null) return -1;
        Matcher m = Pattern.compile("\"running\"\\s*:\\s*true").matcher(j);
        int n = 0;
        while (m.find()) n++;
        return n;
    }

    // ------------------------------------------------------------------
    // Proteção contra processos órfãos
    // ------------------------------------------------------------------
    /**
     * Para os subprocessos (produtor e consumidor). Lê a entrada padrão numa
     * thread daemon, entregando cada linha (sem espaços nas pontas) a aoLer, e
     * encerra o processo com código 3 quando a entrada chega ao fim. O
     * orquestrador nunca fecha a entrada dos filhos: o fim do fluxo só chega
     * quando ele morre, mesmo à força (Stop da IDE, terminal fechado), e assim
     * o filho não continua rodando sozinho.
     */
    public static void vigiarEntrada(java.util.function.Consumer<String> aoLer) {
        Thread t = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                String l;
                while ((l = in.readLine()) != null) aoLer.accept(l.trim());
            } catch (IOException ignored) {
            }
            System.err.println("Entrada padrão fechada: o orquestrador encerrou. Saindo para não ficar órfão.");
            Runtime.getRuntime().halt(3);
        }, "vigia-stdin");
        t.setDaemon(true);
        t.start();
    }

    /**
     * Para o orquestrador: no Ctrl+C ou no encerramento normal da JVM, mata os
     * subprocessos ainda vivos. Kill forçado não executa hooks; esse caso fica
     * com vigiarEntrada nos filhos. Devolve o hook para removerGancho.
     */
    public static Thread matarFilhosAoEncerrar(String nome) {
        Thread t = new Thread(() -> ProcessHandle.current().descendants()
                .forEach(ProcessHandle::destroyForcibly), nome);
        Runtime.getRuntime().addShutdownHook(t);
        return t;
    }

    /** Remove o hook de matarFilhosAoEncerrar ao fim do cenário. */
    public static void removerGancho(Thread t) {
        try {
            Runtime.getRuntime().removeShutdownHook(t);
        } catch (IllegalStateException ignored) {
            // a JVM já está encerrando: o hook vai rodar de qualquer forma
        }
    }

    // ------------------------------------------------------------------
    // Ritmo e estatística
    // ------------------------------------------------------------------
    /** Mesmo método dos outros cenários: dorme a maior parte e termina em busy-wait. */
    public static void aguardarAte(long alvoNs) {
        long faltaNs;
        while ((faltaNs = alvoNs - System.nanoTime()) > 0) {
            if (faltaNs > 20_000_000L) {
                try {
                    Thread.sleep((faltaNs - 3_000_000L) / 1_000_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } else {
                Thread.onSpinWait();
            }
        }
    }

    /** {média, mediana, mínimo, máximo} em ms, de latências em ns. */
    public static double[] resumoMs(long[] ns, int n) {
        long[] ord = Arrays.copyOf(ns, n);
        Arrays.sort(ord);
        double media = Arrays.stream(ord).average().orElse(0) / 1e6;
        double mediana = n % 2 == 1 ? ord[n / 2] / 1e6 : (ord[n / 2 - 1] + ord[n / 2]) / 2.0 / 1e6;
        return new double[]{media, mediana, ord[0] / 1e6, ord[n - 1] / 1e6};
    }
}