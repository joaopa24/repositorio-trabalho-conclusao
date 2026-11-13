package com.benchmarking.kafka.cenario3_comum;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível avançado — auxiliares comuns aos quatro orquestradores e aos
 * processos de produtor e consumidor (os mesmos métodos que os cenários
 * básicos e intermediários repetem em cada classe).
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
    // Kafka
    // ------------------------------------------------------------------
    public static AdminClient admin(String bootstrap) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put("auto.include.jmx.reporter", "false");
        config.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, "");
        return AdminClient.create(config);
    }

    /** Apaga (se existirem) e recria os tópicos; espera a exclusão terminar; para a campanha se falhar. */
    public static void recriarTopicos(AdminClient admin, List<NewTopic> topicos) throws Exception {
        Set<String> nomes = new HashSet<>();
        for (NewTopic t : topicos) nomes.add(t.name());

        Set<String> existentes = admin.listTopics().names().get(30, TimeUnit.SECONDS);
        List<String> apagar = new ArrayList<>();
        for (String n : nomes) if (existentes.contains(n)) apagar.add(n);
        if (!apagar.isEmpty()) admin.deleteTopics(apagar).all().get(30, TimeUnit.SECONDS);

        long limite = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < limite) {
            Set<String> atuais = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            if (Collections.disjoint(atuais, nomes)) break;
            Thread.sleep(250);
        }
        // Cria em rodadas, só os que ainda faltam. Com muitos tópicos, parte deles pode
        // ainda estar sendo apagada no broker ("marked for deletion") e a criação falha só
        // para esses; os outros já ficam criados. Repetir o lote inteiro daria
        // TopicExistsException para sempre, então cada rodada tenta só os que não existem.
        Map<String, NewTopic> porNome = new HashMap<>();
        for (NewTopic t : topicos) porNome.put(t.name(), t);
        Exception ultimoErro = null;
        for (int tentativa = 1; tentativa <= 60; tentativa++) {
            Set<String> atuais = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            List<NewTopic> faltando = new ArrayList<>();
            for (String n : nomes) if (!atuais.contains(n)) faltando.add(porNome.get(n));
            if (faltando.isEmpty()) {
                Thread.sleep(500);   // metadados chegando a todos os brokers
                return;
            }
            Map<String, KafkaFuture<Void>> futuros = admin.createTopics(faltando).values();
            for (Map.Entry<String, KafkaFuture<Void>> f : futuros.entrySet()) {
                try {
                    f.getValue().get(30, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    // TopicExistsException aqui = ainda sendo apagado; a próxima rodada confere
                    ultimoErro = e;
                }
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Não foi possível recriar os tópicos " + nomes, ultimoErro);
    }

    /** Quantos brokers o cluster tem agora. */
    public static int brokers(AdminClient admin) throws Exception {
        return admin.describeCluster().nodes().get(15, TimeUnit.SECONDS).size();
    }

    /** Desliga o JMX e os reporters de métricas (menos ruído e menos memória nos subprocessos). */
    public static void semMetricas(Properties p) {
        p.put("auto.include.jmx.reporter", "false");
        p.put("metric.reporters", "");
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