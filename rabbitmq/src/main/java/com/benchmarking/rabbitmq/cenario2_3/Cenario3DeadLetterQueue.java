package com.benchmarking.rabbitmq.cenario2_3;

import com.rabbitmq.client.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível intermediário — Cenário 3: Dead Letter Queue (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: verificar se as mensagens que sempre falham são isoladas numa
 * DLQ sem perder nem travar as válidas, comparar o mecanismo de cada
 * tecnologia e medir quanto o tratamento das inválidas custa em vazão.
 *
 * Desenho:
 *   - 5.000 mensagens por execução; duas configurações intercaladas em ordem
 *     rotativa: 0% de inválidas (referência) e 10% (1 a cada 10).
 *   - Política igual nas duas tecnologias: até 3 tentativas por mensagem;
 *     esgotadas, a mensagem vai para a DLQ.
 *   - No RabbitMQ, o desvio é NATIVO: a fila principal é declarada com
 *     x-dead-letter-exchange; a mensagem rejeitada sem requeue é encaminhada
 *     pelo próprio broker à DLX (fanout), que a entrega à fila DLQ.
 *
 * Verificação de cada execução (depois que o consumidor termina):
 *   - a fila DLQ é esvaziada mensagem a mensagem (basicGet);
 *   - as sequências na DLQ têm de ser EXATAMENTE as inválidas enviadas;
 *   - cada mensagem na DLQ tem de trazer o header x-death, que o broker
 *     acrescenta sozinho (motivo "rejected" e fila de origem).
 *
 * Mudanças em relação à versão anterior: aquecimento, configuração de
 * referência com 0% de inválidas, ordem rotativa, STATUS=PRONTO no lugar do
 * sleep, validação dos códigos de saída e conferência exata da DLQ (antes só
 * a contagem de mensagens na fila).
 *
 * Métricas: processadas OK, tentativas gastas com inválidas, mensagens na
 * DLQ, conferência da DLQ, e vazão de ponta a ponta das 5.000 mensagens
 * (do primeiro envio até o fim do tratamento da última).
 *
 * Arquivo gerado em PASTA_RESULTADOS:
 *   resultados-rabbitmq-dlq.csv (1 linha por execução e configuração)
 */
@Component
public class Cenario3DeadLetterQueue {

    private static final String FILA       = "garantias-cenario3";
    private static final String DLX        = "garantias-cenario3-dlx";
    private static final String FILA_DLQ   = "garantias-cenario3-dlq";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_2_3";
    private static final String CSV = "resultados-rabbitmq-dlq.csv";

    private static final int   MSGS           = 5_000;
    private static final int[] INTERVALOS     = {0, 10};   // 0% e 10% de inválidas
    private static final int   MAX_TENTATIVAS = 3;
    private static final int   PREFETCH       = 100;       // igual ao max.poll.records do Kafka

    private static final long PAUSA_ENTRE_EXECUCOES_MS = 2_000;
    private static final long TIMEOUT_PRONTO_S   = 60;
    private static final long TIMEOUT_PROCESSO_S = 300;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.rabbitmq.cenario2_3.ConsumidorDLQ";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.rabbitmq.cenario2_3.ProdutorCenario3";

    @Value("${benchmarking.execucoes}")         private int totalExecucoes;
    @Value("${benchmarking.host:localhost}")    private String host;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private record Resultado(int invalidas, int ok, int tentativas, int naDlq, boolean dlqConfere,
                             boolean metadadosOk, double duracaoS) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Intermediário — Cenário 3 RabbitMQ — Dead Letter Queue");
        System.out.println("====================================================================");
        System.out.printf("%d mensagens | inválidas: 0%% e 10%% | até %d tentativas antes da DLQ%n", MSGS, MAX_TENTATIVAS);

        File pasta = new File(PASTA_RESULTADOS);
        if (!pasta.exists() && !pasta.mkdirs()) {
            throw new IOException("Não foi possível criar a pasta: " + pasta.getAbsolutePath());
        }
        System.out.println("Gravando em: " + pasta.getAbsolutePath());
        String csv = PASTA_RESULTADOS + File.separator + CSV;
        apagarSeExistir(csv);

        for (int intervalo : INTERVALOS) {
            System.out.printf("%nAquecimento (descartado) — %d%% inválidas%n", pct(intervalo));
            rodar(intervalo, 0);
            Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
        }

        try (PrintWriter w = new PrintWriter(new FileWriter(csv))) {
            w.println("execucao,ordem,taxa_invalidas_pct,enviadas,invalidas_enviadas,processadas_ok," +
                    "tentativas_invalidas,na_dlq,dlq_confere,metadados_ok,duracao_s,vazao_msg_s");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int pos = 0; pos < INTERVALOS.length; pos++) {
                    int intervalo = INTERVALOS[(pos + exec - 1) % INTERVALOS.length];   // ordem rotativa
                    System.out.printf("%nExecução %d/%d — %d%% inválidas (posição %d)%n",
                            exec, totalExecucoes, pct(intervalo), pos + 1);
                    Resultado r = rodar(intervalo, exec);
                    w.printf(Locale.US, "%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%.4f,%.2f%n",
                            exec, pos + 1, pct(intervalo), MSGS, r.invalidas, r.ok, r.tentativas, r.naDlq,
                            r.dlqConfere, r.metadadosOk, r.duracaoS, MSGS / r.duracaoS);
                    w.flush();
                    System.out.printf(Locale.US, "  → OK: %d | DLQ: %d (confere: %s) | %.0f msg/s%n",
                            r.ok, r.naDlq, r.dlqConfere, MSGS / r.duracaoS);
                    Thread.sleep(PAUSA_ENTRE_EXECUCOES_MS);
                }
            }
        }

        System.out.println();
        System.out.println("[SUCESSO] Intermediário — Cenário 3 RabbitMQ concluído.");
        System.out.println("  " + new File(csv).getAbsolutePath());
    }

    private static int pct(int intervalo) {
        return intervalo == 0 ? 0 : 100 / intervalo;
    }

    // ------------------------------------------------------------------
    // Uma execução
    // ------------------------------------------------------------------
    private Resultado rodar(int intervalo, int exec) throws Exception {
        configurarFilas();

        Process consumidor = new ProcessBuilder(comando(CONSUMIDOR_CLASS,
                host, FILA, String.valueOf(MSGS), String.valueOf(exec),
                String.valueOf(PREFETCH), String.valueOf(MAX_TENTATIVAS)))
                .redirectErrorStream(true).start();
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> saidaC = new ConcurrentHashMap<>();
        ExecutorService ex = Executors.newSingleThreadExecutor();
        Future<?> leitor = ex.submit(() -> ler(consumidor, "CONSUMIDOR", saidaC, pronto));

        Process produtor = null;
        try {
            if (!pronto.await(TIMEOUT_PRONTO_S, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timeout aguardando STATUS=PRONTO do consumidor.");
            }
            produtor = new ProcessBuilder(comando(PRODUTOR_CLASS,
                    host, FILA, String.valueOf(MSGS), String.valueOf(exec), String.valueOf(intervalo)))
                    .redirectErrorStream(true).start();
            Map<String, String> saidaP = new HashMap<>();
            ler(produtor, "PRODUTOR", saidaP, null);
            esperar(produtor, "produtor");
            esperar(consumidor, "consumidor");
            leitor.get();

            int invalidas = Integer.parseInt(obrigatorio(saidaP, "INVALIDAS_ENVIADAS"));
            int ok        = Integer.parseInt(obrigatorio(saidaC, "PROCESSADAS_OK"));
            int desviadas = Integer.parseInt(obrigatorio(saidaC, "ENVIADAS_PARA_DLQ"));
            int tent      = Integer.parseInt(obrigatorio(saidaC, "TENTATIVAS_INVALIDAS"));
            long t0       = Long.parseLong(obrigatorio(saidaP, "T0"));
            long fim      = Long.parseLong(obrigatorio(saidaC, "ULTIMO_FIM"));
            if (ok + desviadas != MSGS) {
                throw new IllegalStateException("OK + DLQ = " + (ok + desviadas) + ", esperado " + MSGS);
            }

            Auditoria a = auditarDlq(esperadas(intervalo));
            if (a.total != desviadas) {
                System.out.printf("  [AVISO] o consumidor desviou %d, mas a DLQ tem %d%n", desviadas, a.total);
            }
            return new Resultado(invalidas, ok, tent, a.total, a.confere, a.metadadosOk, (fim - t0) / 1e9);
        } finally {
            if (consumidor.isAlive()) consumidor.destroyForcibly();
            if (produtor != null && produtor.isAlive()) produtor.destroyForcibly();
            ex.shutdownNow();
        }
    }

    private static Set<Integer> esperadas(int intervalo) {
        Set<Integer> s = new HashSet<>();
        if (intervalo > 0) for (int i = intervalo; i <= MSGS; i += intervalo) s.add(i);
        return s;
    }

    private record Auditoria(int total, boolean confere, boolean metadadosOk) {}

    /**
     * Esvazia a fila DLQ e confere sequências e o header x-death. Antes, espera
     * a fila chegar ao número esperado (o desvio para a DLX é assíncrono).
     */
    private Auditoria auditarDlq(Set<Integer> esperadas) throws Exception {
        List<Integer> seqs = new ArrayList<>();
        boolean metadados = true;
        try (Connection conn = fabrica().newConnection(); Channel ch = conn.createChannel()) {
            long limite = System.currentTimeMillis() + 10_000;
            while (ch.queueDeclarePassive(FILA_DLQ).getMessageCount() < esperadas.size()
                    && System.currentTimeMillis() < limite) {
                Thread.sleep(100);
            }
            GetResponse r;
            while ((r = ch.basicGet(FILA_DLQ, true)) != null) {
                Map<String, Object> h = r.getProps().getHeaders();
                Object seq = h == null ? null : h.get("sequencia");
                if (seq != null) seqs.add(((Number) seq).intValue());
                Object xDeath = h == null ? null : h.get("x-death");
                if (!(xDeath instanceof List<?> lista) || lista.isEmpty()
                        || !String.valueOf(((Map<?, ?>) lista.get(0)).get("reason")).equals("rejected")
                        || !String.valueOf(((Map<?, ?>) lista.get(0)).get("queue")).equals(FILA)) {
                    metadados = false;
                }
            }
        }
        boolean confere = seqs.size() == esperadas.size() && new HashSet<>(seqs).equals(esperadas);
        return new Auditoria(seqs.size(), confere, metadados);
    }

    // ------------------------------------------------------------------
    // Filas e auxiliares
    // ------------------------------------------------------------------
    /** Apaga e recria: DLX (fanout) → fila DLQ; fila principal com x-dead-letter-exchange. */
    private void configurarFilas() throws Exception {
        try (Connection conn = fabrica().newConnection(); Channel ch = conn.createChannel()) {
            ch.queueDelete(FILA);
            ch.queueDelete(FILA_DLQ);
            ch.exchangeDelete(DLX);
            ch.exchangeDeclare(DLX, BuiltinExchangeType.FANOUT, true);
            ch.queueDeclare(FILA_DLQ, true, false, false, null);
            ch.queueBind(FILA_DLQ, DLX, "");
            ch.queueDeclare(FILA, true, false, false, Map.of("x-dead-letter-exchange", DLX));
        }
    }

    private ConnectionFactory fabrica() {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        return f;
    }

    private static Void ler(Process p, String nome, Map<String, String> destino, CountDownLatch pronto) {
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            String linha;
            while ((linha = r.readLine()) != null) {
                System.out.println("  [" + nome + "] " + linha);
                if (pronto != null && linha.contains("STATUS=PRONTO")) pronto.countDown();
                int i = linha.indexOf('=');
                if (i > 0) destino.put(linha.substring(0, i).trim(), linha.substring(i + 1).trim());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void esperar(Process proc, String nome) throws InterruptedException {
        if (!proc.waitFor(TIMEOUT_PROCESSO_S, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Timeout aguardando o " + nome + " terminar.");
        }
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
