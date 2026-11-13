package com.benchmarking.rabbitmq.cenario2_4;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Nível intermediário — Cenário 4: Sobrecarga prolongada (RabbitMQ) — ORQUESTRADOR.
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
 *   - Três políticas, intercaladas em ordem rotativa (as três reações de
 *     KLEPPMANN; RICCOMINI, 2026: armazenar, descartar, frear o produtor):
 *       sem-limite        : fila durável sem limite. A fila simplesmente cresce
 *                           (ARMAZENAR). Igual ao "sem-limite" do Kafka.
 *       com-limite        : fila com x-max-length = 2.000 mensagens (≈ os 2 MB
 *                           do Kafka) e x-overflow = drop-head: com a fila
 *                           cheia, o broker aceita a nova mensagem e APAGA a
 *                           mais antiga ainda não entregue (DESCARTAR). É o
 *                           mesmo mecanismo do "com-limite" do Kafka (retenção),
 *                           com o mesmo nome, para a comparação ser direta.
 *       com-limite-recusa : mesmo limite, com x-overflow = reject-publish: com a
 *                           fila cheia, o broker RECUSA novas publicações (nack)
 *                           e o produtor precisa reenviar (FREAR O PRODUTOR).
 *                           Só existe no RabbitMQ: o broker do Kafka não tem
 *                           recusa ligada ao acúmulo (as cotas de produtor
 *                           limitam uma taxa fixa, sem olhar a fila).
 *   - A cada 1 s, uma linha da série temporal: enviadas, consumidas, fila e
 *     bytes das mensagens na fila.
 *   - Fila = prontas (queue.declare passivo, valor exato a cada segundo) +
 *     entregues ainda sem ack (management API, até PREFETCH). Assim ela
 *     conta o mesmo que a fila do Kafka (log end offset − commit), que inclui
 *     o que o consumidor já buscou e ainda não confirmou. Se a management API
 *     não responder, a fila fica só com as prontas e os bytes ficam em -1.
 *     Observação: a management API atualiza esses números a cada ~5 s
 *     (collect_statistics_interval); como as não confirmadas ficam limitadas
 *     ao prefetch (100), o erro na fila é pequeno.
 *
 * Uma fila NOVA por execução (garantias-cenario4-<política>-e<n>-<ms>), como
 * o tópico novo do Kafka. Um produtor órfão de uma execução anterior publica
 * na fila antiga; quando ela já foi apagada, o default exchange descarta a
 * mensagem (sem mandatory) e ela nunca chega à fila medida. Antes, com nome
 * fixo, essas mensagens entravam na execução seguinte sem dar erro.
 * A fila de cada execução é apagada no fim dela; as que sobrarem de sessões
 * interrompidas são apagadas no início.
 *
 * Redes de segurança:
 *   - os subprocessos encerram sozinhos quando o stdin fecha (o orquestrador
 *     morreu) e o orquestrador mata os filhos num shutdown hook;
 *   - antes de começar, procura conexões bench-c4-* na management API
 *     (processos de execuções anteriores ainda vivos) e para se encontrar;
 *   - depois de criar a fila, espera ~3 s e confere que ela continua vazia
 *     (ninguém além do produtor desta execução publica nela).
 *
 * É um cenário de COMPORTAMENTO, com séries temporais longas (~2 min cada):
 * 1 execução de aquecimento por política (descartada, fora dos CSVs) + 10
 * válidas por política, em ordem rotativa, como nos outros cenários do
 * nível intermediário. Na "com-limite-recusa", a produção se
 * estende até ~2 min, porque o produtor passa a andar no ritmo do consumidor.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-rabbitmq-sobrecarga.csv        (1 linha por execução e política)
 *   resultados-rabbitmq-sobrecarga-serie.csv  (1 linha por segundo)
 */
@Component
public class Cenario4SobrecargaProlongada {

    // cada execução usa <PREFIXO_FILA>-<política>-e<exec>-<ms>
    private static final String PREFIXO_FILA = "garantias-cenario4";
    // nomes das conexões do consumidor e do produtor (ver as classes)
    private static final String PREFIXO_CONEXAO = "bench-c4-";
    // mesmo padrão dos outros cenários: caminho relativo ao diretório de execução
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_2_4";
    private static final String CSV_RESUMO = "resultados-rabbitmq-sobrecarga.csv";
    private static final String CSV_SERIE  = "resultados-rabbitmq-sobrecarga-serie.csv";

    private static final int  TAXA_PRODUCAO = 1_000;      // msg/s
    private static final int  DURACAO_PRODUCAO_S = 20;
    private static final int  MSGS = TAXA_PRODUCAO * DURACAO_PRODUCAO_S;
    private static final long PROC_MS = 5;               // consumidor lento
    private static final int  PREFETCH = 100;            // igual ao max.poll.records do Kafka

    private static final String[] POLITICAS = {"sem-limite", "com-limite", "com-limite-recusa"};
    private static final int  LIMITE_FILA = 2_000;      // ≈ 2 MB de mensagens de 1 KB

    private static final long TIMEOUT_PROCESSO_S = 1_800;
    // um produtor órfão pode passar um instante sem publicar (esperando
    // confirmações); 3 s cobrem isso com folga
    private static final long ESPERA_FILA_VAZIA_MS = 3_000;
    // a management API pode levar alguns segundos para tirar da lista uma
    // conexão que acabou de fechar
    private static final long ESPERA_ORFAOS_MS = 15_000;

    private static final String CONSUMIDOR_CLASS = "com.benchmarking.rabbitmq.cenario2_4.ConsumidorSustentado";
    private static final String PRODUTOR_CLASS   = "com.benchmarking.rabbitmq.cenario2_4.ProdutorSustentado";

    @Value("${benchmarking.execucoes:10}")    private int totalExecucoes;
    @Value("${benchmarking.host:localhost}")        private String host;
    @Value("${benchmarking.management-port:15672}") private int    managementPort;
    @Value("${benchmarking.management-user:guest}") private String managementUser;
    @Value("${benchmarking.management-pass:guest}") private String managementPass;

    private final HttpClient http = HttpClient.newHttpClient();

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Intermediário — Cenário 4 RabbitMQ — Sobrecarga prolongada");
        System.out.println("====================================================================");
        System.out.printf("Produtor: %d msg/s por %d s | consumidor: %d ms/msg | políticas: %s%n",
                TAXA_PRODUCAO, DURACAO_PRODUCAO_S, PROC_MS, Arrays.toString(POLITICAS));

        // Ctrl+C / encerramento normal da JVM: mata os subprocessos ainda vivos.
        // (Kill forçado não executa hooks; para esse caso os filhos vigiam o stdin.)
        Thread gancho = new Thread(() -> ProcessHandle.current().descendants()
                .forEach(ProcessHandle::destroyForcibly), "mata-subprocessos-cenario4");
        Runtime.getRuntime().addShutdownHook(gancho);

        try {
            conferirClientesOrfaos();
            apagarFilasAntigas();

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
            System.out.println("[SUCESSO] Intermediário — Cenário 4 RabbitMQ concluído.");
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
        String fila = PREFIXO_FILA + "-" + politica + "-e" + exec + "-" + System.currentTimeMillis();
        System.out.println("  fila: " + fila);
        prepararFila(fila, politica);

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
        try (Connection conn = fabrica().newConnection(); Channel admin = conn.createChannel()) {
            // O stdin dos filhos fica aberto (pipe) durante toda a execução: o EOF
            // só chega se este processo morrer, e aí os filhos encerram sozinhos.
            consumidor = new ProcessBuilder(comando(CONSUMIDOR_CLASS, host, fila,
                    String.valueOf(MSGS), String.valueOf(PROC_MS), String.valueOf(PREFETCH)))
                    .redirectErrorStream(true).start();
            final Process c = consumidor;
            Future<?> leitorC = ex.submit(() -> ler(c, "CONSUMIDOR", saidaC, pronto, "CONSUMIDAS", consumidas));

            if (!pronto.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");

            long inicio = System.nanoTime();
            monitor.scheduleAtFixedRate(() -> {
                try {
                    long[] f = filaEArmazenamento(admin, fila);
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

            produtor = new ProcessBuilder(comando(PRODUTOR_CLASS, host, fila,
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
            // sem limite, nada se perde; com recusa, o broker recusa e o produtor reenvia, então também nada se perde.
            // Com descarte (drop-head), as perdas são o resultado esperado e não um erro.
            if (!politica.equals("com-limite") && uni != MSGS) {
                throw new IllegalStateException("O consumidor recebeu " + uni + " de " + MSGS + " mensagens distintas");
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
            apagarFila(fila);
        }
    }

    /**
     * {fila = prontas + entregues sem ack, bytes das mensagens na fila}.
     * As prontas vêm do protocolo AMQP (exatas); as sem ack e os bytes, da
     * management API (-1 nos bytes e 0 nas sem ack, se a API falhar).
     */
    private synchronized long[] filaEArmazenamento(Channel ch, String fila) throws Exception {
        long prontas = ch.queueDeclarePassive(fila).getMessageCount();
        long semAck = 0;
        long bytes = -1;
        String corpo = apiGet("/api/queues/%2F/" + URLEncoder.encode(fila, StandardCharsets.UTF_8));
        if (corpo != null) {
            bytes = numeroJson(corpo, "message_bytes");
            semAck = Math.max(0, numeroJson(corpo, "messages_unacknowledged"));
        }
        if (!avisoApi && bytes < 0) {
            avisoApi = true;
            System.out.println("  [AVISO] management API sem resposta: a fila conta só as mensagens prontas e os bytes ficam em -1");
        }
        return new long[]{prontas + semAck, bytes};
    }

    private volatile boolean avisoApi = false;

    /** GET na management API; devolve o corpo se a resposta for 200 e null em qualquer falha. */
    private String apiGet(String caminho) {
        try {
            String url = String.format("http://%s:%d%s", host, managementPort, caminho);
            String auth = Base64.getEncoder().encodeToString((managementUser + ":" + managementPass).getBytes(StandardCharsets.UTF_8));
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).header("Authorization", "Basic " + auth)
                    .timeout(java.time.Duration.ofSeconds(2)).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200 ? resp.body() : null;
        } catch (Exception e) {
            return null;   // a management API é opcional
        }
    }

    private static long numeroJson(String json, String chave) {
        String k = "\"" + chave + "\":";
        int i = json.indexOf(k);
        if (i < 0) return -1;
        int a = i + k.length(), b = a;
        while (b < json.length() && Character.isDigit(json.charAt(b))) b++;
        return b > a ? Long.parseLong(json.substring(a, b)) : -1;
    }

    /** Todos os valores de uma chave de texto num JSON ("chave":"valor"). */
    private static List<String> textosJson(String json, String chave) {
        List<String> r = new ArrayList<>();
        Matcher m = Pattern.compile("\"" + Pattern.quote(chave) + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
        while (m.find()) r.add(m.group(1));
        return r;
    }

    /**
     * Procura conexões bench-c4-* (consumidor ou produtor de uma execução
     * anterior ainda vivo). Com uma fila nova por execução, eles não
     * contaminam os dados, mas disputam CPU e broker com a medição. Exige a
     * management API; sem ela, só avisa. Processos do código antigo (sem nome
     * de conexão) não aparecem aqui: para eles, use jps -l.
     */
    private void conferirClientesOrfaos() throws Exception {
        long ate = System.currentTimeMillis() + ESPERA_ORFAOS_MS;
        boolean avisou = false;
        while (true) {
            String corpo = apiGet("/api/connections?columns=user_provided_name");
            if (corpo == null) {
                System.out.println("  [AVISO] management API sem resposta: não foi possível procurar clientes de"
                        + " execuções anteriores. Confira com 'jps -l' antes de rodar.");
                return;
            }
            List<String> orfaos = new ArrayList<>();
            for (String nome : textosJson(corpo, "user_provided_name")) {
                if (nome.startsWith(PREFIXO_CONEXAO)) orfaos.add(nome);
            }
            if (orfaos.isEmpty()) return;

            if (System.currentTimeMillis() > ate) {
                throw new IllegalStateException("Há clientes de execuções anteriores ainda conectados: " + orfaos
                        + ". Eles disputariam CPU e broker com a medição. Rode 'jps -l', encerre os processos"
                        + " ConsumidorSustentado/ProdutorSustentado ('taskkill /F /PID <pid>' no Windows,"
                        + " 'kill -9 <pid>' no Linux) e rode de novo.");
            }
            if (!avisou) {
                System.out.println("  [AVISO] clientes de execuções anteriores ainda conectados: " + orfaos
                        + " — aguardando até " + ESPERA_ORFAOS_MS / 1000 + " s...");
                avisou = true;
            }
            Thread.sleep(2_000);
        }
    }

    private static boolean ehFilaDoCenario(String nome) {
        return nome.equals(PREFIXO_FILA) || nome.startsWith(PREFIXO_FILA + "-");
    }

    /**
     * Apaga as filas deste cenário que sobraram de sessões interrompidas
     * (inclusive a garantias-cenario4 de nome fixo das versões anteriores).
     * Lista as filas pela management API; sem ela, apaga só a de nome fixo.
     */
    private void apagarFilasAntigas() {
        Set<String> antigas = new LinkedHashSet<>();
        antigas.add(PREFIXO_FILA);
        String corpo = apiGet("/api/queues/%2F?columns=name");
        if (corpo != null) {
            for (String nome : textosJson(corpo, "name")) {
                if (ehFilaDoCenario(nome)) antigas.add(nome);
            }
        } else {
            System.out.println("  [AVISO] management API sem resposta: só a fila de nome fixo das versões anteriores"
                    + " será apagada; filas de sessões interrompidas podem sobrar no broker.");
        }
        if (antigas.size() > 1) System.out.println("Apagando filas de sessões anteriores: " + antigas);
        for (String nome : antigas) apagarFila(nome);
    }

    /**
     * Cria a fila desta execução conforme a política:
     *   sem-limite        → sem argumentos
     *   com-limite        → x-max-length + x-overflow = drop-head (descarta as mais antigas)
     *   com-limite-recusa → x-max-length + x-overflow = reject-publish (recusa as novas)
     * Depois espera ~3 s e confere que ninguém publicou nela.
     */
    private void prepararFila(String fila, String politica) throws Exception {
        Map<String, Object> argumentos = switch (politica) {
            case "sem-limite"        -> null;
            case "com-limite"        -> Map.of("x-max-length", LIMITE_FILA, "x-overflow", "drop-head");
            case "com-limite-recusa" -> Map.of("x-max-length", LIMITE_FILA, "x-overflow", "reject-publish");
            default -> throw new IllegalArgumentException("Política desconhecida: " + politica);
        };
        try (Connection conn = fabrica().newConnection(); Channel ch = conn.createChannel()) {
            ch.queueDeclare(fila, true, false, false, argumentos);
            Thread.sleep(ESPERA_FILA_VAZIA_MS);
            long n = ch.queueDeclarePassive(fila).getMessageCount();
            if (n != 0) {
                throw new IllegalStateException("A fila " + fila + " recebeu " + n + " mensagem(ns) antes de o produtor"
                        + " desta execução começar: há outro cliente publicando nela. Rode 'jps -l', encerre"
                        + " processos antigos do benchmark e rode de novo.");
            }
        }
    }

    /**
     * Apaga uma fila. Uma fila inexistente não é erro (algumas versões do
     * broker respondem com erro nesse caso; aí não há o que apagar). Outras
     * falhas viram aviso: a fila é apagada no próximo início.
     */
    private void apagarFila(String fila) {
        try (Connection conn = fabrica().newConnection(); Channel ch = conn.createChannel()) {
            ch.queueDelete(fila);
        } catch (Exception e) {
            String msg = String.valueOf(e.getMessage());
            if (!msg.contains("NOT_FOUND") && !msg.contains("404")) {
                System.out.println("  [AVISO] não foi possível apagar a fila " + fila + ": " + msg);
            }
        }
    }

    private ConnectionFactory fabrica() {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(host);
        return f;
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
