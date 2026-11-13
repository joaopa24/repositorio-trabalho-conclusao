package com.benchmarking.rabbitmq.cenario3_2;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível avançado — Cenário 2: Impacto da replicação (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: medir quanto a redundância custa em latência e em vazão.
 * No RabbitMQ a replicação é feita pela quorum queue (protocolo Raft): a fila
 * tem N membros em nós diferentes e uma mensagem só é confirmada e entregue
 * depois que a maioria a gravou.
 *
 * Precisa do CLUSTER de 3 nós (docker-compose-cluster-rabbitmq.yml). O
 * orquestrador confere pela management API que há 3 nós rodando.
 *
 * Configurações (fila recriada a cada execução; produtor e consumidor
 * conectados ao nó 1, onde fica o líder — x-queue-leader-locator=client-local):
 *   classica  — fila clássica durável: 1 cópia, sem replicação (a dos cenários básicos)
 *   quorum-1  — quorum queue com 1 membro: sem replicação, mas com o log do Raft;
 *               separa o custo do TIPO de fila do custo da REPLICAÇÃO
 *   quorum-2  — 2 membros (maioria = 2: as duas cópias)
 *   quorum-3  — 3 membros (maioria = 2; o padrão recomendado)
 * Paralelo com o Kafka: classica/quorum-1 ≈ RF 1 e quorum-2 ≈ RF 2 (nas duas,
 * todas as cópias precisam gravar). A quorum-3 NÃO equivale exatamente ao RF 3:
 * o Raft confirma com a MAIORIA (2 de 3), enquanto o acks=all do Kafka espera
 * TODAS as réplicas em sincronia (3 de 3); o min.insync.replicas=2 do Kafka é só
 * o mínimo para aceitar a escrita. É uma diferença de projeto (quórum × ISR), a
 * registrar no texto; a comparação direta entre as tecnologias é a quorum-2.
 *
 * Outra diferença a registrar: o RabbitMQ (fila clássica persistente e quorum
 * queue) confirma depois de gravar em disco, e o Kafka confirma com a mensagem
 * na memória do sistema operacional. Compare o aumento de custo DENTRO de cada
 * tecnologia (quorum-1 → 2 → 3), e não os valores absolutos entre elas.
 *
 * O consumidor confirma cada mensagem (basicAck), e numa quorum queue cada ACK
 * também é replicado; o consumidor do Kafka lê sem commit. Mantido de propósito
 * (diferença da arquitetura): registrar no texto, pesa sobretudo na Parte B.
 *
 * Duas partes, como na versão Kafka:
 *   Parte A — LATÊNCIA: 5.000 mensagens a 200 msg/s.
 *   Parte B — VAZÃO MÁXIMA: 20.000 mensagens sem controle de taxa (ponta a ponta).
 * Aquecimento descartado e configurações em ordem rotativa.
 *
 * Limitação a registrar: os 3 nós rodam na mesma máquina, sem rede física.
 *
 * Isolamento entre execuções (o mesmo do Cenário 4 intermediário):
 *   - uma fila NOVA por execução (avancado-replicacao-<config>-e<n>-<ms>),
 *     apagada no fim dela; as que sobrarem de sessões interrompidas são
 *     apagadas no início (lista pela management API). Um produtor órfão só
 *     publica no nome antigo, que não existe mais, e nada chega à fila medida;
 *   - produtor e consumidor encerram sozinhos se o orquestrador morrer
 *     (vigia do stdin), e o orquestrador mata os filhos num shutdown hook.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-rabbitmq-replicacao-latencia.csv
 *   resultados-rabbitmq-replicacao-latencia-detalhado.csv
 *   resultados-rabbitmq-replicacao-vazao.csv
 */
@Component
public class Cenario2Replicacao {

    // cada execução usa <PREFIXO_FILA>-<configuração>-e<execução>-<ms>
    private static final String PREFIXO_FILA = "avancado-replicacao";
    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_3_2";
    private static final String CSV_LATENCIA  = "resultados-rabbitmq-replicacao-latencia.csv";
    private static final String CSV_DETALHADO = "resultados-rabbitmq-replicacao-latencia-detalhado.csv";
    private static final String CSV_VAZAO     = "resultados-rabbitmq-replicacao-vazao.csv";

    private record Config(String nome, String tipo, int copias) {}
    private static final List<Config> CONFIGS = List.of(
            new Config("classica", "classic", 1),
            new Config("quorum-1", "quorum", 1),
            new Config("quorum-2", "quorum", 2),
            new Config("quorum-3", "quorum", 3));

    private static final int MSGS_LATENCIA = 5_000;
    private static final int TAXA_LATENCIA = 200;
    private static final int MSGS_VAZAO = 20_000;
    private static final long PAUSA_MS = 3_000;
    private static final long TIMEOUT_S = 360;

    private static final String PRODUTOR   = "com.benchmarking.rabbitmq.cenario3_2.ProdutorReplicacao";
    private static final String CONSUMIDOR = "com.benchmarking.rabbitmq.cenario3_2.ConsumidorReplicacao";

    @Value("${benchmarking.execucoes}") private int totalExecucoes;
    @Value("${benchmarking.cluster-host:localhost}")         private String host;
    @Value("${benchmarking.cluster-porta-1:5681}")          private int porta;
    @Value("${benchmarking.cluster-management-1:15681}")    private int portaManagement;
    @Value("${benchmarking.management-user:guest}")         private String usuario;
    @Value("${benchmarking.management-pass:guest}")         private String senha;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private record Resultado(long t0, long t1, long ultimaChegada, double media, double mediana, double min, double max) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 2 RabbitMQ — Impacto da replicação");
        System.out.println("====================================================================");

        Thread gancho = Util.matarFilhosAoEncerrar("mata-subprocessos-replicacao");
        try {
            executarCampanha();
        } finally {
            Util.removerGancho(gancho);
        }
    }

    private void executarCampanha() throws Exception {
        int nos = Util.nosRodando(host, portaManagement, usuario, senha);
        System.out.println("Nós rodando no cluster: " + nos);
        if (nos < 3) throw new IllegalStateException("Este cenário precisa do cluster de 3 nós "
                + "(docker-compose-cluster-rabbitmq.yml) com a management API em " + host + ":" + portaManagement
                + ". Encontrados: " + nos);
        apagarFilasAntigas();

        Util.pasta(PASTA_RESULTADOS);
        String csvLat = PASTA_RESULTADOS + File.separator + CSV_LATENCIA;
        String csvDet = PASTA_RESULTADOS + File.separator + CSV_DETALHADO;
        String csvVaz = PASTA_RESULTADOS + File.separator + CSV_VAZAO;
        Util.apagarSeExistir(csvLat);
        Util.apagarSeExistir(csvDet);
        Util.apagarSeExistir(csvVaz);

        // Parte A
        for (Config c : CONFIGS) {
            System.out.printf("%nAquecimento (descartado) — %s%n", c.nome);
            rodar(c, MSGS_LATENCIA, TAXA_LATENCIA, 0, "-");
            Thread.sleep(PAUSA_MS);
        }
        try (PrintWriter w = new PrintWriter(new FileWriter(csvLat))) {
            w.println("execucao,ordem,configuracao,tipo_fila,copias,num_msgs,taxa_alvo_msg_s,taxa_envio_msg_s," +
                    "latencia_media_ms,latencia_mediana_ms,latencia_min_ms,latencia_max_ms");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int pos = 0; pos < CONFIGS.size(); pos++) {
                    Config c = CONFIGS.get((pos + exec - 1) % CONFIGS.size());
                    System.out.printf("%n[A] Execução %d/%d — %s (posição %d)%n", exec, totalExecucoes, c.nome, pos + 1);
                    Resultado r = rodar(c, MSGS_LATENCIA, TAXA_LATENCIA, exec, csvDet);
                    double taxa = MSGS_LATENCIA / ((r.t1 - r.t0) / 1e9);
                    w.printf(Locale.US, "%d,%d,%s,%s,%d,%d,%d,%.2f,%.4f,%.4f,%.4f,%.4f%n", exec, pos + 1, c.nome, c.tipo,
                            c.copias, MSGS_LATENCIA, TAXA_LATENCIA, taxa, r.media, r.mediana, r.min, r.max);
                    w.flush();
                    System.out.printf(Locale.US, "  → média %.2f ms | mediana %.2f ms%n", r.media, r.mediana);
                    Thread.sleep(PAUSA_MS);
                }
            }
        }

        // Parte B
        for (Config c : CONFIGS) {
            System.out.printf("%nAquecimento (descartado) — %s%n", c.nome);
            rodar(c, MSGS_VAZAO, 0, 0, "-");
            Thread.sleep(PAUSA_MS);
        }
        try (PrintWriter w = new PrintWriter(new FileWriter(csvVaz))) {
            w.println("execucao,ordem,configuracao,tipo_fila,copias,num_msgs,duracao_s,vazao_msg_s,vazao_produtor_msg_s");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int pos = 0; pos < CONFIGS.size(); pos++) {
                    Config c = CONFIGS.get((pos + exec - 1) % CONFIGS.size());
                    System.out.printf("%n[B] Execução %d/%d — %s (posição %d)%n", exec, totalExecucoes, c.nome, pos + 1);
                    Resultado r = rodar(c, MSGS_VAZAO, 0, exec, "-");
                    double dur = (r.ultimaChegada - r.t0) / 1e9;
                    w.printf(Locale.US, "%d,%d,%s,%s,%d,%d,%.4f,%.2f,%.2f%n", exec, pos + 1, c.nome, c.tipo, c.copias,
                            MSGS_VAZAO, dur, MSGS_VAZAO / dur, MSGS_VAZAO / ((r.t1 - r.t0) / 1e9));
                    w.flush();
                    System.out.printf(Locale.US, "  → %.0f msg/s (ponta a ponta)%n", MSGS_VAZAO / dur);
                    Thread.sleep(PAUSA_MS);
                }
            }
        }

        System.out.println();
        System.out.println("[SUCESSO] Avançado — Cenário 2 RabbitMQ concluído.");
        System.out.println("  " + new File(csvLat).getAbsolutePath());
        System.out.println("  " + new File(csvDet).getAbsolutePath());
        System.out.println("  " + new File(csvVaz).getAbsolutePath());
    }

    /** Cria a fila da execução pela conexão com o nó 1 e confere quantos membros ela tem. */
    private void prepararFila(Config c, String fila) throws Exception {
        try (Connection conn = Util.conectar(host, porta, 120); Channel ch = conn.createChannel()) {
            ch.queueDelete(fila);
            Map<String, Object> a = new HashMap<>();
            a.put("x-queue-type", c.tipo);
            if (c.tipo.equals("quorum")) {
                a.put("x-quorum-initial-group-size", c.copias);
                a.put("x-queue-leader-locator", "client-local");
            }
            ch.queueDeclare(fila, true, false, false, a);
        }
        if (c.tipo.equals("quorum")) {
            long limite = System.currentTimeMillis() + 30_000;
            List<String> online = List.of();
            while (System.currentTimeMillis() < limite) {
                String j = Util.http(host, portaManagement, usuario, senha, "/api/queues/%2F/" + fila);
                online = Util.listaJson(j, "online");
                if (online.size() == c.copias) {
                    System.out.println("  fila " + c.nome + ": líder " + Util.textoJson(j, "leader") + ", membros " + online);
                    return;
                }
                Thread.sleep(250);
            }
            throw new IllegalStateException("A quorum queue não ficou com " + c.copias + " membros online: " + online);
        }
    }

    private Resultado rodar(Config c, int numMsgs, int taxa, int exec, String csvDet) throws Exception {
        String fila = PREFIXO_FILA + "-" + c.nome + "-e" + exec + "-" + System.currentTimeMillis();
        prepararFila(c, fila);
        Thread.sleep(1_000);

        ExecutorService ex = Executors.newSingleThreadExecutor();
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> sc = new ConcurrentHashMap<>();
        Process consumidor = Util.iniciar(javaCmd, classpath, CONSUMIDOR, host, String.valueOf(porta), fila,
                String.valueOf(numMsgs), String.valueOf(exec), c.nome, csvDet);
        Future<?> leitor = Util.lerEmSegundoPlano(ex, consumidor, "CONSUMIDOR", sc, pronto, null);
        Process produtor = null;
        try {
            if (!pronto.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");
            produtor = Util.iniciar(javaCmd, classpath, PRODUTOR, host, String.valueOf(porta), fila,
                    String.valueOf(numMsgs), String.valueOf(exec), String.valueOf(taxa));
            Map<String, String> sp = new HashMap<>();
            Util.ler(produtor, "PRODUTOR", sp, null, null);
            Util.esperar(produtor, "produtor", 0, TIMEOUT_S);
            Util.esperar(consumidor, "consumidor", 0, TIMEOUT_S);
            leitor.get();
            if (Integer.parseInt(Util.obrigatorio(sc, "RECEBIDAS")) != numMsgs) {
                throw new IllegalStateException("Contagem errada no consumidor");
            }
            return new Resultado(Long.parseLong(Util.obrigatorio(sp, "T0")), Long.parseLong(Util.obrigatorio(sp, "T1")),
                    Long.parseLong(Util.obrigatorio(sc, "ULTIMA_CHEGADA")),
                    Double.parseDouble(Util.obrigatorio(sc, "MEDIA")), Double.parseDouble(Util.obrigatorio(sc, "MEDIANA")),
                    Double.parseDouble(Util.obrigatorio(sc, "MIN")), Double.parseDouble(Util.obrigatorio(sc, "MAX")));
        } finally {
            Util.matar(consumidor, produtor);
            ex.shutdownNow();
            apagarFila(fila);
        }
    }

    // ------------------------------------------------------------------
    // Filas
    // ------------------------------------------------------------------
    /** Apaga a fila da execução. Falha vira aviso (ela é apagada no próximo início). */
    private void apagarFila(String fila) {
        try (Connection conn = Util.conectar(host, porta, 30); Channel ch = conn.createChannel()) {
            ch.queueDelete(fila);
        } catch (Exception e) {
            System.out.println("  [AVISO] não foi possível apagar a fila " + fila + " (será apagada no próximo início): "
                    + e.getMessage());
        }
    }

    /**
     * Apaga as filas deste cenário que sobraram de sessões interrompidas
     * (inclusive a de nome fixo das versões anteriores), listadas pela
     * management API. Sem a API, só avisa.
     */
    private void apagarFilasAntigas() throws Exception {
        String j = Util.http(host, portaManagement, usuario, senha, "/api/queues/%2F?columns=name");
        if (j == null) {
            System.out.println("  [AVISO] management API sem resposta: filas antigas não foram conferidas");
            return;
        }
        List<String> antigas = new ArrayList<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"name\"\\s*:\\s*\"([^\"]*)\"").matcher(j);
        while (m.find()) {
            String nome = m.group(1);
            if (nome.equals(PREFIXO_FILA) || nome.startsWith(PREFIXO_FILA + "-")) antigas.add(nome);
        }
        if (antigas.isEmpty()) return;
        System.out.println("Apagando " + antigas.size() + " fila(s) de sessões anteriores: " + antigas);
        try (Connection conn = Util.conectar(host, porta, 60); Channel ch = conn.createChannel()) {
            for (String nome : antigas) ch.queueDelete(nome);
        }
    }
}