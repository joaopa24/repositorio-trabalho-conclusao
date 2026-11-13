package com.benchmarking.kafka.cenario3_2;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível avançado — Cenário 2: Impacto da replicação (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: medir quanto a redundância custa em latência e em vazão,
 * variando o fator de replicação do tópico: 1 (sem cópia), 2 e 3.
 *
 * Precisa do CLUSTER de 3 brokers (docker-compose-cluster.yml, portas
 * 19092–19094). O orquestrador confere que há 3 brokers antes de começar.
 *
 * Configuração de cada fator:
 *   tópico com 1 partição, replication.factor = RF,
 *   min.insync.replicas = min(RF, 2) (o valor recomendado para RF = 3);
 *   produtor com acks=all (ver ProdutorReplicacao).
 *
 * Duas partes, como nos Cenários 3 básico e 1 intermediário:
 *   Parte A — LATÊNCIA: 5.000 mensagens a 200 msg/s.
 *   Parte B — VAZÃO MÁXIMA: 20.000 mensagens sem controle de taxa; vazão de
 *             ponta a ponta (primeiro envio até a última chegada).
 * Aquecimento descartado e fatores intercalados em ordem rotativa.
 *
 * Limitação a registrar no texto: os 3 brokers rodam na MESMA máquina, sem
 * rede física entre eles; o custo de replicação medido é menor que o de um
 * cluster real e reflete principalmente o trabalho extra de CPU e disco.
 *
 * Diferenças da arquitetura a registrar no texto:
 *   - acks=all espera TODAS as réplicas em sincronia (ISR), e não só as do
 *     min.insync.replicas, que é apenas o mínimo para aceitar a escrita. Com
 *     RF 3, o Kafka espera 3 cópias; a quorum queue de 3 membros do RabbitMQ
 *     espera a maioria (2). A comparação direta é RF 2 × quorum-2.
 *   - O Kafka confirma com a mensagem na memória do sistema operacional (sem
 *     fsync por mensagem) e conta com as réplicas para a durabilidade; o
 *     RabbitMQ confirma depois de gravar em disco. Compare o aumento de custo
 *     DENTRO de cada tecnologia (RF 1 → 2 → 3), e não os valores absolutos.
 *
 * Isolamento entre execuções (o mesmo do Cenário 4 intermediário):
 *   - um tópico NOVO por execução (avancado-replicacao-rf<RF>-e<n>-<ms>),
 *     apagado no fim dela; os que sobrarem de sessões interrompidas são
 *     apagados no início;
 *   - depois de criar, confere o fator de replicação, as réplicas em sincronia
 *     e o min.insync.replicas do tópico;
 *   - produtor e consumidor encerram sozinhos se o orquestrador morrer
 *     (vigia do stdin), e o orquestrador mata os filhos num shutdown hook.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-kafka-replicacao-latencia.csv
 *   resultados-kafka-replicacao-latencia-detalhado.csv
 *   resultados-kafka-replicacao-vazao.csv
 */
@Component
public class Cenario2Replicacao {

    // cada execução usa <PREFIXO_TOPICO>-rf<RF>-e<execução>-<ms>
    private static final String PREFIXO_TOPICO = "avancado-replicacao";
    private static final String PASTA_RESULTADOS = "resultado_cenario_3_2";
    private static final String CSV_LATENCIA  = "resultados-kafka-replicacao-latencia.csv";
    private static final String CSV_DETALHADO = "resultados-kafka-replicacao-latencia-detalhado.csv";
    private static final String CSV_VAZAO     = "resultados-kafka-replicacao-vazao.csv";

    private static final int[] FATORES = {1, 2, 3};
    private static final int MSGS_LATENCIA = 5_000;
    private static final int TAXA_LATENCIA = 200;
    private static final int MSGS_VAZAO = 20_000;
    private static final long PAUSA_MS = 3_000;
    private static final long TIMEOUT_S = 360;

    private static final String PRODUTOR   = "com.benchmarking.kafka.cenario3_2.ProdutorReplicacao";
    private static final String CONSUMIDOR = "com.benchmarking.kafka.cenario3_2.ConsumidorReplicacao";

    @Value("${benchmarking.execucoes}") private int totalExecucoes;
    @Value("${benchmarking.bootstrap-servers-cluster:127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094}")
    private String bootstrap;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private record Resultado(long t0, long t1, long ultimaChegada, double media, double mediana, double min, double max) {}

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 2 Kafka — Impacto da replicação");
        System.out.println("====================================================================");

        Thread gancho = Util.matarFilhosAoEncerrar("mata-subprocessos-replicacao");
        try {
            executarCampanha();
        } finally {
            Util.removerGancho(gancho);
        }
    }

    private void executarCampanha() throws Exception {
        try (AdminClient admin = Util.admin(bootstrap)) {
            int n = Util.brokers(admin);
            System.out.println("Brokers no cluster: " + n + " (" + bootstrap + ")");
            if (n < 3) {
                throw new IllegalStateException("Este cenário precisa do cluster de 3 brokers (docker-compose-cluster.yml). "
                        + "Encontrados: " + n);
            }
        }
        apagarTopicosAntigos();

        Util.pasta(PASTA_RESULTADOS);
        String csvLat = PASTA_RESULTADOS + File.separator + CSV_LATENCIA;
        String csvDet = PASTA_RESULTADOS + File.separator + CSV_DETALHADO;
        String csvVaz = PASTA_RESULTADOS + File.separator + CSV_VAZAO;
        Util.apagarSeExistir(csvLat);
        Util.apagarSeExistir(csvDet);
        Util.apagarSeExistir(csvVaz);

        // Parte A
        for (int rf : FATORES) {
            System.out.printf("%nAquecimento (descartado) — RF %d%n", rf);
            rodar(rf, MSGS_LATENCIA, TAXA_LATENCIA, 0, "-");
            Thread.sleep(PAUSA_MS);
        }
        try (PrintWriter w = new PrintWriter(new FileWriter(csvLat))) {
            w.println("execucao,ordem,fator_replicacao,min_insync,num_msgs,taxa_alvo_msg_s,taxa_envio_msg_s," +
                    "latencia_media_ms,latencia_mediana_ms,latencia_min_ms,latencia_max_ms");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int pos = 0; pos < FATORES.length; pos++) {
                    int rf = FATORES[(pos + exec - 1) % FATORES.length];
                    System.out.printf("%n[A] Execução %d/%d — RF %d (posição %d)%n", exec, totalExecucoes, rf, pos + 1);
                    Resultado r = rodar(rf, MSGS_LATENCIA, TAXA_LATENCIA, exec, csvDet);
                    double taxa = MSGS_LATENCIA / ((r.t1 - r.t0) / 1e9);
                    w.printf(Locale.US, "%d,%d,%d,%d,%d,%d,%.2f,%.4f,%.4f,%.4f,%.4f%n", exec, pos + 1, rf, minIsr(rf),
                            MSGS_LATENCIA, TAXA_LATENCIA, taxa, r.media, r.mediana, r.min, r.max);
                    w.flush();
                    System.out.printf(Locale.US, "  → média %.2f ms | mediana %.2f ms%n", r.media, r.mediana);
                    Thread.sleep(PAUSA_MS);
                }
            }
        }

        // Parte B
        for (int rf : FATORES) {
            System.out.printf("%nAquecimento (descartado) — RF %d%n", rf);
            rodar(rf, MSGS_VAZAO, 0, 0, "-");
            Thread.sleep(PAUSA_MS);
        }
        try (PrintWriter w = new PrintWriter(new FileWriter(csvVaz))) {
            w.println("execucao,ordem,fator_replicacao,min_insync,num_msgs,duracao_s,vazao_msg_s,vazao_produtor_msg_s");
            for (int exec = 1; exec <= totalExecucoes; exec++) {
                for (int pos = 0; pos < FATORES.length; pos++) {
                    int rf = FATORES[(pos + exec - 1) % FATORES.length];
                    System.out.printf("%n[B] Execução %d/%d — RF %d (posição %d)%n", exec, totalExecucoes, rf, pos + 1);
                    Resultado r = rodar(rf, MSGS_VAZAO, 0, exec, "-");
                    double dur = (r.ultimaChegada - r.t0) / 1e9;
                    w.printf(Locale.US, "%d,%d,%d,%d,%d,%.4f,%.2f,%.2f%n", exec, pos + 1, rf, minIsr(rf), MSGS_VAZAO, dur,
                            MSGS_VAZAO / dur, MSGS_VAZAO / ((r.t1 - r.t0) / 1e9));
                    w.flush();
                    System.out.printf(Locale.US, "  → %.0f msg/s (ponta a ponta)%n", MSGS_VAZAO / dur);
                    Thread.sleep(PAUSA_MS);
                }
            }
        }

        System.out.println();
        System.out.println("[SUCESSO] Avançado — Cenário 2 Kafka concluído.");
        System.out.println("  " + new File(csvLat).getAbsolutePath());
        System.out.println("  " + new File(csvDet).getAbsolutePath());
        System.out.println("  " + new File(csvVaz).getAbsolutePath());
    }

    private static int minIsr(int rf) { return Math.min(rf, 2); }

    private Resultado rodar(int rf, int numMsgs, int taxa, int exec, String csvDet) throws Exception {
        String topico = PREFIXO_TOPICO + "-rf" + rf + "-e" + exec + "-" + System.currentTimeMillis();
        try (AdminClient admin = Util.admin(bootstrap)) {
            NewTopic t = new NewTopic(topico, 1, (short) rf)
                    .configs(Map.of("min.insync.replicas", String.valueOf(minIsr(rf))));
            Util.recriarTopicos(admin, List.of(t));
            conferirTopico(admin, topico, rf);
        }
        Thread.sleep(1_000);

        ExecutorService ex = Executors.newSingleThreadExecutor();
        CountDownLatch pronto = new CountDownLatch(1);
        Map<String, String> sc = new ConcurrentHashMap<>();
        Process consumidor = Util.iniciar(javaCmd, classpath, CONSUMIDOR, bootstrap, topico, String.valueOf(numMsgs),
                String.valueOf(exec), String.valueOf(rf), csvDet);
        Future<?> leitor = Util.lerEmSegundoPlano(ex, consumidor, "CONSUMIDOR", sc, pronto, null);
        Process produtor = null;
        try {
            if (!pronto.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");
            produtor = Util.iniciar(javaCmd, classpath, PRODUTOR, bootstrap, topico, String.valueOf(numMsgs),
                    String.valueOf(exec), String.valueOf(taxa));
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
            apagarTopico(topico);
        }
    }

    // ------------------------------------------------------------------
    // Tópicos
    // ------------------------------------------------------------------
    /**
     * Confere que o tópico recém-criado tem exatamente RF réplicas, todas em
     * sincronia, e o min.insync.replicas esperado. Protege os dados contra um
     * tópico com a configuração padrão do cluster. Logo depois da criação os
     * metadados podem ainda estar chegando, por isso tenta por até 15 s.
     */
    private void conferirTopico(AdminClient admin, String topico, int rf) throws Exception {
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, topico);
        long limite = System.currentTimeMillis() + 15_000;
        String ultimo = "sem resposta";
        while (System.currentTimeMillis() < limite) {
            try {
                TopicDescription d = admin.describeTopics(List.of(topico)).allTopicNames()
                        .get(10, TimeUnit.SECONDS).get(topico);
                Config cfg = admin.describeConfigs(List.of(recurso)).all().get(10, TimeUnit.SECONDS).get(recurso);
                ConfigEntry e = cfg.get("min.insync.replicas");
                int replicas = d.partitions().get(0).replicas().size();
                int isr = d.partitions().get(0).isr().size();
                String minIsrAtual = e == null ? null : e.value();
                ultimo = "réplicas=" + replicas + ", em sincronia=" + isr + ", min.insync.replicas=" + minIsrAtual;
                if (replicas == rf && isr == rf && String.valueOf(minIsr(rf)).equals(minIsrAtual)) {
                    System.out.println("  tópico " + topico + ": líder no broker "
                            + d.partitions().get(0).leader().id() + ", " + ultimo);
                    return;
                }
            } catch (ExecutionException ignorada) {
                // tópico ainda não propagado para todos os brokers
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("O tópico " + topico + " não ficou com RF " + rf + " e min.insync.replicas "
                + minIsr(rf) + " (" + ultimo + ").");
    }

    /** Apaga o tópico da execução, sem esperar o broker terminar. Falha vira aviso. */
    private void apagarTopico(String topico) {
        try (AdminClient admin = Util.admin(bootstrap)) {
            admin.deleteTopics(List.of(topico)).all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("  [AVISO] não foi possível apagar o tópico " + topico + " (será apagado no próximo início): "
                    + e.getMessage());
        }
    }

    /** Apaga os tópicos deste cenário que sobraram de sessões interrompidas (inclusive o de nome fixo antigo). */
    private void apagarTopicosAntigos() throws Exception {
        try (AdminClient admin = Util.admin(bootstrap)) {
            List<String> antigos = new ArrayList<>();
            for (String nome : admin.listTopics().names().get(30, TimeUnit.SECONDS)) {
                if (nome.equals(PREFIXO_TOPICO) || nome.startsWith(PREFIXO_TOPICO + "-")) antigos.add(nome);
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
}