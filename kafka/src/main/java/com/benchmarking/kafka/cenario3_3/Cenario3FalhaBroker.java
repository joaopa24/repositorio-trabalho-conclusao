package com.benchmarking.kafka.cenario3_3;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;

/**
 * Nível avançado — Cenário 3: Falha de broker (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: ver o que acontece com as mensagens quando o broker LÍDER cai no
 * meio do envio: quantas confirmadas se perdem, quantas chegam duplicadas e
 * por quanto tempo o fluxo para até o novo líder assumir.
 *
 * Precisa do CLUSTER de 3 brokers (docker-compose-cluster.yml) e do comando
 * "docker" no PATH: o orquestrador derruba e religa o container do líder.
 *
 * Tópico: 1 partição, replication.factor=3, min.insync.replicas=2, com nome
 * NOVO a cada execução (avancado-falha-broker-<config>-e<n>-<ms>). Um produtor
 * ou consumidor de uma execução anterior não enxerga o tópico novo e não pode
 * misturar mensagens nele (as sequências repetidas seriam contadas como
 * duplicadas). O tópico de cada execução é apagado no fim dela; os que
 * sobrarem de sessões interrompidas são apagados no início. Depois de criado,
 * o tópico tem o fator de replicação e o min.insync.replicas conferidos.
 *
 * Líder escolhido: o tópico é criado com as réplicas definidas, pondo como
 * líder preferido um broker que NÃO é o controlador do cluster (o de menor id
 * entre os outros dois). Com ZooKeeper, o controlador é um dos brokers; se o
 * líder derrubado fosse também o controlador, o cluster teria de eleger um
 * controlador novo antes de eleger o líder, e a pausa seria mais longa. Sem a
 * escolha, isso aconteceria em cerca de 1 de cada 3 execuções, por sorteio.
 * Assim, todas as execuções medem a mesma situação, como no RabbitMQ, em que
 * o líder da fila é sempre o nó 1. A queda simultânea do líder e do
 * controlador não é avaliada. A coluna era_controlador fica como conferência
 * (sempre false; se der true, o controlador mudou durante a execução).
 * Como o controlador nunca é derrubado, ele é o mesmo durante toda a campanha.
 *
 * Rebalanceamento de liderança: o docker-compose do cluster deve ter
 * KAFKA_AUTO_LEADER_REBALANCE_ENABLE: "false", para a liderança não voltar
 * sozinha ao broker religado no meio da medição (a quorum queue também não a
 * devolve). A coluna lider_final confere isso.
 *
 * Configurações (os três níveis de garantia do produtor; ordem rotativa):
 *   acks-all : acks=all, idempotência ligada   — par do RabbitMQ "confirms"
 *   acks-1   : acks=1,   idempotência desligada — sem par (a quorum queue não
 *              confirma só pelo líder); exclusiva do Kafka
 *   acks-0   : acks=0,   idempotência desligada — par do RabbitMQ "sem-confirms"
 * A coluna "garantia" do CSV (replicas, lider, nenhuma) indica os pares.
 *
 * Linha do tempo de cada execução (contada do início do envio):
 *   0 s   produtor começa: 200 msg/s durante 60 s (12.000 mensagens)
 *   20 s  docker kill no container do líder da partição (queda abrupta)
 *   40 s  docker start no mesmo container (ele volta como seguidor). A busca
 *         pelo novo líder nunca atrasa a volta: se até os 40 s ainda não houver
 *         líder novo, a busca continua depois do docker start.
 *   60 s  fim do envio; o consumidor espera mais 10 s por mensagens atrasadas
 * Depois de cada execução espera o cluster voltar a 3 brokers e a partição a
 * ter 3 réplicas em sincronia, e mais 20 s (a mesma espera da versão
 * RabbitMQ), para a próxima começar do mesmo estado.
 *
 * Métricas por execução (CSV no MESMO formato da versão RabbitMQ):
 *   confirmadas         — o broker confirmou ao produtor (-1 com acks=0)
 *   perdidas            — confirmadas que NUNCA chegaram ao consumidor (-1 com acks=0)
 *   nao_entregues       — enviadas que nunca chegaram (a única medida de perda
 *                         que vale para todas as configurações)
 *   duplicadas          — entregas repetidas da mesma sequência (no Kafka, só
 *                         podem vir de reenvios do produtor)
 *   era_controlador     — o broker derrubado era o controlador ativo
 *                         (conferência: deve ser sempre false)
 *   lider_final         — líder ao fim do envio; diferente do novo_lider se a
 *                         liderança voltou ao broker religado durante a execução
 *                         (não deve acontecer com o rebalanceamento desligado)
 *   eleicao_ms          — da queda até o metadado mostrar um novo líder
 *   maior_intervalo_sem_confirmacao_ms (produtor) e maior_pausa_entrega_ms
 *                       (consumidor) — quanto tempo o fluxo ficou parado
 *
 * Cenário de comportamento: 1 execução de aquecimento por configuração
 * (com queda e volta, conferida, mas fora dos CSVs) e 3 execuções válidas.
 *
 * Arquivos gerados em PASTA_RESULTADOS:
 *   resultados-kafka-falha-broker.csv         (1 linha por execução)
 *   resultados-kafka-falha-broker-serie.csv   (1 linha por mensagem recebida:
 *                                              tempo de envio, chegada e latência)
 */
@Component
public class Cenario3FalhaBroker {

    private static final String PREFIXO_TOPICO = "avancado-falha-broker";
    private static final String PASTA_RESULTADOS = "resultado_cenario_3_3";
    private static final String CSV       = "resultados-kafka-falha-broker.csv";
    private static final String CSV_SERIE = "resultados-kafka-falha-broker-serie.csv";

    /** Mesmo cabeçalho da versão RabbitMQ. */
    static final String CABECALHO = "execucao,ordem,configuracao,garantia,idempotencia,enviadas,confirmadas," +
            "falhas_envio,sem_resposta,recebidas_unicas,entregas,duplicadas,redeliveries,perdidas,nao_entregues," +
            "cancelamentos,lider_derrubado,era_controlador,novo_lider,lider_final,t_queda_s,t_volta_s,eleicao_ms," +
            "maior_intervalo_sem_confirmacao_ms,maior_pausa_entrega_ms";
    static final String CABECALHO_SERIE = "execucao,configuracao,sequencia,envio_s,chegada_s,latencia_ms,duplicada,redelivered";

    /** nome no CSV, valor de acks, nível de garantia (para parear com o RabbitMQ). */
    private record Configuracao(String nome, String acks, String garantia) {
        boolean idempotente() { return acks.equals("all"); }
        boolean temConfirmacao() { return !acks.equals("0"); }
    }
    private static final List<Configuracao> CONFIGS = List.of(
            new Configuracao("acks-all", "all", "replicas"),
            new Configuracao("acks-1", "1", "lider"),
            new Configuracao("acks-0", "0", "nenhuma"));

    private static final int TAXA = 200;
    private static final int DURACAO_S = 60;
    private static final int MSGS = TAXA * DURACAO_S;
    private static final long QUEDA_S = 20;
    private static final long VOLTA_S = 40;
    private static final int EXECUCOES = 3;
    private static final long TIMEOUT_S = 400;
    private static final long ESPERA_ESTAVEL_MS = 20_000;

    private static final String PRODUTOR   = "com.benchmarking.kafka.cenario3_3.ProdutorFalhaBroker";
    private static final String CONSUMIDOR = "com.benchmarking.kafka.cenario3_3.ConsumidorFalhaBroker";

    @Value("${benchmarking.bootstrap-servers-cluster:127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094}")
    private String bootstrap;
    /** Prefixo do nome dos containers: kafka-1, kafka-2, kafka-3 (= broker.id). */
    @Value("${benchmarking.prefixo-container:kafka-}")
    private String prefixoContainer;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 3 Kafka — Falha de broker");
        System.out.println("====================================================================");

        Thread gancho = Util.matarFilhosAoEncerrar("mata-subprocessos-falha-broker");
        try {
            try (AdminClient admin = Util.admin(bootstrap)) {
                int n = Util.brokers(admin);
                System.out.println("Brokers no cluster: " + n);
                if (n < 3) throw new IllegalStateException("Precisa do cluster de 3 brokers (docker-compose-cluster.yml). Encontrados: " + n);
                apagarTopicosAntigos(admin);
            }
            System.out.println("Docker: " + Util.rodarComando(30, "docker", "--version"));

            File pasta = Util.pasta(PASTA_RESULTADOS);
            String csv = PASTA_RESULTADOS + File.separator + CSV;
            String csvSerie = PASTA_RESULTADOS + File.separator + CSV_SERIE;
            Util.apagarSeExistir(csv);
            Util.apagarSeExistir(csvSerie);

            try (PrintWriter w = new PrintWriter(new FileWriter(csv));
                 PrintWriter ws = new PrintWriter(new BufferedWriter(new FileWriter(csvSerie)))) {
                w.println(CABECALHO);
                ws.println(CABECALHO_SERIE);
                for (Configuracao c : CONFIGS) {
                    System.out.printf("%nAquecimento (descartado) — %s%n", c.nome);
                    rodar(c, 0, 0, pasta, null, null);
                }
                for (int exec = 1; exec <= EXECUCOES; exec++) {
                    for (int pos = 0; pos < CONFIGS.size(); pos++) {
                        Configuracao c = CONFIGS.get((pos + exec - 1) % CONFIGS.size());
                        System.out.printf("%nExecução %d/%d — %s (posição %d)%n", exec, EXECUCOES, c.nome, pos + 1);
                        rodar(c, exec, pos + 1, pasta, w, ws);
                        w.flush();
                        ws.flush();
                    }
                }
            }
            System.out.println();
            System.out.println("[SUCESSO] Avançado — Cenário 3 Kafka concluído.");
            System.out.println("  " + new File(csv).getAbsolutePath());
            System.out.println("  " + new File(csvSerie).getAbsolutePath());
        } finally {
            Util.removerGancho(gancho);
        }
    }

    /** w == null: aquecimento (executa e confere tudo, mas não grava). */
    private void rodar(Configuracao c, int exec, int ordem, File pasta, PrintWriter w, PrintWriter ws) throws Exception {
        File arqConf = new File(pasta, "temp-confirmadas.txt");
        File arqRec  = new File(pasta, "temp-recebidas.txt");
        Util.apagarSeExistir(arqConf.getPath());
        Util.apagarSeExistir(arqRec.getPath());

        String topico = PREFIXO_TOPICO + "-" + c.nome + "-e" + exec + "-" + System.currentTimeMillis();
        System.out.println("  tópico: " + topico);
        int liderEscolhido;
        try (AdminClient admin = Util.admin(bootstrap)) {
            liderEscolhido = criarTopico(admin, topico);
            esperarClusterSaudavel(admin, topico, false);
        }

        ExecutorService ex = Executors.newFixedThreadPool(2);
        CountDownLatch prontoC = new CountDownLatch(1);
        CountDownLatch prontoP = new CountDownLatch(1);
        Map<String, String> sc = new ConcurrentHashMap<>();
        Map<String, String> sp = new ConcurrentHashMap<>();
        Process consumidor = null, produtor = null;
        String containerDerrubado = null;
        boolean topicoApagado = false;
        try (AdminClient admin = Util.admin(bootstrap)) {
            consumidor = Util.iniciar(javaCmd, classpath, CONSUMIDOR, bootstrap, topico, arqRec.getAbsolutePath());
            Future<?> lc = Util.lerEmSegundoPlano(ex, consumidor, "CONSUMIDOR", sc, prontoC, null);
            if (!prontoC.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidor não ficou pronto.");

            produtor = Util.iniciar(javaCmd, classpath, PRODUTOR, bootstrap, topico, String.valueOf(MSGS),
                    String.valueOf(TAXA), c.acks, arqConf.getAbsolutePath());
            Future<?> lp = Util.lerEmSegundoPlano(ex, produtor, "PRODUTOR", sp, prontoP, Set.of("ENVIADAS"));
            if (!prontoP.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("Produtor não ficou pronto.");
            long t0 = Long.parseLong(Util.obrigatorio(sp, "T0"));

            // --- queda do líder ---
            Util.aguardarAte(t0 + QUEDA_S * 1_000_000_000L);
            int lider = lider(admin, topico);
            if (lider != liderEscolhido) {
                throw new IllegalStateException("O líder mudou antes da queda: " + lider + " (escolhido " + liderEscolhido + ").");
            }
            int controlador = controladorAtivo(admin);
            if (lider == controlador) {
                System.out.println("  [AVISO] o controlador mudou para o broker líder desde a criação do tópico;"
                        + " esta execução derruba os dois (era_controlador = true)");
            }
            containerDerrubado = prefixoContainer + lider;
            long tQueda = System.nanoTime();
            Util.rodarComando(60, "docker", "kill", containerDerrubado);
            System.out.printf("  >>> docker kill %s (líder%s)%n", containerDerrubado, lider == controlador ? " e controlador" : "");

            // procura o novo líder só até a hora da volta: a volta acontece SEMPRE aos 40 s
            long tVoltaAlvo = t0 + VOLTA_S * 1_000_000_000L;
            int[] novo = {-1};
            long[] tEl = {-1};
            procurarNovoLider(admin, topico, lider, tVoltaAlvo, novo, tEl);

            // --- volta do broker ---
            Util.aguardarAte(tVoltaAlvo);
            long tVolta = System.nanoTime();
            Util.rodarComando(60, "docker", "start", containerDerrubado);
            containerDerrubado = null;
            System.out.println("  >>> docker start " + prefixoContainer + lider);
            if (tEl[0] < 0) procurarNovoLider(admin, topico, lider, tQueda + 120_000_000_000L, novo, tEl);
            int novoLider = novo[0];
            long tEleicao = tEl[0];
            System.out.printf(Locale.US, "  >>> novo líder: %d (%.0f ms após a queda)%n", novoLider,
                    tEleicao > 0 ? (tEleicao - tQueda) / 1e6 : -1.0);

            Util.esperar(produtor, "produtor", 0, TIMEOUT_S);
            lp.get();
            int liderFinal = -1;
            try {
                liderFinal = lider(admin, topico);
            } catch (Exception ignored) {
                // sem metadado agora: fica -1
            }
            Util.enviar(consumidor, "PRODUTOR_TERMINOU");
            Util.esperar(consumidor, "consumidor", 0, TIMEOUT_S);
            lc.get();

            // --- auditoria ---
            int confirmadasN = Integer.parseInt(Util.obrigatorio(sp, "CONFIRMADAS"));
            Set<Integer> confirmadas = new HashSet<>();
            for (String l : Files.readAllLines(arqConf.toPath())) if (!l.isBlank()) confirmadas.add(Integer.parseInt(l.trim()));
            if (c.temConfirmacao() && confirmadas.size() != confirmadasN) {
                throw new IllegalStateException("Arquivo de confirmadas não bate com o produtor");
            }
            if (!c.temConfirmacao() && confirmadasN != -1) {
                throw new IllegalStateException("Com acks=0 o produtor não deveria contar confirmadas");
            }
            Set<Integer> unicas = new HashSet<>();
            int entregas = 0, duplicadas = 0;
            long chegadaAnterior = 0, maiorPausa = 0;
            List<String> serie = new ArrayList<>();
            for (String l : Files.readAllLines(arqRec.toPath())) {
                if (l.isBlank()) continue;
                String[] p = l.split(";");
                int seq = Integer.parseInt(p[0]);
                long envio = Long.parseLong(p[1]);
                long chegada = Long.parseLong(p[2]);
                boolean dup = !unicas.add(seq);
                if (dup) duplicadas++;
                entregas++;
                if (chegadaAnterior > 0) maiorPausa = Math.max(maiorPausa, chegada - chegadaAnterior);
                chegadaAnterior = chegada;
                serie.add(String.format(Locale.US, "%d,%s,%d,%.4f,%.4f,%.3f,%s,%s", exec, c.nome, seq, (envio - t0) / 1e9,
                        (chegada - t0) / 1e9, (chegada - envio) / 1e6, dup, false));
            }
            if (entregas != Integer.parseInt(Util.obrigatorio(sc, "RECEBIDAS"))) {
                throw new IllegalStateException("Arquivo de recebidas não bate com o consumidor");
            }
            for (int s : unicas) {
                if (s < 1 || s > MSGS) throw new IllegalStateException("Sequência fora do intervalo no tópico: " + s);
            }
            int perdidas = -1;
            if (c.temConfirmacao()) {
                perdidas = 0;
                for (int s : confirmadas) if (!unicas.contains(s)) perdidas++;
            }
            int naoEntregues = MSGS - unicas.size();
            int falhas = Integer.parseInt(Util.obrigatorio(sp, "FALHAS"));

            if (w != null) {
                w.printf(Locale.US, "%d,%d,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s,%d,%d,%.3f,%.3f,%.1f,%s,%.1f%n",
                        exec, ordem, c.nome, c.garantia, c.idempotente(), MSGS, c.temConfirmacao() ? confirmadas.size() : -1,
                        falhas, 0, unicas.size(), entregas, duplicadas, 0, perdidas, naoEntregues, 0,
                        lider, lider == controlador, novoLider, liderFinal,
                        (tQueda - t0) / 1e9, (tVolta - t0) / 1e9, tEleicao > 0 ? (tEleicao - tQueda) / 1e6 : -1.0,
                        Util.obrigatorio(sp, "MAIOR_INTERVALO_SEM_CONFIRMACAO_MS"), maiorPausa / 1e6);
                for (String l : serie) ws.println(l);
            }
            System.out.printf(Locale.US, "  → confirmadas %s | recebidas %d/%d | perdidas %s | não entregues %d | duplicadas %d | pausa %.0f ms%n",
                    c.temConfirmacao() ? String.valueOf(confirmadas.size()) : "-", unicas.size(), MSGS,
                    perdidas < 0 ? "-" : String.valueOf(perdidas), naoEntregues, duplicadas, maiorPausa / 1e6);

            Util.apagarSeExistir(arqConf.getPath());
            Util.apagarSeExistir(arqRec.getPath());
            esperarClusterSaudavel(admin, topico, true);
            apagarTopico(admin, topico);
            topicoApagado = true;
        } finally {
            Util.matar(consumidor, produtor);
            ex.shutdownNow();
            if (containerDerrubado != null) {
                // a execução falhou com o broker fora: religa para não deixar o cluster quebrado
                try {
                    Util.rodarComando(60, "docker", "start", containerDerrubado);
                } catch (Exception e) {
                    System.err.println("Não consegui religar " + containerDerrubado + ": " + e.getMessage());
                }
            }
            if (!topicoApagado) {
                try (AdminClient admin = Util.admin(bootstrap)) {
                    apagarTopico(admin, topico);
                } catch (Exception ignored) {
                    // apagado no próximo início
                }
            }
        }
    }

    private void procurarNovoLider(AdminClient admin, String topico, int antigo, long limiteNs, int[] novo, long[] t)
            throws InterruptedException {
        while (System.nanoTime() < limiteNs) {
            try {
                int l = lider(admin, topico);
                if (l >= 0 && l != antigo) {
                    novo[0] = l;
                    t[0] = System.nanoTime();
                    return;
                }
            } catch (Exception ignored) {
                // metadado indisponível enquanto o cluster se reorganiza
            }
            Thread.sleep(100);
        }
    }

    /** Id do broker líder da partição 0 (−1 se não houver líder no momento). */
    private int lider(AdminClient admin, String topico) throws Exception {
        TopicDescription d = admin.describeTopics(List.of(topico)).allTopicNames().get(5, TimeUnit.SECONDS).get(topico);
        Node l = d.partitions().get(0).leader();
        return l == null || l.isEmpty() ? -1 : l.id();
    }

    /**
     * Id do controlador ativo. Em KRaft, o describeCluster() não informa o
     * controlador (os brokers devolvem um broker qualquer), então usa o líder
     * do quórum de metadados. Em modo combinado (broker e controlador no mesmo
     * nó) esse id é o do broker; com controladores dedicados, nunca coincide
     * com um broker, e era_controlador fica false, o que também é correto.
     * Com ZooKeeper, describeMetadataQuorum não existe e o describeCluster()
     * informa o controlador de verdade.
     */
    private int controladorAtivo(AdminClient admin) {
        if (!Boolean.FALSE.equals(kraft)) {
            try {
                int id = admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS).leaderId();
                kraft = true;
                return id;
            } catch (Exception kraftIndisponivel) {
                if (kraft == null) {
                    kraft = false;   // ZooKeeper: não pergunta de novo
                    System.out.println("Cluster em modo ZooKeeper: controlador lido do describeCluster()");
                }
            }
        }
        try {
            return admin.describeCluster().controller().get(15, TimeUnit.SECONDS).id();
        } catch (Exception e) {
            return -1;
        }
    }

    /** null até a primeira consulta; true = KRaft, false = ZooKeeper. */
    private Boolean kraft = null;

    /**
     * Cria o tópico da execução com o líder preferido num broker que não é o
     * controlador e confere RF = 3, min.insync.replicas = 2 e o líder.
     * Devolve o id do líder escolhido.
     */
    private int criarTopico(AdminClient admin, String topico) throws Exception {
        int controlador = controladorAtivo(admin);
        List<Integer> brokers = new ArrayList<>();
        for (Node n : admin.describeCluster().nodes().get(15, TimeUnit.SECONDS)) brokers.add(n.id());
        Collections.sort(brokers);
        if (brokers.size() != 3) throw new IllegalStateException("Esperava 3 brokers, encontrei " + brokers);
        if (controlador < 0) throw new IllegalStateException("Não consegui descobrir o controlador do cluster.");
        // líder preferido = primeiro da lista: o broker de menor id que não é o controlador
        int escolhido = -1;
        for (int b : brokers) if (b != controlador) { escolhido = b; break; }
        List<Integer> replicas = new ArrayList<>();
        replicas.add(escolhido);
        for (int b : brokers) if (b != escolhido) replicas.add(b);   // os outros dois, inclusive o controlador
        System.out.println("  controlador: " + controlador + " | réplicas " + replicas + " (líder escolhido: " + escolhido + ")");

        admin.createTopics(List.of(new NewTopic(topico, Map.of(0, replicas))
                .configs(Map.of("min.insync.replicas", "2")))).all().get(30, TimeUnit.SECONDS);

        long limite = System.currentTimeMillis() + 15_000;
        Exception ultimo = null;
        while (System.currentTimeMillis() < limite) {
            try {
                TopicPartitionInfo p = admin.describeTopics(List.of(topico)).allTopicNames().get(5, TimeUnit.SECONDS)
                        .get(topico).partitions().get(0);
                ConfigResource rt = new ConfigResource(ConfigResource.Type.TOPIC, topico);
                Config cfg = admin.describeConfigs(List.of(rt)).all().get(5, TimeUnit.SECONDS).get(rt);
                ConfigEntry mi = cfg.get("min.insync.replicas");
                String minIsr = mi == null ? null : mi.value();
                if (p.replicas().size() != 3 || !"2".equals(minIsr)) {
                    throw new IllegalStateException("O tópico " + topico + " ficou com " + p.replicas().size()
                            + " réplicas e min.insync.replicas=" + minIsr + " (esperado 3 e 2).");
                }
                if (p.leader() == null || p.leader().isEmpty() || p.leader().id() != escolhido) {
                    Thread.sleep(250);   // líder ainda sendo eleito
                    continue;
                }
                return escolhido;
            } catch (ExecutionException | TimeoutException e) {
                ultimo = e;   // metadado ainda chegando aos brokers
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("Não consegui conferir o tópico " + topico + " (líder " + escolhido + ")", ultimo);
    }

    private static boolean ehTopicoDoCenario(String nome) {
        return nome.equals(PREFIXO_TOPICO) || nome.startsWith(PREFIXO_TOPICO + "-");
    }

    /** Apaga os tópicos deste cenário que sobraram de sessões anteriores (inclusive o de nome fixo). */
    private void apagarTopicosAntigos(AdminClient admin) throws Exception {
        List<String> antigos = new ArrayList<>();
        for (String n : admin.listTopics().names().get(30, TimeUnit.SECONDS)) if (ehTopicoDoCenario(n)) antigos.add(n);
        if (antigos.isEmpty()) return;
        System.out.println("Apagando " + antigos.size() + " tópico(s) de sessões anteriores: " + antigos);
        try {
            admin.deleteTopics(antigos).all().get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            System.out.println("  [AVISO] nem todos os tópicos antigos foram apagados: " + e.getCause().getMessage());
        }
    }

    private void apagarTopico(AdminClient admin, String topico) {
        try {
            admin.deleteTopics(List.of(topico)).all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("  [AVISO] não foi possível apagar o tópico " + topico + " (será apagado no próximo início): "
                    + e.getMessage());
        }
    }

    /**
     * Espera 3 brokers no cluster e 3 réplicas em sincronia na partição (até
     * 3 min). Com pausa = true, espera mais 20 s depois disso, como a versão
     * RabbitMQ, para a próxima execução começar do mesmo estado.
     */
    private void esperarClusterSaudavel(AdminClient admin, String topico, boolean pausa) throws Exception {
        long limite = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < limite) {
            try {
                if (Util.brokers(admin) == 3) {
                    TopicPartitionInfo p = admin.describeTopics(List.of(topico)).allTopicNames()
                            .get(5, TimeUnit.SECONDS).get(topico).partitions().get(0);
                    if (p.isr().size() == 3 && p.leader() != null && !p.leader().isEmpty()) {
                        if (pausa) {
                            System.out.println("  cluster estável; aguardando " + ESPERA_ESTAVEL_MS / 1000 + " s antes da próxima execução");
                            Thread.sleep(ESPERA_ESTAVEL_MS);
                        }
                        return;
                    }
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("O cluster não voltou a 3 brokers / 3 réplicas em sincronia em 3 min.");
    }
}
