package com.benchmarking.kafka.cenario3_1;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;

/**
 * Nível avançado — Cenário 1: Roteamento (Kafka) — ORQUESTRADOR.
 *
 * Objetivo: comparar como cada tecnologia entrega a cada consumidor só as
 * mensagens que ele quer. O RabbitMQ faz isso no broker (exchanges direct,
 * topic e fanout com bindings). O Kafka não tem exchanges: o roteamento é
 * escolher o tópico na publicação e/ou filtrar na aplicação.
 *
 * Mensagens: 6.000, cada uma com a chave pedidos.<região>.<evento>
 * (4 regiões × 3 eventos). Três topologias, cada uma em duas abordagens
 * possíveis no Kafka quando há mais de uma:
 *
 *   direto  — cada consumidor quer UMA região (4 consumidores)
 *       filtro  : 1 tópico; cada consumidor lê tudo e filtra a sua região
 *       topicos : 1 tópico por região; cada consumidor assina o seu
 *   curinga — A quer "pedidos.*.criado"; B quer "pedidos.sudeste.#"
 *       filtro  : 1 tópico; cada consumidor lê tudo e filtra
 *       padrao  : 1 tópico por chave (12); cada consumidor assina por
 *                 expressão regular sobre o NOME do tópico
 *   fanout  — 3 consumidores querem tudo
 *       grupos  : 1 tópico; cada consumidor num grupo próprio
 *
 * Conferência de cada consumidor (a execução falha se qualquer item falhar):
 *   - as aceitas são exatamente as esperadas, sem duplicatas;
 *   - as recebidas são exatamente as que estão nos tópicos que ele assina
 *     (calculadas aqui pela mesma regra de tópico do produtor e pela mesma
 *     regra de assinatura do KafkaConsumer);
 *   - os tópicos lidos são exatamente os que a assinatura deveria pegar.
 * E da configuração: o log guardou cada mensagem uma vez (soma dos offsets
 * finais = 6.000) e as mensagens que nenhum consumidor quer são as previstas.
 *
 * CSV no MESMO formato da versão RabbitMQ (ver CABECALHO): as colunas de
 * recursos (exchanges, filas, bindings, tópicos) ficam em 0 onde não se
 * aplicam, e sem_destino / copias_no_broker permitem comparar o que acontece
 * com as mensagens que ninguém quer (no Kafka ficam no log; no RabbitMQ o
 * broker as descarta) e quantas cópias o broker guarda.
 *
 * Cenário de comportamento (o resultado é determinístico): 1 execução de
 * aquecimento por configuração (descartada, fora do CSV, mas conferida) e 3
 * execuções válidas, em ordem rotativa.
 *
 * Arquivo gerado em PASTA_RESULTADOS:
 *   resultados-kafka-roteamento.csv (1 linha por execução, configuração e consumidor)
 */
@Component
public class Cenario1Roteamento {

    static final String[] REGIOES = {"sudeste", "sul", "nordeste", "norte"};
    static final String[] EVENTOS = {"criado", "pago", "cancelado"};

    static String regiao(int seq) { return REGIOES[(seq - 1) % REGIOES.length]; }
    static String evento(int seq) { return EVENTOS[((seq - 1) / REGIOES.length) % EVENTOS.length]; }

    /** Tópico em que a mensagem é publicada (regra única do produtor e do orquestrador). */
    static String topico(String modo, String prefixo, int seq) {
        return switch (modo) {
            case "unico"      -> prefixo;
            case "por-regiao" -> prefixo + "." + regiao(seq);
            case "por-chave"  -> prefixo + "." + regiao(seq) + "." + evento(seq);
            default -> throw new IllegalArgumentException("Modo desconhecido: " + modo);
        };
    }

    /** O tópico entra na assinatura? Mesma regra do KafkaConsumer: nome exato ou Pattern.matches. */
    static boolean assina(String assinatura, String topico) {
        if (assinatura.startsWith("topicos:")) return Arrays.asList(assinatura.substring(8).split(",")).contains(topico);
        if (assinatura.startsWith("padrao:")) return Pattern.compile(assinatura.substring(7)).matcher(topico).matches();
        throw new IllegalArgumentException("Assinatura inválida: " + assinatura);
    }

    private static final String PASTA_RESULTADOS = "resultado_cenario_3_1";
    private static final String CSV = "resultados-kafka-roteamento.csv";
    /** Mesmo cabeçalho da versão RabbitMQ. */
    static final String CABECALHO = "execucao,ordem,topologia,abordagem,onde_roteia,exchanges,filas,bindings,topicos," +
            "consumidor,regra,destinos_lidos,esperadas,recebidas,aceitas,descartadas,faltando,duplicadas," +
            "sem_destino,copias_no_broker,correto";
    private static final int MSGS = 6_000;
    private static final int EXECUCOES = 3;
    private static final long TIMEOUT_S = 300;

    private static final String PRODUTOR   = "com.benchmarking.kafka.cenario3_1.ProdutorRoteamento";
    private static final String CONSUMIDOR = "com.benchmarking.kafka.cenario3_1.ConsumidorRoteamento";

    /** Um consumidor da configuração: o que assina, o que filtra e o que deveria aceitar. */
    private record Cons(String nome, String regra, String assinatura, String filtro, IntPredicate esperado) {}

    /**
     * ondeRoteia: quem decide o que cada consumidor recebe — "consumidor" (filtro
     * na aplicação), "produtor" (escolha do tópico) ou "nenhum" (todos querem tudo).
     */
    private record Config(String topologia, String abordagem, String ondeRoteia, String modoProdutor, String prefixo,
                          List<String> topicos, List<Cons> consumidores) {}

    /** Resultado de um consumidor, para escrever depois de conferir a configuração inteira. */
    private record Linha(Cons k, int destinos, int esperadas, int recebidas, int aceitas, int descartadas,
                         int faltando, int duplicadas) {}

    @Value("${benchmarking.bootstrap-servers}") private String bootstrap;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private static List<Config> configs() {
        List<Config> c = new ArrayList<>();

        // direto — filtro
        List<Cons> d1 = new ArrayList<>();
        for (String r : REGIOES) d1.add(new Cons("C-" + r, "regiao = " + r, "topicos:rot-direto", "regiao=" + r,
                s -> regiao(s).equals(r)));
        c.add(new Config("direto", "filtro", "consumidor", "unico", "rot-direto", List.of("rot-direto"), d1));

        // direto — um tópico por região
        List<Cons> d2 = new ArrayList<>();
        List<String> t2 = new ArrayList<>();
        for (String r : REGIOES) {
            t2.add("rot-regiao." + r);
            d2.add(new Cons("C-" + r, "regiao = " + r, "topicos:rot-regiao." + r, "todas", s -> regiao(s).equals(r)));
        }
        c.add(new Config("direto", "topicos", "produtor", "por-regiao", "rot-regiao", t2, d2));

        // curinga — filtro
        c.add(new Config("curinga", "filtro", "consumidor", "unico", "rot-curinga", List.of("rot-curinga"), List.of(
                new Cons("A", "pedidos.*.criado", "topicos:rot-curinga", "evento=criado", s -> evento(s).equals("criado")),
                new Cons("B", "pedidos.sudeste.#", "topicos:rot-curinga", "regiao=sudeste", s -> regiao(s).equals("sudeste")))));

        // curinga — um tópico por chave, assinatura por expressão regular
        List<String> t4 = new ArrayList<>();
        for (String r : REGIOES) for (String e : EVENTOS) t4.add("rot-chave." + r + "." + e);
        c.add(new Config("curinga", "padrao", "produtor", "por-chave", "rot-chave", t4, List.of(
                new Cons("A", "pedidos.*.criado", "padrao:rot-chave\\.[a-z]+\\.criado", "todas", s -> evento(s).equals("criado")),
                new Cons("B", "pedidos.sudeste.#", "padrao:rot-chave\\.sudeste\\..+", "todas", s -> regiao(s).equals("sudeste")))));

        // fanout
        List<Cons> f = new ArrayList<>();
        for (int i = 1; i <= 3; i++) f.add(new Cons("C" + i, "todas", "topicos:rot-fanout", "todas", s -> true));
        c.add(new Config("fanout", "grupos", "nenhum", "unico", "rot-fanout", List.of("rot-fanout"), f));
        return c;
    }

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 1 Kafka — Roteamento");
        System.out.println("====================================================================");

        Thread gancho = Util.matarFilhosAoEncerrar("mata-subprocessos-roteamento");
        try {
            File pasta = Util.pasta(PASTA_RESULTADOS);
            String csv = PASTA_RESULTADOS + File.separator + CSV;
            Util.apagarSeExistir(csv);
            List<Config> configs = configs();

            try (PrintWriter w = new PrintWriter(new FileWriter(csv)); AdminClient admin = Util.admin(bootstrap)) {
                w.println(CABECALHO);
                for (Config c : configs) {
                    System.out.printf("%nAquecimento (descartado) — %s / %s%n", c.topologia, c.abordagem);
                    rodar(admin, c, 0, 0, pasta, null);
                    Thread.sleep(2_000);
                }
                for (int exec = 1; exec <= EXECUCOES; exec++) {
                    for (int pos = 0; pos < configs.size(); pos++) {
                        Config c = configs.get((pos + exec - 1) % configs.size());
                        System.out.printf("%nExecução %d/%d — %s / %s%n", exec, EXECUCOES, c.topologia, c.abordagem);
                        rodar(admin, c, exec, pos + 1, pasta, w);
                        Thread.sleep(2_000);
                    }
                }
            }
            System.out.println();
            System.out.println("[SUCESSO] Avançado — Cenário 1 Kafka concluído.");
            System.out.println("  " + new File(csv).getAbsolutePath());
        } finally {
            Util.removerGancho(gancho);
        }
    }

    /** w == null: aquecimento (confere tudo, mas não grava). */
    private void rodar(AdminClient admin, Config c, int exec, int ordem, File pasta, PrintWriter w) throws Exception {
        List<NewTopic> novos = new ArrayList<>();
        for (String t : c.topicos) novos.add(new NewTopic(t, 1, (short) 1));
        Util.recriarTopicos(admin, novos);

        int n = c.consumidores.size();
        ExecutorService ex = Executors.newFixedThreadPool(n);
        CountDownLatch prontos = new CountDownLatch(n);
        List<Process> procs = new ArrayList<>();
        List<Map<String, String>> saidas = new ArrayList<>();
        List<Future<?>> leitores = new ArrayList<>();
        List<File> arquivos = new ArrayList<>();
        Process produtor = null;
        try {
            for (Cons k : c.consumidores) {
                File arq = new File(pasta, "temp-roteamento-" + k.nome + ".txt");
                Util.apagarSeExistir(arq.getPath());
                arquivos.add(arq);
                String grupo = "rot-" + c.topologia + "-" + c.abordagem + "-" + k.nome + "-" + exec + "-" + System.nanoTime();
                Process p = Util.iniciar(javaCmd, classpath, CONSUMIDOR, bootstrap, grupo, k.assinatura, k.filtro,
                        arq.getAbsolutePath(), String.valueOf(exec));
                Map<String, String> s = new ConcurrentHashMap<>();
                procs.add(p);
                saidas.add(s);
                leitores.add(Util.lerEmSegundoPlano(ex, p, "CONSUMIDOR " + k.nome, s, prontos, null));
            }
            if (!prontos.await(90, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidores não ficaram prontos.");

            produtor = Util.iniciar(javaCmd, classpath, PRODUTOR, bootstrap, c.modoProdutor, c.prefixo,
                    String.valueOf(MSGS), String.valueOf(exec));
            Util.ler(produtor, "PRODUTOR", new HashMap<>(), null, null);
            Util.esperar(produtor, "produtor", 0, TIMEOUT_S);

            for (Process p : procs) Util.enviar(p, "PRODUTOR_TERMINOU");
            for (int i = 0; i < n; i++) {
                Util.esperar(procs.get(i), "consumidor " + c.consumidores.get(i).nome, 0, TIMEOUT_S);
                leitores.get(i).get();
            }

            // ---------------- conferência de cada consumidor
            List<Linha> linhas = new ArrayList<>();
            Set<Integer> aceitasPorAlguem = new HashSet<>();
            for (int i = 0; i < n; i++) {
                Cons k = c.consumidores.get(i);
                Map<String, String> s = saidas.get(i);
                List<Integer> lista = new ArrayList<>();
                for (String l : Files.readAllLines(arquivos.get(i).toPath())) if (!l.isBlank()) lista.add(Integer.parseInt(l.trim()));
                Util.apagarSeExistir(arquivos.get(i).getPath());
                Set<Integer> aceitas = new HashSet<>(lista);
                aceitasPorAlguem.addAll(aceitas);

                Set<Integer> esperadas = new HashSet<>();
                int recebidasEsperadas = 0;
                for (int seq = 1; seq <= MSGS; seq++) {
                    if (k.esperado.test(seq)) esperadas.add(seq);
                    if (assina(k.assinatura, topico(c.modoProdutor, c.prefixo, seq))) recebidasEsperadas++;
                }
                Set<String> topicosEsperados = new TreeSet<>();
                for (String t : c.topicos) if (assina(k.assinatura, t)) topicosEsperados.add(t);
                Set<String> topicosLidos = new TreeSet<>();
                for (String t : Util.obrigatorio(s, "TOPICOS").split(",")) if (!t.isBlank()) topicosLidos.add(t.trim());

                int recebidas = Integer.parseInt(Util.obrigatorio(s, "RECEBIDAS"));
                int aceitasN  = Integer.parseInt(Util.obrigatorio(s, "ACEITAS"));
                int faltando = 0;
                for (int seq : esperadas) if (!aceitas.contains(seq)) faltando++;
                int duplicadas = lista.size() - aceitas.size();

                List<String> erros = new ArrayList<>();
                if (aceitasN != lista.size()) erros.add("arquivo de aceitas incompleto (" + lista.size() + " de " + aceitasN + ")");
                if (!aceitas.equals(esperadas)) erros.add("aceitas diferentes das esperadas (faltando " + faltando + ")");
                if (duplicadas != 0) erros.add(duplicadas + " aceitas duplicadas");
                if (recebidas != recebidasEsperadas) erros.add("recebidas " + recebidas + ", deveriam ser " + recebidasEsperadas);
                if (!topicosLidos.equals(topicosEsperados)) erros.add("tópicos lidos " + topicosLidos + ", deveriam ser " + topicosEsperados);

                linhas.add(new Linha(k, topicosLidos.size(), esperadas.size(), recebidas, aceitasN, recebidas - aceitasN,
                        faltando, duplicadas));
                System.out.printf("  → %s (%s): esperadas %d | recebidas %d | aceitas %d | descartadas %d | tópicos %d | correto: %s%n",
                        k.nome, k.regra, esperadas.size(), recebidas, aceitasN, recebidas - aceitasN, topicosLidos.size(),
                        erros.isEmpty());
                if (!erros.isEmpty()) throw new IllegalStateException("Consumidor " + k.nome + ": " + String.join("; ", erros));
            }

            // ---------------- conferência da configuração
            int semDestino = MSGS - aceitasPorAlguem.size();
            int semDestinoEsperado = 0;
            for (int seq = 1; seq <= MSGS; seq++) {
                boolean alguem = false;
                for (Cons k : c.consumidores) alguem |= k.esperado.test(seq);
                if (!alguem) semDestinoEsperado++;
            }
            if (semDestino != semDestinoEsperado) {
                throw new IllegalStateException("Mensagens sem destino: " + semDestino + ", deveriam ser " + semDestinoEsperado);
            }
            long copias = copiasNoBroker(admin, c.topicos);
            if (copias != MSGS) {
                throw new IllegalStateException("O broker guardou " + copias + " mensagens nos tópicos; deveriam ser " + MSGS);
            }
            System.out.printf("  → sem destino: %d (ficam no log) | cópias no broker: %d%n", semDestino, copias);

            if (w != null) {
                for (Linha l : linhas) {
                    w.printf(Locale.US, "%d,%d,%s,%s,%s,%d,%d,%d,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s%n",
                            exec, ordem, c.topologia, c.abordagem, c.ondeRoteia, 0, 0, 0, c.topicos.size(),
                            l.k.nome, l.k.regra, l.destinos, l.esperadas, l.recebidas, l.aceitas, l.descartadas,
                            l.faltando, l.duplicadas, semDestino, copias, true);
                }
                w.flush();
            }
        } finally {
            Util.matar(produtor);
            for (Process p : procs) Util.matar(p);
            ex.shutdownNow();
        }
    }

    /** Mensagens guardadas nos tópicos da configuração (soma dos offsets finais; 1 partição por tópico). */
    private static long copiasNoBroker(AdminClient admin, List<String> topicos) throws Exception {
        Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
        for (String t : topicos) consulta.put(new TopicPartition(t, 0), OffsetSpec.latest());
        long total = 0;
        for (var info : admin.listOffsets(consulta).all().get(30, TimeUnit.SECONDS).values()) total += info.offset();
        return total;
    }
}
