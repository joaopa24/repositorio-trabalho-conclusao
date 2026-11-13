package com.benchmarking.rabbitmq.cenario3_1;

import com.benchmarking.rabbitmq.cenario3_comum.Util;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntPredicate;

/**
 * Nível avançado — Cenário 1: Roteamento (RabbitMQ) — ORQUESTRADOR.
 *
 * Objetivo: comparar como cada tecnologia entrega a cada consumidor só as
 * mensagens que ele quer. Aqui o roteamento é feito NO BROKER: o produtor
 * publica numa exchange e os bindings decidem para quais filas cada
 * mensagem vai. Mesmas mensagens e mesmas regras da versão Kafka.
 *
 *   direto  — exchange direct; 4 filas, binding = região (4 consumidores)
 *   curinga — exchange topic; fila A com "pedidos.*.criado",
 *             fila B com "pedidos.sudeste.#"
 *   fanout  — exchange fanout; 3 filas (3 consumidores querem tudo)
 *
 * No curinga, as mensagens que não casam com nenhum binding (nem "criado"
 * nem "sudeste": 3.000 das 6.000) não vão para fila nenhuma. O produtor as
 * conta com mandatory=true (NAO_ROTEADAS), e o orquestrador confere que são
 * exatamente as que nenhum consumidor recebeu.
 *
 * Conferência de cada consumidor (a execução falha se qualquer item falhar):
 * recebeu exatamente as esperadas, sem duplicatas e sem nada a mais.
 *
 * CSV no MESMO formato da versão Kafka (ver CABECALHO). Aqui o consumidor
 * não filtra, então aceitas = recebidas que ele queria e descartadas deve
 * ser 0. copias_no_broker = mensagens que o broker colocou nas filas (soma
 * das recebidas, já que cada fila é lida até o fim): no fanout, uma cópia
 * por fila; no topic, a mensagem que casa com os dois bindings vai às duas.
 *
 * Cenário de comportamento (resultado determinístico): 1 execução de
 * aquecimento por configuração (descartada, fora do CSV, mas conferida) e 3
 * execuções válidas, em ordem rotativa.
 *
 * Arquivo gerado em PASTA_RESULTADOS:
 *   resultados-rabbitmq-roteamento.csv (1 linha por execução, configuração e consumidor)
 */
@Component
public class Cenario1Roteamento {

    static final String[] REGIOES = ProdutorRoteamento.REGIOES;
    static final String[] EVENTOS = ProdutorRoteamento.EVENTOS;

    static String regiao(int seq) { return REGIOES[(seq - 1) % REGIOES.length]; }
    static String evento(int seq) { return EVENTOS[((seq - 1) / REGIOES.length) % EVENTOS.length]; }

    private static final String PASTA_RESULTADOS = "resultado_rabbitmq_cenario_3_1";
    private static final String CSV = "resultados-rabbitmq-roteamento.csv";
    /** Mesmo cabeçalho da versão Kafka. */
    static final String CABECALHO = "execucao,ordem,topologia,abordagem,onde_roteia,exchanges,filas,bindings,topicos," +
            "consumidor,regra,destinos_lidos,esperadas,recebidas,aceitas,descartadas,faltando,duplicadas," +
            "sem_destino,copias_no_broker,correto";
    private static final int MSGS = 6_000;
    private static final int EXECUCOES = 3;
    private static final long TIMEOUT_S = 300;

    private static final String PRODUTOR   = "com.benchmarking.rabbitmq.cenario3_1.ProdutorRoteamento";
    private static final String CONSUMIDOR = "com.benchmarking.rabbitmq.cenario3_1.ConsumidorRoteamento";

    /** Um consumidor: a fila, o binding dela e o que deveria receber. */
    private record Cons(String nome, String regra, String fila, String binding, IntPredicate esperado) {}

    private record Config(String topologia, String exchange, BuiltinExchangeType tipo, String modoChave,
                          int naoRoteadasEsperadas, List<Cons> consumidores) {}

    /** Resultado de um consumidor, para escrever depois de conferir a configuração inteira. */
    private record Linha(Cons k, int esperadas, int recebidas, int aceitas, int descartadas, int faltando,
                         int duplicadas) {}

    @Value("${benchmarking.host:localhost}") private String host;
    @Value("${benchmarking.port:5672}")      private int porta;

    private final String classpath = System.getProperty("java.class.path");
    private final String javaCmd   = ProcessHandle.current().info().command().orElse("java");

    private static List<Config> configs() {
        List<Config> c = new ArrayList<>();

        List<Cons> d = new ArrayList<>();
        for (String r : REGIOES) d.add(new Cons("C-" + r, "regiao = " + r, "rot.direct." + r, r, s -> regiao(s).equals(r)));
        c.add(new Config("direto", "rot.direct", BuiltinExchangeType.DIRECT, "regiao", 0, d));

        c.add(new Config("curinga", "rot.topic", BuiltinExchangeType.TOPIC, "completa", 3_000, List.of(
                new Cons("A", "pedidos.*.criado", "rot.topic.A", "pedidos.*.criado", s -> evento(s).equals("criado")),
                new Cons("B", "pedidos.sudeste.#", "rot.topic.B", "pedidos.sudeste.#", s -> regiao(s).equals("sudeste")))));

        List<Cons> f = new ArrayList<>();
        for (int i = 1; i <= 3; i++) f.add(new Cons("C" + i, "todas", "rot.fanout." + i, "", s -> true));
        c.add(new Config("fanout", "rot.fanout", BuiltinExchangeType.FANOUT, "completa", 0, f));
        return c;
    }

    public void executar() throws Exception {
        System.out.println();
        System.out.println("====================================================================");
        System.out.println("   Avançado — Cenário 1 RabbitMQ — Roteamento");
        System.out.println("====================================================================");

        Thread gancho = Util.matarFilhosAoEncerrar("mata-subprocessos-roteamento");
        try {
            File pasta = Util.pasta(PASTA_RESULTADOS);
            String csv = PASTA_RESULTADOS + File.separator + CSV;
            Util.apagarSeExistir(csv);
            List<Config> configs = configs();

            try (PrintWriter w = new PrintWriter(new FileWriter(csv))) {
                w.println(CABECALHO);
                for (Config c : configs) {
                    System.out.printf("%nAquecimento (descartado) — %s (exchange %s)%n", c.topologia,
                            c.tipo.name().toLowerCase());
                    rodar(c, 0, 0, pasta, null);
                    Thread.sleep(2_000);
                }
                for (int exec = 1; exec <= EXECUCOES; exec++) {
                    for (int pos = 0; pos < configs.size(); pos++) {
                        Config c = configs.get((pos + exec - 1) % configs.size());
                        System.out.printf("%nExecução %d/%d — %s (exchange %s)%n", exec, EXECUCOES, c.topologia,
                                c.tipo.name().toLowerCase());
                        rodar(c, exec, pos + 1, pasta, w);
                        Thread.sleep(2_000);
                    }
                }
            }
            System.out.println();
            System.out.println("[SUCESSO] Avançado — Cenário 1 RabbitMQ concluído.");
            System.out.println("  " + new File(csv).getAbsolutePath());
        } finally {
            Util.removerGancho(gancho);
        }
    }

    /** Apaga e recria a exchange, as filas e os bindings da configuração. */
    private void preparar(Config c) throws Exception {
        try (Connection conn = Util.fabrica(host, porta).newConnection(); Channel ch = conn.createChannel()) {
            ch.exchangeDelete(c.exchange);
            ch.exchangeDeclare(c.exchange, c.tipo, true);
            for (Cons k : c.consumidores) {
                ch.queueDelete(k.fila);
                ch.queueDeclare(k.fila, true, false, false, null);
                ch.queueBind(k.fila, c.exchange, k.binding);
            }
        }
    }

    /** w == null: aquecimento (confere tudo, mas não grava). */
    private void rodar(Config c, int exec, int ordem, File pasta, PrintWriter w) throws Exception {
        preparar(c);
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
                Process p = Util.iniciar(javaCmd, classpath, CONSUMIDOR, host, String.valueOf(porta), k.fila,
                        arq.getAbsolutePath(), String.valueOf(exec));
                Map<String, String> s = new ConcurrentHashMap<>();
                procs.add(p);
                saidas.add(s);
                leitores.add(Util.lerEmSegundoPlano(ex, p, "CONSUMIDOR " + k.nome, s, prontos, null));
            }
            if (!prontos.await(90, TimeUnit.SECONDS)) throw new IllegalStateException("Consumidores não ficaram prontos.");

            produtor = Util.iniciar(javaCmd, classpath, PRODUTOR, host, String.valueOf(porta), c.exchange, c.modoChave,
                    String.valueOf(MSGS), String.valueOf(exec));
            Map<String, String> sp = new HashMap<>();
            Util.ler(produtor, "PRODUTOR", sp, null, null);
            Util.esperar(produtor, "produtor", 0, TIMEOUT_S);
            int naoRoteadas = Integer.parseInt(Util.obrigatorio(sp, "NAO_ROTEADAS"));

            for (Process p : procs) Util.enviar(p, "PRODUTOR_TERMINOU");
            for (int i = 0; i < n; i++) {
                Util.esperar(procs.get(i), "consumidor " + c.consumidores.get(i).nome, 0, TIMEOUT_S);
                leitores.get(i).get();
            }

            // ---------------- conferência de cada consumidor
            List<Linha> linhas = new ArrayList<>();
            Set<Integer> recebidasPorAlguem = new HashSet<>();
            long copias = 0;
            for (int i = 0; i < n; i++) {
                Cons k = c.consumidores.get(i);
                List<Integer> lidas = new ArrayList<>();
                for (String l : Files.readAllLines(arquivos.get(i).toPath())) if (!l.isBlank()) lidas.add(Integer.parseInt(l.trim()));
                Util.apagarSeExistir(arquivos.get(i).getPath());

                Set<Integer> esperadas = new HashSet<>();
                for (int seq = 1; seq <= MSGS; seq++) if (k.esperado.test(seq)) esperadas.add(seq);
                Set<Integer> unicas = new HashSet<>(lidas);
                recebidasPorAlguem.addAll(unicas);
                int descartadas = 0;
                for (int s : lidas) if (!esperadas.contains(s)) descartadas++;
                int faltando = 0;
                for (int s : esperadas) if (!unicas.contains(s)) faltando++;
                int duplicadas = lidas.size() - unicas.size();
                int recebidas = Integer.parseInt(Util.obrigatorio(saidas.get(i), "RECEBIDAS"));
                copias += recebidas;

                List<String> erros = new ArrayList<>();
                if (recebidas != lidas.size()) erros.add("arquivo de recebidas incompleto (" + lidas.size() + " de " + recebidas + ")");
                if (!unicas.equals(esperadas)) erros.add("recebidas diferentes das esperadas (faltando " + faltando
                        + ", a mais " + descartadas + ")");
                if (duplicadas != 0) erros.add(duplicadas + " recebidas duplicadas");

                linhas.add(new Linha(k, esperadas.size(), recebidas, recebidas - descartadas, descartadas, faltando, duplicadas));
                System.out.printf("  → %s (%s): esperadas %d | recebidas %d | descartadas %d | correto: %s%n",
                        k.nome, k.regra, esperadas.size(), recebidas, descartadas, erros.isEmpty());
                if (!erros.isEmpty()) throw new IllegalStateException("Consumidor " + k.nome + ": " + String.join("; ", erros));
            }

            // ---------------- conferência da configuração
            int semDestino = MSGS - recebidasPorAlguem.size();
            if (naoRoteadas != c.naoRoteadasEsperadas || semDestino != naoRoteadas) {
                throw new IllegalStateException("Não roteadas pelo broker: " + naoRoteadas + " (esperado "
                        + c.naoRoteadasEsperadas + "); não recebidas por ninguém: " + semDestino + ".");
            }
            System.out.printf("  → sem destino: %d (descartadas pelo broker) | cópias no broker: %d%n", semDestino, copias);

            if (w != null) {
                for (Linha l : linhas) {
                    w.printf(Locale.US, "%d,%d,%s,%s,%s,%d,%d,%d,%d,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%s%n",
                            exec, ordem, c.topologia, c.tipo.name().toLowerCase(), "broker", 1, n, n, 0,
                            l.k.nome, l.k.regra, 1, l.esperadas, l.recebidas, l.aceitas, l.descartadas,
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
}
