package com.benchmarking.rabbitmq.cenario2_2;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifica a contagem de perdas e duplicatas do Cenário 2 do nível
 * intermediário (falha do consumidor) com entradas conhecidas.
 *
 * O teste chama o método privado calcular() do orquestrador por reflexão,
 * sem alterar o código usado nos experimentos. A entrada é o mesmo arquivo
 * que os consumidores gravam ("id;sequência", uma linha por mensagem
 * processada) e a saída dos consumidores (PROCESSADAS, DESCARTADAS, QUEDA,
 * PRIMEIRA_RECEBIDA). Não usa broker, rede nem Docker.
 */
class CalculoFalhaConsumidorTest {

    private static final int MSGS = 5_000;          // igual ao orquestrador
    private static final String CONSUMIDOR = Cenario2FalhaConsumidor.class.getName() + "$Consumidor";

    @TempDir
    Path pasta;

    // ------------------------------------------------------------------
    // Casos com resultado conhecido
    // ------------------------------------------------------------------

    @Test
    @DisplayName("Sem falhas: 5.000 únicas, nenhuma perdida e nenhuma duplicada")
    void semFalhas() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, MSGS));

        Map<String, Object> r = calcular(linhas, 4_000, 0);

        assertEquals(1_000, r.get("processadasC1"));
        assertEquals(4_000, r.get("processadasC2"));
        assertEquals(MSGS, r.get("unicas"));
        assertEquals(0, r.get("duplicadas"));
        assertEquals(0, r.get("perdidas"));
    }

    @Test
    @DisplayName("Lacuna: uma sequência nunca processada é contada como perdida")
    void lacuna() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, MSGS));
        linhas.remove("2;3000");

        Map<String, Object> r = calcular(linhas, 3_999, 0);

        assertEquals(MSGS - 1, r.get("unicas"));
        assertEquals(1, r.get("perdidas"));
        assertEquals(0, r.get("duplicadas"));
    }

    @Test
    @DisplayName("Várias lacunas, incluindo a primeira e a última sequência")
    void variasLacunas() throws Exception {
        List<String> linhas = linhas(1, 2, 1_000);              // falta a 1
        linhas.addAll(linhas(2, 1_001, MSGS - 1));               // falta a 5.000
        linhas.remove("2;2500");                                 // falta a 2.500

        Map<String, Object> r = calcular(linhas, 3_998, 0);

        assertEquals(3, r.get("perdidas"));
        assertEquals(0, r.get("duplicadas"));
    }

    @Test
    @DisplayName("Duplicata: mensagem processada pelo #1 e de novo pelo #2")
    void duplicata() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_000, MSGS));                   // o #2 reprocessa a 1.000

        Map<String, Object> r = calcular(linhas, 4_001, 0);

        assertEquals(MSGS, r.get("unicas"));
        assertEquals(1, r.get("duplicadas"));
        assertEquals(0, r.get("perdidas"));
    }

    @Test
    @DisplayName("Mensagem processada três vezes conta como duas duplicatas")
    void processadaTresVezes() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, MSGS));
        linhas.add("2;42");
        linhas.add("2;42");

        Map<String, Object> r = calcular(linhas, 4_002, 0);

        assertEquals(2, r.get("duplicadas"));
        assertEquals(0, r.get("perdidas"));
    }

    @Test
    @DisplayName("Lote inteiro reprocessado: 100 duplicatas")
    void loteReprocessado() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 901, MSGS));                     // 901..1.000 de novo

        Map<String, Object> r = calcular(linhas, 4_100, 0);

        assertEquals(100, r.get("duplicadas"));
        assertEquals(0, r.get("perdidas"));
    }

    @Test
    @DisplayName("Perda e duplicata na mesma execução são contadas separadamente")
    void perdaEDuplicata() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_000, MSGS));                   // duplica a 1.000
        linhas.remove("2;4000");                                 // perde a 4.000

        Map<String, Object> r = calcular(linhas, 4_000, 0);

        assertEquals(1, r.get("duplicadas"));
        assertEquals(1, r.get("perdidas"));
        assertEquals(MSGS - 1, r.get("unicas"));
    }

    @Test
    @DisplayName("Ordem trocada no arquivo não muda as contagens")
    void ordemTrocada() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_000, MSGS));
        linhas.remove("2;4000");
        Collections.reverse(linhas);
        Collections.swap(linhas, 10, 3_000);

        Map<String, Object> r = calcular(linhas, 4_000, 0);

        assertEquals(1, r.get("duplicadas"));
        assertEquals(1, r.get("perdidas"));
    }

    @Test
    @DisplayName("Falha antes de processar: nada no arquivo = todas perdidas")
    void falhaAntesDeProcessar() throws Exception {
        Map<String, Object> r = calcular(new ArrayList<>(), 0, 0);

        assertEquals(0, r.get("unicas"));
        assertEquals(MSGS, r.get("perdidas"));
    }

    @Test
    @DisplayName("Descartadas pela deduplicação vêm da saída do #2")
    void descartadas() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, MSGS));

        Map<String, Object> r = calcular(linhas, 4_000, 37);

        assertEquals(37, r.get("descartadas"));
    }

    @Test
    @DisplayName("Recuperação = 1ª mensagem do #2 − queda do #1, em ms")
    void recuperacao() throws Exception {
        Map<String, String> s1 = Map.of("QUEDA", "1000000000");
        Map<String, String> s2 = saida2(4_000, 0);
        s2.put("PRIMEIRA_RECEBIDA", "7912500000");               // 6.912,5 ms depois
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, MSGS));

        Map<String, Object> r = calcular(linhas, s1, s2);

        assertEquals(6_912.5, (double) r.get("recuperacaoMs"), 1e-9);
    }

    @Test
    @DisplayName("Sem a 1ª mensagem do #2, a recuperação fica sem valor (−1)")
    void semRecuperacao() throws Exception {
        Map<String, Object> r = calcular(new ArrayList<>(), 0, 0);

        assertEquals(-1.0, (double) r.get("recuperacaoMs"), 1e-9);
    }

    // ------------------------------------------------------------------
    // Entradas inválidas: devem interromper, nunca virar número no CSV
    // ------------------------------------------------------------------

    @Test
    @DisplayName("Arquivo incompleto: o #2 informa mais do que o arquivo tem")
    void arquivoIncompleto() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, 4_990));                  // faltam 10 linhas do #2

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> calcular(linhas, 4_000, 0));
        assertTrue(e.getMessage().contains("informou"), e.getMessage());
    }

    @Test
    @DisplayName("Última linha do #2 cortada no meio é detectada pela contagem")
    void linhaCortadaDoConsumidor2() throws Exception {
        List<String> linhas = linhas(1, 1, 1_000);
        linhas.addAll(linhas(2, 1_001, MSGS - 1));
        linhas.add("2;");                                       // gravação interrompida

        assertThrows(IllegalStateException.class, () -> calcular(linhas, 4_000, 0));
    }

    @Test
    @DisplayName("Sequência 0 ou acima de 5.000 interrompe a execução")
    void sequenciaForaDoIntervalo() {
        assertThrows(IllegalStateException.class, () -> calcular(List.of("2;0"), 1, 0));
        assertThrows(IllegalStateException.class, () -> calcular(List.of("2;5001"), 1, 0));
    }

    @Test
    @DisplayName("Sequência que não é número interrompe a execução")
    void sequenciaInvalida() {
        assertThrows(NumberFormatException.class, () -> calcular(List.of("2;abc"), 1, 0));
    }

    @Test
    @DisplayName("Métrica ausente na saída do #2 interrompe a execução")
    void metricaAusente() {
        Map<String, String> s2 = new HashMap<>();                // sem PROCESSADAS
        assertThrows(IllegalStateException.class,
                () -> calcular(linhas(2, 1, 10), new HashMap<>(), s2));
    }

    @Test
    @DisplayName("O arquivo temporário é apagado depois da contagem")
    void apagaArquivo() throws Exception {
        File f = gravar(linhas(2, 1, MSGS));
        invocar(f, new HashMap<>(), saida2(MSGS, 0));
        assertFalse(f.exists());
    }

    // ------------------------------------------------------------------
    // Ordem rotativa dos níveis
    // ------------------------------------------------------------------

    @Test
    @DisplayName("Ordem rotativa: cada nível aparece uma vez por execução e em cada posição")
    void ordemRotativa() {
        int n = Cenario2FalhaConsumidor.GARANTIAS.length;
        for (int pos = 0; pos < n; pos++) {
            java.util.Set<String> naPosicao = new java.util.HashSet<>();
            for (int exec = 1; exec <= n; exec++) {
                String[] o = Cenario2FalhaConsumidor.ordemDaExecucao(exec);
                assertEquals(n, new java.util.HashSet<>(List.of(o)).size(), "nível repetido na execução " + exec);
                naPosicao.add(o[pos]);
            }
            assertEquals(n, naPosicao.size(), "posição " + (pos + 1) + " não passou por todos os níveis");
        }
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    /** Linhas "id;seq" de 'de' até 'ate', inclusive. */
    private static List<String> linhas(int id, int de, int ate) {
        List<String> l = new ArrayList<>();
        for (int s = de; s <= ate; s++) l.add(id + ";" + s);
        return l;
    }

    private static Map<String, String> saida2(int processadas, int descartadas) {
        Map<String, String> s = new HashMap<>();
        s.put("PROCESSADAS", String.valueOf(processadas));
        s.put("DESCARTADAS", String.valueOf(descartadas));
        return s;
    }

    private Map<String, Object> calcular(List<String> linhas, int processadas2, int descartadas2) throws Exception {
        return calcular(linhas, new HashMap<>(), saida2(processadas2, descartadas2));
    }

    private Map<String, Object> calcular(List<String> linhas, Map<String, String> s1, Map<String, String> s2)
            throws Exception {
        return invocar(gravar(linhas), s1, s2);
    }

    private File gravar(List<String> linhas) throws Exception {
        File f = Files.createTempFile(pasta, "processadas", ".txt").toFile();
        Files.write(f.toPath(), linhas, StandardCharsets.US_ASCII);
        return f;
    }

    /** Chama calcular(arquivo, c1, c2) e devolve os campos do Resultado por nome. */
    private static Map<String, Object> invocar(File arquivo, Map<String, String> s1, Map<String, String> s2)
            throws Exception {
        Class<?> tipo = Class.forName(CONSUMIDOR);
        Method m = Cenario2FalhaConsumidor.class.getDeclaredMethod("calcular", File.class, tipo, tipo);
        m.setAccessible(true);
        Object resultado;
        try {
            resultado = m.invoke(new Cenario2FalhaConsumidor(), arquivo, consumidor(tipo, s1), consumidor(tipo, s2));
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Exception ex) throw ex;
            throw e;
        }
        Map<String, Object> campos = new HashMap<>();
        for (var c : resultado.getClass().getRecordComponents()) {
            Method acessor = c.getAccessor();
            acessor.setAccessible(true);
            campos.put(c.getName(), acessor.invoke(resultado));
        }
        return campos;
    }

    /** Cria um Consumidor (classe interna privada) sem processo, só com a saída. */
    @SuppressWarnings("unchecked")
    private static Object consumidor(Class<?> tipo, Map<String, String> saida) throws Exception {
        Constructor<?> k = tipo.getDeclaredConstructor(Process.class);
        k.setAccessible(true);
        Object c = k.newInstance((Process) null);
        Field f = tipo.getDeclaredField("saida");
        f.setAccessible(true);
        ((Map<String, String>) f.get(c)).putAll(saida);
        return c;
    }
}
