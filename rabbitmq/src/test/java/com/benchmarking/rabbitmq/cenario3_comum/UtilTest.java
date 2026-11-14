package com.benchmarking.rabbitmq.cenario3_comum;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifica os auxiliares que os orquestradores usam para ler a saída dos
 * subprocessos (chave=valor), exigir métricas, esperar o término com o
 * código certo e interromper por timeout.
 *
 * Os subprocessos são um programa Java mínimo, gerado na hora numa pasta
 * temporária: escreve as linhas pedidas, dorme e sai com o código pedido.
 * Não usa broker.
 */
class UtilTest {

    private static final String JAVA = ProcessHandle.current().info().command().orElse("java");

    @TempDir
    Path pasta;

    // ------------------------------------------------------------------
    // obrigatorio
    // ------------------------------------------------------------------

    @Test
    @DisplayName("obrigatorio devolve o valor quando a métrica existe")
    void obrigatorioPresente() {
        assertEquals("42", Util.obrigatorio(Map.of("RECEBIDAS", "42"), "RECEBIDAS"));
    }

    @Test
    @DisplayName("obrigatorio interrompe quando a métrica falta ou está vazia")
    void obrigatorioAusente() {
        assertThrows(IllegalStateException.class, () -> Util.obrigatorio(Map.of(), "RECEBIDAS"));
        assertThrows(IllegalStateException.class, () -> Util.obrigatorio(Map.of("RECEBIDAS", "  "), "RECEBIDAS"));
    }

    // ------------------------------------------------------------------
    // ler
    // ------------------------------------------------------------------

    @Test
    @DisplayName("ler guarda as linhas chave=valor, ignora as outras e sinaliza STATUS=PRONTO")
    void lerSaida() throws Exception {
        Process p = programa(0, 0, "STATUS=PRONTO", "RECEBIDAS = 42 ", "linha sem chave",
                "=sem nome", "TOPICOS=a,b", "N=1", "N=2");
        Map<String, String> saida = new HashMap<>();
        CountDownLatch pronto = new CountDownLatch(1);

        Util.ler(p, "teste", saida, pronto, null);

        assertEquals("PRONTO", saida.get("STATUS"));
        assertEquals("42", saida.get("RECEBIDAS"));
        assertEquals("a,b", saida.get("TOPICOS"));
        assertEquals("2", saida.get("N"), "o último valor de uma chave repetida vale");
        assertFalse(saida.containsKey(""));
        assertEquals(4, saida.size());   // STATUS, RECEBIDAS, TOPICOS e N
        assertEquals(0, pronto.getCount());
    }

    @Test
    @DisplayName("ler sem STATUS=PRONTO não libera o latch")
    void lerSemPronto() throws Exception {
        Process p = programa(0, 0, "RECEBIDAS=1");
        CountDownLatch pronto = new CountDownLatch(1);

        Util.ler(p, "teste", new HashMap<>(), pronto, null);

        assertEquals(1, pronto.getCount());
    }

    // ------------------------------------------------------------------
    // esperar
    // ------------------------------------------------------------------

    @Test
    @DisplayName("esperar aceita o processo que termina com o código esperado")
    void esperarCodigoCerto() throws Exception {
        Process p = programa(0, 0);
        assertDoesNotThrow(() -> Util.esperar(p, "teste", 0, 60));
    }

    @Test
    @DisplayName("esperar interrompe quando o código de saída é outro")
    void esperarCodigoErrado() throws Exception {
        Process p = programa(3, 0);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Util.esperar(p, "teste", 0, 60));
        assertTrue(e.getMessage().contains("código 3"), e.getMessage());
    }

    @Test
    @DisplayName("esperar aceita um código diferente de 0 quando ele é o esperado (ex.: queda simulada)")
    void esperarCodigoEsperadoDiferenteDeZero() throws Exception {
        Process p = programa(137, 0);
        assertDoesNotThrow(() -> Util.esperar(p, "teste", 137, 60));
    }

    @Test
    @DisplayName("Timeout: esperar interrompe e mata o processo que não termina")
    void esperarTimeout() throws Exception {
        Process p = programa(0, 120_000);   // dormiria 2 minutos
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> Util.esperar(p, "teste", 0, 2));
        assertTrue(e.getMessage().startsWith("Timeout"), e.getMessage());
        assertTrue(p.waitFor(10, TimeUnit.SECONDS), "o processo deveria ter sido morto");
        assertFalse(p.isAlive());
    }

    // ------------------------------------------------------------------
    // Leitura da API de gerenciamento (JSON)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("textoJson lê o primeiro \"chave\":\"valor\" e devolve null se não houver")
    void textoJson() {
        String json = "{\"name\":\"fila-1\", \"leader\" : \"rabbit@rabbit-2\",\"node\":\"rabbit@rabbit-1\"}";
        assertEquals("rabbit@rabbit-2", Util.textoJson(json, "leader"));
        assertEquals("fila-1", Util.textoJson(json, "name"));
        assertEquals(null, Util.textoJson(json, "ausente"));
        assertEquals(null, Util.textoJson(null, "leader"));
    }

    @Test
    @DisplayName("listaJson lê os itens de \"chave\":[...] e devolve lista vazia se não houver")
    void listaJson() {
        String json = "{\"members\":[\"rabbit@rabbit-1\",\"rabbit@rabbit-2\", \"rabbit@rabbit-3\"],\"online\":[]}";
        assertEquals(java.util.List.of("rabbit@rabbit-1", "rabbit@rabbit-2", "rabbit@rabbit-3"),
                Util.listaJson(json, "members"));
        assertTrue(Util.listaJson(json, "online").isEmpty());
        assertTrue(Util.listaJson(json, "ausente").isEmpty());
        assertTrue(Util.listaJson(null, "members").isEmpty());
    }

    // ------------------------------------------------------------------
    // Estatística
    // ------------------------------------------------------------------

    @Test
    @DisplayName("resumoMs: média, mediana (ímpar), mínimo e máximo em ms")
    void resumoImpar() {
        long[] ns = {5_000_000, 1_000_000, 3_000_000};
        double[] r = Util.resumoMs(ns, 3);
        assertEquals(3.0, r[0], 1e-9);
        assertEquals(3.0, r[1], 1e-9);
        assertEquals(1.0, r[2], 1e-9);
        assertEquals(5.0, r[3], 1e-9);
    }

    @Test
    @DisplayName("resumoMs: mediana de quantidade par é a média dos dois do meio")
    void resumoPar() {
        long[] ns = {4_000_000, 1_000_000, 2_000_000, 10_000_000};
        double[] r = Util.resumoMs(ns, 4);
        assertEquals(4.25, r[0], 1e-9);
        assertEquals(3.0, r[1], 1e-9);
        assertEquals(1.0, r[2], 1e-9);
        assertEquals(10.0, r[3], 1e-9);
    }

    @Test
    @DisplayName("resumoMs considera só os n primeiros valores e não altera o vetor original")
    void resumoUsaSoN() {
        long[] ns = {3_000_000, 1_000_000, 999_000_000};
        double[] r = Util.resumoMs(ns, 2);
        assertEquals(3.0, r[3], 1e-9);
        assertEquals(3_000_000, ns[0]);
    }

    // ------------------------------------------------------------------
    // Auxiliar: subprocesso Java mínimo
    // ------------------------------------------------------------------

    /**
     * Inicia, pelo próprio Util.iniciar, um programa que escreve as linhas,
     * dorme 'dormirMs' e sai com 'codigo'.
     */
    private Process programa(int codigo, long dormirMs, String... linhas) throws Exception {
        Path fonte = pasta.resolve("Programa.java");
        if (!Files.exists(fonte)) {
            Files.writeString(fonte, String.join("\n",
                    "public class Programa {",
                    "  public static void main(String[] a) throws Exception {",
                    "    for (int i = 2; i < a.length; i++) System.out.println(a[i]);",
                    "    System.out.flush();",
                    "    Thread.sleep(Long.parseLong(a[1]));",
                    "    System.exit(Integer.parseInt(a[0]));",
                    "  }",
                    "}"), StandardCharsets.UTF_8);
        }
        String[] args = new String[linhas.length + 2];
        args[0] = String.valueOf(codigo);
        args[1] = String.valueOf(dormirMs);
        System.arraycopy(linhas, 0, args, 2, linhas.length);
        return Util.iniciar(JAVA, pasta.toString(), fonte.toString(), args);
    }
}
