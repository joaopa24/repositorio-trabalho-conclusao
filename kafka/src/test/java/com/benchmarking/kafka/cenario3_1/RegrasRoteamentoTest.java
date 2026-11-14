package com.benchmarking.kafka.cenario3_1;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifica as regras que o Cenário 1 do nível avançado (roteamento) usa como
 * gabarito: a chave de cada mensagem, o tópico em que o produtor publica e
 * quais tópicos cada assinatura lê. É desse gabarito que saem as contagens
 * "esperadas" e "recebidas esperadas" conferidas em cada execução.
 *
 * Os totais abaixo são os da Tabela 11 do TCC; se a regra mudar, o teste
 * mostra qual número deixaria de bater. Não usa broker.
 */
class RegrasRoteamentoTest {

    private static final int MSGS = 6_000;   // igual ao orquestrador

    @Test
    @DisplayName("Chaves: 1.500 mensagens por região, 2.000 por evento e 500 por combinação")
    void distribuicaoDasChaves() {
        Map<String, Integer> porRegiao = new HashMap<>();
        Map<String, Integer> porEvento = new HashMap<>();
        Map<String, Integer> porChave = new HashMap<>();
        for (int s = 1; s <= MSGS; s++) {
            porRegiao.merge(Cenario1Roteamento.regiao(s), 1, Integer::sum);
            porEvento.merge(Cenario1Roteamento.evento(s), 1, Integer::sum);
            porChave.merge(Cenario1Roteamento.regiao(s) + "." + Cenario1Roteamento.evento(s), 1, Integer::sum);
        }
        assertEquals(4, porRegiao.size());
        porRegiao.values().forEach(n -> assertEquals(1_500, n));
        assertEquals(3, porEvento.size());
        porEvento.values().forEach(n -> assertEquals(2_000, n));
        assertEquals(12, porChave.size());
        porChave.values().forEach(n -> assertEquals(500, n));
    }

    @Test
    @DisplayName("Primeiras chaves seguem a regra: região varia primeiro, depois o evento")
    void primeirasChaves() {
        assertEquals("sudeste", Cenario1Roteamento.regiao(1));
        assertEquals("sul", Cenario1Roteamento.regiao(2));
        assertEquals("norte", Cenario1Roteamento.regiao(4));
        assertEquals("sudeste", Cenario1Roteamento.regiao(5));
        assertEquals("criado", Cenario1Roteamento.evento(1));
        assertEquals("criado", Cenario1Roteamento.evento(4));
        assertEquals("pago", Cenario1Roteamento.evento(5));
        assertEquals("cancelado", Cenario1Roteamento.evento(9));
        assertEquals("criado", Cenario1Roteamento.evento(13));
    }

    @Test
    @DisplayName("Tópico de publicação em cada modo do produtor")
    void topicoDePublicacao() {
        assertEquals("rot-direto", Cenario1Roteamento.topico("unico", "rot-direto", 7));
        assertEquals("rot-regiao.nordeste", Cenario1Roteamento.topico("por-regiao", "rot-regiao", 3));
        assertEquals("rot-chave.sul.pago", Cenario1Roteamento.topico("por-chave", "rot-chave", 6));
        assertThrows(IllegalArgumentException.class, () -> Cenario1Roteamento.topico("outro", "x", 1));
    }

    @Test
    @DisplayName("Assinatura por lista: só o nome exato")
    void assinaturaPorLista() {
        assertTrue(Cenario1Roteamento.assina("topicos:rot-regiao.sul", "rot-regiao.sul"));
        assertFalse(Cenario1Roteamento.assina("topicos:rot-regiao.sul", "rot-regiao.sudeste"));
        assertTrue(Cenario1Roteamento.assina("topicos:a,b,c", "b"));
        assertThrows(IllegalArgumentException.class, () -> Cenario1Roteamento.assina("xyz", "rot"));
    }

    @Test
    @DisplayName("Assinatura por padrão casa o nome inteiro, como no KafkaConsumer")
    void assinaturaPorPadrao() {
        String a = "padrao:rot-chave\\.[a-z]+\\.criado";
        String b = "padrao:rot-chave\\.sudeste\\..+";
        assertTrue(Cenario1Roteamento.assina(a, "rot-chave.norte.criado"));
        assertFalse(Cenario1Roteamento.assina(a, "rot-chave.norte.pago"));
        assertFalse(Cenario1Roteamento.assina(a, "rot-chave.norte.criado.extra"));   // casamento parcial não vale
        assertTrue(Cenario1Roteamento.assina(b, "rot-chave.sudeste.cancelado"));
        assertFalse(Cenario1Roteamento.assina(b, "rot-chave.sul.criado"));
    }

    // ------------------------------------------------------------------
    // Totais da Tabela 11 (por execução)
    // ------------------------------------------------------------------

    @Test
    @DisplayName("Direto com filtro: 4 consumidores leem 24.000 para aproveitar 6.000 (4 por útil)")
    void diretoComFiltro() {
        List<Cons> cs = new ArrayList<>();
        for (String r : Cenario1Roteamento.REGIOES) cs.add(new Cons("topicos:rot-direto", s -> Cenario1Roteamento.regiao(s).equals(r)));
        Totais t = totais("unico", "rot-direto", cs);
        assertEquals(24_000, t.lidas);
        assertEquals(6_000, t.uteis);
        assertEquals(0, t.semDestino);
    }

    @Test
    @DisplayName("Direto com tópico por região: lidas = úteis = 6.000")
    void diretoPorTopico() {
        List<Cons> cs = new ArrayList<>();
        for (String r : Cenario1Roteamento.REGIOES) cs.add(new Cons("topicos:rot-regiao." + r, s -> Cenario1Roteamento.regiao(s).equals(r)));
        Totais t = totais("por-regiao", "rot-regiao", cs);
        assertEquals(6_000, t.lidas);
        assertEquals(6_000, t.uteis);
        assertEquals(0, t.excedentes);
    }

    @Test
    @DisplayName("Curinga com filtro: 2 consumidores leem 12.000 para aproveitar 3.500; 3.000 sem destino")
    void curingaComFiltro() {
        Totais t = totais("unico", "rot-curinga", List.of(
                new Cons("topicos:rot-curinga", s -> Cenario1Roteamento.evento(s).equals("criado")),
                new Cons("topicos:rot-curinga", s -> Cenario1Roteamento.regiao(s).equals("sudeste"))));
        assertEquals(12_000, t.lidas);
        assertEquals(3_500, t.uteis);
        assertEquals(3_000, t.semDestino);
        assertEquals(500, t.paraOsDois);   // pedidos.sudeste.criado
    }

    @Test
    @DisplayName("Curinga com tópicos por padrão: lidas = úteis = 3.500, e 6 dos 12 tópicos sem leitor")
    void curingaPorPadrao() {
        List<Cons> cs = List.of(
                new Cons("padrao:rot-chave\\.[a-z]+\\.criado", s -> Cenario1Roteamento.evento(s).equals("criado")),
                new Cons("padrao:rot-chave\\.sudeste\\..+", s -> Cenario1Roteamento.regiao(s).equals("sudeste")));
        Totais t = totais("por-chave", "rot-chave", cs);
        assertEquals(3_500, t.lidas);
        assertEquals(3_500, t.uteis);
        assertEquals(0, t.excedentes);

        Set<String> lidos = new HashSet<>();
        for (String r : Cenario1Roteamento.REGIOES) {
            for (String e : Cenario1Roteamento.EVENTOS) {
                String topico = "rot-chave." + r + "." + e;
                for (Cons c : cs) if (Cenario1Roteamento.assina(c.assinatura, topico)) lidos.add(topico);
            }
        }
        assertEquals(6, lidos.size());
        assertEquals(6, 12 - lidos.size());
    }

    @Test
    @DisplayName("Fanout com 3 grupos: cada grupo lê as 6.000 (18.000 no total)")
    void fanout() {
        List<Cons> cs = new ArrayList<>();
        for (int i = 0; i < 3; i++) cs.add(new Cons("topicos:rot-fanout", s -> true));
        Totais t = totais("unico", "rot-fanout", cs);
        assertEquals(18_000, t.lidas);
        assertEquals(18_000, t.uteis);
    }

    // ------------------------------------------------------------------
    // Auxiliares: mesmo cálculo de "esperadas" e "recebidas esperadas" do orquestrador
    // ------------------------------------------------------------------

    private record Cons(String assinatura, IntPredicate esperado) {}

    private static final class Totais {
        int lidas, uteis, excedentes, semDestino, paraOsDois;
    }

    private static Totais totais(String modo, String prefixo, List<Cons> consumidores) {
        Totais t = new Totais();
        for (int s = 1; s <= MSGS; s++) {
            String topico = Cenario1Roteamento.topico(modo, prefixo, s);
            int interessados = 0;
            for (Cons c : consumidores) {
                boolean recebe = Cenario1Roteamento.assina(c.assinatura, topico);
                boolean quer = c.esperado.test(s);
                if (quer && !recebe) throw new AssertionError("Mensagem " + s + " nunca chegaria a quem a quer");
                if (recebe) t.lidas++;
                if (recebe && quer) t.uteis++;
                if (recebe && !quer) t.excedentes++;
                if (quer) interessados++;
            }
            if (interessados == 0) t.semDestino++;
            if (interessados == 2) t.paraOsDois++;
        }
        return t;
    }
}
