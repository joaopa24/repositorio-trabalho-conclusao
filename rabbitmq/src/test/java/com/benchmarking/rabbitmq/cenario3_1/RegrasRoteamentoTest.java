package com.benchmarking.rabbitmq.cenario3_1;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifica o gabarito do Cenário 1 do nível avançado no RabbitMQ: a chave de
 * cada mensagem e quantas mensagens cada fila deveria receber. O orquestrador
 * compara o que chegou com essas regras; os totais abaixo são os da Tabela 11
 * do TCC. Não usa broker.
 */
class RegrasRoteamentoTest {

    private static final int MSGS = 6_000;   // igual ao orquestrador

    @Test
    @DisplayName("Orquestrador e produtor usam as mesmas regiões e eventos")
    void mesmasListas() {
        assertArrayEquals(ProdutorRoteamento.REGIOES, Cenario1Roteamento.REGIOES);
        assertArrayEquals(ProdutorRoteamento.EVENTOS, Cenario1Roteamento.EVENTOS);
    }

    @Test
    @DisplayName("Mesma distribuição do Kafka: 1.500 por região, 2.000 por evento, 500 por combinação")
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
        assertEquals("norte", Cenario1Roteamento.regiao(4));
        assertEquals("criado", Cenario1Roteamento.evento(4));
        assertEquals("pago", Cenario1Roteamento.evento(5));
        assertEquals("cancelado", Cenario1Roteamento.evento(9));
    }

    @Test
    @DisplayName("Direct: cada uma das 4 filas recebe 1.500, 6.000 no total")
    void direct() {
        int total = 0;
        for (String r : Cenario1Roteamento.REGIOES) {
            int n = contar(s -> Cenario1Roteamento.regiao(s).equals(r));
            assertEquals(1_500, n, "fila de " + r);
            total += n;
        }
        assertEquals(6_000, total);
    }

    @Test
    @DisplayName("Topic: A recebe 2.000, B 1.500 (3.500 ocorrências), 500 vão às duas e 3.000 ficam sem destino")
    void topic() {
        IntPredicate a = s -> Cenario1Roteamento.evento(s).equals("criado");     // pedidos.*.criado
        IntPredicate b = s -> Cenario1Roteamento.regiao(s).equals("sudeste");    // pedidos.sudeste.#
        assertEquals(2_000, contar(a));
        assertEquals(1_500, contar(b));
        assertEquals(3_500, contar(a) + contar(b));
        assertEquals(500, contar(a.and(b)));
        assertEquals(3_000, contar(a.negate().and(b.negate())));   // naoRoteadasEsperadas do orquestrador
    }

    @Test
    @DisplayName("Fanout: 3 filas com as 6.000 cada, 18.000 ocorrências")
    void fanout() {
        assertEquals(18_000, 3 * contar(s -> true));
    }

    private static int contar(IntPredicate p) {
        int n = 0;
        for (int s = 1; s <= MSGS; s++) if (p.test(s)) n++;
        return n;
    }
}
