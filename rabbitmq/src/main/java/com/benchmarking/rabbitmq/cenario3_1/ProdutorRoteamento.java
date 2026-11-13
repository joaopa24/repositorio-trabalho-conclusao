package com.benchmarking.rabbitmq.cenario3_1;

import com.rabbitmq.client.*;
import com.benchmarking.rabbitmq.cenario3_comum.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível avançado — Cenário 1: Roteamento — PRODUTOR (RabbitMQ).
 *
 * Argumentos: host, porta, exchange, modo da routing key ("regiao" ou
 * "completa"), número de mensagens, execução.
 *
 * Cada mensagem tem a chave pedidos.<região>.<evento> (mesma distribuição da
 * versão Kafka: região = (seq−1) % 4, evento = ((seq−1)/4) % 3). A routing key
 * enviada ao broker é a região (exchange direct) ou a chave completa
 * (exchanges topic e fanout). O producer NÃO sabe quem vai receber: quem
 * decide é o broker, pelos bindings.
 *
 * Publica com mandatory=true e publisher confirms: mensagens que não casam
 * com nenhum binding voltam pelo ReturnListener e são contadas como
 * NAO_ROTEADAS (sem mandatory, o broker as descartaria em silêncio).
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim, o
 * orquestrador morreu, e o produtor encerra na hora (Util.vigiarEntrada).
 */
public class ProdutorRoteamento {

    static final String[] REGIOES = {"sudeste", "sul", "nordeste", "norte"};
    static final String[] EVENTOS = {"criado", "pago", "cancelado"};

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ProdutorRoteamento <host> <porta> <exchange> <regiao|completa> <numMsgs> <execucao>");
            System.exit(2);
        }
        String host     = args[0];
        int    porta    = Integer.parseInt(args[1]);
        String exchange = args[2];
        String modo     = args[3];
        int    numMsgs  = Integer.parseInt(args[4]);
        int    execucao = Integer.parseInt(args[5]);

        Util.vigiarEntrada(linha -> { });   // só interessa o fim da entrada

        byte[] payload = new byte[1024];
        AtomicInteger naoRoteadas = new AtomicInteger();
        try (Connection conn = Util.fabrica(host, porta).newConnection();
             Channel ch = conn.createChannel()) {
            ch.confirmSelect();
            ch.addReturnListener(r -> naoRoteadas.incrementAndGet());
            for (int seq = 1; seq <= numMsgs; seq++) {
                String regiao = REGIOES[(seq - 1) % REGIOES.length];
                String evento = EVENTOS[((seq - 1) / REGIOES.length) % EVENTOS.length];
                String chave = "pedidos." + regiao + "." + evento;
                Map<String, Object> h = new HashMap<>();
                h.put("chave", chave);
                h.put("sequencia", seq);
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(h).deliveryMode(2).build();
                ch.basicPublish(exchange, modo.equals("regiao") ? regiao : chave, true, props, payload);
            }
            // os retornos chegam antes da confirmação da mesma mensagem
            ch.waitForConfirmsOrDie(60_000);
        }
        System.out.println("MSGS=" + numMsgs);
        System.out.println("NAO_ROTEADAS=" + naoRoteadas.get());
        System.out.println("EXECUCAO=" + execucao);
        System.out.flush();
    }
}
