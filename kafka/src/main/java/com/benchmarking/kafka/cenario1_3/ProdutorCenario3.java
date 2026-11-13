package com.benchmarking.kafka.cenario1_3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cenário 3 — Variação do tamanho da mensagem — PRODUTOR (Kafka).
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - número de mensagens
 *   3 - execução (0 = aquecimento)
 *   4 - tamanho da mensagem em bytes
 *   5 - taxa alvo em msg/s (0 = sem controle de taxa, o mais rápido possível)
 *
 * O cenário tem duas partes, e o produtor serve às duas:
 *
 *   Parte A (latência): taxa fixa e baixa (10 msg/s), IGUAL para todos os
 *   tamanhos. Como a taxa fica bem abaixo da capacidade do ambiente, não se
 *   forma fila, e a diferença de latência entre os tamanhos passa a ser o
 *   custo de transportar mensagens maiores.
 *
 *   Parte B (vazão máxima): taxa = 0, o produtor envia o mais rápido que o
 *   cliente deixar. Aqui formar fila é esperado; mede-se só a vazão, nunca
 *   a latência.
 *
 * MUDANÇAS em relação à versão anterior:
 *
 *   - Configuração IGUAL à do Cenário 1: acks=1, linger.ms=0, batch.size de
 *     64 KB, idempotência desligada explicitamente, max.in.flight padrão (5).
 *     Antes era acks=all, linger.ms=5 e max.in.flight=300 sem idempotência —
 *     configuração diferente dos outros cenários e que, com mais de 5
 *     requisições em voo sem idempotência, ainda permite reordenação de
 *     mensagens em caso de reenvio.
 *
 *   - Removido o semáforo de 300 mensagens em voo. Na Parte A ele é
 *     desnecessário (o controle de taxa já impede acúmulo); na Parte B o
 *     próprio cliente segura o produtor quando o buffer (buffer.memory)
 *     enche.
 *
 *   - Controle de taxa com aguardarAte(), o mesmo método do Cenário 1.
 *
 *   - partitionsFor() antes de t0: metadados e conexão ficam fora da medição.
 *
 *   - Qualquer falha de envio faz o processo terminar com código 1, e o
 *     orquestrador interrompe a campanha (antes a execução seguia).
 *
 * Observação sobre o batch.size (64 KB): o cliente só junta mensagens num
 * mesmo lote quando há acúmulo esperando envio, o que acontece na Parte B
 * mesmo com linger.ms=0. Um lote de 64 KB comporta três mensagens de 20 KB,
 * duas de ~30 KB e apenas uma de 50 KB ou mais (e mensagens maiores que
 * 64 KB vão num lote do próprio tamanho). Na Parte A, a taxa baixa faz cada
 * mensagem sair sozinha em qualquer tamanho.
 */
public class ProdutorCenario3 {

    private static final String HEADER_TS = "ts-envio";

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ProdutorCenario3 <bootstrap> <topico> <numMsgs> <execucao> <tamanhoBytes> <taxaMsgS>");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        int    tamanhoBytes     = Integer.parseInt(args[4]);
        int    taxaMsgS         = Integer.parseInt(args[5]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // mesma configuração do Cenário 1
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        props.put("auto.include.jmx.reporter", "false");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        byte[] payload = new byte[tamanhoBytes];
        AtomicInteger falhas = new AtomicInteger(0);

        System.out.printf("Produtor: %d msgs de %d bytes, taxa %s%n", numMsgs, tamanhoBytes,
                taxaMsgS > 0 ? taxaMsgS + " msg/s" : "máxima (sem controle)");

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {

            // metadados + conexão com o broker FORA da medição
            producer.partitionsFor(topico);

            long t0 = System.nanoTime();
            long intervaloNs = taxaMsgS > 0 ? 1_000_000_000L / taxaMsgS : 0L;

            for (int i = 0; i < numMsgs; i++) {
                if (intervaloNs > 0) {
                    aguardarAte(t0 + i * intervaloNs);
                }

                long ts = System.nanoTime();
                final int seq = i + 1;

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topico, payload);
                record.headers().add(HEADER_TS, ByteBuffer.allocate(Long.BYTES).putLong(ts).array());

                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        if (falhas.incrementAndGet() <= 5) {
                            System.err.println("Falha ao enviar mensagem " + seq + ": " + ex.getMessage());
                        }
                    }
                });
            }

            // espera todas as confirmações do broker
            producer.flush();
            long t1 = System.nanoTime();

            System.out.println("T0="       + t0);
            System.out.println("T1="       + t1);
            System.out.println("MSGS="     + numMsgs);
            System.out.println("FALHAS="   + falhas.get());
            System.out.println("EXECUCAO=" + execucao);
        }

        if (falhas.get() > 0) {
            System.err.printf("Falhas de envio: %d/%d%n", falhas.get(), numMsgs);
            System.exit(1);
        }
    }

    /**
     * Espera até o instante alvo (nanoTime). Esperas longas (> 20 ms) dormem
     * a maior parte do tempo; o trecho final é feito em busy-wait, imune à
     * resolução do timer do Windows (~15,6 ms). Mesmo método do Cenário 1.
     */
    static void aguardarAte(long alvoNs) {
        long faltaNs;
        while ((faltaNs = alvoNs - System.nanoTime()) > 0) {
            if (faltaNs > 20_000_000L) {
                try {
                    Thread.sleep((faltaNs - 3_000_000L) / 1_000_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } else {
                Thread.onSpinWait();
            }
        }
    }
}