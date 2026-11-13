package com.benchmarking.kafka.cenario3_4;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nível avançado — Cenário 4: Replay — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens, pausa no meio (ms).
 *
 * Publica as mensagens de uma vez na partição 0, com uma pausa no meio: envia
 * a 1ª metade, espera as confirmações (flush), imprime MEIO_MS (relógio de
 * parede), espera a pausa e envia a 2ª metade. É o mesmo desenho da versão
 * RabbitMQ.
 *
 * As mensagens NÃO levam timestamp do produtor: o tópico é criado com
 * message.timestamp.type = LogAppendTime, e quem carimba a hora é o broker no
 * momento da gravação, como no stream do RabbitMQ. Assim, a releitura por
 * tempo usa o mesmo método nas duas tecnologias (hora real do broker, pedido
 * no meio da pausa) e depende do mesmo relógio (o do host contra o da VM do
 * Docker). A diferença que sobra é a da tecnologia: o Kafka posiciona na
 * mensagem exata, em milissegundos; o stream, no início do bloco, em segundos.
 *
 * linger.ms = 0, como nos demais cenários. Idempotência ligada: sem
 * reordenação nem duplicatas, o que garante offset = sequência − 1.
 * Util.vigiarEntrada encerra o processo se o orquestrador morrer.
 */
public class ProdutorReplay {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: ProdutorReplay <bootstrap> <topico> <numMsgs> <pausaMs>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topico    = args[1];
        int    numMsgs   = Integer.parseInt(args[2]);
        long   pausa     = Long.parseLong(args[3]);
        Util.vigiarEntrada(l -> { });

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Idempotência ligada (padrão com acks=all): sem reordenação nem duplicatas,
        // o que garante offset = sequência − 1.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        Util.semMetricas(props);

        byte[] payload = new byte[1024];
        AtomicInteger falhas = new AtomicInteger();
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.partitionsFor(topico);
            long t0 = System.nanoTime();
            for (int i = 0; i < numMsgs; i++) {
                if (i == numMsgs / 2) {
                    producer.flush();   // a 1ª metade já está gravada e confirmada
                    System.out.println("MEIO_MS=" + System.currentTimeMillis());
                    System.out.flush();
                    Thread.sleep(pausa);
                }
                final int seq = i + 1;
                // sem timestamp: com LogAppendTime, a hora é a da gravação no broker
                ProducerRecord<String, byte[]> r = new ProducerRecord<>(topico, 0, null, payload);
                r.headers().add("sequencia", ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                producer.send(r, (m, ex) -> {
                    if (ex != null && falhas.incrementAndGet() <= 5) System.err.println("Falha no envio " + seq + ": " + ex.getMessage());
                });
            }
            producer.flush();
            long t1 = System.nanoTime();
            System.out.println("T0=" + t0);
            System.out.println("T1=" + t1);
            System.out.println("MSGS=" + numMsgs);
            System.out.println("FALHAS=" + falhas.get());
        }
        if (falhas.get() > 0) System.exit(1);
    }
}