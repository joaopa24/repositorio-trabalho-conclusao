package com.benchmarking.kafka.cenario2_4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível intermediário — Cenário 4: Sobrecarga prolongada — CONSUMIDOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, group.id, número de mensagens
 * enviadas, tempo de processamento por mensagem (ms), max.poll.records,
 * max.partition.fetch.bytes (opcional; padrão 112640).
 *
 * Consumidor lento: Thread.sleep(procMs) por mensagem (o mesmo método do
 * Cenário 4 básico) e commit do lote depois de processar. A capacidade fica
 * em ~1000/procMs msg/s, abaixo da taxa do produtor.
 *
 * Buffer do cliente igualado ao prefetch do RabbitMQ: max.poll.records só
 * limita o que cada poll() devolve. O cliente busca do broker até
 * max.partition.fetch.bytes por partição (padrão 1 MB, ~900 mensagens de
 * 1 KB) e guarda isso em memória. Na política com-limite, o que já está no
 * buffer escapa da retenção do broker. Com ~110 KB, o buffer fica em ~100
 * mensagens, como o prefetch de 100 do RabbitMQ. O limite não é exato: o
 * broker sempre devolve pelo menos um lote inteiro.
 *
 * auto.offset.reset=earliest: se o broker apagar mensagens que o consumidor
 * ainda não leu (limite de retenção), a posição dele deixa de existir e ele
 * pula para a mais antiga disponível. As mensagens puladas estão PERDIDAS
 * para este consumidor; o número de sequência permite contá-las.
 *
 * allow.auto.create.topics=false: o consumidor nunca cria tópico. Um
 * consumidor que sobrevivesse a uma execução anterior não recria o tópico
 * dela depois de apagado.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim (EOF), o
 * orquestrador morreu (mesmo à força), e o consumidor encerra na hora.
 *
 * Conversa com o orquestrador:
 *   stdout STATUS=PRONTO           depois de receber a partição
 *   stdout CONSUMIDAS=<n>          a cada ~1 s (série temporal)
 *   stdin  PRODUTOR_TERMINOU       encerra depois de 5 s sem mensagens
 *   stdin  EOF                     o orquestrador morreu: encerra (código 3)
 */
public class ConsumidorSustentado {

    private static final String HEADER_SEQ = "sequencia";
    private static final long OCIOSO_MS = 5_000;
    private static final long MAX_TOTAL_MS = 1_800_000;
    private static final int  FETCH_BYTES_PADRAO = 112_640;

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ConsumidorSustentado <bootstrap> <topico> <groupId> <numMsgs> <procMs> <maxPoll> [maxPartitionFetchBytes]");
            System.exit(2);
        }
        String bootstrapServers = args[0];
        String topico           = args[1];
        String groupId          = args[2];
        int    numMsgs          = Integer.parseInt(args[3]);
        long   procMs           = Long.parseLong(args[4]);
        int    maxPoll          = Integer.parseInt(args[5]);
        int    fetchBytes       = args.length >= 7 ? Integer.parseInt(args[6]) : FETCH_BYTES_PADRAO;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,          bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                   groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,          "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,     StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,   ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,         "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,           String.valueOf(maxPoll));
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,  String.valueOf(fetchBytes));
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,   "false");
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        AtomicLong sinal = new AtomicLong(0);
        Thread stdin = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                String l;
                while ((l = in.readLine()) != null) {
                    if (l.trim().equals("PRODUTOR_TERMINOU")) sinal.set(System.nanoTime());
                }
            } catch (IOException ignored) {
            }
            // Fim da entrada padrão: o orquestrador encerrou (normalmente ou à força).
            // Numa execução normal isso nunca acontece antes de o consumidor terminar,
            // porque o orquestrador espera por ele com o stdin ainda aberto.
            System.err.println("Entrada padrão fechada: o orquestrador encerrou. Saindo para não ficar órfão.");
            Runtime.getRuntime().halt(3);
        }, "vigia-stdin");
        stdin.setDaemon(true);
        stdin.start();

        boolean[] vista = new boolean[numMsgs + 1];
        int recebidas = 0, unicas = 0, saltos = 0, ultimaSeq = 0;
        long ultimaAtividade = 0, ultimoFim = 0;
        long inicio = System.nanoTime();
        long proximoRelato = inicio + 1_000_000_000L;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topico));
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
                if ((System.nanoTime() - inicio) / 1_000_000 > 60_000) {
                    System.err.println("Partição não atribuída em 60 s");
                    System.exit(1);
                }
            }
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, byte[]> r : records) {
                    try {
                        Thread.sleep(procMs);   // processamento lento
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    Header h = r.headers().lastHeader(HEADER_SEQ);
                    if (h != null) {
                        int seq = ByteBuffer.wrap(h.value()).getInt();
                        if (seq > ultimaSeq + 1) saltos++;   // o broker apagou mensagens não lidas
                        ultimaSeq = Math.max(ultimaSeq, seq);
                        if (seq >= 1 && seq <= numMsgs && !vista[seq]) {
                            vista[seq] = true;
                            unicas++;
                        }
                    }
                    recebidas++;
                    offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                    ultimaAtividade = ultimoFim = System.nanoTime();

                    if (System.nanoTime() >= proximoRelato) {
                        System.out.println("CONSUMIDAS=" + recebidas);
                        System.out.flush();
                        proximoRelato += 1_000_000_000L;
                    }
                }
                if (!offsets.isEmpty()) consumer.commitSync(offsets);

                long agora = System.nanoTime();
                if (agora >= proximoRelato) {
                    System.out.println("CONSUMIDAS=" + recebidas);
                    System.out.flush();
                    proximoRelato = agora + 1_000_000_000L;
                }
                long s = sinal.get();
                if (s > 0 && (agora - Math.max(s, ultimaAtividade)) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
        }

        System.out.println("CONSUMIDAS=" + recebidas);
        System.out.println("RECEBIDAS=" + recebidas);
        System.out.println("UNICAS=" + unicas);
        System.out.println("SALTOS=" + saltos);
        System.out.println("ULTIMO_FIM=" + ultimoFim);
        System.out.println("STATUS=CONCLUIDO");
        System.out.flush();
    }
}
