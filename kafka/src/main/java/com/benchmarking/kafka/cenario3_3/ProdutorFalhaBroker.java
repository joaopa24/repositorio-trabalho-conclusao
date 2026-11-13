package com.benchmarking.kafka.cenario3_3;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível avançado — Cenário 3: Falha de broker — PRODUTOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, número de mensagens, taxa (msg/s),
 * acks ("all", "1" ou "0"), arquivo onde gravar as sequências CONFIRMADAS.
 *
 * Envia em taxa fixa (mesmo método dos outros cenários) enquanto o
 * orquestrador derruba o líder da partição. Para cada mensagem guarda se o
 * broker confirmou. "Confirmada" é a promessa que o broker fez ao produtor:
 * o orquestrador depois confere se toda mensagem confirmada chegou ao
 * consumidor (as que não chegaram são PERDIDAS).
 *
 * Configurações comparadas (os três níveis de garantia do produtor):
 *   acks=all + idempotência: o líder só confirma depois que as réplicas em
 *     sincronia gravaram; a idempotência evita duplicatas nos reenvios.
 *     Par do RabbitMQ: confirms.
 *   acks=1: o líder confirma sozinho. Se ele cair antes de os seguidores
 *     copiarem, a mensagem confirmada some. Sem par no RabbitMQ (a quorum
 *     queue não confirma só pelo líder). O Kafka só aceita idempotência com
 *     acks=all, então aqui ela fica desligada.
 *   acks=0: o produtor não espera resposta nenhuma. O callback é chamado sem
 *     erro assim que a mensagem sai, então "confirmada" não tem significado:
 *     CONFIRMADAS e o maior intervalo saem como -1. Par do RabbitMQ: sem-confirms.
 *
 * Também mede o MAIOR intervalo sem nenhuma confirmação durante o envio
 * (acks=all e acks=1): é quanto tempo o produtor ficou "travado" esperando o
 * novo líder.
 *
 * Proteção contra processo órfão: se a entrada padrão chegar ao fim, o
 * orquestrador morreu, e o produtor encerra na hora (Util.vigiarEntrada).
 *
 * Saída: T0 e STATUS=PRONTO no início do envio (o orquestrador conta o tempo
 * da queda a partir daí), ENVIADAS a cada segundo, e no fim T1, MSGS,
 * CONFIRMADAS, FALHAS e MAIOR_INTERVALO_SEM_CONFIRMACAO_MS.
 */
public class ProdutorFalhaBroker {

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("Uso: ProdutorFalhaBroker <bootstrap> <topico> <numMsgs> <taxa> <all|1|0> <arquivoConfirmadas>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topico    = args[1];
        int    numMsgs   = Integer.parseInt(args[2]);
        int    taxa      = Integer.parseInt(args[3]);
        String acks      = args[4];
        String arquivo   = args[5];
        if (!acks.equals("all") && !acks.equals("1") && !acks.equals("0")) {
            throw new IllegalArgumentException("acks inválido: " + acks);
        }
        boolean idempotente = acks.equals("all");   // o Kafka exige acks=all para a idempotência
        boolean temConfirmacao = !acks.equals("0");

        Util.vigiarEntrada(linha -> { });   // só interessa o fim da entrada

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotente);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // Reenvia por até 2 min (padrão do cliente): tempo de sobra para a eleição do novo líder.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        Util.semMetricas(props);

        byte[] payload = new byte[1024];
        ConcurrentLinkedQueue<Integer> confirmadas = new ConcurrentLinkedQueue<>();
        AtomicInteger falhas = new AtomicInteger();
        AtomicLong ultimaConfirmacao = new AtomicLong();
        AtomicLong maiorIntervalo = new AtomicLong();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            producer.partitionsFor(topico);
            long t0 = System.nanoTime();
            ultimaConfirmacao.set(t0);
            System.out.println("T0=" + t0);
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            long intervalo = 1_000_000_000L / taxa;
            long proximoRelato = t0 + 1_000_000_000L;
            for (int i = 0; i < numMsgs; i++) {
                Util.aguardarAte(t0 + i * intervalo);
                final int seq = i + 1;
                ProducerRecord<String, byte[]> r = new ProducerRecord<>(topico, payload);
                r.headers().add("ts-envio", ByteBuffer.allocate(Long.BYTES).putLong(System.nanoTime()).array());
                r.headers().add("sequencia", ByteBuffer.allocate(Integer.BYTES).putInt(seq).array());
                producer.send(r, (m, ex) -> {
                    long agora = System.nanoTime();
                    if (ex == null) {
                        if (temConfirmacao) {
                            confirmadas.add(seq);
                            long antes = ultimaConfirmacao.getAndSet(agora);
                            maiorIntervalo.accumulateAndGet(agora - antes, Math::max);
                        }
                    } else if (falhas.incrementAndGet() <= 5) {
                        System.err.println("Falha no envio " + seq + ": " + ex.getMessage());
                    }
                });
                long agora = System.nanoTime();
                if (agora >= proximoRelato) {
                    System.out.println("ENVIADAS=" + seq + ";CONFIRMADAS=" + (temConfirmacao ? confirmadas.size() : -1));
                    proximoRelato += 1_000_000_000L;
                }
            }
            producer.flush();
            long t1 = System.nanoTime();

            try (PrintWriter w = new PrintWriter(new FileWriter(arquivo))) {
                for (int s : confirmadas) w.println(s);   // vazio com acks=0
            }
            System.out.println("T1=" + t1);
            System.out.println("MSGS=" + numMsgs);
            System.out.println("CONFIRMADAS=" + (temConfirmacao ? confirmadas.size() : -1));
            System.out.println("FALHAS=" + falhas.get());
            System.out.println("MAIOR_INTERVALO_SEM_CONFIRMACAO_MS=" + (temConfirmacao
                    ? String.format(java.util.Locale.US, "%.1f", maiorIntervalo.get() / 1e6) : "-1"));
            System.out.flush();
        }
    }
}
