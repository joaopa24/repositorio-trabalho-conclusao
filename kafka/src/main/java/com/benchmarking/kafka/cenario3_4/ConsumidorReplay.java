package com.benchmarking.kafka.cenario3_4;

import com.benchmarking.kafka.cenario3_comum.Util;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Nível avançado — Cenário 4: Replay — CONSUMIDOR (Kafka).
 *
 * Argumentos: bootstrapServers, tópico, modo, parâmetro, número esperado.
 *
 * Modos (como o consumidor decide de onde começar a ler):
 *   normal          — grupo G (parâmetro = nome do grupo), earliest; lê tudo e
 *                     FAZ COMMIT. É o consumo "de produção" que vem antes do replay.
 *   grupo-existente — volta com o MESMO grupo G: deve receber 0 mensagens,
 *                     porque o offset ficou guardado no broker (controle).
 *                     Par do "posicao-salva" do RabbitMQ, em que a posição é
 *                     guardada pela aplicação.
 *   inicio          — assign + seekToBeginning: relê a partição inteira.
 *   timestamp       — assign + offsetsForTimes(parâmetro em ms) + seek:
 *                     relê a partir da primeira mensagem com timestamp ≥ parâmetro.
 *                     O tópico usa LogAppendTime: o timestamp é a hora em que o
 *                     broker gravou a mensagem, como no stream do RabbitMQ.
 *   offset          — assign + seek(parâmetro): relê a partir de um offset exato.
 *   novo-grupo      — grupo novo (parâmetro = nome), earliest: relê tudo,
 *                     como um sistema novo que entra depois. Sem par no RabbitMQ.
 *
 * Medição: o consumidor é criado e se conecta ANTES de medir; imprime
 * STATUS=PRONTO e espera INICIAR na entrada padrão. O tempo conta a partir do
 * INICIAR: tempo até a 1ª mensagem (inclui achar a posição / entrar no grupo)
 * e duração até a última. Confere também se as sequências vieram em ordem e
 * sem buracos (uma partição só).
 *
 * Saída: RECEBIDAS, PRIMEIRA_SEQ, ULTIMA_SEQ, EM_ORDEM, T_PRIMEIRA_MS, DURACAO_MS.
 *
 * Proteção contra órfãos: a entrada padrão é lida por Util.vigiarEntrada, que
 * libera o INICIAR e encerra o processo se o orquestrador morrer. A criação
 * automática de tópicos fica desligada, para que um consumidor perdido não
 * recrie o tópico enquanto o orquestrador o apaga.
 */
public class ConsumidorReplay {

    private static final long OCIOSO_MS = 5_000;
    private static final long MAX_TOTAL_MS = 300_000;
    private static final long SEM_VALOR = Long.MIN_VALUE;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: ConsumidorReplay <bootstrap> <topico> <modo> <parametro> <numEsperado>");
            System.exit(2);
        }
        String bootstrap = args[0];
        String topico    = args[1];
        String modo      = args[2];
        String parametro = args[3];
        int    esperado  = Integer.parseInt(args[4]);
        boolean comGrupo = modo.equals("normal") || modo.equals("grupo-existente") || modo.equals("novo-grupo");

        CountDownLatch iniciar = new CountDownLatch(1);
        Util.vigiarEntrada(l -> { if (l.equals("INICIAR")) iniciar.countDown(); });

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        if (comGrupo) props.put(ConsumerConfig.GROUP_ID_CONFIG, parametro);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        Util.semMetricas(props);

        TopicPartition tp = new TopicPartition(topico, 0);
        int recebidas = 0, primeira = -1, ultima = -1;
        boolean emOrdem = true;
        long tPrimeira = SEM_VALOR, tUltima = SEM_VALOR;

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.partitionsFor(topico);    // conexão e metadados fora da medição
            System.out.println("STATUS=PRONTO");
            System.out.flush();
            iniciar.await();   // se a entrada fechar antes, vigiarEntrada encerra o processo

            long inicio = System.nanoTime();
            switch (modo) {
                case "normal", "grupo-existente", "novo-grupo" -> consumer.subscribe(List.of(topico));
                case "inicio" -> {
                    consumer.assign(List.of(tp));
                    consumer.seekToBeginning(List.of(tp));
                }
                case "timestamp" -> {
                    consumer.assign(List.of(tp));
                    OffsetAndTimestamp o = consumer.offsetsForTimes(Map.of(tp, Long.parseLong(parametro))).get(tp);
                    if (o == null) throw new IllegalStateException("Nenhuma mensagem com timestamp >= " + parametro);
                    consumer.seek(tp, o.offset());
                }
                case "offset" -> {
                    consumer.assign(List.of(tp));
                    consumer.seek(tp, Long.parseLong(parametro));
                }
                default -> throw new IllegalArgumentException("Modo inválido: " + modo);
            }

            long ultimaAtividade = inicio;
            boolean atribuido = !comGrupo;
            while (recebidas < esperado || esperado == 0) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                long agora = System.nanoTime();
                if (!atribuido && !consumer.assignment().isEmpty()) {
                    atribuido = true;
                    ultimaAtividade = agora;
                }
                for (ConsumerRecord<String, byte[]> r : records) {
                    Header h = r.headers().lastHeader("sequencia");
                    if (h == null) continue;
                    int seq = ByteBuffer.wrap(h.value()).getInt();
                    if (primeira < 0) {
                        primeira = seq;
                        tPrimeira = agora;
                    } else if (seq != ultima + 1) {
                        emOrdem = false;
                    }
                    ultima = seq;
                    recebidas++;
                    tUltima = agora;
                    ultimaAtividade = agora;
                }
                // controle "grupo-existente" (esperado 0): termina após 5 s parado já com a partição atribuída
                if (esperado == 0 && atribuido && (agora - ultimaAtividade) / 1_000_000 > OCIOSO_MS) break;
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.printf("Tempo máximo atingido: %d/%d%n", recebidas, esperado);
                    break;
                }
            }
            // confere se não sobrou nada além do esperado
            if (esperado > 0) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(500))) recebidas++;
            }
            if (modo.equals("normal")) consumer.commitSync();

            System.out.println("RECEBIDAS=" + recebidas);
            System.out.println("PRIMEIRA_SEQ=" + primeira);
            System.out.println("ULTIMA_SEQ=" + ultima);
            System.out.println("EM_ORDEM=" + emOrdem);
            System.out.println("T_PRIMEIRA_MS=" + String.format(Locale.US, "%.2f", tPrimeira != SEM_VALOR ? (tPrimeira - inicio) / 1e6 : -1.0));
            System.out.println("DURACAO_MS=" + String.format(Locale.US, "%.2f", tUltima != SEM_VALOR ? (tUltima - inicio) / 1e6 : -1.0));
        }
    }
}