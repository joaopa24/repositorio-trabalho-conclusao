package com.benchmarking.kafka.cenario2_2;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível intermediário — Cenário 2: Falha do consumidor — CONSUMIDOR (Kafka).
 *
 * Dois consumidores no MESMO grupo, tópico com 1 partição:
 *   #1 (ativo)   fica com a partição e cai de propósito no meio do trabalho;
 *   #2 (reserva) fica parado no grupo e assume a partição quando o #1 cai.
 *
 * Argumentos:
 *   0 - bootstrapServers
 *   1 - tópico
 *   2 - group.id (o mesmo para os dois; único por execução)
 *   3 - execução (0 = aquecimento)
 *   4 - id deste consumidor (1 = ativo, 2 = reserva)
 *   5 - garantia: at-most-once | at-least-once | exactly-once
 *   6 - cair depois de N mensagens processadas (-1 = nunca)
 *   7 - arquivo de "processamento" (compartilhado pelos dois consumidores)
 *   8 - max.poll.records
 *
 * "Processar" uma mensagem = gravar "id;sequência" no arquivo de
 * processamento (o efeito do consumidor, como gravar num banco). A gravação
 * vai direto para o sistema operacional, então sobrevive à queda do
 * processo. O orquestrador lê esse arquivo para contar duplicatas e perdas.
 *
 * Garantias (mesma configuração de commit do Cenário 1 intermediário):
 *   at-most-once  : commitSync do lote ANTES de processar. Se cair no meio do
 *                   lote, o resto do lote se perde.
 *   at-least-once : commitAsync do lote DEPOIS de processar (não espera o
 *                   broker, como o basicAck do RabbitMQ). Se cair no meio, a
 *                   parte já processada é processada de novo (duplicata). Se o
 *                   commit assíncrono do lote anterior ainda não tiver chegado
 *                   ao broker na hora da queda, esse lote também é relido.
 *   exactly-once  : at-least-once + consumidor idempotente: antes de
 *                   processar, consulta o arquivo de processamento e descarta
 *                   o que já foi feito. A consulta usa o arquivo, e não a
 *                   memória, porque a memória do #1 se perde na queda.
 *                   Diferente do Cenário 1, não há produtor transacional:
 *                   transações não protegem contra a queda de um consumidor
 *                   que grava fora do Kafka; aqui o efeito vem só da
 *                   deduplicação, como no RabbitMQ.
 *
 * A queda acontece no MEIO de um lote: depois de pelo menos N mensagens
 * processadas e da metade do lote atual. Runtime.halt(137) encerra o
 * processo na hora, sem commit, sem sair do grupo e sem fechar a conexão
 * de forma ordenada — como um processo morto pelo sistema.
 *
 * session.timeout.ms = 6 s (o mínimo que o broker aceita por padrão; o
 * padrão do cliente é 45 s) e heartbeat a cada 2 s. Como o #1 não avisa que
 * saiu, o broker só o remove do grupo depois desse tempo: é isso que o
 * tempo de recuperação do Kafka mede.
 *
 * Conversa com o orquestrador:
 *   stdout STATUS=PRONTO            #1: depois de receber a partição; #2: depois de entrar no grupo
 *   stdout QUEDA=<nanoTime>         #1, logo antes de cair
 *   stdout PRIMEIRA_RECEBIDA=<ns>   #2, na primeira mensagem que recebe
 *   stdin  QUEDA_CONFIRMADA         #2: o #1 já caiu; encerra após 10 s sem trabalho
 */
public class ConsumidorFalhavel {

    private static final String HEADER_TS  = "ts-envio";
    private static final String HEADER_SEQ = "sequencia";

    private static final long OCIOSO_APOS_QUEDA_MS = 10_000; // maior que o session.timeout
    private static final long MAX_TOTAL_MS = 300_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 9) {
            System.err.println("Uso: ConsumidorFalhavel <bootstrap> <topico> <groupId> <execucao> <id> <garantia> <falharApos> <arquivo> <maxPoll>");
            System.exit(2);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        String groupId          = args[2];
        int    execucao         = Integer.parseInt(args[3]);
        int    id               = Integer.parseInt(args[4]);
        String garantia         = args[5];
        int    falharApos       = Integer.parseInt(args[6]);
        File   arquivo          = new File(args[7]);
        int    maxPoll          = Integer.parseInt(args[8]);

        boolean commitAntes = garantia.equals("at-most-once");
        boolean idempotente = garantia.equals("exactly-once");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 groupId);
        // o RangeAssignor ordena os membros pelo id; "consumidor-1" vem antes e fica com a partição
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,                "consumidor-" + id);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         String.valueOf(maxPoll));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,        "0");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,          "1");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       "6000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,    "2000");
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        // sinal do orquestrador (só o reserva usa)
        AtomicLong quedaConfirmadaNs = new AtomicLong(0);
        Thread leitorStdin = new Thread(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
                String linha;
                while ((linha = in.readLine()) != null) {
                    if (linha.trim().equals("QUEDA_CONFIRMADA")) quedaConfirmadaNs.set(System.nanoTime());
                }
            } catch (IOException ignored) {
            }
        });
        leitorStdin.setDaemon(true);
        leitorStdin.start();

        Set<Integer> jaProcessadas = new HashSet<>();
        boolean arquivoCarregado = false;
        int processadas = 0;
        int descartadas = 0;
        int[] falhasCommit = {0};   // commits assíncronos que falharam (contados no callback)
        boolean recebeuAlguma = false;
        long ultimaAtividade = 0;
        long inicio = System.nanoTime();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
             FileOutputStream saida = new FileOutputStream(arquivo, true)) {

            consumer.subscribe(List.of(topico), new ConsumerRebalanceListener() {
                @Override public void onPartitionsRevoked(Collection<TopicPartition> p) { }
                @Override public void onPartitionsAssigned(Collection<TopicPartition> p) {
                    System.out.println("ATRIBUIDAS=" + p);
                }
            });

            if (id == 1) {
                while (consumer.assignment().isEmpty()) {
                    consumer.poll(Duration.ofMillis(100));
                    if ((System.nanoTime() - inicio) / 1_000_000 > 60_000) {
                        System.err.println("Partição não atribuída em 60 s");
                        System.exit(1);
                    }
                }
            } else {
                consumer.poll(Duration.ofMillis(100)); // entra no grupo
            }
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    if (!recebeuAlguma) {
                        recebeuAlguma = true;
                        if (id == 2) {
                            System.out.println("PRIMEIRA_RECEBIDA=" + System.nanoTime());
                            System.out.flush();
                        }
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, byte[]> r : records) {
                        offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                    }

                    // at-most-once: confirma ANTES de processar
                    if (commitAntes) consumer.commitSync(offsets);

                    int tamanhoLote = records.count();
                    int posicao = 0;
                    for (ConsumerRecord<String, byte[]> record : records) {
                        posicao++;
                        Header seqH = record.headers().lastHeader(HEADER_SEQ);
                        Header tsH  = record.headers().lastHeader(HEADER_TS);
                        if (seqH == null || tsH == null) {
                            System.err.println("Mensagem sem headers ts-envio/sequencia");
                            System.exit(1);
                        }
                        int seq = ByteBuffer.wrap(seqH.value()).getInt();

                        if (idempotente) {
                            if (!arquivoCarregado) {
                                carregar(arquivo, jaProcessadas);   // o que o outro consumidor já fez
                                arquivoCarregado = true;
                            }
                            if (jaProcessadas.contains(seq)) {
                                descartadas++;
                                continue;
                            }
                        }

                        // processamento = gravar o efeito
                        saida.write((id + ";" + seq + "\n").getBytes(StandardCharsets.US_ASCII));
                        jaProcessadas.add(seq);
                        processadas++;
                        ultimaAtividade = System.nanoTime();

                        // queda no meio do lote, depois de N processadas
                        if (falharApos > 0 && processadas >= falharApos && posicao >= Math.max(1, tamanhoLote / 2)) {
                            System.out.println("QUEDA=" + System.nanoTime());
                            System.out.println("PROCESSADAS=" + processadas);
                            System.out.flush();
                            Runtime.getRuntime().halt(137);
                        }
                    }

                    // at-least-once e exactly-once: confirma DEPOIS de processar, sem
                    // esperar o broker (como no Cenário 1). O callback roda nesta
                    // mesma thread, dentro de um poll() seguinte.
                    if (!commitAntes) {
                        consumer.commitAsync(offsets, (o, ex) -> {
                            if (ex != null && ++falhasCommit[0] <= 5) {
                                System.err.println("Falha no commit assíncrono: " + ex.getMessage());
                            }
                        });
                    }
                    ultimaAtividade = System.nanoTime();
                }

                long agora = System.nanoTime();
                long sinal = quedaConfirmadaNs.get();
                if (sinal > 0 && (agora - Math.max(sinal, ultimaAtividade)) / 1_000_000 > OCIOSO_APOS_QUEDA_MS) {
                    // fecha o último lote antes de sair (o #2 encerra de forma ordenada)
                    if (!commitAntes) {
                        try {
                            consumer.commitSync();
                        } catch (Exception e) {
                            System.err.println("Falha no commit final: " + e.getMessage());
                        }
                    }
                    break;
                }
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
        }

        System.out.println("CONSUMIDOR_ID=" + id);
        System.out.println("EXECUCAO="      + execucao);
        System.out.println("DESCARTADAS="   + descartadas);
        System.out.println("FALHAS_COMMIT=" + falhasCommit[0]);
        System.out.println("PROCESSADAS="   + processadas);
        System.out.println("STATUS=CONCLUIDO");
    }

    /** Lê "id;sequência" do arquivo de processamento. */
    static void carregar(File arquivo, Set<Integer> destino) throws IOException {
        if (!arquivo.exists()) return;
        for (String linha : Files.readAllLines(arquivo.toPath(), StandardCharsets.US_ASCII)) {
            int i = linha.indexOf(';');
            if (i > 0) destino.add(Integer.parseInt(linha.substring(i + 1).trim()));
        }
    }
}