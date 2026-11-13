package com.benchmarking.rabbitmq.cenario2_2;

import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Nível intermediário — Cenário 2: Falha do consumidor — CONSUMIDOR (RabbitMQ).
 *
 * Dois consumidores na MESMA fila, declarada com "single active consumer":
 *   #1 (ativo)   recebe as mensagens e cai de propósito no meio do trabalho;
 *   #2 (reserva) fica registrado sem receber nada e passa a receber quando
 *                o #1 cai. É o equivalente ao consumidor reserva do Kafka.
 *
 * Argumentos:
 *   0 - host
 *   1 - fila
 *   2 - execução (0 = aquecimento)
 *   3 - id deste consumidor (1 = ativo, 2 = reserva)
 *   4 - garantia: at-most-once | at-least-once | exactly-once
 *   5 - cair depois de N mensagens processadas (-1 = nunca)
 *   6 - arquivo de "processamento" (compartilhado pelos dois consumidores)
 *   7 - prefetch (basicQos; só vale com ack manual)
 *
 * "Processar" = gravar "id;sequência" no arquivo de processamento (mesmo
 * método do consumidor Kafka).
 *
 * Garantias (mesmas do Cenário 1 intermediário):
 *   at-most-once  : autoAck — o broker considera entregue ao enviar. Tudo o
 *                   que já foi empurrado para o cliente e não foi processado
 *                   se perde na queda (sem ack manual, o prefetch não limita).
 *   at-least-once : ack manual DEPOIS de processar. Na queda, as mensagens
 *                   sem ack voltam para a fila; a que foi processada mas não
 *                   confirmada é processada de novo (duplicata).
 *   exactly-once  : at-least-once + consumidor idempotente (consulta o
 *                   arquivo de processamento e descarta o que já foi feito).
 *
 * Queda: Runtime.halt(137) logo depois de processar a N-ésima mensagem,
 * antes do ack. Na mesma máquina, o sistema operacional fecha a conexão TCP
 * e o broker percebe a queda na hora. Numa queda de rede, ele só perceberia
 * pelo heartbeat (padrão de 60 s).
 *
 * Conversa com o orquestrador: igual ao consumidor Kafka (STATUS=PRONTO,
 * QUEDA=, PRIMEIRA_RECEBIDA=, QUEDA_CONFIRMADA).
 */
public class ConsumidorFalhavel {

    private static final String HEADER_SEQ = "sequencia";
    private static final long OCIOSO_APOS_QUEDA_MS = 10_000;  // igual ao Kafka
    private static final long MAX_TOTAL_MS = 300_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 8) {
            System.err.println("Uso: ConsumidorFalhavel <host> <fila> <execucao> <id> <garantia> <falharApos> <arquivo> <prefetch>");
            System.exit(2);
        }

        String host       = args[0];
        String fila       = args[1];
        int    execucao   = Integer.parseInt(args[2]);
        int    id         = Integer.parseInt(args[3]);
        String garantia   = args[4];
        int    falharApos = Integer.parseInt(args[5]);
        File   arquivo    = new File(args[6]);
        int    prefetch   = Integer.parseInt(args[7]);

        boolean autoAck     = garantia.equals("at-most-once");
        boolean idempotente = garantia.equals("exactly-once");

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

        // os callbacks de um canal rodam um de cada vez
        Set<Integer> jaProcessadas = new HashSet<>();
        boolean[] arquivoCarregado = {false};
        int[] processadas = {0};
        int[] descartadas = {0};
        boolean[] recebeuAlguma = {false};
        AtomicLong ultimaAtividade = new AtomicLong(0);
        long inicio = System.nanoTime();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel();
             FileOutputStream saida = new FileOutputStream(arquivo, true)) {

            // a fila (com single active consumer) é criada pelo orquestrador
            channel.queueDeclarePassive(fila);
            if (!autoAck) channel.basicQos(prefetch);

            DeliverCallback onEntrega = (tag, delivery) -> {
                if (!recebeuAlguma[0]) {
                    recebeuAlguma[0] = true;
                    if (id == 2) {
                        System.out.println("PRIMEIRA_RECEBIDA=" + System.nanoTime());
                        System.out.flush();
                    }
                }
                Map<String, Object> h = delivery.getProperties().getHeaders();
                Object seqObj = h == null ? null : h.get(HEADER_SEQ);
                if (seqObj == null) {
                    System.err.println("Mensagem sem header sequencia");
                    Runtime.getRuntime().halt(1);
                }
                int seq = ((Number) seqObj).intValue();
                long tag2 = delivery.getEnvelope().getDeliveryTag();

                if (idempotente) {
                    if (!arquivoCarregado[0]) {
                        carregar(arquivo, jaProcessadas);
                        arquivoCarregado[0] = true;
                    }
                    if (jaProcessadas.contains(seq)) {
                        descartadas[0]++;
                        channel.basicAck(tag2, false);
                        ultimaAtividade.set(System.nanoTime());
                        return;
                    }
                }

                // processamento = gravar o efeito
                saida.write((id + ";" + seq + "\n").getBytes(StandardCharsets.US_ASCII));
                jaProcessadas.add(seq);
                processadas[0]++;
                ultimaAtividade.set(System.nanoTime());

                // queda depois de processar, ANTES do ack
                if (falharApos > 0 && processadas[0] >= falharApos) {
                    System.out.println("QUEDA=" + System.nanoTime());
                    System.out.println("PROCESSADAS=" + processadas[0]);
                    System.out.flush();
                    Runtime.getRuntime().halt(137);
                }

                if (!autoAck) channel.basicAck(tag2, false);
            };

            String consumerTag = channel.basicConsume(fila, autoAck, onEntrega, t -> { });
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            while (true) {
                Thread.sleep(100);
                long agora = System.nanoTime();
                long sinal = quedaConfirmadaNs.get();
                if (sinal > 0 && (agora - Math.max(sinal, ultimaAtividade.get())) / 1_000_000 > OCIOSO_APOS_QUEDA_MS) {
                    break;
                }
                if ((agora - inicio) / 1_000_000 > MAX_TOTAL_MS) {
                    System.err.println("Tempo máximo atingido");
                    System.exit(1);
                }
            }
            try {
                channel.basicCancel(consumerTag);
            } catch (Exception ignored) {
            }
        }

        System.out.println("CONSUMIDOR_ID=" + id);
        System.out.println("EXECUCAO="      + execucao);
        System.out.println("DESCARTADAS="   + descartadas[0]);
        System.out.println("PROCESSADAS="   + processadas[0]);
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
