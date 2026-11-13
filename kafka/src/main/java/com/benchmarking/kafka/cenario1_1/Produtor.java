package com.benchmarking.kafka.cenario1_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Produtor independente do cenário 1 (baseline).
 *
 * Argumentos:
 * 0 - bootstrapServers
 * 1 - tópico
 * 2 - número de mensagens
 * 3 - execução
 * 4 - (opcional) taxa alvo em mensagens/segundo. Se omitido, usa
 *     TAXA_ALVO_PADRAO_MSG_S.
 *
 * FIX (coordinated omission / ausência de pacing): o loop de envio original
 * disparava producer.send() o mais rápido possível, sem nenhum controle de
 * ritmo — o equivalente a "--throughput -1" do kafka-producer-perf-test.
 *
 * Diagnóstico realizado diretamente contra o broker (fora deste projeto,
 * via kafka-producer-perf-test dentro do container) confirmou o mecanismo:
 * com acks=1 e envio em velocidade máxima, a latência de ingestão observada
 * ficava entre 700 e 1.300 ms — mas ao repetir o MESMO teste com uma taxa
 * limitada a 50 mensagens/segundo, a latência caiu para 2-6 ms (10.000
 * mensagens, três execuções consecutivas). Nenhuma configuração de
 * ambiente (CPU, memória, disco, rede, energia) mudou entre os dois
 * testes — só a taxa de envio. Ou seja: os valores altos de latência que
 * este cenário vinha produzindo eram, com grande probabilidade, o mesmo
 * efeito de fila do lado do cliente (requests represados aguardando
 * confirmação, por causa de max.in.flight.requests.per.connection),
 * não custo real de processamento do broker.
 *
 * Como ESTE é o cenário "baseline" — cuja proposta é medir a latência
 * SEM formação de fila —, enviar em velocidade máxima contradiz o
 * propósito do cenário. Por isso o produtor agora espera entre um envio
 * e outro, mantendo uma taxa alvo abaixo da capacidade de absorção
 * sustentada do broker (confirmada em torno de 50 msg/s no teste
 * isolado; usa-se aqui um valor com folga, ver TAXA_ALVO_PADRAO_MSG_S).
 *
 * FIX (relógio): trocado System.currentTimeMillis() por System.nanoTime().
 * Produtor e consumidor rodam no mesmo host, e o HotSpot mapeia nanoTime()
 * para um relógio monotônico de escopo global ao SO (CLOCK_MONOTONIC no
 * Linux, QueryPerformanceCounter no Windows), não por processo — por isso
 * os valores são comparáveis entre os dois subprocessos.
 *
 * FIX (linger.ms): era 5. O tsEnvio é capturado antes do send(), então o
 * tempo de acumulação no batch entrava direto na latência medida. Como
 * este é o cenário cuja latência fim a fim É a métrica comparativa válida
 * (sem backlog para diluir o efeito), esse viés pesa proporcionalmente
 * mais aqui do que em qualquer outro cenário. linger.ms=0 remove o viés.
 *
 * FIX (idempotência x acks=1): desde o Kafka 3.0 (KIP-679), enable.idempotence
 * passou a ser "true" por padrão. Idempotência exige acks=all — definir
 * acks=1 sem tratar essa combinação explicitamente deixa a decisão por
 * conta de uma regra de resolução de conflito implícita do cliente
 * (KAFKA-13673): como idempotência não foi pedida explicitamente aqui,
 * o cliente a desliga silenciosamente para preservar acks=1, em vez de
 * lançar ConfigException. O comportamento efetivo não muda, mas deixamos
 * agora explícito (ENABLE_IDEMPOTENCE_CONFIG=false) para não depender
 * dessa regra implícita e para documentar a intenção: este é um baseline
 * de "melhor esforço", sem garantia de idempotência do produtor. Se o par
 * RabbitMQ deste cenário usa deliveryMode(2)+confirm (durabilidade mais
 * forte, sem deduplicação nativa), documentar essa assimetria no texto.
 *
 * NOTA (teste de isolamento de causa): a causa real do atraso de ~51-54 s
 * observado no Consumidor deste cenário não era DNS nem JMX — era o pipe
 * de stdout do processo Consumidor ficando cheio e bloqueando a escrita,
 * porque o orquestrador só drenava esse stream depois que o Produtor
 * terminava. A leitura assíncrona no orquestrador resolve isso; nenhuma
 * mudança é necessária aqui no Produtor. O desligamento de JMX abaixo é
 * mantido por ser inofensivo, não porque tenha relação com o bug.
 *
 * Captura de latência de ingestão (envio -> confirmação do broker), via
 * callback do send(). Só é decompomível em "ingestão" e "entrega" neste
 * cenário (baseline, sem backlog) — ver nota no ConsumidorLento e no
 * script de plotagem.
 *
 * FIX (aquecimento da conexão): antes, a primeira mensagem de cada execução
 * esperava o cliente buscar os metadados do cluster (líder da partição),
 * e esse tempo entrava na latência medida (600-1.500 ms na 1ª mensagem,
 * com as seguintes retidas atrás dela). No RabbitMQ a conexão já é aberta
 * antes de t0, então o Kafka pagava um custo que o RabbitMQ não pagava.
 * Agora partitionsFor() é chamado ANTES de t0: os metadados são buscados
 * e a conexão com o broker é aberta fora da medição.
 *
 * FIX (controle de taxa): LockSupport.parkNanos(espera) foi trocado por
 * aguardarAte(), o mesmo método usado no produtor do RabbitMQ. No Windows,
 * parkNanos pode dormir até o próximo "tick" do timer (~1 a 15,6 ms), o que
 * fazia o produtor enviar mensagens em rajadas. Para esperas curtas,
 * aguardarAte() faz busy-wait, imune à resolução do timer.
 *
 * FIX (ingestão): a execução de aquecimento (execucao = 0) não grava mais
 * no CSV de ingestão, que agora só contém as execuções válidas.
 *
 * NOVO (atraso do gerador): além do "ts-envio" (instante efetivo, gravado
 * imediatamente antes do send()), cada mensagem leva o header
 * "ts-planejado", com o instante em que ela DEVERIA ter saído
 * (t0 + i * intervalo). O consumidor calcula a diferença e grava a coluna
 * atraso_gerador_ms no CSV detalhado. Assim é possível verificar se o
 * produtor atrasou em algum momento (pausa de GC, escalonamento do SO) —
 * espera que fica FORA da latência medida, porque ocorre antes do registro
 * de ts-envio. Custo: 8 bytes a mais de header por mensagem, igual no
 * produtor do RabbitMQ.
 */
public class Produtor {

    private static final String HEADER_TS = "ts-envio";
    private static final String HEADER_TS_PLANEJADO = "ts-planejado";

    // pasta de saída — precisa bater com PASTA_RESULTADOS do orquestrador
    // Cenario1BaselineJava. Não é recebida via argumento porque o produtor
    // não precisa de mais nada do fluxo do orquestrador.
    private static final String PASTA_RESULTADOS = "resultado_cenario_1_1";

    // taxa alvo padrão, em mensagens/segundo. O teste isolado (fora deste
    // projeto) confirmou latência de ingestão saudável (2-6 ms) a 50 msg/s
    // contra o mesmo broker/ambiente. Usa-se aqui 200 msg/s — ainda bem
    // abaixo do ponto onde a fila se formava, com alguma margem para não
    // ficar artificialmente conservador, e finaliza as 10.000 mensagens em
    // 50 s por execução (contra os ~3min20s que 50 msg/s levaria).
    // Ajustável via 5º argumento se quiser recalibrar depois de validar o
    // ambiente novamente.
    private static final int TAXA_ALVO_PADRAO_MSG_S = 200;

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Uso: Produtor <bootstrapServers> <topico> <numMsgs> <execucao> [taxaAlvoMsgS]");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        int    taxaAlvoMsgS     = args.length >= 5 ? Integer.parseInt(args[4]) : TAXA_ALVO_PADRAO_MSG_S;

        // defensivo: garante que a pasta existe mesmo se este processo for
        // iniciado antes do mkdirs() do orquestrador terminar de propagar
        new File(PASTA_RESULTADOS).mkdirs();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // ACK=1: o líder da partição confirma a escrita. Mantido como no
        // original — é uma escolha legítima de baseline "melhor esforço".
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // FIX (idempotência x acks=1): torna explícito o que antes dependia
        // de resolução de conflito implícita do cliente (ver javadoc acima).
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

        // FIX: era 5.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        // Mantido por ser inofensivo (evita overhead de MBean JMX a cada
        // processo novo) — não tem relação com o bug do pipe de stdout.
        props.put("auto.include.jmx.reporter", "false");
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        byte[] payload = new byte[1024];

        // ingestão: {sequencia, latenciaIngestaoNs}
        List<long[]> ingestao = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger falhasEnvio = new AtomicInteger(0);

        System.out.printf("Produtor iniciado — taxa alvo: %d msg/s (pacing habilitado)%n", taxaAlvoMsgS);

        // DIAGNÓSTICO: mede a construção do KafkaProducer, espelhando a
        // instrumentação do Consumidor.
        long antesConstrutorNs = System.nanoTime();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {

            double construtorMs = (System.nanoTime() - antesConstrutorNs) / 1_000_000.0;
            System.out.printf(Locale.US, "[DIAG] new KafkaProducer() levou %.1f ms%n", construtorMs);

            // FIX (aquecimento da conexão): busca os metadados do tópico e abre
            // a conexão com o broker ANTES de começar a medir.
            long antesMetadadosNs = System.nanoTime();
            producer.partitionsFor(topico);
            System.out.printf(Locale.US, "[DIAG] partitionsFor() (metadados + conexão) levou %.1f ms%n",
                    (System.nanoTime() - antesMetadadosNs) / 1_000_000.0);

            // t0 representa o início da produção. Com nanoTime(), este
            // valor pode ser negativo — a origem é arbitrária, só a
            // diferença (t1 - t0) tem significado. Não validar t0 > 0.
            long t0 = System.nanoTime();

            // FIX (pacing): calcula o instante em que CADA mensagem deveria
            // ser enviada, a partir de t0 e da taxa alvo, e espera até lá
            // antes de mandar a próxima. Isso é o que faz o produtor não
            // mais se comportar como "--throughput -1".
            long intervaloNs = 1_000_000_000L / taxaAlvoMsgS;

            for (int i = 0; i < numMsgs; i++) {
                long tsAlvo = t0 + i * intervaloNs;
                aguardarAte(tsAlvo);

                final long ts  = System.nanoTime();
                final int  seq = i + 1;

                ProducerRecord<String, byte[]> record = new ProducerRecord<>(topico, payload);
                byte[] timestampBytes = ByteBuffer.allocate(Long.BYTES).putLong(ts).array();
                record.headers().add(HEADER_TS, timestampBytes);

                // NOVO (atraso do gerador): instante planejado desta mensagem.
                byte[] planejadoBytes = ByteBuffer.allocate(Long.BYTES).putLong(tsAlvo).array();
                record.headers().add(HEADER_TS_PLANEJADO, planejadoBytes);

                // send() é assíncrono — o callback captura a latência de
                // ingestão sem bloquear o loop. O pacing acima já garante
                // que o produtor não sobrecarrega o broker; o callback
                // assíncrono evita adicionar uma segunda fonte de espera
                // (bloquear esperando o ack de CADA mensagem seria um
                // pacing redundante e mais lento que o necessário).
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        falhasEnvio.incrementAndGet();
                        System.err.println("Falha ao confirmar mensagem " + seq + ": " + ex.getMessage());
                        return;
                    }
                    long confirmadoEm = System.nanoTime();
                    ingestao.add(new long[]{seq, confirmadoEm - ts});
                });

                if ((i + 1) % 2000 == 0) {
                    System.out.printf("  Enviadas: %d/%d%n", i + 1, numMsgs);
                }
            }

            // Espera todos os sends pendentes serem confirmados — quando
            // retorna, todos os callbacks acima já dispararam.
            producer.flush();

            long t1 = System.nanoTime();

            if (falhasEnvio.get() > 0) {
                System.err.printf("Total de falhas de envio: %d/%d%n", falhasEnvio.get(), numMsgs);
            }

            gravarIngestao(ingestao, execucao);

            // RESULTADOS PARA O ORQUESTRADOR
            System.out.println("T0="       + t0);
            System.out.println("T1="       + t1);
            System.out.println("MSGS="     + numMsgs);
            System.out.println("EXECUCAO=" + execucao);
        }
    }

    /**
     * Espera até o instante alvo (nanoTime). Esperas longas (> 20 ms) dormem
     * a maior parte do tempo; o trecho final, e as esperas curtas como os
     * 5 ms deste cenário, são feitos em busy-wait — imune à resolução do
     * timer do Windows. Mesmo método usado no produtor do RabbitMQ.
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

    /** Grava (acumulando entre execuções) o CSV detalhado de latência de ingestão. */
    private static void gravarIngestao(List<long[]> ingestao, int execucao) throws IOException {
        // a execução de aquecimento (0) é descartada — não entra no CSV
        if (execucao == 0) return;

        String arquivo = PASTA_RESULTADOS + File.separator +
                "resultados-kafka-cenario1-ingestao-detalhado.csv";
        boolean novo = !new File(arquivo).exists();
        try (PrintWriter w = new PrintWriter(new FileWriter(arquivo, true))) {
            if (novo) w.println("execucao,sequencia,latencia_ingestao_ms");
            for (long[] r : ingestao) {
                double latMs = r[1] / 1_000_000.0;
                w.printf(Locale.US, "%d,%d,%.4f%n", execucao, r[0], latMs);
            }
        }
    }
}