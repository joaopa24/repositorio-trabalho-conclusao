package com.benchmarking.kafka.cenario1_1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Consumidor independente do cenário 1 (baseline).
 *
 * TESTE DE ISOLAMENTO DE CAUSA: esta versão volta a usar subscribe() em vez
 * de assign()/seekToBeginning(), removendo também CLIENT_DNS_LOOKUP_CONFIG e
 * o CLIENT_ID_CONFIG estático que tinham sido adicionados junto com a
 * hipótese de DNS lento. O objetivo é isolar uma única variável: a leitura
 * assíncrona do stdout no orquestrador (thread dedicada + CountDownLatch)
 * é, sozinha, suficiente para eliminar o atraso de ~51-54 s que aparecia
 * antes? Se a latência por mensagem continuar baixa com subscribe() puro,
 * fica confirmado que a causa raiz sempre foi o pipe de stdout bloqueado
 * (buffer cheio, ninguém lendo enquanto o produtor rodava) — não DNS, não
 * JMX, não o protocolo de consumer group.
 *
 * Mantido: desligamento de JMX (não tem relação com o bug, mas é inofensivo
 * e evita overhead de registro de MBean a cada processo novo), e o sinal
 * STATUS=PRONTO — agora emitido só depois que a atribuição de partição via
 * protocolo de grupo (JoinGroup/SyncGroup) realmente terminar, checando
 * consumer.assignment() em loop curto. Isso é necessário porque, com
 * subscribe(), a atribuição não é imediata como era com assign() manual.
 *
 * FIX (relógio): trocado System.currentTimeMillis() por System.nanoTime(),
 * espelhando a correção do Produtor — os dois precisam usar o mesmo tipo
 * de relógio para a subtração fazer sentido.
 *
 * A latência interna (variável "latencias") é armazenada em NANOSSEGUNDOS,
 * convertida para ms com casas decimais só na hora de escrever/imprimir.
 *
 * FIX (timeout no loop principal): o loop original (`while (latencias.size()
 * < numMsgs) { ... }`) não tinha nenhuma condição de parada além de "recebi
 * todas as mensagens esperadas". Se, por qualquer motivo, o produtor morrer
 * antes de enviar numMsgs mensagens (crash, exceção não tratada, broker
 * fora do ar no meio da execução, etc.), este processo ficaria bloqueado
 * aqui para sempre — o processo pai (orquestrador) está esperando a saída
 * deste processo via waitFor() (ver nota no Produtor sobre o pipe de
 * stdout), então o benchmark inteiro travaria sem nenhuma mensagem de erro
 * explicando o motivo. Um timeout de folga transforma esse travamento
 * silencioso em uma falha visível e diagnosticável: o processo termina,
 * imprime quantas mensagens recebeu de fato, e a checagem de
 * "latencias.size() != numMsgs" logo abaixo do loop já existente lança a
 * IllegalStateException explicando o que faltou.
 *
 * NOVO (atraso do gerador): lê também o header "ts-planejado", enviado
 * pelo Produtor, e grava no CSV detalhado a coluna atraso_gerador_ms
 * (ts-envio - ts-planejado). A latência continua sendo chegada - ts-envio,
 * exatamente como antes; a latência contada a partir do instante planejado
 * pode ser obtida depois, no Python, como latencia_ms + atraso_gerador_ms.
 * O resumo para o orquestrador ganha ATRASO_MEDIANO e ATRASO_MAX (em ms),
 * só informativos — o orquestrador não precisa lê-los.
 */
public class Consumidor {

    private static final String HEADER_TS = "ts-envio";
    private static final String HEADER_TS_PLANEJADO = "ts-planejado";

    // Folga generosa sobre o tempo esperado de uma execução completa a
    // 200 msg/s (~50 s para 10.000 mensagens): tempo suficiente para não
    // disparar em uma execução normal, mas finito o bastante para nunca
    // deixar o benchmark travado indefinidamente caso o produtor morra.
    private static final long TIMEOUT_LOOP_NS = 120_000_000_000L; // 120 s

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Uso: Consumidor <bootstrapServers> <topico> <numMsgs> <execucao> <arquivoCSV>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topico           = args[1];
        int    numMsgs          = Integer.parseInt(args[2]);
        int    execucao         = Integer.parseInt(args[3]);
        String arquivoCSV       = args[4];

        // DIAGNÓSTICO: marca o início do processo, antes de qualquer
        // configuração ou conexão.
        long inicioProcessoNs = System.nanoTime();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Grupo diferente em cada execução — evita reutilização de offsets antigos.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-" + execucao + "-" + System.nanoTime());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // Baseline: minimiza espera artificial do consumer.
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0");
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");

        // Mantido: não tem relação com o bug do pipe, mas evita overhead
        // de registro de MBean JMX a cada processo novo (são ~20 por
        // benchmark completo).
        props.put("auto.include.jmx.reporter", "false");
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "");

        // Latências em NANOSSEGUNDOS, na ordem de chegada (não ordenar esta
        // lista — ela alimenta o CSV detalhado, e a ordem é o que permite
        // plotar latência x sequência depois).
        List<Long> latencias = new ArrayList<>(numMsgs);

        // NOVO: atraso do gerador (ts-envio - ts-planejado), em NANOSSEGUNDOS,
        // na mesma ordem de "latencias" (índice i corresponde à mesma mensagem).
        List<Long> atrasosGerador = new ArrayList<>(numMsgs);

        // DIAGNÓSTICO: flags/contadores para caracterizar o padrão de chegada.
        boolean primeiraMensagemRecebida = false;
        int numeroDoPoll = 0;
        int maiorLoteNumUmPoll = 0;

        long antesConstrutorNs = System.nanoTime();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {

            double construtorMs = (System.nanoTime() - antesConstrutorNs) / 1_000_000.0;
            System.out.printf(Locale.US, "[DIAG] new KafkaConsumer() levou %.1f ms%n", construtorMs);

            long antesSubscribeNs = System.nanoTime();
            consumer.subscribe(List.of(topico));
            double subscribeMs = (System.nanoTime() - antesSubscribeNs) / 1_000_000.0;
            System.out.printf(Locale.US, "[DIAG] subscribe() levou %.1f ms%n", subscribeMs);

            // Com subscribe(), a atribuição de partição só sai depois do
            // protocolo de grupo (JoinGroup/SyncGroup) — não é imediata
            // como era com assign() manual. Faz poll() curto em loop até
            // a atribuição aparecer, ANTES de sinalizar STATUS=PRONTO, para
            // garantir que o produtor só comece depois que o consumidor
            // já esteja de fato apto a receber mensagens.
            long antesAssignmentNs = System.nanoTime();
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }
            double assignmentMs = (System.nanoTime() - antesAssignmentNs) / 1_000_000.0;
            System.out.printf(Locale.US,
                    "[DIAG] Atribuição de partição via protocolo de grupo levou %.1f ms%n", assignmentMs);

            // Sinalização lida pelo orquestrador para autorizar a largada do produtor.
            System.out.println("STATUS=PRONTO");
            System.out.flush();

            // FIX (timeout): marca o início do loop principal de recebimento,
            // para poder detectar se ele está demorando além do razoável.
            long inicioLoopNs = System.nanoTime();

            while (latencias.size() < numMsgs) {
                // FIX (timeout): se passou da folga sem completar, interrompe
                // o loop em vez de esperar para sempre. A checagem de
                // "latencias.size() != numMsgs" logo após o try-with-resources
                // já existia e vai capturar essa condição, lançando uma
                // exceção explicando quantas mensagens faltaram.
                if (System.nanoTime() - inicioLoopNs > TIMEOUT_LOOP_NS) {
                    System.err.printf(Locale.US,
                            "TIMEOUT aguardando mensagens: %d/%d recebidas após %.1f ms. "
                                    + "Encerrando o loop de recebimento.%n",
                            latencias.size(), numMsgs,
                            (System.nanoTime() - inicioLoopNs) / 1_000_000.0);
                    break;
                }

                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                numeroDoPoll++;

                int tamanhoLote = records.count();
                if (tamanhoLote > maiorLoteNumUmPoll) {
                    maiorLoteNumUmPoll = tamanhoLote;
                }

                // DIAGNÓSTICO: primeira vez que um poll() retorna algo.
                if (!primeiraMensagemRecebida && tamanhoLote > 0) {
                    double esperaTotalMs = (System.nanoTime() - inicioProcessoNs) / 1_000_000.0;
                    System.out.printf(Locale.US,
                            "[DIAG] Primeiro poll() não-vazio: poll #%d, %.1f ms após o início do processo, "
                                    + "trouxe %d mensagens de uma vez%n",
                            numeroDoPoll, esperaTotalMs, tamanhoLote);
                    primeiraMensagemRecebida = true;
                }

                if (tamanhoLote > 500) {
                    System.out.printf(Locale.US,
                            "[DIAG] Poll #%d trouxe um lote grande: %d mensagens de uma vez%n",
                            numeroDoPoll, tamanhoLote);
                }

                records.forEach(record -> {
                    if (latencias.size() >= numMsgs) {
                        return; // segurança: poll trouxe mais do que faltava
                    }

                    long chegada = System.nanoTime();

                    Header header = record.headers().lastHeader(HEADER_TS);
                    if (header == null) {
                        System.err.println("Mensagem sem header " + HEADER_TS);
                        return;
                    }

                    byte[] valorHeader = header.value();
                    if (valorHeader == null || valorHeader.length != Long.BYTES) {
                        System.err.println("Header de timestamp inválido.");
                        return;
                    }

                    Header headerPlanejado = record.headers().lastHeader(HEADER_TS_PLANEJADO);
                    byte[] valorPlanejado = headerPlanejado == null ? null : headerPlanejado.value();
                    if (valorPlanejado == null || valorPlanejado.length != Long.BYTES) {
                        System.err.println("Mensagem sem header " + HEADER_TS_PLANEJADO + " válido.");
                        return;
                    }

                    long tsEnvio = ByteBuffer.wrap(valorHeader).getLong();
                    long tsPlanejado = ByteBuffer.wrap(valorPlanejado).getLong();
                    long latenciaNs = chegada - tsEnvio;

                    if (latenciaNs < 0) {
                        System.err.println("Latência negativa detectada: " + latenciaNs + " ns");
                    }

                    latencias.add(latenciaNs);
                    atrasosGerador.add(tsEnvio - tsPlanejado);
                });
            }

            // DIAGNÓSTICO: resumo final do padrão de chegada.
            System.out.printf(Locale.US,
                    "[DIAG] Total de polls até completar: %d | Maior lote em um único poll: %d mensagens%n",
                    numeroDoPoll, maiorLoteNumUmPoll);
        }

        if (latencias.size() != numMsgs) {
            throw new IllegalStateException(
                    "Número incorreto de mensagens. Esperado=" + numMsgs +
                            ", recebido=" + latencias.size());
        }

        // CSV DETALHADO — execucao,mensagem,latencia_ms,atraso_gerador_ms.
        File arquivo = new File(arquivoCSV);
        boolean novoArquivo = !arquivo.exists() || arquivo.length() == 0;

        try (PrintWriter writer = new PrintWriter(new FileWriter(arquivo, true))) {
            if (novoArquivo) {
                writer.println("execucao,mensagem,latencia_ms,atraso_gerador_ms");
            }
            // Escrevemos "latencias" (ordem de chegada), não uma cópia ordenada.
            for (int i = 0; i < latencias.size(); i++) {
                double latMs = latencias.get(i) / 1_000_000.0;
                double atrasoMs = atrasosGerador.get(i) / 1_000_000.0;
                writer.printf(Locale.US, "%d,%d,%.4f,%.4f%n", execucao, i + 1, latMs, atrasoMs);
            }
        }

        // Cópia ordenada SOMENTE para as estatísticas — não altera o CSV detalhado.
        List<Long> sorted = new ArrayList<>(latencias);
        Collections.sort(sorted);

        double mediaNs = sorted.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long minNs = sorted.get(0);
        long maxNs = sorted.get(sorted.size() - 1);
        long p95Ns = calcularPercentil(sorted, 0.95);
        long p99Ns = calcularPercentil(sorted, 0.99);

        // SAÍDA PARA O ORQUESTRADOR — em milissegundos, para bater com o
        // cabeçalho do CSV agregado ("latencia_media_ms" etc.)
        System.out.println("MEDIA=" + String.format(Locale.US, "%.4f", mediaNs / 1_000_000.0));
        System.out.println("MIN="   + String.format(Locale.US, "%.4f", minNs / 1_000_000.0));
        System.out.println("MAX="   + String.format(Locale.US, "%.4f", maxNs / 1_000_000.0));
        System.out.println("P95="   + String.format(Locale.US, "%.4f", p95Ns / 1_000_000.0));
        System.out.println("P99="   + String.format(Locale.US, "%.4f", p99Ns / 1_000_000.0));
        // NOVO: resumo do atraso do gerador (informativo; o orquestrador
        // ignora chaves que não usa).
        List<Long> atrasosOrdenados = new ArrayList<>(atrasosGerador);
        Collections.sort(atrasosOrdenados);
        System.out.println("ATRASO_MEDIANO=" + String.format(Locale.US, "%.4f",
                calcularPercentil(atrasosOrdenados, 0.50) / 1_000_000.0));
        System.out.println("ATRASO_MAX=" + String.format(Locale.US, "%.4f",
                atrasosOrdenados.get(atrasosOrdenados.size() - 1) / 1_000_000.0));

        System.out.println("RECEBIDAS=" + latencias.size());
    }

    /** Calcula percentil usando nearest-rank (posição = ceil(p * n), 1-based). */
    private static long calcularPercentil(List<Long> sorted, double percentil) {
        if (sorted.isEmpty()) {
            throw new IllegalArgumentException("Lista vazia.");
        }
        int n = sorted.size();
        int posicao = (int) Math.ceil(percentil * n);
        int indice = posicao - 1;
        indice = Math.max(0, Math.min(indice, n - 1));
        return sorted.get(indice);
    }
}