package com.benchmarking.kafka.util;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class ExecutorComando {

    public static String executar(List<String> comando) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(comando);
        pb.redirectErrorStream(true);
        Process processo = pb.start();

        StringBuilder saida = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(processo.getInputStream()))) {
            String linha;
            while ((linha = reader.readLine()) != null) {
                saida.append(linha).append("\n");
                System.out.println(linha);
            }
        }

        processo.waitFor();
        return saida.toString();
    }

    public static ResultadoExecucao parsearKafka(String saida) {
        double vazao = 0;
        double latenciaMedia = 0;
        double latenciaP99 = 0;

        String[] linhas = saida.split("\n");
        for (String linha : linhas) {
            if (linha.contains("records sent") && linha.contains("records/sec")) {
                Pattern pVazao = Pattern.compile("([0-9.]+) records/sec");
                Matcher mVazao = pVazao.matcher(linha);
                if (mVazao.find()) vazao = Double.parseDouble(mVazao.group(1));

                Pattern pMedia = Pattern.compile("([0-9.]+) ms avg latency");
                Matcher mMedia = pMedia.matcher(linha);
                if (mMedia.find()) latenciaMedia = Double.parseDouble(mMedia.group(1));

                Pattern pP99 = Pattern.compile("([0-9]+) ms 99th");
                Matcher mP99 = pP99.matcher(linha);
                if (mP99.find()) latenciaP99 = Double.parseDouble(mP99.group(1));
            }
        }

        return new ResultadoExecucao(vazao, latenciaMedia, latenciaP99);
    }

    public static void exportarCsv(String arquivo, String tituloCenario, List<ResultadoExecucao> resultados) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(arquivo, true))) {
            if (resultados.isEmpty()) return;

            // Título do cenário em linha separada
            writer.println(tituloCenario);
            writer.println("execucao,vazao_msg_s,latencia_media_ms,latencia_p99_ms");

            for (int i = 0; i < resultados.size(); i++) {
                ResultadoExecucao r = resultados.get(i);
                writer.printf(Locale.US, "%d,%.2f,%.2f,%.2f%n",
                        i + 1, r.vazao(), r.latenciaMedia(), r.latenciaP99());
            }

            double mediaVazao = resultados.stream().mapToDouble(ResultadoExecucao::vazao).average().orElse(0);
            double mediaLatencia = resultados.stream().mapToDouble(ResultadoExecucao::latenciaMedia).average().orElse(0);
            double mediaP99 = resultados.stream().mapToDouble(ResultadoExecucao::latenciaP99).average().orElse(0);

            writer.printf(Locale.US, "MEDIA,%.2f,%.2f,%.2f%n", mediaVazao, mediaLatencia, mediaP99);
            writer.println();

            System.out.printf(Locale.US, "[%s] Vazão média: %.2f msg/s | Latência média: %.2f ms | P99: %.2f ms%n",
                    tituloCenario, mediaVazao, mediaLatencia, mediaP99);

        } catch (IOException e) {
            System.err.println("Erro ao exportar CSV: " + e.getMessage());
        }
    }

    public record ResultadoExecucao(double vazao, double latenciaMedia, double latenciaP99) {}
}