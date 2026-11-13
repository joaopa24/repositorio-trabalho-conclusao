package com.benchmarking.kafka;

import com.benchmarking.kafka.cenario1_1.Cenario1BaselineJava;
import com.benchmarking.kafka.cenario1_2.Cenario2ConsumidorLento;
import com.benchmarking.kafka.cenario1_3.Cenario3TamanhoMensagem;
import com.benchmarking.kafka.cenario1_4.Cenario4MultiplosConsumidores;
import com.benchmarking.kafka.cenario2_1.Cenario1ComparacaoGarantias;
import com.benchmarking.kafka.cenario2_2.Cenario2FalhaConsumidor;
import com.benchmarking.kafka.cenario2_3.Cenario3DeadLetterQueue;
import com.benchmarking.kafka.cenario2_4.Cenario4SobrecargaProlongada;
import com.benchmarking.kafka.cenario3_1.Cenario1Roteamento;
import com.benchmarking.kafka.cenario3_2.Cenario2Replicacao;
import com.benchmarking.kafka.cenario3_3.Cenario3FalhaBroker;
import com.benchmarking.kafka.cenario3_4.Cenario4Replay;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BenchmarkingKafkaApplication implements CommandLineRunner {

    private final Cenario1BaselineJava    cenario1;
    private final Cenario2ConsumidorLento cenario2;
    private final Cenario3TamanhoMensagem cenario3;
    private final Cenario4MultiplosConsumidores cenario4;
    private final Cenario1ComparacaoGarantias cenario2_1;
    private final Cenario2FalhaConsumidor cenario2_2;
    private final Cenario3DeadLetterQueue cenario2_3;
    private final Cenario4SobrecargaProlongada cenario2_4;
    private final Cenario1Roteamento cenario3_1;
    private final Cenario2Replicacao cenario3_2;
    private final Cenario3FalhaBroker cenario3_3;
    private final Cenario4Replay cenario3_4;

    public BenchmarkingKafkaApplication(Cenario1BaselineJava cenario1,
                                         Cenario2ConsumidorLento cenario2,
                                         Cenario3TamanhoMensagem cenario3, Cenario4MultiplosConsumidores cenario4, Cenario1ComparacaoGarantias cenario2_1, Cenario2FalhaConsumidor cenario2_2,
                                         Cenario3DeadLetterQueue cenario2_3, Cenario4SobrecargaProlongada cenario2_4,  Cenario1Roteamento cenario3_1, Cenario2Replicacao cenario3_2, Cenario3FalhaBroker cenario3_3, Cenario4Replay cenario3_4
                                        ) {
        this.cenario1 = cenario1;
        this.cenario2 = cenario2;
        this.cenario3 = cenario3;   
        this.cenario4 = cenario4;
        this.cenario2_1 = cenario2_1;
        this.cenario2_2 = cenario2_2;
        this.cenario2_3 = cenario2_3;
        this.cenario2_4 = cenario2_4;
        this.cenario3_1 = cenario3_1;
        this.cenario3_2 = cenario3_2;
        this.cenario3_3 = cenario3_3;
        this.cenario3_4 = cenario3_4;
    }

    public static void main(String[] args) {
        SpringApplication.run(BenchmarkingKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Iniciando benchmarking Kafka...");

        // Cenário 1 — baseline (sem atraso artificial)
        cenario1.executar();

        // Cenário 2 — consumidor lento (backlog)
        //cenario2.executar();

        // Cenário 3 — variação de tamanho de mensagem
        //cenario3.executar();

        // Cenário 4 — variação de tamanho de mensagem
        // cenario4.executar();

        // Cenario 2_1 - Garantias
        // cenario2_1.executar(); ------------------- refazerrr
        
        // Cenario 2_2 - Falhas
        //cenario2_2.executar();

        // Cenario 2_3 - Dead Letter Queue
        // cenario2_3.executar();

        // Cenario 2_4 - Sobrecarga
        //cenario2_4.executar();

        // Cenario 3_1 - Roteamento
        // cenario3_1.executar();

        // Cenario 3_2 - Replicação
        //cenario3_2.executar();

         // Cenario 3_3 - Falha Broker
        // cenario3_3.executar();

         // Cenario 3_4 - Replay
        //cenario3_4.executar();

        System.out.println("\nBenchmarking concluído.");
        System.exit(0);
    }
}