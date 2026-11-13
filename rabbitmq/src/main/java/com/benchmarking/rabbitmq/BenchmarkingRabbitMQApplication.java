package com.benchmarking.rabbitmq;

import com.benchmarking.rabbitmq.cenario1_1.Cenario1BaselineJava;
import com.benchmarking.rabbitmq.cenario1_2.Cenario2ConsumidorLento;
import com.benchmarking.rabbitmq.cenario1_3.Cenario3TamanhoMensagem;
import com.benchmarking.rabbitmq.cenario1_4.Cenario4MultiplosConsumidores;
import com.benchmarking.rabbitmq.cenario2_1.Cenario1ComparacaoGarantias;
import com.benchmarking.rabbitmq.cenario2_2.Cenario2FalhaConsumidor;
import com.benchmarking.rabbitmq.cenario2_3.Cenario3DeadLetterQueue;
import com.benchmarking.rabbitmq.cenario2_4.Cenario4SobrecargaProlongada;
import com.benchmarking.rabbitmq.cenario3_1.Cenario1Roteamento;
import com.benchmarking.rabbitmq.cenario3_2.Cenario2Replicacao;
import com.benchmarking.rabbitmq.cenario3_3.Cenario3FalhaBroker;
import com.benchmarking.rabbitmq.cenario3_4.Cenario4Replay;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BenchmarkingRabbitMQApplication implements CommandLineRunner {

    private final Cenario1BaselineJava cenario1;
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
    

    public BenchmarkingRabbitMQApplication(Cenario1BaselineJava cenario1, Cenario2ConsumidorLento cenario2,
        Cenario3TamanhoMensagem cenario3, Cenario4MultiplosConsumidores cenario4, Cenario1ComparacaoGarantias cenario2_1,
        Cenario2FalhaConsumidor cenario2_2, Cenario3DeadLetterQueue cenario2_3,Cenario4SobrecargaProlongada cenario2_4, Cenario1Roteamento cenario3_1,
        Cenario2Replicacao cenario3_2, Cenario3FalhaBroker cenario3_3, Cenario4Replay cenario3_4
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
        SpringApplication.run(BenchmarkingRabbitMQApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Iniciando benchmarking RabbitMQ...");
        cenario1.executar();
        System.out.println("\nBenchmarking concluído.");
        System.exit(0);
    }
}