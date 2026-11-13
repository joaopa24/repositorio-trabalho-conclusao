# RabbitMQ × Apache Kafka: ferramenta de testes do TCC

Código da ferramenta de testes usada no trabalho de conclusão de curso **"Análise comparativa entre RabbitMQ e Apache Kafka"** (Ciência da Computação, UFLA, 2026), de João Lucas Pereira de Almeida.

A ferramenta executa **12 cenários** em três níveis (básico, intermediário e avançado), sempre com o mesmo desenho nas duas tecnologias, e grava os resultados de cada execução em arquivos CSV.

---

## Estrutura do repositório

```
.
├── kafka/                                   projeto Spring Boot dos testes do Kafka
│   ├── docker-compose.yml                   broker único (Kafka 3.5 + ZooKeeper)
│   ├── docker-compose-cluster.yml           cluster de 3 brokers (nível avançado, cenários 2 e 3)
│   └── src/main/java/com/benchmarking/kafka/
│       ├── BenchmarkingKafkaApplication.java   ponto de entrada: escolhe o cenário
│       ├── cenario1_1 … cenario1_4/            nível básico
│       ├── cenario2_1 … cenario2_4/            nível intermediário
│       └── cenario3_1 … cenario3_4/            nível avançado
└── rabbitmq/                                projeto Spring Boot dos testes do RabbitMQ
    ├── docker-compose.yml                   broker único (RabbitMQ 3.12)
    ├── docker-compose-cluster-rabbitmq.yml  cluster de 3 nós (nível avançado, cenários 2 e 3)
    ├── rabbitmq-cluster.conf                configuração do cluster (usada pelo compose acima)
    └── src/main/java/com/benchmarking/rabbitmq/
        └── (mesma organização do projeto kafka)
```

Cada pasta `cenarioX_Y` tem três classes:

- **orquestrador** (`CenarioN…java`): prepara o broker, inicia os outros processos, repete as execuções e grava os CSVs;
- **produtor**: envia as mensagens;
- **consumidor**: recebe as mensagens e informa as métricas.

Produtor e consumidor rodam como **processos Java separados**, iniciados pelo orquestrador.

---

## Requisitos

| Ferramenta | Versão | Observação |
|---|---|---|
| Java (JDK) | 21 ou mais recente | o TCC usou o Oracle Java SE 25.0.2 |
| Docker | Docker Desktop (Windows/macOS) ou Docker Engine + Compose v2 (Linux) | o comando `docker` precisa estar no PATH |
| Memória para o Docker | cerca de 4 GB | os clusters usam 3 brokers ao mesmo tempo |
| Git | qualquer | para clonar o repositório |

**Não é preciso instalar o Maven.** Cada projeto traz o Maven Wrapper (`mvnw` / `mvnw.cmd`), que baixa a versão certa na primeira execução.

Portas usadas no computador:

| Tecnologia | Broker único | Cluster |
|---|---|---|
| Kafka | 9092 | 19092, 19093, 19094 |
| RabbitMQ | 5672 (AMQP), 15672 (painel) | 5681–5683 (AMQP), 15681–15683 (painel) |

---

## Início rápido: Cenário 1 do Kafka

```bash
git clone https://github.com/joaopa24/repositorio-trabalho-conclusao.git
cd repositorio-trabalho-conclusao/kafka

# 1. sobe o broker
docker compose up -d

# 2. espera uns 20 s para o Kafka terminar de iniciar e roda os testes
./mvnw spring-boot:run          # Linux / macOS
.\mvnw.cmd spring-boot:run      # Windows (PowerShell)

# 3. ao terminar, desliga o broker
docker compose down
```

Ao fim, o programa imprime o caminho dos CSVs gerados e a mensagem `Benchmarking concluído.`.

Para o RabbitMQ, é o mesmo procedimento dentro da pasta `rabbitmq/`.

> **Importante:** rode sempre com `mvnw spring-boot:run` ou pela sua IDE (executando a classe `Benchmarking…Application`). **Não** use `java -jar` com o JAR gerado pelo `package`: o orquestrador inicia o produtor e o consumidor com o mesmo classpath da aplicação, e isso não funciona dentro do JAR executável do Spring Boot.

---

## Como escolher o cenário

O cenário executado é definido no método `run()` da classe principal:

- Kafka: `kafka/src/main/java/com/benchmarking/kafka/BenchmarkingKafkaApplication.java`
- RabbitMQ: `rabbitmq/src/main/java/com/benchmarking/rabbitmq/BenchmarkingRabbitMQApplication.java`

Deixe ativa **somente a linha do cenário desejado** e comente as demais. No projeto do Kafka, as linhas de todos os cenários já estão lá, comentadas. No do RabbitMQ, troque a linha `cenario1.executar();` pela do cenário desejado.

```java
@Override
public void run(String... args) throws Exception {
    // cenario1.executar();
    cenario2_1.executar();   // ← apenas o cenário que você quer rodar
    ...
}
```

| Nível | Cenário (como no TCC) | Linha em `run()` | Broker | Execuções válidas |
|---|---|---|---|---|
| Básico | 1 – Baseline | `cenario1.executar();` | único | 10 |
| Básico | 2 – Consumidor lento | `cenario2.executar();` | único | 10 por valor de prefetch / `max.poll.records` |
| Básico | 3 – Tamanho da mensagem | `cenario3.executar();` | único | 10 por tamanho (Partes A e B) |
| Básico | 4 – Múltiplos consumidores | `cenario4.executar();` | único | 10 por configuração |
| Intermediário | 1 – Garantias de entrega | `cenario2_1.executar();` | único | 10 por nível (Partes A e B) |
| Intermediário | 2 – Falha do consumidor | `cenario2_2.executar();` | único | 10 por nível |
| Intermediário | 3 – Dead letter queue | `cenario2_3.executar();` | único | 10 por configuração |
| Intermediário | 4 – Sobrecarga prolongada | `cenario2_4.executar();` | único | 10 por política |
| Avançado | 1 – Roteamento | `cenario3_1.executar();` | único | 3 por configuração |
| Avançado | 2 – Replicação | `cenario3_2.executar();` | **cluster** | 10 por configuração (Partes A e B) |
| Avançado | 3 – Falha de broker | `cenario3_3.executar();` | **cluster** + comando `docker` | 3 por configuração |
| Avançado | 4 – Replay | `cenario3_4.executar();` | único | 10 por modo |

Todos os cenários fazem uma execução de **aquecimento** antes das válidas e a descartam. Quando há mais de uma configuração, elas rodam **intercaladas, em ordem rotativa**.

O desenho completo de cada cenário (cargas, taxas, métricas e colunas do CSV) está no comentário no topo da classe do orquestrador.

---

## Broker único ou cluster

Use **uma tecnologia por vez** e **um compose por vez**, para os brokers não disputarem CPU e memória.

### Broker único (todos os cenários, exceto Avançado 2 e 3)

```bash
cd kafka        # ou: cd rabbitmq
docker compose up -d
# … rode o cenário …
docker compose down
```

### Cluster de 3 nós (Avançado 2 – Replicação e Avançado 3 – Falha de broker)

**Kafka**

```bash
cd kafka
docker compose down                                   # desliga o broker único, se estiver ligado
docker compose -f docker-compose-cluster.yml up -d    # sobe kafka-1, kafka-2, kafka-3 e o ZooKeeper
# espere cerca de 30 s e rode o cenário
docker compose -f docker-compose-cluster.yml down
```

**RabbitMQ**

```bash
cd rabbitmq
docker compose down
docker compose -f docker-compose-cluster-rabbitmq.yml up -d
docker exec rabbit-1 rabbitmqctl cluster_status        # deve listar rabbit@rabbit-1, -2 e -3
# rode o cenário
docker compose -f docker-compose-cluster-rabbitmq.yml down
```

O orquestrador confere se o cluster tem 3 nós antes de começar e para com uma mensagem de erro se não tiver.

No **Avançado 3 – Falha de broker**, o orquestrador derruba (`docker kill`) e religa (`docker start`) o container do líder. Por isso:

- o comando `docker` precisa funcionar no mesmo terminal em que a ferramenta roda;
- os nomes dos containers não podem ser alterados (`kafka-1` a `kafka-3` e `rabbit-1` a `rabbit-3`).

---

## Teste rápido com menos execuções

O número de execuções vem da propriedade `benchmarking.execucoes` (padrão: 10), em `src/main/resources/application.properties`. Para conferir se tudo funciona antes de rodar uma campanha completa, passe um valor menor na linha de comando:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--benchmarking.execucoes=2
```

No PowerShell, coloque o argumento entre aspas:

```powershell
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.arguments=--benchmarking.execucoes=2"
```

Os cenários Avançado 1 e Avançado 3 têm número de execuções fixo (3) e não usam essa propriedade.

---

## Onde ficam os resultados

Cada cenário cria uma pasta **dentro da pasta do projeto em que foi executado** (`kafka/` ou `rabbitmq/`):

| Tecnologia | Pasta | Exemplo |
|---|---|---|
| Kafka | `resultado_cenario_<nível>_<cenário>/` | `kafka/resultado_cenario_2_1/resultados-kafka-garantias-latencia.csv` |
| RabbitMQ | `resultado_rabbitmq_cenario_<nível>_<cenário>/` | `rabbitmq/resultado_rabbitmq_cenario_2_1/resultados-rabbitmq-garantias-latencia.csv` |

Em geral, cada cenário gera:

- um CSV **resumido**, com uma linha por execução;
- um CSV **detalhado** ou de **série**, com uma linha por mensagem ou por segundo, quando o cenário mede latência ou comportamento ao longo do tempo.

> Rodar o mesmo cenário de novo **substitui** os CSVs dele. Copie a pasta antes, se quiser guardar a campanha anterior.

---

## Tempo de execução e cuidados

- Uma campanha pode levar de minutos a várias horas. Exemplo: o Cenário 3 do nível básico leva cerca de **3 h só na Parte A**, em cada tecnologia.
- Evite usar o computador para outras tarefas durante as medições. Isso altera latência e vazão.
- Recrie os containers entre campanhas (`docker compose down` e `docker compose up -d`) para começar sempre do mesmo estado. Os composes não usam volumes, então os dados do broker são apagados junto com o container.
- Se a campanha for interrompida, alguns cenários apagam sozinhos os tópicos e filas que sobraram na execução seguinte. Na dúvida, recrie os containers.
- Qualquer inconsistência (contagem errada de mensagens, processo que termina com erro, métrica ausente) **interrompe a campanha**. Nenhum valor é preenchido com zero no CSV.

---

## Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `port is already allocated` ao subir o compose | outro compose (ou outro programa) usando a porta | `docker ps` e `docker compose down` no que estiver ligado |
| Kafka recusa conexão logo depois do `up -d` | o broker ainda está iniciando | espere 20–30 s e rode de novo |
| "Este cenário precisa do cluster de 3 brokers/nós" | cluster desligado, incompleto ou ainda subindo | confira com `docker ps` (e `rabbitmqctl cluster_status` no RabbitMQ) |
| Falha de broker não consegue derrubar o container | comando `docker` fora do PATH do terminal | rode a ferramenta no mesmo terminal em que `docker ps` funciona |
| Containers do cluster caem ou ficam lentos | pouca memória para o Docker | libere cerca de 4 GB para o Docker (no Docker Desktop: Settings → Resources, ou o `.wslconfig` no WSL2) |
| Erro ao apagar um CSV antigo (Windows) | arquivo aberto no Excel ou sincronizado pelo OneDrive | feche o arquivo ou rode o projeto fora de uma pasta sincronizada |

---

## Ambiente usado no TCC

| Item | Valor |
|---|---|
| Computador | Intel Core i5-1135G7 (4 núcleos, 8 processadores lógicos), 8 GB de RAM |
| Sistema | Windows 11, Docker Desktop com backend WSL2 (cerca de 3,8 GB de memória) |
| Java | compilado para Java 21, executado no Oracle Java SE 25.0.2 |
| Spring Boot | 3.2.5 (clientes `kafka-clients` 3.6.2 e `amqp-client` 5.19.0) |
| Kafka | 3.5, imagem `confluentinc/cp-kafka:7.5.0`, modo ZooKeeper; 2 GB de heap (broker único) e 768 MB por broker (cluster) |
| RabbitMQ | 3.12, imagem `rabbitmq:3.12-management` |

Os brokers rodaram nos containers, e a ferramenta de testes rodou diretamente no Windows. Nos clusters, os três nós rodam **na mesma máquina**, sem rede física entre eles.

---

## Como citar

ALMEIDA, João Lucas Pereira de. **Repositório do trabalho de conclusão de curso**: código, dados brutos e scripts. [*S. l.*]: GitHub, 2026. Disponível em: https://github.com/joaopa24/repositorio-trabalho-conclusao.
