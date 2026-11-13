#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 2 — Consumidor Lento — COMPARATIVO Kafka x RabbitMQ.

Coloca as duas tecnologias no mesmo gráfico, com o parâmetro do consumidor
no eixo X (prefetch no RabbitMQ, max.poll.records no Kafka):
    Gráfico 1 — utilização do consumidor (%)
    Gráfico 2 — tempo ocioso médio por mensagem (ms)
    Gráfico 3 — vazão efetiva do consumidor (msg/s)

Cada ponto é a MEDIANA das 10 execuções; a barra vertical vai do 1º ao 3º
quartil (a mesma faixa da caixa do boxplot).

Uso (com o script em uma pasta própria, ao lado das pastas de resultados):

    TCC\
    ├── comparativo\
    │   └── plotar_cenario2_comparativo.py
    ├── resultado_cenario_1_2\
    └── resultado_rabbitmq_cenario_1_2\

    cd comparativo
    python plotar_cenario2_comparativo.py

O PDF é gerado dentro da pasta "comparativo". Se as pastas de resultados
estiverem dentro da mesma pasta do script, ele também as encontra.
ou:
    python plotar_cenario2_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

Depende de: pandas, matplotlib, numpy
"""

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

DELAY_MS = 5
VAZAO_TEORICA = 1000.0 / DELAY_MS

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA_ACIMA  = os.path.dirname(PASTA_SCRIPT)


def localizar(nome_pasta, nome_csv):
    """Procura o CSV agregado na pasta de resultados ao lado da pasta deste
    script (nível de cima) e, se não achar, dentro da pasta do próprio script."""
    tentativas = []
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        pasta = os.path.join(base, nome_pasta)
        tentativas.append(os.path.join(pasta, nome_csv))
        if os.path.isfile(os.path.join(pasta, nome_csv)):
            return pasta
    print(f"[aviso] '{nome_csv}' não encontrado. Procurei em:")
    for t in tentativas:
        print(f"         {t}")
    pasta = os.path.join(PASTA_ACIMA, nome_pasta)
    if os.path.isdir(pasta):
        print(f"         A pasta existe e contém: {sorted(os.listdir(pasta))}")
        print("         Se só há CSVs antigos (ex.: ...-maxpoll1.csv), rode antes a versão nova do Java.")
    return pasta


PASTA_KAFKA  = sys.argv[1] if len(sys.argv) > 1 else localizar(
    "resultado_cenario_1_2", "resultados-kafka-cenario2.csv")
PASTA_RABBIT = sys.argv[2] if len(sys.argv) > 2 else localizar(
    "resultado_rabbitmq_cenario_1_2", "resultados-rabbitmq-cenario2.csv")
SAIDA        = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, "cenario2-comparativo.pdf")

# (nome, arquivo, coluna do parâmetro, rótulo do parâmetro, cor, marcador)
TECNOLOGIAS = [
    ("RabbitMQ", os.path.join(PASTA_RABBIT, "resultados-rabbitmq-cenario2.csv"),
     "prefetch", "prefetch", "#D85A30", "o"),
    ("Kafka", os.path.join(PASTA_KAFKA, "resultados-kafka-cenario2.csv"),
     "max_poll_records", "max.poll.records", "#185FA5", "s"),
]

plt.rcParams.update({
    "font.size": 10,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 120,
})


def carregar():
    dados = []
    for nome, arq, col, rotulo, cor, marcador in TECNOLOGIAS:
        if not os.path.isfile(arq):
            print(f"[aviso] não encontrado: {arq}")
            continue
        df = pd.read_csv(arq)
        df.columns = [c.strip() for c in df.columns]
        df = df.rename(columns={col: "parametro"})
        dados.append((nome, rotulo, cor, marcador, df))
    return dados


def grafico(pdf, dados, coluna, ylabel, titulo, subtitulo, referencia=None, rotulo_ref=None):
    fig, ax = plt.subplots(figsize=(7.4, 5.0))
    todos_valores = set()
    deslocamento = {0: 0.96, 1: 1.04}  # afasta levemente as duas séries no eixo X

    for i, (nome, rotulo, cor, marcador, df) in enumerate(dados):
        g = df.groupby("parametro")[coluna]
        valores = np.array(sorted(df["parametro"].unique()), dtype=float)
        mediana = g.median().reindex(valores).values
        q1 = g.quantile(0.25).reindex(valores).values
        q3 = g.quantile(0.75).reindex(valores).values
        x = valores * deslocamento.get(i, 1.0)
        todos_valores.update(valores.tolist())

        ax.errorbar(x, mediana, yerr=[mediana - q1, q3 - mediana], color=cor,
                    marker=marcador, markersize=8, markeredgecolor="white",
                    linewidth=2, capsize=4, label=f"{nome} ({rotulo})")
        # rótulo direto no último ponto
        ax.annotate(nome, xy=(x[-1], mediana[-1]), xytext=(8, 0),
                    textcoords="offset points", va="center", fontsize=9, color="#333333")

    if referencia is not None:
        ax.axhline(referencia, color="#555555", linestyle="--", linewidth=1)
        ax.annotate(rotulo_ref, xy=(0.01, referencia), xycoords=("axes fraction", "data"),
                    xytext=(0, -4), textcoords="offset points", va="top",
                    fontsize=8.5, color="#555555")

    ticks = sorted(todos_valores)
    ax.set_xscale("log")
    ax.set_xticks(ticks)
    ax.set_xticklabels([str(int(t)) for t in ticks])
    ax.minorticks_off()
    ax.set_xlim(min(ticks) * 0.7, max(ticks) * 1.9)
    ax.set_xlabel("Parâmetro do consumidor (prefetch / max.poll.records) — escala log")
    ax.set_ylabel(ylabel)
    ax.set_title(titulo, fontsize=11.5, pad=22)
    ax.text(0.5, 1.02, subtitulo, transform=ax.transAxes, ha="center",
            fontsize=8.5, color="#555555")
    ax.legend(frameon=False, fontsize=9, loc="best")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def main():
    dados = carregar()
    if not dados:
        print("Nenhum CSV encontrado — nada a plotar.")
        return

    sub = "Ponto = mediana das 10 execuções; barra = 1º ao 3º quartil"
    with PdfPages(SAIDA) as pdf:
        grafico(pdf, dados, "utilizacao_consumidor_pct", "Utilização do consumidor (%)",
                "Cenário 2 — utilização do consumidor", sub,
                referencia=100, rotulo_ref="100% = nunca ficou parado")
        grafico(pdf, dados, "ocioso_medio_ms", "Tempo ocioso médio por mensagem (ms)",
                "Cenário 2 — tempo ocioso por mensagem", sub)
        grafico(pdf, dados, "vazao_consumidor_msg_s", "Vazão do consumidor (msg/s)",
                "Cenário 2 — vazão efetiva do consumidor", sub,
                referencia=VAZAO_TEORICA, rotulo_ref=f"teórico: {VAZAO_TEORICA:.0f} msg/s")
    print(f"PDF gerado: {SAIDA}")


if __name__ == "__main__":
    main()