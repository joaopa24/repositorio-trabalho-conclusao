#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 2: Falha do consumidor — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — duplicadas e perdidas por nível, as duas tecnologias lado a lado
    Gráfico 2 — tempo de recuperação das duas tecnologias (escala log)
    Tabela    — resumo das duas tecnologias (também gravado em CSV)

Organização esperada (o script numa pasta própria, ao lado das pastas de resultados):

    TCC\\
    ├── comparativo_cenario2_2\\
    │   └── plotar_falha_comparativo.py
    ├── resultado_cenario_2_2\\              (Kafka)
    └── resultado_rabbitmq_cenario_2_2\\     (RabbitMQ)

Também aceita os caminhos como argumentos:
    python plotar_falha_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

Depende de: pandas, matplotlib, numpy
"""

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA_ACIMA = os.path.dirname(PASTA_SCRIPT)

GARANTIAS = ["at-most-once", "at-least-once", "exactly-once"]
NOMES = {"at-most-once": "at most once", "at-least-once": "at least once", "exactly-once": "exactly once"}
TECNOLOGIAS = {
    # nome: (pasta, arquivo, cor da barra, cor dos pontos)
    "Kafka":    ("resultado_cenario_2_2",          "resultados-kafka-falha-consumidor.csv",    "#185FA5", "#D85A30"),
    "RabbitMQ": ("resultado_rabbitmq_cenario_2_2", "resultados-rabbitmq-falha-consumidor.csv", "#D85A30", "#1B6B2F"),
}
SAIDA = "falha-comparativo.pdf"
ARQ_RESUMO = "falha-comparativo-resumo.csv"

plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})
RNG = np.random.default_rng(42)


def fmt(v, casas=0):
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def faixa(s, casas=0):
    s = s.dropna()
    if s.empty:
        return "—"
    return fmt(s.min(), casas) if s.min() == s.max() else f"{fmt(s.min(), casas)} a {fmt(s.max(), casas)}"


def localizar(nome_pasta, nome_csv):
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        pasta = os.path.join(base, nome_pasta)
        if os.path.isfile(os.path.join(pasta, nome_csv)):
            return pasta
    print(f"[aviso] '{nome_csv}' não encontrado em {os.path.join(PASTA_ACIMA, nome_pasta)}")
    return None


def carregar():
    args = sys.argv[1:]
    dados = {}
    for i, (tec, (pasta_nome, arq, _, _)) in enumerate(TECNOLOGIAS.items()):
        pasta = args[i] if len(args) >= 2 else localizar(pasta_nome, arq)
        if pasta is None or not os.path.isfile(os.path.join(pasta, arq)):
            continue
        df = pd.read_csv(os.path.join(pasta, arq), encoding="utf-8-sig")
        df.columns = [str(c).strip() for c in df.columns]
        dados[tec] = df[df["execucao"] > 0].copy()
        print(f"{tec}: {os.path.abspath(pasta)}")
    return dados


def titulo(fig, texto, sub):
    fig.suptitle(texto, fontsize=11.5, y=0.975)
    fig.text(0.5, 0.975 - 0.34 / fig.get_figheight(), sub, ha="center", fontsize=8.5, color="#555555")


def barras_agrupadas(ax, dados, coluna):
    larg = 0.36
    for k, (tec, df) in enumerate(dados.items()):
        _, _, cor, cor_ponto = TECNOLOGIAS[tec]
        for j, g in enumerate(GARANTIAS, start=1):
            v = df.loc[df["garantia"] == g, coluna].dropna().values
            if len(v) == 0:
                continue
            x = j + (k - 0.5) * larg
            ax.bar(x, np.median(v), width=larg, color=cor, alpha=0.5, edgecolor=cor,
                   label=tec if j == 1 else None)
            ax.scatter(x + RNG.uniform(-0.06, 0.06, len(v)), v, s=14, color=cor_ponto,
                       edgecolor="white", linewidth=0.4, zorder=3)
            ax.text(x, np.median(v), fmt(np.median(v)), ha="center", va="bottom", fontsize=7.5)
    ax.set_xticks(range(1, len(GARANTIAS) + 1))
    ax.set_xticklabels([NOMES[g] for g in GARANTIAS])
    ax.grid(False, axis="x")
    ax.set_ylim(bottom=0)


def grafico1_integridade(pdf, dados):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 5.4))
    barras_agrupadas(ax1, dados, "duplicadas")
    ax1.set_title("Duplicadas (processadas mais de uma vez)", fontsize=10.5)
    ax1.set_ylabel("Mensagens por execução")
    barras_agrupadas(ax2, dados, "perdidas")
    ax2.set_title("Perdidas (nunca processadas)", fontsize=10.5)
    ax2.set_ylabel("Mensagens por execução")
    ax1.legend(frameon=True, facecolor="white", edgecolor="none", loc="upper right")
    titulo(fig, "Falha do consumidor — duplicadas e perdidas por nível de garantia",
           "Barra = mediana das execuções; cada ponto = 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_recuperacao(pdf, dados):
    fig, ax = plt.subplots(figsize=(8.4, 5.6))
    larg = 0.36
    algum = False
    for k, (tec, df) in enumerate(dados.items()):
        _, _, cor, cor_ponto = TECNOLOGIAS[tec]
        for j, g in enumerate(GARANTIAS, start=1):
            v = df.loc[df["garantia"] == g, "recuperacao_ms"].dropna().values
            v = v[v > 0]
            if len(v) == 0:
                continue
            algum = True
            x = j + (k - 0.5) * larg
            ax.boxplot([v], positions=[x], widths=larg * 0.9, patch_artist=True, showfliers=False,
                       medianprops=dict(color="black", linewidth=1.4),
                       boxprops=dict(facecolor=cor, alpha=0.5, edgecolor=cor),
                       whiskerprops=dict(color=cor), capprops=dict(color=cor))
            ax.scatter(x + RNG.uniform(-0.05, 0.05, len(v)), v, s=16, color=cor_ponto,
                       edgecolor="white", linewidth=0.4, zorder=3)
        ax.plot([], [], color=cor, linewidth=8, alpha=0.5, label=tec)
    if not algum:
        plt.close(fig)
        return
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    ax.set_xticks(range(1, len(GARANTIAS) + 1))
    ax.set_xticklabels([NOMES[g] for g in GARANTIAS])
    ax.set_xlim(0.4, len(GARANTIAS) + 0.6)
    ax.grid(False, axis="x")
    ax.set_ylabel("Tempo de recuperação (ms) — escala log")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", loc="center right")
    titulo(fig, "Falha do consumidor — tempo até o consumidor reserva receber mensagens",
           "Da queda do #1 até a 1ª mensagem recebida pelo #2; cada ponto = 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(dados):
    linhas = []
    for g in GARANTIAS:
        for tec, df in dados.items():
            d = df[df["garantia"] == g]
            rec = d["recuperacao_ms"].dropna()
            linhas.append({
                "garantia": g, "tecnologia": tec, "execucoes": len(d),
                "duplicadas_mediana": d["duplicadas"].median(), "duplicadas_min": d["duplicadas"].min(),
                "duplicadas_max": d["duplicadas"].max(),
                "perdidas_mediana": d["perdidas"].median(), "perdidas_min": d["perdidas"].min(),
                "perdidas_max": d["perdidas"].max(),
                "descartadas_mediana": d["descartadas_dedup"].median(),
                "recuperacao_mediana_ms": rec.median() if len(rec) else np.nan,
                "recuperacao_min_ms": rec.min() if len(rec) else np.nan,
                "recuperacao_max_ms": rec.max() if len(rec) else np.nan,
            })
    return pd.DataFrame(linhas)


def tabela_resumo(pdf, dados, res):
    cel = []
    for _, r in res.iterrows():
        d = dados[r["tecnologia"]]
        d = d[d["garantia"] == r["garantia"]]
        cel.append([NOMES[r["garantia"]], r["tecnologia"], faixa(d["duplicadas"]), faixa(d["perdidas"]),
                    faixa(d["descartadas_dedup"]),
                    "—" if np.isnan(r["recuperacao_mediana_ms"]) else fmt(r["recuperacao_mediana_ms"]),
                    faixa(d["recuperacao_ms"])])
    fig, ax = plt.subplots(figsize=(10, 2.4 + 0.38 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Nível", "Tecnologia", "Duplicadas", "Perdidas",
                                          "Descartadas\n(deduplicação)", "Recuperação\n(mediana, ms)",
                                          "Recuperação\n(menor a maior, ms)"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.6)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Falha do consumidor — resumo Kafka × RabbitMQ", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Valores 'menor a maior' entre as execuções. Recuperação vazia = o reserva não recebeu "
             "nenhuma mensagem.", ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.07, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    dados = carregar()
    if not dados:
        print("Nenhum CSV encontrado. Nada a fazer.")
        return
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, SAIDA)
    with PdfPages(saida) as pdf:
        grafico1_integridade(pdf, dados)
        grafico2_recuperacao(pdf, dados)
        res = resumo(dados)
        tabela_resumo(pdf, dados, res)
    res.to_csv(os.path.join(os.path.dirname(saida), ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
