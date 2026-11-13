#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 2: Falha do consumidor — gráficos do KAFKA.

Lê o CSV gerado pelo orquestrador Cenario2FalhaConsumidor:
    resultados-kafka-falha-consumidor.csv  (1 linha por execução e nível)

Gera um PDF com:
    Gráfico 1 — mensagens duplicadas e perdidas por nível (cada ponto = 1 execução)
    Gráfico 2 — tempo de recuperação por nível
    Tabela    — resumo por nível (também gravado em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_falha_kafka.py
ou:
    python plotar_falha_kafka.py <pasta_do_csv> [saida.pdf]

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

# ----------------------------------------------------------------------
# Configuração específica desta tecnologia
# ----------------------------------------------------------------------
TECNOLOGIA   = "Kafka"
ARQ_CSV      = "resultados-kafka-falha-consumidor.csv"
ARQ_RESUMO   = "falha-kafka-resumo.csv"
SAIDA_PADRAO = "falha-kafka.pdf"
COR_CAIXA    = "#185FA5"   # azul
COR_PONTO    = "#D85A30"   # laranja
NOME_PONTO   = "laranja"
# ----------------------------------------------------------------------

CENARIO = "Intermediário — Cenário 2: Falha do consumidor"
GARANTIAS = ["at-most-once", "at-least-once", "exactly-once"]
NOMES = {"at-most-once": "at most once", "at-least-once": "at least once", "exactly-once": "exactly once"}

plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA = sys.argv[1] if len(sys.argv) > 1 else PASTA_SCRIPT
SAIDA = sys.argv[2] if len(sys.argv) > 2 else SAIDA_PADRAO
RNG = np.random.default_rng(42)


def fmt(v, casas=0):
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def faixa(s, casas=0):
    s = s.dropna()
    if s.empty:
        return "—"
    if s.min() == s.max():
        return fmt(s.min(), casas)
    return f"{fmt(s.min(), casas)} a {fmt(s.max(), casas)}"


def ler():
    p = os.path.join(PASTA, ARQ_CSV)
    if not os.path.isfile(p):
        print(f"[aviso] {ARQ_CSV} não encontrado em {os.path.abspath(PASTA)}")
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df[df["execucao"] > 0].copy()


def titulo(fig, texto, sub):
    fig.suptitle(texto, fontsize=11.5, y=0.975)
    fig.text(0.5, 0.975 - 0.34 / fig.get_figheight(), sub, ha="center", fontsize=8.5, color="#555555")


def pontos_por_nivel(ax, df, coluna):
    for i, g in enumerate(GARANTIAS, start=1):
        v = df.loc[df["garantia"] == g, coluna].dropna().values
        if len(v) == 0:
            continue
        ax.bar(i, np.median(v), width=0.55, color=COR_CAIXA, alpha=0.45, edgecolor=COR_CAIXA)
        ax.scatter(i + RNG.uniform(-0.12, 0.12, len(v)), v, s=24, color=COR_PONTO,
                   edgecolor="white", linewidth=0.5, zorder=3)
        ax.text(i, np.median(v), f"{fmt(np.median(v))}", ha="center", va="bottom", fontsize=8.5)
    ax.set_xticks(range(1, len(GARANTIAS) + 1))
    ax.set_xticklabels([NOMES[g] for g in GARANTIAS])
    ax.grid(False, axis="x")
    ax.set_ylim(bottom=0)


def grafico1_integridade(pdf, df):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5.2))
    pontos_por_nivel(ax1, df, "duplicadas")
    ax1.set_title("Mensagens processadas mais de uma vez", fontsize=10.5)
    ax1.set_ylabel("Duplicadas por execução")
    pontos_por_nivel(ax2, df, "perdidas")
    ax2.set_title("Mensagens nunca processadas", fontsize=10.5)
    ax2.set_ylabel("Perdidas por execução")
    n = int(df.groupby("garantia").size().median())
    titulo(fig, f"{TECNOLOGIA} — o que acontece com as mensagens quando o consumidor cai",
           f"Barra = mediana de {n} execuções; cada ponto {NOME_PONTO} = 1 execução "
           f"({fmt(df['enviadas'].iloc[0])} mensagens; queda depois de ~1.000 processadas)")
    fig.tight_layout(rect=[0, 0, 1, 0.88])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_recuperacao(pdf, df):
    series = [df.loc[df["garantia"] == g, "recuperacao_ms"].dropna().values for g in GARANTIAS]
    if all(len(s) == 0 for s in series):
        return
    fig, ax = plt.subplots(figsize=(7.4, 5.4))
    pos = [i for i, s in enumerate(series, start=1) if len(s)]
    ax.boxplot([s for s in series if len(s)], positions=pos, widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    for i, s in zip(pos, [s for s in series if len(s)]):
        ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=26, color=COR_PONTO,
                   edgecolor="white", linewidth=0.6, zorder=3)
    ax.set_xticks(range(1, len(GARANTIAS) + 1))
    ax.set_xticklabels([NOMES[g] for g in GARANTIAS])
    ax.grid(False, axis="x")
    ax.set_ylim(bottom=0)
    ax.set_ylabel("Tempo de recuperação (ms)")
    ax.set_xlabel("Nível de garantia")
    titulo(fig, f"{TECNOLOGIA} — tempo até o consumidor reserva receber mensagens",
           f"Da queda do consumidor #1 até a 1ª mensagem recebida pelo #2; cada ponto {NOME_PONTO} = 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(df):
    linhas = []
    for g in GARANTIAS:
        d = df[df["garantia"] == g]
        rec = d["recuperacao_ms"].dropna()
        linhas.append({
            "tecnologia": TECNOLOGIA, "garantia": g, "execucoes": len(d),
            "processadas_c1_mediana": d["processadas_c1"].median(),
            "duplicadas_mediana": d["duplicadas"].median(), "duplicadas_min": d["duplicadas"].min(),
            "duplicadas_max": d["duplicadas"].max(),
            "perdidas_mediana": d["perdidas"].median(), "perdidas_min": d["perdidas"].min(),
            "perdidas_max": d["perdidas"].max(),
            "descartadas_mediana": d["descartadas_dedup"].median(),
            "recuperacao_mediana_ms": rec.median() if len(rec) else np.nan,
            "recuperacao_min_ms": rec.min() if len(rec) else np.nan,
            "recuperacao_max_ms": rec.max() if len(rec) else np.nan,
            "execucoes_sem_recuperacao": int(d["recuperacao_ms"].isna().sum()),
        })
    return pd.DataFrame(linhas)


def tabela_resumo(pdf, df, res):
    cel = []
    for _, r in res.iterrows():
        d = df[df["garantia"] == r["garantia"]]
        cel.append([NOMES[r["garantia"]], str(r["execucoes"]),
                    faixa(d["processadas_c1"]), faixa(d["duplicadas"]), faixa(d["perdidas"]),
                    faixa(d["descartadas_dedup"]),
                    "—" if np.isnan(r["recuperacao_mediana_ms"]) else fmt(r["recuperacao_mediana_ms"]),
                    faixa(d["recuperacao_ms"])])
    fig, ax = plt.subplots(figsize=(10, 2.6 + 0.4 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Nível", "Execuções", "Processadas\npelo #1", "Duplicadas",
                                          "Perdidas", "Descartadas\n(deduplicação)", "Recuperação\n(mediana, ms)",
                                          "Recuperação\n(menor a maior, ms)"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.7)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(f"{TECNOLOGIA} — {CENARIO} — resumo", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Valores 'menor a maior' entre as execuções. Recuperação vazia = o #2 não recebeu nenhuma\n"
             "mensagem (tudo já tinha sido entregue ao #1 antes da queda).", ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.1, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    df = ler()
    if df is None or df.empty:
        print("Nada a fazer.")
        return
    with PdfPages(saida) as pdf:
        grafico1_integridade(pdf, df)
        grafico2_recuperacao(pdf, df)
        res = resumo(df)
        tabela_resumo(pdf, df, res)
    res.to_csv(os.path.join(PASTA, ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
