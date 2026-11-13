#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 3: Dead Letter Queue — gráficos do RABBITMQ.

Lê o CSV gerado pelo orquestrador Cenario3DeadLetterQueue:
    resultados-rabbitmq-dlq.csv  (1 linha por execução e taxa de inválidas)

Gera um PDF com:
    Tabela 1  — verificação do isolamento (a DLQ tem exatamente as inválidas?)
    Gráfico 1 — vazão com 0% e com 10% de inválidas (cada ponto = 1 execução)
    Tabela 2  — resumo (também gravado em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_dlq_rabbitmq.py
ou:
    python plotar_dlq_rabbitmq.py <pasta_do_csv> [saida.pdf]

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
TECNOLOGIA   = "RabbitMQ"
ARQ_CSV      = "resultados-rabbitmq-dlq.csv"
ARQ_RESUMO   = "dlq-rabbitmq-resumo.csv"
SAIDA_PADRAO = "dlq-rabbitmq.pdf"
COR_CAIXA    = "#D85A30"   # laranja
COR_PONTO    = "#1B6B2F"   # verde
NOME_PONTO   = "verde"
# ----------------------------------------------------------------------

CENARIO = "Intermediário — Cenário 3: Dead Letter Queue"

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
    return fmt(s.min(), casas) if s.min() == s.max() else f"{fmt(s.min(), casas)} a {fmt(s.max(), casas)}"


def booleano(s):
    return s.astype(str).str.strip().str.lower().isin(["true", "1"])


def ler():
    p = os.path.join(PASTA, ARQ_CSV)
    if not os.path.isfile(p):
        print(f"[aviso] {ARQ_CSV} não encontrado em {os.path.abspath(PASTA)}")
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df["execucao"] > 0].copy()
    df["dlq_confere"] = booleano(df["dlq_confere"])
    df["metadados_ok"] = booleano(df["metadados_ok"])
    return df


def taxas(df):
    return sorted(df["taxa_invalidas_pct"].unique())


def tabela(pdf, linhas, colunas, texto, nota):
    fig, ax = plt.subplots(figsize=(10, 2.4 + 0.42 * len(linhas)))
    ax.axis("off")
    t = ax.table(cellText=linhas, colLabels=colunas, loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.7)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(texto, fontsize=11, pad=10)
    fig.text(0.5, 0.03, nota, ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.1, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def tabela1_verificacao(pdf, df):
    linhas = []
    for t in taxas(df):
        d = df[df["taxa_invalidas_pct"] == t]
        tent = (d["tentativas_invalidas"] / d["invalidas_enviadas"].replace(0, np.nan)).median()
        linhas.append([f"{t}%", str(len(d)), faixa(d["invalidas_enviadas"]), faixa(d["processadas_ok"]),
                       faixa(d["na_dlq"]), f"{int(d['dlq_confere'].sum())} de {len(d)}",
                       f"{int(d['metadados_ok'].sum())} de {len(d)}" if d["na_dlq"].sum() > 0 else "—",
                       "—" if np.isnan(tent) else fmt(tent, 1)])
    tabela(pdf, linhas,
           ["Inválidas", "Execuções", "Inválidas\nenviadas", "Processadas\nOK", "Mensagens\nna DLQ",
            "DLQ = exatamente\nas inválidas", "Com dados de\ndiagnóstico", "Tentativas por\ninválida"],
           f"{TECNOLOGIA} — verificação do isolamento",
           "'DLQ = exatamente as inválidas': as sequências lidas da DLQ são as mesmas que foram enviadas como inválidas.\n"
           "'Com dados de diagnóstico': toda mensagem da DLQ traz motivo e origem.")


def grafico1_vazao(pdf, df):
    ts = taxas(df)
    series = [df.loc[df["taxa_invalidas_pct"] == t, "vazao_msg_s"].values for t in ts]
    fig, ax = plt.subplots(figsize=(7, 5.4))
    pos = list(range(1, len(ts) + 1))
    ax.boxplot(series, positions=pos, widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    for i, s in zip(pos, series):
        ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=26, color=COR_PONTO,
                   edgecolor="white", linewidth=0.6, zorder=3)
    ax.set_xticks(pos)
    ax.set_xticklabels([f"{t}% inválidas" for t in ts])
    ax.grid(False, axis="x")
    ax.set_ylim(bottom=0)
    ax.set_ylabel("Vazão de ponta a ponta (msg/s)")
    ax.set_title(f"{TECNOLOGIA} — vazão com e sem mensagens inválidas", fontsize=11.5, pad=24)
    ax.text(0.5, 1.02, f"Do primeiro envio até o fim do tratamento da última mensagem; cada ponto {NOME_PONTO} = 1 execução",
            transform=ax.transAxes, ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def resumo(df):
    linhas = []
    base = df.loc[df["taxa_invalidas_pct"] == 0, "vazao_msg_s"].median()
    for t in taxas(df):
        d = df[df["taxa_invalidas_pct"] == t]
        v = d["vazao_msg_s"]
        linhas.append({
            "tecnologia": TECNOLOGIA, "taxa_invalidas_pct": t, "execucoes": len(d),
            "vazao_mediana_msg_s": v.median(), "vazao_min_msg_s": v.min(), "vazao_max_msg_s": v.max(),
            "variacao_vs_0pct": (v.median() / base - 1) * 100 if base and not np.isnan(base) else np.nan,
            "dlq_confere": f"{int(d['dlq_confere'].sum())}/{len(d)}",
            "metadados_ok": f"{int(d['metadados_ok'].sum())}/{len(d)}",
        })
    return pd.DataFrame(linhas)


def tabela2_resumo(pdf, res):
    linhas = [[f"{int(r['taxa_invalidas_pct'])}%", fmt(r["vazao_mediana_msg_s"]),
               f"{fmt(r['vazao_min_msg_s'])} a {fmt(r['vazao_max_msg_s'])}",
               "—" if r["taxa_invalidas_pct"] == 0 or np.isnan(r["variacao_vs_0pct"])
               else f"{fmt(r['variacao_vs_0pct'], 1)}%",
               r["dlq_confere"]] for _, r in res.iterrows()]
    tabela(pdf, linhas, ["Inválidas", "Vazão\n(mediana, msg/s)", "Vazão\n(menor a maior)",
                         "Variação vs.\n0% inválidas", "DLQ confere"],
           f"{TECNOLOGIA} — {CENARIO} — resumo",
           "'Mediana' = a mediana entre as execuções.")


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    df = ler()
    if df is None or df.empty:
        print("Nada a fazer.")
        return
    with PdfPages(saida) as pdf:
        tabela1_verificacao(pdf, df)
        grafico1_vazao(pdf, df)
        res = resumo(df)
        tabela2_resumo(pdf, res)
    res.to_csv(os.path.join(PASTA, ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
