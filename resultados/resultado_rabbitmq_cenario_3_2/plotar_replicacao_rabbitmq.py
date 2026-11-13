#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível avançado — Cenário 2: Impacto da replicação — gráficos do RABBITMQ.

Lê os CSVs gerados pelo orquestrador do cenário:
    resultados-rabbitmq-replicacao-latencia.csv   (Parte A: 1 linha por execução e configuração)
    resultados-rabbitmq-replicacao-vazao.csv      (Parte B: 1 linha por execução e configuração)

Gera um PDF só com o essencial:
    Gráfico 1 — latência mediana de cada execução, por configuração de fila
    Gráfico 2 — vazão de ponta a ponta de cada execução, por configuração de fila
    Tabela    — medianas, faixas e aumento em relação à referência (também em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_replicacao_rabbitmq.py
ou:
    python plotar_replicacao_rabbitmq.py <pasta_dos_csvs> [saida.pdf]

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
ARQ_LATENCIA = "resultados-rabbitmq-replicacao-latencia.csv"
ARQ_VAZAO    = "resultados-rabbitmq-replicacao-vazao.csv"
ARQ_RESUMO   = "replicacao-rabbitmq-resumo.csv"
SAIDA_PADRAO = "replicacao-rabbitmq.pdf"
COR          = "#D85A30"
COLUNA_CFG   = "configuracao"
ORDEM        = ["classica", "quorum-1", "quorum-2", "quorum-3"]
NOMES        = {"classica": "Clássica", "quorum-1": "Quorum-1", "quorum-2": "Quorum-2", "quorum-3": "Quorum-3"}
BASE         = {"quorum-1": "classica", "quorum-2": "quorum-1", "quorum-3": "quorum-1"}   # referência de cada uma
NOTA_BASE    = ("Quorum-1: em relação à clássica (custo do tipo de fila). Quorum-2 e quorum-3: em relação à quorum-1 "
                "(custo das cópias). A quorum-3 confirma com a maioria (2 de 3).")
# ----------------------------------------------------------------------

CENARIO = "Avançado — Cenário 2: Impacto da replicação"
plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})
RNG = np.random.default_rng(42)
PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA = sys.argv[1] if len(sys.argv) > 1 else PASTA_SCRIPT
SAIDA = sys.argv[2] if len(sys.argv) > 2 else SAIDA_PADRAO


def fmt(v, casas=0):
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def ler(nome):
    p = os.path.join(PASTA, nome)
    if not os.path.isfile(p):
        print(f"[aviso] {nome} não encontrado em {os.path.abspath(PASTA)}")
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df[df["execucao"] > 0].copy()


def limite_superior(grupos):
    """Topo do eixo: o maior valor que não é ponto extremo (Tukey, 3 × IQR), com folga."""
    tops = []
    for v in grupos:
        if len(v) == 0:
            continue
        q1, q3 = np.percentile(v, [25, 75])
        corte = q3 + 3 * (q3 - q1)
        dentro = v[v <= corte]
        tops.append(dentro.max() if len(dentro) else v.max())
    return max(tops) * 1.15


def boxplot(pdf, df, coluna, ylabel, titulo, sub):
    cfgs = [c for c in ORDEM if c in set(df[COLUNA_CFG])]
    grupos = [df.loc[df[COLUNA_CFG] == c, coluna].values for c in cfgs]
    topo = limite_superior(grupos)
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.boxplot(grupos, positions=range(1, len(cfgs) + 1), widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.5),
               boxprops=dict(facecolor=COR, alpha=0.45, edgecolor=COR),
               whiskerprops=dict(color=COR), capprops=dict(color=COR))
    fora = []
    for i, v in enumerate(grupos, start=1):
        x = i + RNG.uniform(-0.08, 0.08, len(v))
        dentro = v <= topo
        ax.scatter(x[dentro], v[dentro], s=18, color="#1B6B2F", edgecolor="white", linewidth=0.5, zorder=3)
        for xv, yv in zip(x[~dentro], v[~dentro]):
            ax.scatter(xv, topo * 0.985, marker="^", s=40, color="#1B6B2F", zorder=3)
            fora.append(f"{NOMES[cfgs[i - 1]]}: {fmt(yv, 1)}")
    ax.set_xticks(range(1, len(cfgs) + 1))
    ax.set_xticklabels([NOMES[c] for c in cfgs])
    ax.grid(False, axis="x")
    ax.set_ylim(0, topo)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, p: fmt(v, 0 if topo > 50 else 1)))
    ax.set_ylabel(ylabel)
    fig.suptitle(f"{TECNOLOGIA} — {titulo}", fontsize=11.5, y=0.98)
    nota = sub + (f"  ▲ fora da escala: {'; '.join(fora)}" if fora else "")
    fig.text(0.5, 0.925, nota, ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(lat, vaz):
    linhas = []
    med = lambda df, c, col: df.loc[df[COLUNA_CFG] == c, col].median()
    for c in ORDEM:
        if c not in set(lat[COLUNA_CFG]) and c not in set(vaz[COLUNA_CFG]):
            continue
        b = BASE.get(c)
        l, v = med(lat, c, "latencia_mediana_ms"), med(vaz, c, "vazao_msg_s")
        linhas.append({
            "tecnologia": TECNOLOGIA, "configuracao": NOMES[c], "execucoes": int((lat[COLUNA_CFG] == c).sum()),
            "latencia_mediana_ms": l,
            "latencia_min_ms": lat.loc[lat[COLUNA_CFG] == c, "latencia_mediana_ms"].min(),
            "latencia_max_ms": lat.loc[lat[COLUNA_CFG] == c, "latencia_mediana_ms"].max(),
            "aumento_latencia_ms": np.nan if b is None else l - med(lat, b, "latencia_mediana_ms"),
            "aumento_latencia_pct": np.nan if b is None else (l / med(lat, b, "latencia_mediana_ms") - 1) * 100,
            "vazao_mediana_msg_s": v,
            "vazao_min_msg_s": vaz.loc[vaz[COLUNA_CFG] == c, "vazao_msg_s"].min(),
            "vazao_max_msg_s": vaz.loc[vaz[COLUNA_CFG] == c, "vazao_msg_s"].max(),
            "variacao_vazao_pct": np.nan if b is None else (v / med(vaz, b, "vazao_msg_s") - 1) * 100,
            "vazao_produtor_mediana_msg_s": med(vaz, c, "vazao_produtor_msg_s"),
            "referencia": "—" if b is None else NOMES[b],
        })
    return pd.DataFrame(linhas)


def tabela(pdf, res):
    def sinal(v, casas, suf):
        return "—" if pd.isna(v) else ("+" if v > 0 else "") + fmt(v, casas) + suf
    cel = [[r["configuracao"], fmt(r["latencia_mediana_ms"], 2),
            f"{fmt(r['latencia_min_ms'], 2)} a {fmt(r['latencia_max_ms'], 2)}",
            sinal(r["aumento_latencia_ms"], 2, " ms") + ("" if pd.isna(r["aumento_latencia_pct"])
                                                         else f" ({sinal(r['aumento_latencia_pct'], 0, '%')})"),
            fmt(r["vazao_mediana_msg_s"]), f"{fmt(r['vazao_min_msg_s'])} a {fmt(r['vazao_max_msg_s'])}",
            sinal(r["variacao_vazao_pct"], 0, "%"), fmt(r["vazao_produtor_mediana_msg_s"])]
           for _, r in res.iterrows()]
    fig, ax = plt.subplots(figsize=(11.5, 2.3 + 0.45 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Configuração", "Latência\n(mediana, ms)", "Latência\n(faixa, ms)",
                                          "Aumento da\nlatência", "Vazão\n(mediana, msg/s)", "Vazão\n(faixa, msg/s)",
                                          "Variação\nda vazão", "Vazão do\nprodutor (msg/s)"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.8)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(f"{TECNOLOGIA} — {CENARIO} — resumo", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Latência: mediana, entre as execuções, da latência mediana de cada execução (Parte A, 200 msg/s). "
             "Vazão: de ponta a ponta (Parte B).\n" + NOTA_BASE, ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.1, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    lat, vaz = ler(ARQ_LATENCIA), ler(ARQ_VAZAO)
    if lat is None or vaz is None:
        print("Nada a fazer.")
        return
    with PdfPages(saida) as pdf:
        boxplot(pdf, lat, "latencia_mediana_ms", "Latência mediana da execução (ms)",
                "latência por configuração", "Parte A: 5.000 mensagens a 200 msg/s; cada ponto = 1 execução")
        boxplot(pdf, vaz, "vazao_msg_s", "Vazão de ponta a ponta (msg/s)",
                "vazão por configuração", "Parte B: 20.000 mensagens sem controle de taxa; cada ponto = 1 execução")
        res = resumo(lat, vaz)
        tabela(pdf, res)
    res.to_csv(os.path.join(PASTA, ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
