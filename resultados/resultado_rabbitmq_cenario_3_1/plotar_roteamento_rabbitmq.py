#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível avançado — Cenário 1: Roteamento — gráficos do RABBITMQ.

Lê o CSV gerado pelo orquestrador do cenário:
    resultados-rabbitmq-roteamento.csv   (1 linha por execução, configuração e consumidor)

Gera um PDF com:
    Gráfico 1 — mensagens lidas × aceitas por consumidor, em cada configuração
    Gráfico 2 — cópias guardadas no broker por configuração (× mensagens enviadas)
    Tabela    — verificação do roteamento por configuração e consumidor (também em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_roteamento_rabbitmq.py
ou:
    python plotar_roteamento_rabbitmq.py <pasta_do_csv> [saida.pdf]

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
TECNOLOGIA    = "RabbitMQ"
ARQ_ENTRADA   = "resultados-rabbitmq-roteamento.csv"
ARQ_RESUMO    = "roteamento-rabbitmq-resumo.csv"
SAIDA_PADRAO  = "roteamento-rabbitmq.pdf"
COR           = "#D85A30"   # laranja
# nome legível de cada abordagem (o que não estiver aqui aparece como no CSV)
ABORDAGENS = {
    "direct": "exchange direct",
    "topic":  "exchange topic",
    "fanout": "exchange fanout",
}
# ----------------------------------------------------------------------

CENARIO = "Avançado — Cenário 1: Roteamento"
TOPOLOGIAS = ["direto", "curinga", "fanout"]
NOMES_TOPOLOGIA = {"direto": "Direto", "curinga": "Curinga", "fanout": "Fanout"}

plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})
PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA = sys.argv[1] if len(sys.argv) > 1 else PASTA_SCRIPT
SAIDA = sys.argv[2] if len(sys.argv) > 2 else SAIDA_PADRAO


def fmt(v, casas=0):
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def faixa(s):
    s = s.dropna()
    if s.empty:
        return "—"
    return fmt(s.min()) if s.min() == s.max() else f"{fmt(s.min())} a {fmt(s.max())}"


def ler():
    p = os.path.join(PASTA, ARQ_ENTRADA)
    if not os.path.isfile(p):
        print(f"[aviso] {ARQ_ENTRADA} não encontrado em {os.path.abspath(PASTA)}")
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df["execucao"] > 0].copy()
    df["correto"] = df["correto"].astype(str).str.strip().str.lower().isin(["true", "1"])
    return df


def configuracoes(df):
    """Lista (topologia, abordagem) na ordem das topologias e da primeira aparição."""
    out = []
    for top in TOPOLOGIAS + sorted(set(df["topologia"]) - set(TOPOLOGIAS)):
        for ab in df.loc[df["topologia"] == top, "abordagem"].drop_duplicates():
            out.append((top, ab))
    return out


def nome_cfg(top, ab):
    return f"{NOMES_TOPOLOGIA.get(top, top)} — {ABORDAGENS.get(ab, ab)}"


def estrutura(d):
    r = d.iloc[0]
    if r["topicos"] > 0:
        return f"{int(r['topicos'])} tópico" + ("s" if r["topicos"] > 1 else "")
    return (f"{int(r['exchanges'])} exchange, {int(r['filas'])} fila" + ("s" if r["filas"] > 1 else "")
            + f", {int(r['bindings'])} binding" + ("s" if r["bindings"] > 1 else ""))


def grafico1_lidas_aceitas(pdf, df):
    cfgs = configuracoes(df)
    linhas = []   # (rótulo, recebidas, aceitas, é_início_de_config)
    for top, ab in cfgs:
        d = df[(df["topologia"] == top) & (df["abordagem"] == ab)]
        primeira = True
        for cons in d["consumidor"].drop_duplicates():
            c = d[d["consumidor"] == cons]
            linhas.append((f"{nome_cfg(top, ab)}  ·  {cons}" if primeira else cons,
                           c["recebidas"].median(), c["aceitas"].median(), primeira))
            primeira = False
    n = len(linhas)
    fig, ax = plt.subplots(figsize=(11, 1.0 + 0.36 * n))
    y = np.arange(n)[::-1]
    ax.barh(y + 0.18, [l[1] for l in linhas], height=0.36, color=COR, alpha=0.30, edgecolor=COR,
            label="lidas pelo consumidor")
    ax.barh(y - 0.18, [l[2] for l in linhas], height=0.36, color=COR, alpha=0.85, label="aceitas (úteis)")
    for yi, l in zip(y, linhas):
        ax.text(l[1], yi + 0.18, " " + fmt(l[1]), va="center", fontsize=7.5)
        ax.text(l[2], yi - 0.18, " " + fmt(l[2]), va="center", fontsize=7.5)
        if l[3] and yi != y[0]:
            ax.axhline(yi + 0.55, color="#999999", linewidth=0.6)
    ax.set_yticks(y)
    ax.set_yticklabels([l[0] for l in linhas], fontsize=8)
    ax.grid(False, axis="y")
    ax.set_xlabel("Mensagens por execução (mediana das execuções)")
    ax.set_xlim(0, max(l[1] for l in linhas) * 1.15)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, p: fmt(v)))
    h = fig.get_figheight()
    hs, ls = ax.get_legend_handles_labels()
    fig.legend(hs, ls, frameon=False, loc="lower center", ncol=2, fontsize=8.5, bbox_to_anchor=(0.5, 0.02 / h))
    fig.suptitle(f"{TECNOLOGIA} — mensagens lidas × aceitas por consumidor", fontsize=11.5, y=1 - 0.12 / h)
    fig.text(0.5, 1 - 0.40 / h, "A diferença entre as barras é o que o consumidor leu e descartou por não ser para ele",
             ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0.32 / h, 1, 1 - 0.55 / h])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_copias(pdf, df):
    cfgs = configuracoes(df)
    enviadas = df["recebidas"].max()   # filtro e fanout recebem tudo o que foi enviado
    vals = [df[(df["topologia"] == t) & (df["abordagem"] == a)]["copias_no_broker"].median() for t, a in cfgs]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    x = np.arange(len(cfgs))
    ax.bar(x, vals, width=0.55, color=COR, alpha=0.7, edgecolor=COR)
    for xi, v in zip(x, vals):
        ax.text(xi, v, fmt(v), ha="center", va="bottom", fontsize=8.5)
    ax.axhline(enviadas, color="#555555", linestyle="--", linewidth=1)
    ax.set_xticks(x)
    ax.set_xticklabels([nome_cfg(t, a).replace(" — ", "\n") for t, a in cfgs], fontsize=8)
    ax.grid(False, axis="x")
    ax.set_ylim(0, max(max(vals), enviadas) * 1.18)
    ax.set_ylabel("Cópias guardadas no broker")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, p: fmt(v)))
    fig.suptitle(f"{TECNOLOGIA} — quanto o broker guarda em cada configuração", fontsize=11.5, y=0.98)
    fig.text(0.5, 0.92, f"Linha tracejada = mensagens enviadas ({fmt(enviadas)}). Acima dela: uma cópia por destino; "
             "abaixo: mensagens sem destino descartadas", ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(df):
    linhas = []
    for top, ab in configuracoes(df):
        d = df[(df["topologia"] == top) & (df["abordagem"] == ab)]
        for cons in d["consumidor"].drop_duplicates():
            c = d[d["consumidor"] == cons]
            linhas.append({
                "tecnologia": TECNOLOGIA, "topologia": top, "abordagem": ab,
                "onde_roteia": c["onde_roteia"].iloc[0], "estrutura": estrutura(c),
                "consumidor": cons, "regra": c["regra"].iloc[0], "execucoes": len(c),
                "esperadas": c["esperadas"].median(), "recebidas": c["recebidas"].median(),
                "aceitas": c["aceitas"].median(), "descartadas": c["descartadas"].median(),
                "faltando_max": c["faltando"].max(), "duplicadas_max": c["duplicadas"].max(),
                "sem_destino": c["sem_destino"].median(), "copias_no_broker": c["copias_no_broker"].median(),
                "corretas": f"{int(c['correto'].sum())}/{len(c)}",
            })
    return pd.DataFrame(linhas)


def tabela(pdf, df, res):
    cel = []
    for _, x in res.iterrows():
        d = df[(df["topologia"] == x["topologia"]) & (df["abordagem"] == x["abordagem"])
               & (df["consumidor"] == x["consumidor"])]
        cel.append([NOMES_TOPOLOGIA.get(x["topologia"], x["topologia"]), ABORDAGENS.get(x["abordagem"], x["abordagem"]),
                    x["onde_roteia"], x["estrutura"], x["consumidor"], x["regra"],
                    faixa(d["esperadas"]), faixa(d["recebidas"]), faixa(d["aceitas"]),
                    faixa(d["faltando"]), faixa(d["duplicadas"]), x["corretas"]])
    fig, ax = plt.subplots(figsize=(11.7, 1.6 + 0.30 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Topologia", "Abordagem", "Onde\nroteia", "Estrutura", "Consumidor", "Regra",
                                          "Esperadas", "Lidas", "Aceitas", "Faltando", "Duplicadas", "Corretas"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(7.6)
    t.scale(1, 1.45)
    t.auto_set_column_width(list(range(12)))
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(f"{TECNOLOGIA} — {CENARIO} — verificação", fontsize=11, pad=8)
    fig.text(0.5, 0.015, "Valores por execução ('a' = menor a maior entre as execuções). "
             "Corretas = execuções em que o consumidor aceitou exatamente as mensagens esperadas, sem faltas nem duplicatas.",
             ha="center", fontsize=7.5, color="#555555")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    df = ler()
    if df is None or df.empty:
        print("Nada a fazer.")
        return
    with PdfPages(saida) as pdf:
        grafico1_lidas_aceitas(pdf, df)
        grafico2_copias(pdf, df)
        res = resumo(df)
        tabela(pdf, df, res)
    res.to_csv(os.path.join(PASTA, ARQ_RESUMO), index=False, float_format="%.0f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
