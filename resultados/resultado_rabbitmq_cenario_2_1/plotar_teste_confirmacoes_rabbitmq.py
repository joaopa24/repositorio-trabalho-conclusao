#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Intermediário — Cenário 1 — Parte C: teste de confirmações do RABBITMQ.

Lê o CSV gerado pela Parte C do orquestrador Cenario1ComparacaoGarantias:
    resultados-rabbitmq-garantias-teste-confirmacoes.csv

Variações (todas com 20.000 mensagens de 1 KB, sem controle de taxa):
    v1  sem confirms | autoAck            (= at most once)
    v2  com confirms | autoAck            (só os confirms)
    v3  sem confirms | ACK por mensagem   (só o ACK manual)
    v4  com confirms | ACK por mensagem   (= at least once)
    v5  com confirms | ACK em lote        (multiple=true a cada 50)

Gera um PDF com:
    Gráfico 1 — vazão ponta a ponta por variação (1 caixa = N execuções)
    Gráfico 2 — vazão do produtor por variação
    Tabela 1  — mediana, quartis e diferença em relação à v1
    Tabela 2  — contrastes: o efeito de cada mudança isolada, com o critério
                das caixas (os intervalos interquartis se sobrepõem ou não)

Também grava as duas tabelas em CSV e a figura para o TCC em PNG
(teste-confirmacoes-rabbitmq-vazao.png: Gráfico 1 sem título, 200 dpi, no
mesmo tamanho das outras figuras do trabalho).

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_teste_confirmacoes_rabbitmq.py
ou:
    python plotar_teste_confirmacoes_rabbitmq.py <pasta_do_csv> [saida.pdf]

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

ARQ_TESTE      = "resultados-rabbitmq-garantias-teste-confirmacoes.csv"
ARQ_RESUMO     = "teste-confirmacoes-rabbitmq-resumo.csv"
ARQ_CONTRASTES = "teste-confirmacoes-rabbitmq-contrastes.csv"
SAIDA_PADRAO   = "teste-confirmacoes-rabbitmq-boxplot.pdf"
ARQ_FIGURA_TCC = "teste-confirmacoes-rabbitmq-vazao.png"
COR_CAIXA      = "#D85A30"   # laranja (mesma cor do RabbitMQ nos outros cenários)
COR_PONTO      = "#8A3313"

ORDEM = ["v1-semconfirms-auto", "v2-confirms-auto", "v3-semconfirms-manual",
         "v4-confirms-manual", "v5-confirms-lote"]
ROTULOS = {
    "v1-semconfirms-auto":   "v1\nsem confirms\nautoAck",
    "v2-confirms-auto":      "v2\ncom confirms\nautoAck",
    "v3-semconfirms-manual": "v3\nsem confirms\nACK por msg",
    "v4-confirms-manual":    "v4\ncom confirms\nACK por msg",
    "v5-confirms-lote":      "v5\ncom confirms\nACK em lote",
}
CURTO = {v: v.split("-")[0] for v in ORDEM}

# (nome do efeito, variação de base, variação comparada)
CONTRASTES = [
    ("Só os confirms (com autoAck)",            "v1-semconfirms-auto",   "v2-confirms-auto"),
    ("Só o ACK por mensagem (sem confirms)",    "v1-semconfirms-auto",   "v3-semconfirms-manual"),
    ("Os dois juntos (at most → at least once)", "v1-semconfirms-auto",  "v4-confirms-manual"),
    ("Confirms, já com ACK por mensagem",       "v3-semconfirms-manual", "v4-confirms-manual"),
    ("ACK por mensagem, já com confirms",       "v2-confirms-auto",      "v4-confirms-manual"),
    ("ACK em lote no lugar do ACK por mensagem", "v4-confirms-manual",   "v5-confirms-lote"),
]


def q(s, p):
    return float(np.percentile(s, p))


def resumo(df):
    base = df[df.variacao == ORDEM[0]].vazao_msg_s.median()
    linhas = []
    for v in ORDEM:
        d = df[df.variacao == v]
        if d.empty:
            continue
        s, sp = d.vazao_msg_s, d.vazao_produtor_msg_s
        linhas.append({
            "variacao": v,
            "confirms": d.confirms.iloc[0],
            "modo_ack": d.modo_ack.iloc[0],
            "execucoes": len(d),
            "vazao_mediana_msg_s": s.median(),
            "vazao_q1_msg_s": q(s, 25),
            "vazao_q3_msg_s": q(s, 75),
            "dif_vs_v1_pct": (s.median() / base - 1) * 100,
            "produtor_mediana_msg_s": sp.median(),
            "duplicatas_total": int(d.duplicatas.sum()),
        })
    return pd.DataFrame(linhas)


def contrastes(df):
    linhas = []
    for nome, a, b in CONTRASTES:
        sa = df[df.variacao == a].vazao_msg_s
        sb = df[df.variacao == b].vazao_msg_s
        if sa.empty or sb.empty:
            continue
        qa, qb = (q(sa, 25), q(sa, 75)), (q(sb, 25), q(sb, 75))
        sobrepoe = not (qa[1] < qb[0] or qb[1] < qa[0])
        linhas.append({
            "efeito": nome,
            "de": CURTO[a], "para": CURTO[b],
            "mediana_de": sa.median(), "mediana_para": sb.median(),
            "dif_pct": (sb.median() / sa.median() - 1) * 100,
            "caixas_se_sobrepoem": "sim" if sobrepoe else "não",
        })
    return pd.DataFrame(linhas)


def boxplot(ax, df, coluna, titulo, anotar=True):
    vs = [v for v in ORDEM if v in set(df.variacao)]
    dados = [df[df.variacao == v][coluna].values for v in vs]
    ax.boxplot(dados, widths=0.55, patch_artist=True,
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.45, edgecolor=COR_PONTO),
               medianprops=dict(color=COR_PONTO, linewidth=2),
               whiskerprops=dict(color=COR_PONTO), capprops=dict(color=COR_PONTO),
               flierprops=dict(marker="o", markersize=4, markerfacecolor="none",
                               markeredgecolor=COR_PONTO))
    rng = np.random.default_rng(0)
    for i, d in enumerate(dados, start=1):
        ax.scatter(i + rng.uniform(-0.12, 0.12, len(d)), d, s=12, color=COR_PONTO, zorder=3)
        if not anotar:
            continue
        ax.annotate(f"{np.median(d):,.0f}".replace(",", "."), (i, np.median(d)),
                    xytext=(42, 0), textcoords="offset points", va="center", fontsize=8,
                    color=COR_PONTO)
    ax.set_xticks(range(1, len(vs) + 1))
    ax.set_xticklabels([ROTULOS[v] for v in vs], fontsize=8)
    ax.set_ylabel("Vazão (msg/s)")
    ax.set_ylim(bottom=0)
    if titulo:
        ax.set_title(titulo, fontsize=11)
    ax.grid(axis="y", alpha=0.3)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:,.0f}".replace(",", ".")))


def tabela(pdf, df, titulo, colunas, fmt):
    fig, ax = plt.subplots(figsize=(11, 0.6 + 0.45 * (len(df) + 1)))
    ax.axis("off")
    celulas = [[fmt.get(c, str)(r[c]) for c in colunas] for _, r in df.iterrows()]
    # largura de cada coluna proporcional ao texto mais longo dela
    larg = [max(len(str(c)), *(len(l[i]) for l in celulas)) for i, c in enumerate(colunas)]
    t = ax.table(cellText=celulas, colLabels=colunas, loc="center", cellLoc="center",
                 colWidths=[w / sum(larg) for w in larg])
    t.auto_set_font_size(False)
    t.set_fontsize(8)
    t.scale(1, 1.4)
    ax.set_title(titulo, fontsize=11)
    pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def main():
    pasta = sys.argv[1] if len(sys.argv) > 1 else "."
    saida = sys.argv[2] if len(sys.argv) > 2 else os.path.join(pasta, SAIDA_PADRAO)
    df = pd.read_csv(os.path.join(pasta, ARQ_TESTE))

    desconhecidas = set(df.variacao) - set(ORDEM)
    if desconhecidas:
        sys.exit(f"Variações desconhecidas no CSV: {sorted(desconhecidas)}")

    r = resumo(df)
    c = contrastes(df)
    r.to_csv(os.path.join(pasta, ARQ_RESUMO), index=False, float_format="%.2f")
    c.to_csv(os.path.join(pasta, ARQ_CONTRASTES), index=False, float_format="%.2f")

    num = lambda x: f"{x:,.0f}".replace(",", ".")
    pct = lambda x: f"{x:+.1f}%".replace(".", ",")

    with PdfPages(saida) as pdf:
        fig, ax = plt.subplots(figsize=(10, 5.5))
        boxplot(ax, df, "vazao_msg_s",
                f"RabbitMQ — vazão ponta a ponta por variação ({df.num_msgs.iloc[0]:,} mensagens de 1 KB)".replace(",", "."))
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(10, 5.5))
        boxplot(ax, df, "vazao_produtor_msg_s", "RabbitMQ — vazão do produtor por variação")
        pdf.savefig(fig, bbox_inches="tight")
        plt.close(fig)

        r2 = r.assign(variacao=r.variacao.map(CURTO))
        tabela(pdf, r2, "Tabela 1 — Resumo por variação (vazão ponta a ponta)",
               ["variacao", "confirms", "modo_ack", "execucoes", "vazao_mediana_msg_s",
                "vazao_q1_msg_s", "vazao_q3_msg_s", "dif_vs_v1_pct", "produtor_mediana_msg_s"],
               {"vazao_mediana_msg_s": num, "vazao_q1_msg_s": num, "vazao_q3_msg_s": num,
                "dif_vs_v1_pct": pct, "produtor_mediana_msg_s": num})
        tabela(pdf, c, "Tabela 2 — Efeito de cada mudança (diferença das medianas)",
               ["efeito", "de", "para", "mediana_de", "mediana_para", "dif_pct", "caixas_se_sobrepoem"],
               {"mediana_de": num, "mediana_para": num, "dif_pct": pct})

    # figura para o TCC: sem título (a legenda fica no Word), 7,2 × 4,2 pol a 200 dpi
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    boxplot(ax, df, "vazao_msg_s", None, anotar=False)
    ax.set_ylabel("Vazão de ponta a ponta (msg/s)")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    figura = os.path.join(pasta, ARQ_FIGURA_TCC)
    fig.savefig(figura, dpi=200, bbox_inches="tight")
    plt.close(fig)

    pd.set_option("display.width", 160)
    print("\nResumo por variação:")
    print(r.to_string(index=False, float_format=lambda x: f"{x:.1f}"))
    print("\nContrastes:")
    print(c.to_string(index=False, float_format=lambda x: f"{x:.1f}"))
    if r.duplicatas_total.sum() > 0:
        print("\nAviso: houve duplicatas, o que não deveria acontecer sem falha provocada.")
    print(f"\nPDF: {saida}")
    print(f"Figura para o TCC: {figura}")


if __name__ == "__main__":
    main()