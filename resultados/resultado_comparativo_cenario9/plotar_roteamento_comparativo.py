#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível avançado — Cenário 1: Roteamento — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — mensagens lidas × úteis (soma dos consumidores), por topologia e abordagem
    Gráfico 2 — cópias guardadas no broker, por topologia e abordagem
    Tabela    — resumo das duas tecnologias (também gravado em CSV)

Organização esperada (o script numa pasta própria, ao lado das pastas de resultados):

    TCC\\
    ├── comparativo_cenario3_1\\
    │   └── plotar_roteamento_comparativo.py
    ├── resultado_cenario_3_1\\              (Kafka)
    └── resultado_rabbitmq_cenario_3_1\\     (RabbitMQ)

Também aceita os caminhos como argumentos:
    python plotar_roteamento_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

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

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA_ACIMA = os.path.dirname(PASTA_SCRIPT)
TECNOLOGIAS = {
    "RabbitMQ": ("resultado_rabbitmq_cenario_3_1", "resultados-rabbitmq-roteamento.csv", "#D85A30"),
    "Kafka":    ("resultado_cenario_3_1",          "resultados-kafka-roteamento.csv",    "#185FA5"),
}
ABORDAGENS = {
    "direct": "exchange direct", "topic": "exchange topic", "fanout": "exchange fanout",
    "filtro": "filtro no consumidor", "topicos": "um tópico por região",
    "padrao": "tópicos + padrão", "grupos": "grupos de consumidores",
}
TOPOLOGIAS = ["direto", "curinga", "fanout"]
NOMES_TOPOLOGIA = {"direto": "Direto", "curinga": "Curinga", "fanout": "Fanout"}
SAIDA = "roteamento-comparativo.pdf"
ARQ_RESUMO = "roteamento-comparativo-resumo.csv"

plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})


def fmt(v, casas=0):
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def localizar(nome_pasta, nome_csv):
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        pasta = os.path.join(base, nome_pasta)
        if os.path.isfile(os.path.join(pasta, nome_csv)):
            return pasta
    print(f"[aviso] '{nome_csv}' não encontrado em {os.path.join(PASTA_ACIMA, nome_pasta)}")
    return None


def carregar():
    """Lê os dois CSVs na ordem: argumento 1 = Kafka, argumento 2 = RabbitMQ."""
    args = sys.argv[1:]
    pastas_arg = {"Kafka": args[0], "RabbitMQ": args[1]} if len(args) >= 2 else {}
    dados = []
    for tec, (pasta_nome, arq, _) in TECNOLOGIAS.items():
        pasta = pastas_arg.get(tec) or localizar(pasta_nome, arq)
        if pasta is None or not os.path.isfile(os.path.join(pasta, arq)):
            continue
        df = pd.read_csv(os.path.join(pasta, arq), encoding="utf-8-sig")
        df.columns = [str(c).strip() for c in df.columns]
        df = df[df["execucao"] > 0].copy()
        df["correto"] = df["correto"].astype(str).str.strip().str.lower().isin(["true", "1"])
        df["tecnologia"] = tec
        dados.append(df)
        print(f"{tec}: {os.path.abspath(pasta)}")
    return pd.concat(dados, ignore_index=True) if dados else None


def estrutura(r):
    if r["topicos"] > 0:
        return f"{int(r['topicos'])} tópico" + ("s" if r["topicos"] > 1 else "")
    return f"{int(r['exchanges'])} exchange, {int(r['filas'])} fila" + ("s" if r["filas"] > 1 else "")


def resumo(df):
    """Uma linha por tecnologia, topologia e abordagem; totais somados entre os consumidores."""
    linhas = []
    enviadas = df["recebidas"].max()   # filtro e fanout recebem tudo o que foi enviado
    for top in TOPOLOGIAS + sorted(set(df["topologia"]) - set(TOPOLOGIAS)):
        for tec in TECNOLOGIAS:
            d = df[(df["topologia"] == top) & (df["tecnologia"] == tec)]
            for ab in d["abordagem"].drop_duplicates():
                c = d[d["abordagem"] == ab]
                por_exec = c.groupby("execucao").agg(lidas=("recebidas", "sum"), uteis=("aceitas", "sum"),
                                                     copias=("copias_no_broker", "first"),
                                                     sem=("sem_destino", "first"),
                                                     ok=("correto", "all"))
                lidas, uteis = por_exec["lidas"].median(), por_exec["uteis"].median()
                copias, sem = por_exec["copias"].median(), por_exec["sem"].median()
                if sem == 0:
                    destino_sem = "—"
                else:
                    destino_sem = "descartadas pelo broker" if copias < enviadas else "guardadas no broker"
                r0 = c.iloc[0]
                linhas.append({
                    "topologia": top, "tecnologia": tec, "abordagem": ab, "onde_roteia": r0["onde_roteia"],
                    "estrutura": estrutura(r0), "consumidores": c["consumidor"].nunique(),
                    "execucoes": len(por_exec), "lidas_total": lidas, "uteis_total": uteis,
                    "lidas_por_util": lidas / uteis if uteis else np.nan,
                    "sem_destino": sem, "sem_destino_vao_para": destino_sem,
                    "copias_no_broker": copias, "copias_por_enviada": copias / enviadas,
                    "corretas": f"{int(por_exec['ok'].sum())}/{len(por_exec)}",
                })
    return pd.DataFrame(linhas), enviadas


def rotulo(x):
    return f"{x['tecnologia']}\n{ABORDAGENS.get(x['abordagem'], x['abordagem'])}"


def grafico1_lidas_uteis(pdf, res):
    tops = [t for t in TOPOLOGIAS if t in set(res["topologia"])]
    fig, eixos = plt.subplots(1, len(tops), figsize=(13, 5.2), squeeze=False)
    for ax, top in zip(eixos[0], tops):
        r = res[res["topologia"] == top].reset_index(drop=True)
        x = np.arange(len(r))
        for i, row in r.iterrows():
            cor = TECNOLOGIAS[row["tecnologia"]][2]
            ax.bar(x[i] - 0.19, row["lidas_total"], width=0.38, color=cor, alpha=0.28, edgecolor=cor)
            ax.bar(x[i] + 0.19, row["uteis_total"], width=0.38, color=cor, alpha=0.85)
            ax.text(x[i] - 0.19, row["lidas_total"], fmt(row["lidas_total"]), ha="center", va="bottom", fontsize=7.3)
            ax.text(x[i] + 0.19, row["uteis_total"], fmt(row["uteis_total"]), ha="center", va="bottom", fontsize=7.3)
        ax.set_xticks(x)
        ax.set_xticklabels([rotulo(row) for _, row in r.iterrows()], fontsize=7.8)
        ax.grid(False, axis="x")
        ax.set_title(NOMES_TOPOLOGIA.get(top, top), fontsize=10.5)
        ax.set_ylim(0, res["lidas_total"].max() * 1.14)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, p: fmt(v)))
    eixos[0][0].set_ylabel("Mensagens por execução (soma dos consumidores)")
    from matplotlib.patches import Patch
    eixos[0][-1].legend(handles=[Patch(facecolor="#777777", alpha=0.28, edgecolor="#777777", label="lidas"),
                                 Patch(facecolor="#777777", alpha=0.85, label="úteis (aceitas)")],
                        frameon=True, facecolor="white", edgecolor="none", fontsize=8.5, loc="upper right")
    fig.suptitle("Roteamento — quanto cada consumidor precisa ler para receber o que é dele", fontsize=11.5, y=0.99)
    fig.text(0.5, 0.935, "Barra clara = mensagens lidas; barra escura = mensagens úteis. "
             "Diferença = lido e descartado pelo próprio consumidor. Mediana das execuções",
             ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_copias(pdf, res, enviadas):
    tops = [t for t in TOPOLOGIAS if t in set(res["topologia"])]
    fig, eixos = plt.subplots(1, len(tops), figsize=(13, 5.0), squeeze=False, sharey=True)
    for ax, top in zip(eixos[0], tops):
        r = res[res["topologia"] == top].reset_index(drop=True)
        x = np.arange(len(r))
        for i, row in r.iterrows():
            cor = TECNOLOGIAS[row["tecnologia"]][2]
            ax.bar(x[i], row["copias_no_broker"], width=0.55, color=cor, alpha=0.7, edgecolor=cor)
            ax.text(x[i], row["copias_no_broker"], fmt(row["copias_no_broker"]), ha="center", va="bottom", fontsize=8)
        ax.axhline(enviadas, color="#555555", linestyle="--", linewidth=1)
        ax.set_xticks(x)
        ax.set_xticklabels([rotulo(row) for _, row in r.iterrows()], fontsize=7.8)
        ax.grid(False, axis="x")
        ax.set_title(NOMES_TOPOLOGIA.get(top, top), fontsize=10.5)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, p: fmt(v)))
    eixos[0][0].set_ylabel("Cópias guardadas no broker")
    eixos[0][0].set_ylim(0, max(res["copias_no_broker"].max(), enviadas) * 1.15)
    fig.suptitle("Roteamento — quanto o broker guarda", fontsize=11.5, y=0.99)
    fig.text(0.5, 0.935, f"Linha tracejada = mensagens enviadas ({fmt(enviadas)}). Acima: uma cópia por destino; "
             "abaixo: mensagens sem destino descartadas pelo broker", ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    pdf.savefig(fig)
    plt.close(fig)


def tabela(pdf, res):
    cel = []
    for _, x in res.iterrows():
        cel.append([NOMES_TOPOLOGIA.get(x["topologia"], x["topologia"]), x["tecnologia"],
                    ABORDAGENS.get(x["abordagem"], x["abordagem"]), x["onde_roteia"], x["estrutura"],
                    fmt(x["lidas_total"]), fmt(x["uteis_total"]), fmt(x["lidas_por_util"], 1).replace(",0", ""),
                    fmt(x["sem_destino"]) if x["sem_destino"] else "0",
                    x["sem_destino_vao_para"], fmt(x["copias_no_broker"]), x["corretas"]])
    fig, ax = plt.subplots(figsize=(12.2, 1.9 + 0.36 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Topologia", "Tecnologia", "Abordagem", "Onde\nroteia", "Estrutura",
                                          "Lidas", "Úteis", "Lidas\npor útil", "Sem\ndestino", "Sem destino\nvão para",
                                          "Cópias no\nbroker", "Corretas"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8)
    t.scale(1, 1.75)
    t.auto_set_column_width(list(range(12)))
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Roteamento — resumo Kafka × RabbitMQ", fontsize=11, pad=8)
    fig.text(0.5, 0.02, "Lidas e úteis: soma dos consumidores, mediana das execuções. "
             "Lidas por útil = quantas mensagens o consumidor leu para cada uma que era dele (1 = nenhum desperdício).\n"
             "Corretas = execuções em que todos os consumidores aceitaram exatamente as mensagens esperadas, "
             "sem faltas nem duplicatas.", ha="center", fontsize=7.5, color="#555555")
    fig.tight_layout(rect=[0, 0.07, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    df = carregar()
    if df is None or df.empty:
        print("Nenhum CSV encontrado. Nada a fazer.")
        return
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, SAIDA)
    res, enviadas = resumo(df)
    with PdfPages(saida) as pdf:
        grafico1_lidas_uteis(pdf, res)
        grafico2_copias(pdf, res, enviadas)
        tabela(pdf, res)
    res.to_csv(os.path.join(os.path.dirname(os.path.abspath(saida)), ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
