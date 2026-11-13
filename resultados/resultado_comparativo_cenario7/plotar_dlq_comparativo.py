#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 3: Dead Letter Queue — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — vazão com 0% e com 10% de inválidas, as duas tecnologias lado a lado
    Gráfico 2 — quanto a vazão cai com 10% de inválidas, em cada tecnologia
    Tabela    — verificação do isolamento e resumo (também gravado em CSV)

Organização esperada (o script numa pasta própria, ao lado das pastas de resultados):

    TCC\\
    ├── comparativo_cenario2_3\\
    │   └── plotar_dlq_comparativo.py
    ├── resultado_cenario_2_3\\              (Kafka)
    └── resultado_rabbitmq_cenario_2_3\\     (RabbitMQ)

Também aceita os caminhos como argumentos:
    python plotar_dlq_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

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
    "Kafka":    ("resultado_cenario_2_3",          "resultados-kafka-dlq.csv",    "#185FA5", "#D85A30"),
    "RabbitMQ": ("resultado_rabbitmq_cenario_2_3", "resultados-rabbitmq-dlq.csv", "#D85A30", "#1B6B2F"),
}
SAIDA = "dlq-comparativo.pdf"
ARQ_RESUMO = "dlq-comparativo-resumo.csv"

plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})
RNG = np.random.default_rng(42)


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
    args = sys.argv[1:]
    dados = {}
    for i, (tec, (pasta_nome, arq, _, _)) in enumerate(TECNOLOGIAS.items()):
        pasta = args[i] if len(args) >= 2 else localizar(pasta_nome, arq)
        if pasta is None or not os.path.isfile(os.path.join(pasta, arq)):
            continue
        df = pd.read_csv(os.path.join(pasta, arq), encoding="utf-8-sig")
        df.columns = [str(c).strip() for c in df.columns]
        df = df[df["execucao"] > 0].copy()
        for c in ("dlq_confere", "metadados_ok"):
            df[c] = df[c].astype(str).str.strip().str.lower().isin(["true", "1"])
        dados[tec] = df
        print(f"{tec}: {os.path.abspath(pasta)}")
    return dados


def titulo(fig, texto, sub):
    fig.suptitle(texto, fontsize=11.5, y=0.975)
    fig.text(0.5, 0.975 - 0.34 / fig.get_figheight(), sub, ha="center", fontsize=8.5, color="#555555")


def grafico1_vazao(pdf, dados):
    ts = sorted(set().union(*[set(d["taxa_invalidas_pct"]) for d in dados.values()]))
    fig, ax = plt.subplots(figsize=(8, 5.6))
    larg = 0.34
    for k, (tec, df) in enumerate(dados.items()):
        _, _, cor, cor_ponto = TECNOLOGIAS[tec]
        for j, t in enumerate(ts, start=1):
            v = df.loc[df["taxa_invalidas_pct"] == t, "vazao_msg_s"].values
            if len(v) == 0:
                continue
            x = j + (k - 0.5) * 0.4
            ax.boxplot([v], positions=[x], widths=larg, patch_artist=True, showfliers=False,
                       medianprops=dict(color="black", linewidth=1.5),
                       boxprops=dict(facecolor=cor, alpha=0.5, edgecolor=cor),
                       whiskerprops=dict(color=cor), capprops=dict(color=cor))
            ax.scatter(x + RNG.uniform(-0.05, 0.05, len(v)), v, s=18, color=cor_ponto,
                       edgecolor="white", linewidth=0.5, zorder=3)
        ax.plot([], [], color=cor, linewidth=8, alpha=0.5, label=tec)
    ax.set_xticks(range(1, len(ts) + 1))
    ax.set_xticklabels([f"{t}% inválidas" for t in ts])
    ax.set_xlim(0.4, len(ts) + 0.6)
    ax.grid(False, axis="x")
    ax.set_ylim(bottom=0)
    ax.set_ylabel("Vazão de ponta a ponta (msg/s)")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", loc="lower right")
    titulo(fig, "Dead Letter Queue — vazão com e sem mensagens inválidas",
           "Cada ponto = 1 execução; do primeiro envio até o fim do tratamento da última mensagem")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def variacoes(dados):
    out = {}
    for tec, df in dados.items():
        base = df.loc[df["taxa_invalidas_pct"] == 0, "vazao_msg_s"].median()
        for t in sorted(df["taxa_invalidas_pct"].unique()):
            if t == 0 or np.isnan(base):
                continue
            out.setdefault(t, {})[tec] = (df.loc[df["taxa_invalidas_pct"] == t, "vazao_msg_s"].median() / base - 1) * 100
    return out


def grafico2_custo(pdf, dados):
    var = variacoes(dados)
    if not var:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    t = sorted(var)[0]
    tecs = list(var[t].keys())
    for i, tec in enumerate(tecs):
        v = var[t][tec]
        ax.bar(i, v, width=0.5, color=TECNOLOGIAS[tec][2], alpha=0.75)
        ax.text(i, v, f"{fmt(v, 1)}%", ha="center", va="top" if v < 0 else "bottom", fontsize=9.5)
    ax.axhline(0, color="#555555", linewidth=0.8)
    ax.set_xticks(range(len(tecs)))
    ax.set_xticklabels(tecs)
    ax.grid(False, axis="x")
    ax.set_ylabel(f"Variação da vazão com {t}% de inválidas (%)")
    titulo(fig, "Dead Letter Queue — custo do tratamento das inválidas",
           f"(vazão mediana com {t}% ÷ vazão mediana com 0%) − 1")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(dados):
    linhas = []
    var = variacoes(dados)
    for tec, df in dados.items():
        for t in sorted(df["taxa_invalidas_pct"].unique()):
            d = df[df["taxa_invalidas_pct"] == t]
            linhas.append({
                "tecnologia": tec, "taxa_invalidas_pct": t, "execucoes": len(d),
                "vazao_mediana_msg_s": d["vazao_msg_s"].median(),
                "vazao_min_msg_s": d["vazao_msg_s"].min(), "vazao_max_msg_s": d["vazao_msg_s"].max(),
                "variacao_vs_0pct": var.get(t, {}).get(tec, np.nan),
                "na_dlq_mediana": d["na_dlq"].median(),
                "dlq_confere": f"{int(d['dlq_confere'].sum())}/{len(d)}",
                "metadados_ok": f"{int(d['metadados_ok'].sum())}/{len(d)}",
                # com 0% não há inválidas: a razão não existe (evita o aviso "Mean of empty slice")
                "tentativas_por_invalida": (d["tentativas_invalidas"] / d["invalidas_enviadas"]).median()
                                           if t > 0 else np.nan,
            })
    return pd.DataFrame(linhas).sort_values(["taxa_invalidas_pct", "tecnologia"]).reset_index(drop=True)


def tabela_resumo(pdf, res):
    cel = []
    for _, r in res.iterrows():
        t = int(r["taxa_invalidas_pct"])
        cel.append([f"{t}%", r["tecnologia"], fmt(r["vazao_mediana_msg_s"]),
                    f"{fmt(r['vazao_min_msg_s'])} a {fmt(r['vazao_max_msg_s'])}",
                    "—" if t == 0 or np.isnan(r["variacao_vs_0pct"]) else f"{fmt(r['variacao_vs_0pct'], 1)}%",
                    fmt(r["na_dlq_mediana"]), r["dlq_confere"], r["metadados_ok"] if t > 0 else "—",
                    "—" if np.isnan(r["tentativas_por_invalida"]) else fmt(r["tentativas_por_invalida"], 1)])
    fig, ax = plt.subplots(figsize=(10.5, 2.4 + 0.4 * len(cel)))
    ax.axis("off")
    tb = ax.table(cellText=cel, colLabels=["Inválidas", "Tecnologia", "Vazão\n(mediana, msg/s)", "Vazão\n(menor a maior)",
                                           "Variação vs.\n0%", "Na DLQ\n(mediana)", "DLQ\nconfere",
                                           "Dados de\ndiagnóstico", "Tentativas\npor inválida"],
                  loc="center", cellLoc="center")
    tb.auto_set_font_size(False)
    tb.set_fontsize(8.5)
    tb.scale(1, 1.6)
    for (rr, _), c in tb.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Dead Letter Queue — resumo Kafka × RabbitMQ", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "'DLQ confere' = execuções em que a DLQ tinha exatamente as inválidas enviadas. "
             "Dados de diagnóstico: headers da aplicação (Kafka) ou x-death do broker (RabbitMQ).",
             ha="center", fontsize=8, color="#555555")
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
        grafico1_vazao(pdf, dados)
        grafico2_custo(pdf, dados)
        res = resumo(dados)
        tabela_resumo(pdf, res)
    res.to_csv(os.path.join(os.path.dirname(saida), ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()