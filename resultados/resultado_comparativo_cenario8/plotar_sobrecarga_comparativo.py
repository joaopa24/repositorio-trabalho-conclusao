#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 4: Sobrecarga prolongada — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — mensagens na fila do broker ao longo do tempo (2 × 3: tecnologia × política)
    Gráfico 2 — taxa de produção efetiva e mensagens perdidas, por política
    Tabela    — resumo das duas tecnologias (também gravado em CSV)

Organização esperada (o script numa pasta própria, ao lado das pastas de resultados):

    TCC\\
    ├── comparativo_cenario2_4\\
    │   └── plotar_sobrecarga_comparativo.py
    ├── resultado_cenario_2_4\\              (Kafka)
    └── resultado_rabbitmq_cenario_2_4\\     (RabbitMQ)

Também aceita os caminhos como argumentos:
    python plotar_sobrecarga_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

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
    "Kafka":    ("resultado_cenario_2_4",          "resultados-kafka-sobrecarga",    "#185FA5"),
    "RabbitMQ": ("resultado_rabbitmq_cenario_2_4", "resultados-rabbitmq-sobrecarga", "#D85A30"),
}
# "com-limite" é o mesmo mecanismo nas duas (descarte das mais antigas);
# "com-limite-recusa" só existe no RabbitMQ (o broker recusa e o produtor reenvia)
POLITICAS = ["sem-limite", "com-limite", "com-limite-recusa"]
NOMES = {"sem-limite": "sem limite", "com-limite": "com limite (descarte)",
         "com-limite-recusa": "com limite (recusa)"}
SAIDA = "sobrecarga-comparativo.pdf"
ARQ_RESUMO = "sobrecarga-comparativo-resumo.csv"

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


def ler(p):
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df[df["execucao"] > 0].copy()


def carregar():
    args = sys.argv[1:]
    dados = {}
    for i, (tec, (pasta_nome, prefixo, _)) in enumerate(TECNOLOGIAS.items()):
        pasta = args[i] if len(args) >= 2 else localizar(pasta_nome, f"{prefixo}.csv")
        if pasta is None or not os.path.isfile(os.path.join(pasta, f"{prefixo}.csv")):
            continue
        dados[tec] = {"res": ler(os.path.join(pasta, f"{prefixo}.csv")),
                      "serie": ler(os.path.join(pasta, f"{prefixo}-serie.csv"))}
        print(f"{tec}: {os.path.abspath(pasta)}")
    return dados


def grafico1_filas(pdf, dados):
    fig, eixos = plt.subplots(len(dados), len(POLITICAS), figsize=(13, 3.8 * len(dados)), sharex=True,
                              squeeze=False)
    ymax = max(d["serie"]["fila"].max() for d in dados.values()) * 1.05
    for i, (tec, d) in enumerate(dados.items()):
        cor = TECNOLOGIAS[tec][2]
        for j, pol in enumerate(POLITICAS):
            ax = eixos[i][j]
            s = d["serie"][d["serie"]["politica"] == pol]
            if s.empty:
                ax.text(0.5, 0.5, f"Não existe no {tec}" if pol == "com-limite-recusa" else "Sem dados",
                        ha="center", va="center", transform=ax.transAxes, fontsize=10, color="#777777")
            for ex, g in s.groupby("execucao"):
                primeira = ex == s["execucao"].min()
                ax.plot(g["tempo_s"], g["fila"], color=cor, alpha=1 if primeira else 0.35,
                        linewidth=1.5 if primeira else 0.9)
            ax.set_title(f"{tec} — {NOMES[pol]}", fontsize=10.5)
            ax.set_ylim(0, ymax)
            if j == 0:
                ax.set_ylabel("Mensagens na fila do broker")
            if i == len(dados) - 1:
                ax.set_xlabel("Tempo desde o início do envio (s)")
    fig.suptitle("Sobrecarga prolongada — mensagens acumuladas no broker", fontsize=11.5, y=0.985)
    fig.text(0.5, 0.945, "Produtor a 1.000 msg/s por 20 s; consumidor a 5 ms por mensagem. Linha forte = 1ª execução; "
             "mesma escala em todos os painéis", ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_barras(pdf, dados):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    larg = 0.36
    for k, (tec, d) in enumerate(dados.items()):
        cor = TECNOLOGIAS[tec][2]
        r = d["res"]
        for j, pol in enumerate(POLITICAS, start=1):
            x = j + (k - 0.5) * larg
            for ax, col in ((ax1, "taxa_producao_efetiva_msg_s"), (ax2, "perdidas")):
                v = r.loc[r["politica"] == pol, col].values
                if len(v) == 0:
                    continue
                ax.bar(x, np.median(v), width=larg, color=cor, alpha=0.55, edgecolor=cor,
                       label=tec if (j == 1) else None)
                ax.scatter(x + RNG.uniform(-0.05, 0.05, len(v)), v, s=16, color="black", zorder=3)
                ax.text(x, np.median(v), fmt(np.median(v)), ha="center", va="bottom", fontsize=8)
    for ax in (ax1, ax2):
        ax.set_xticks(range(1, len(POLITICAS) + 1))
        ax.set_xticklabels([NOMES[p].replace(" (", "\n(") for p in POLITICAS])
        ax.grid(False, axis="x")
        ax.set_ylim(bottom=0)
    ax1.axhline(1000, color="#555555", linestyle="--", linewidth=1)
    ax1.set_title("Taxa de produção efetiva (alvo = 1.000 msg/s)", fontsize=10.5)
    ax1.set_ylabel("msg/s")
    ax2.set_title("Mensagens perdidas pelo consumidor", fontsize=10.5)
    ax2.set_ylabel("Mensagens por execução")
    ax1.legend(frameon=True, facecolor="white", edgecolor="none", loc="upper right")
    fig.suptitle("Sobrecarga prolongada — o produtor foi freado? Alguém perdeu mensagens?", fontsize=11.5, y=0.98)
    fig.text(0.5, 0.92, "Barra = mediana das execuções; cada ponto = 1 execução", ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(dados):
    linhas = []
    for pol in POLITICAS:
        for tec, d in dados.items():
            r = d["res"][d["res"]["politica"] == pol]
            if r.empty:
                continue
            arm = r["pico_armazenamento_bytes"].where(r["pico_armazenamento_bytes"] >= 0)
            linhas.append({
                "politica": pol, "tecnologia": tec, "execucoes": len(r),
                "taxa_producao_mediana_msg_s": r["taxa_producao_efetiva_msg_s"].median(),
                "rejeicoes_mediana": r["rejeicoes"].median(),
                "pico_fila_mediana": r["pico_fila"].median(),
                "perdidas_mediana": r["perdidas"].median(),
                "tempo_total_mediana_s": r["tempo_total_s"].median(),
                "pico_armazenamento_mb_mediana": arm.median() / 2**20 if arm.notna().any() else np.nan,
            })
    return pd.DataFrame(linhas)


def tabela(pdf, dados, res):
    cel = []
    for _, x in res.iterrows():
        r = dados[x["tecnologia"]]["res"]
        r = r[r["politica"] == x["politica"]]
        arm = x["pico_armazenamento_mb_mediana"]
        cel.append([NOMES[x["politica"]].replace(" (", "\n("), x["tecnologia"], faixa(r["taxa_producao_efetiva_msg_s"]),
                    faixa(r["rejeicoes"]), faixa(r["pico_fila"]), faixa(r["perdidas"]),
                    "—" if np.isnan(arm) else fmt(arm, 1), faixa(r["tempo_total_s"], 1)])
    fig, ax = plt.subplots(figsize=(11.5, 3.0 + 0.45 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Política", "Tecnologia", "Produção efetiva\n(msg/s)", "Publicações\nrecusadas",
                                          "Pico da fila", "Perdidas", "Pico de\narmazenamento (MB)", "Tempo até\ndrenar (s)"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 2.0)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Sobrecarga prolongada — resumo Kafka × RabbitMQ", fontsize=11, pad=10)
    fig.text(0.5, 0.02, "Valores 'menor a maior' entre as execuções.\n"
             "Descarte: Kafka = retention.bytes 2 MB; RabbitMQ = x-max-length 2.000 com drop-head. "
             "Recusa: RabbitMQ = x-max-length 2.000 com reject-publish (não existe no Kafka).\n"
             "Armazenamento: Kafka = log em disco, incluindo mensagens já consumidas; "
             "RabbitMQ = bytes das mensagens ainda na fila (não são a mesma medida).",
             ha="center", fontsize=7.5, color="#555555")
    fig.tight_layout(rect=[0, 0.14, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    dados = carregar()
    if not dados:
        print("Nenhum CSV encontrado. Nada a fazer.")
        return
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, SAIDA)
    with PdfPages(saida) as pdf:
        grafico1_filas(pdf, dados)
        grafico2_barras(pdf, dados)
        res = resumo(dados)
        tabela(pdf, dados, res)
    res.to_csv(os.path.join(os.path.dirname(saida), ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()