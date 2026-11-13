#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 4: Sobrecarga prolongada — gráficos do KAFKA.

Lê os CSVs gerados pelo orquestrador Cenario4SobrecargaProlongada:
    resultados-kafka-sobrecarga.csv        (1 linha por execução e política)
    resultados-kafka-sobrecarga-serie.csv  (1 linha por segundo)

Gera um PDF com:
    Gráfico 1 — mensagens na fila do broker ao longo do tempo, por política
    Gráfico 2 — enviadas e consumidas (acumulado) ao longo do tempo, por política
    Gráfico 3 — armazenamento ocupado no broker ao longo do tempo
    Tabela    — resumo por política (também gravado em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_sobrecarga_kafka.py
ou:
    python plotar_sobrecarga_kafka.py <pasta_dos_csvs> [saida.pdf]

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
ARQ_RESUMO_IN = "resultados-kafka-sobrecarga.csv"
ARQ_SERIE    = "resultados-kafka-sobrecarga-serie.csv"
ARQ_RESUMO   = "sobrecarga-kafka-resumo.csv"
SAIDA_PADRAO = "sobrecarga-kafka.pdf"
COR          = "#185FA5"   # azul
# ----------------------------------------------------------------------

CENARIO = "Intermediário — Cenário 4: Sobrecarga prolongada"
POLITICAS = ["sem-limite", "com-limite"]
NOMES = {"sem-limite": "sem limite", "com-limite": "com limite (descarte)"}

plt.rcParams.update({
    "font.size": 10, "axes.grid": True, "grid.alpha": 0.3, "grid.linestyle": "--",
    "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 120,
})
PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA = sys.argv[1] if len(sys.argv) > 1 else PASTA_SCRIPT
SAIDA = sys.argv[2] if len(sys.argv) > 2 else SAIDA_PADRAO


def fmt(v, casas=0):
    return f"{v:,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def faixa(s, casas=0):
    s = s.dropna()
    if s.empty:
        return "—"
    return fmt(s.min(), casas) if s.min() == s.max() else f"{fmt(s.min(), casas)} a {fmt(s.max(), casas)}"


def ler(nome):
    p = os.path.join(PASTA, nome)
    if not os.path.isfile(p):
        print(f"[aviso] {nome} não encontrado em {os.path.abspath(PASTA)}")
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df[df["execucao"] > 0].copy()


def paineis_por_politica(pdf, serie, coluna, ylabel, titulo_txt, sub, escala=1.0):
    fig, eixos = plt.subplots(1, 2, figsize=(11, 4.8), sharey=True)
    for ax, pol in zip(eixos, POLITICAS):
        d = serie[serie["politica"] == pol]
        for ex, g in d.groupby("execucao"):
            v = g[coluna].where(g[coluna] >= 0) * escala
            ax.plot(g["tempo_s"], v, color=COR, alpha=0.35 + 0.65 * (ex == d["execucao"].min()),
                    linewidth=1.4 if ex == d["execucao"].min() else 0.9,
                    label="1ª execução" if ex == d["execucao"].min()
                    else ("demais execuções" if ex == sorted(d["execucao"].unique())[1] else None))
        ax.set_title(NOMES[pol], fontsize=10.5)
        ax.set_xlabel("Tempo desde o início do envio (s)")
        ax.set_ylim(bottom=0)
        ax.legend(frameon=False, fontsize=7.5)
    eixos[0].set_ylabel(ylabel)
    fig.suptitle(titulo_txt, fontsize=11.5, y=0.98)
    fig.text(0.5, 0.92, sub, ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_acumulado(pdf, serie):
    fig, eixos = plt.subplots(1, 2, figsize=(11, 4.8), sharey=True)
    for ax, pol in zip(eixos, POLITICAS):
        d = serie[serie["politica"] == pol]
        if d.empty:
            continue
        g = d[d["execucao"] == d["execucao"].min()]
        ax.plot(g["tempo_s"], g["enviadas"], color=COR, linewidth=1.6, label="enviadas (confirmadas)")
        ax.plot(g["tempo_s"], g["consumidas"], color="#D85A30", linewidth=1.6, label="consumidas")
        ax.set_title(NOMES[pol], fontsize=10.5)
        ax.set_xlabel("Tempo desde o início do envio (s)")
        ax.legend(frameon=False, fontsize=8)
        ax.set_ylim(bottom=0)
    eixos[0].set_ylabel("Mensagens (acumulado)")
    fig.suptitle(f"{TECNOLOGIA} — produção e consumo ao longo do tempo (1ª execução)", fontsize=11.5, y=0.98)
    fig.text(0.5, 0.92, "A inclinação de cada linha é a taxa; a distância vertical entre elas é o que ficou para trás",
             ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(r):
    linhas = []
    for pol in POLITICAS:
        d = r[r["politica"] == pol]
        if d.empty:
            continue
        linhas.append({
            "tecnologia": TECNOLOGIA, "politica": pol, "execucoes": len(d),
            "taxa_producao_mediana_msg_s": d["taxa_producao_efetiva_msg_s"].median(),
            "rejeicoes_mediana": d["rejeicoes"].median(),
            "pico_fila_mediana": d["pico_fila"].median(),
            "perdidas_mediana": d["perdidas"].median(), "perdidas_min": d["perdidas"].min(),
            "perdidas_max": d["perdidas"].max(),
            "tempo_total_mediana_s": d["tempo_total_s"].median(),
            "pico_armazenamento_mb_mediana": d["pico_armazenamento_bytes"].where(d["pico_armazenamento_bytes"] >= 0).median() / 2**20,
        })
    return pd.DataFrame(linhas)


def tabela(pdf, r, res):
    cel = []
    for _, x in res.iterrows():
        d = r[r["politica"] == x["politica"]]
        arm = x["pico_armazenamento_mb_mediana"]
        cel.append([NOMES[x["politica"]].replace(" (", "\n("), str(x["execucoes"]), faixa(d["taxa_producao_efetiva_msg_s"]),
                    faixa(d["rejeicoes"]), faixa(d["pico_fila"]), faixa(d["perdidas"]),
                    "—" if np.isnan(arm) else fmt(arm, 1), faixa(d["tempo_total_s"], 1)])
    fig, ax = plt.subplots(figsize=(10.5, 2.4 + 0.42 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Política", "Execuções", "Taxa de produção\nefetiva (msg/s)",
                                          "Publicações\nrecusadas", "Pico da fila\n(mensagens)", "Perdidas",
                                          "Pico de\narmazenamento (MB)", "Tempo até\ndrenar (s)"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 2.0)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(f"{TECNOLOGIA} — {CENARIO} — resumo", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Valores 'menor a maior' entre as execuções. Taxa de produção efetiva = mensagens confirmadas ÷ "
             "duração do envio (alvo: 1.000 msg/s).\nTempo até drenar = do primeiro envio até a última mensagem processada.",
             ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.1, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    r = ler(ARQ_RESUMO_IN)
    s = ler(ARQ_SERIE)
    if r is None or s is None:
        print("Nada a fazer.")
        return
    with PdfPages(saida) as pdf:
        paineis_por_politica(pdf, s, "fila", "Mensagens na fila do broker",
                             f"{TECNOLOGIA} — mensagens acumuladas no broker ao longo do tempo",
                             "Produtor a 1.000 msg/s por 20 s; consumidor a 5 ms por mensagem. Linha forte = 1ª execução")
        grafico2_acumulado(pdf, s)
        paineis_por_politica(pdf, s, "armazenamento_bytes", "Armazenamento ocupado (MB)",
                             f"{TECNOLOGIA} — espaço ocupado pelas mensagens no broker",
                             "Tamanho do log em disco (inclui as mensagens já consumidas, que o Kafka mantém até a retenção)",
                             escala=1 / 2**20)
        res = resumo(r)
        tabela(pdf, r, res)
    res.to_csv(os.path.join(PASTA, ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()