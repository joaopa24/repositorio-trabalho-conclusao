#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível avançado — Cenário 2: Impacto da replicação — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF só com o essencial:
    Gráfico 1 — latência mediana de cada execução: um painel por tecnologia, com ESCALA PRÓPRIA
    Gráfico 2 — vazão de ponta a ponta de cada execução: idem
    Tabela    — aumento de custo dentro de cada tecnologia (também gravada em CSV)

Os painéis têm escalas diferentes de propósito: o RabbitMQ confirma depois de
gravar em disco e o Kafka confirma com a mensagem na memória, então o que se
compara é o AUMENTO dentro de cada tecnologia, e não os valores absolutos.

Organização esperada (o script numa pasta própria, ao lado das pastas de resultados):

    TCC\\
    ├── comparativo_cenario3_2\\
    │   └── plotar_replicacao_comparativo.py
    ├── resultado_cenario_3_2\\              (Kafka)
    └── resultado_rabbitmq_cenario_3_2\\     (RabbitMQ)

Também aceita os caminhos como argumentos:
    python plotar_replicacao_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

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
    "Kafka": dict(pasta="resultado_cenario_3_2", lat="resultados-kafka-replicacao-latencia.csv",
                  vaz="resultados-kafka-replicacao-vazao.csv", cor="#185FA5", ponto="#D85A30",
                  coluna="fator_replicacao", ordem=[1, 2, 3], nomes={1: "RF 1", 2: "RF 2", 3: "RF 3"},
                  base={2: 1, 3: 1}),
    "RabbitMQ": dict(pasta="resultado_rabbitmq_cenario_3_2", lat="resultados-rabbitmq-replicacao-latencia.csv",
                     vaz="resultados-rabbitmq-replicacao-vazao.csv", cor="#D85A30", ponto="#1B6B2F",
                     coluna="configuracao", ordem=["classica", "quorum-1", "quorum-2", "quorum-3"],
                     nomes={"classica": "Clássica", "quorum-1": "Quorum-1", "quorum-2": "Quorum-2",
                            "quorum-3": "Quorum-3"},
                     base={"quorum-1": "classica", "quorum-2": "quorum-1", "quorum-3": "quorum-1"}),
}
SAIDA = "replicacao-comparativo.pdf"
ARQ_RESUMO = "replicacao-comparativo-resumo.csv"

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


def ler(p):
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df[df["execucao"] > 0].copy()


def carregar():
    """Argumento 1 = pasta do Kafka; argumento 2 = pasta do RabbitMQ."""
    args = sys.argv[1:]
    pastas = {"Kafka": args[0], "RabbitMQ": args[1]} if len(args) >= 2 else {}
    dados = {}
    for tec, c in TECNOLOGIAS.items():
        pasta = pastas.get(tec) or localizar(c["pasta"], c["lat"])
        if pasta is None or not os.path.isfile(os.path.join(pasta, c["lat"])) \
                or not os.path.isfile(os.path.join(pasta, c["vaz"])):
            continue
        dados[tec] = {"lat": ler(os.path.join(pasta, c["lat"])), "vaz": ler(os.path.join(pasta, c["vaz"]))}
        print(f"{tec}: {os.path.abspath(pasta)}")
    return dados


def limite_superior(grupos):
    """Topo do eixo: o maior valor que não é ponto extremo (Tukey, 3 × IQR), com folga."""
    tops = []
    for v in grupos:
        if len(v) == 0:
            continue
        q1, q3 = np.percentile(v, [25, 75])
        dentro = v[v <= q3 + 3 * (q3 - q1)]
        tops.append(dentro.max() if len(dentro) else v.max())
    return max(tops) * 1.15


def paineis(pdf, dados, parte, coluna, ylabel, titulo, sub):
    fig, eixos = plt.subplots(1, len(dados), figsize=(12, 5), squeeze=False,
                              gridspec_kw={"width_ratios": [len(TECNOLOGIAS[t]["ordem"]) for t in dados]})
    fora = []
    for ax, (tec, d) in zip(eixos[0], dados.items()):
        c = TECNOLOGIAS[tec]
        df = d[parte]
        cfgs = [x for x in c["ordem"] if x in set(df[c["coluna"]])]
        grupos = [df.loc[df[c["coluna"]] == x, coluna].values for x in cfgs]
        topo = limite_superior(grupos)
        ax.boxplot(grupos, positions=range(1, len(cfgs) + 1), widths=0.5, patch_artist=True, showfliers=False,
                   medianprops=dict(color="black", linewidth=1.5),
                   boxprops=dict(facecolor=c["cor"], alpha=0.45, edgecolor=c["cor"]),
                   whiskerprops=dict(color=c["cor"]), capprops=dict(color=c["cor"]))
        for i, v in enumerate(grupos, start=1):
            x = i + RNG.uniform(-0.08, 0.08, len(v))
            dentro = v <= topo
            ax.scatter(x[dentro], v[dentro], s=16, color=c["ponto"], edgecolor="white", linewidth=0.5, zorder=3)
            for xv, yv in zip(x[~dentro], v[~dentro]):
                ax.scatter(xv, topo * 0.985, marker="^", s=40, color=c["ponto"], zorder=3)
                fora.append(f"{tec} {c['nomes'][cfgs[i - 1]]}: {fmt(yv, 1)}")
        ax.set_xticks(range(1, len(cfgs) + 1))
        ax.set_xticklabels([c["nomes"][x] for x in cfgs])
        ax.grid(False, axis="x")
        ax.set_ylim(0, topo)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, p, t=topo: fmt(v, 0 if t > 50 else 1)))
        ax.set_title(tec, fontsize=10.5)
    eixos[0][0].set_ylabel(ylabel)
    fig.suptitle(f"Replicação — {titulo}", fontsize=11.5, y=0.985)
    nota = sub + (f"\n▲ fora da escala: {'; '.join(fora)}" if fora else "")
    fig.text(0.5, 0.935, nota, ha="center", va="top", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.86 if fora else 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo(dados):
    linhas = []
    for tec, d in dados.items():
        c = TECNOLOGIAS[tec]
        lat, vaz, col = d["lat"], d["vaz"], c["coluna"]
        med = lambda df, k, m: df.loc[df[col] == k, m].median()
        for k in c["ordem"]:
            if k not in set(lat[col]):
                continue
            b = c["base"].get(k)
            l, v = med(lat, k, "latencia_mediana_ms"), med(vaz, k, "vazao_msg_s")
            linhas.append({
                "tecnologia": tec, "configuracao": c["nomes"][k], "referencia": "—" if b is None else c["nomes"][b],
                "latencia_mediana_ms": l,
                "aumento_latencia_ms": np.nan if b is None else l - med(lat, b, "latencia_mediana_ms"),
                "aumento_latencia_pct": np.nan if b is None else (l / med(lat, b, "latencia_mediana_ms") - 1) * 100,
                "vazao_mediana_msg_s": v,
                "variacao_vazao_pct": np.nan if b is None else (v / med(vaz, b, "vazao_msg_s") - 1) * 100,
                "vazao_produtor_mediana_msg_s": med(vaz, k, "vazao_produtor_msg_s"),
            })
    return pd.DataFrame(linhas)


def tabela(pdf, res):
    def sinal(v, casas, suf):
        return "—" if pd.isna(v) else ("+" if v > 0 else "") + fmt(v, casas) + suf
    cel = [[r["tecnologia"], r["configuracao"], r["referencia"], fmt(r["latencia_mediana_ms"], 2),
            sinal(r["aumento_latencia_ms"], 2, " ms") + ("" if pd.isna(r["aumento_latencia_pct"])
                                                         else f" ({sinal(r['aumento_latencia_pct'], 0, '%')})"),
            fmt(r["vazao_mediana_msg_s"]), sinal(r["variacao_vazao_pct"], 0, "%"),
            fmt(r["vazao_produtor_mediana_msg_s"])] for _, r in res.iterrows()]
    fig, ax = plt.subplots(figsize=(11.5, 1.9 + 0.34 * len(cel)))
    ax.axis("off")
    t = ax.table(cellText=cel, colLabels=["Tecnologia", "Configuração", "Comparada\ncom", "Latência\n(mediana, ms)",
                                          "Aumento da\nlatência", "Vazão\n(mediana, msg/s)", "Variação\nda vazão",
                                          "Vazão do\nprodutor (msg/s)"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.75)
    for (rr, _), cc in t.get_celld().items():
        if rr == 0:
            cc.set_facecolor("#E8EEF5")
            cc.set_text_props(weight="bold")
    ax.set_title("Replicação — aumento de custo dentro de cada tecnologia", fontsize=11, pad=10)
    fig.text(0.5, 0.02,
             "Compare os aumentos, não os valores absolutos: o RabbitMQ confirma depois de gravar em disco; o Kafka, "
             "com a mensagem na memória.\nKafka: acks=all espera todas as réplicas em sincronia (RF 3 = 3 cópias). "
             "RabbitMQ: a quorum queue confirma com a maioria (quorum-3 = 2 de 3).\n"
             "Na quorum queue, cada ACK do consumidor também é replicado; o consumidor do Kafka lê sem commit.",
             ha="center", fontsize=7.8, color="#555555")
    fig.tight_layout(rect=[0, 0.17, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    dados = carregar()
    if not dados:
        print("Nenhum CSV encontrado. Nada a fazer.")
        return
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, SAIDA)
    res = resumo(dados)
    with PdfPages(saida) as pdf:
        paineis(pdf, dados, "lat", "latencia_mediana_ms", "Latência mediana da execução (ms)", "latência",
                "Parte A: 5.000 mensagens a 200 msg/s; cada ponto = 1 execução; escala própria em cada painel")
        paineis(pdf, dados, "vaz", "vazao_msg_s", "Vazão de ponta a ponta (msg/s)", "vazão",
                "Parte B: 20.000 mensagens sem controle de taxa; cada ponto = 1 execução; escala própria em cada painel")
        tabela(pdf, res)
    res.to_csv(os.path.join(os.path.dirname(os.path.abspath(saida)), ARQ_RESUMO), index=False, float_format="%.2f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
