#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 4 — Múltiplos consumidores — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — vazão por número de consumidores, as duas tecnologias lado a
                lado, com a linha ideal N × V(1) de cada uma
    Gráfico 2 — eficiência S(N) ÷ N
    Gráfico 2b — utilização dos consumidores (independe de V(1))
    Gráfico 3 — divisão das mensagens entre os consumidores
    Gráfico 4 — tempo de espera (início do processamento − envio)
    Gráfico 5 — limite das partições no Kafka (6 consumidores com 4 e com 6
                partições), se a configuração extra foi executada
    Tabela    — resumo das duas tecnologias (também gravado em CSV)

Organização esperada (o script numa pasta própria, ao lado das pastas de
resultados):

    TCC\\
    ├── comparativo_cenario4\\
    │   └── plotar_cenario4_comparativo.py
    ├── resultado_cenario_1_4\\              (Kafka)
    └── resultado_rabbitmq_cenario_1_4\\     (RabbitMQ)

    cd comparativo_cenario4
    python plotar_cenario4_comparativo.py

Também aceita os caminhos como argumentos:
    python plotar_cenario4_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

Depende de: pandas, matplotlib, numpy
"""

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.backends.backend_pdf import PdfPages

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA_ACIMA = os.path.dirname(PASTA_SCRIPT)

TECNOLOGIAS = {
    # nome: (pasta, arquivo agregado, cor da caixa, cor dos pontos)
    "Kafka":    ("resultado_cenario_1_4",          "resultados-kafka-cenario4.csv",    "#185FA5", "#D85A30"),
    "RabbitMQ": ("resultado_rabbitmq_cenario_1_4", "resultados-rabbitmq-cenario4.csv", "#D85A30", "#1B6B2F"),
}
COR_IDEAL = "#555555"

plt.rcParams.update({
    "font.size": 10,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 120,
})
RNG = np.random.default_rng(42)

SAIDA = "cenario4-comparativo.pdf"
ARQ_RESUMO = "cenario4-comparativo-resumo.csv"
CENARIO = "Cenário 4 — Múltiplos consumidores"


def localizar(nome_pasta, nome_csv):
    """Procura a pasta de resultados ao lado da pasta do script e, se não
    achar, dentro dela."""
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        pasta = os.path.join(base, nome_pasta)
        if os.path.isfile(os.path.join(pasta, nome_csv)):
            return pasta
    print(f"[aviso] '{nome_csv}' não encontrado em:")
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        p = os.path.join(base, nome_pasta)
        print(f"         {p}")
        if os.path.isdir(p):
            print(f"         (conteúdo: {', '.join(sorted(os.listdir(p))) or 'vazia'})")
    return None


def ler(pasta, nome):
    p = os.path.join(pasta, nome)
    if not os.path.isfile(p):
        print(f"[aviso] não encontrado: {p}")
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df["execucao"] > 0].copy()
    if "num_particoes" not in df.columns:
        df["num_particoes"] = np.nan
    df["msgs_por_consumidor"] = df["msgs_por_consumidor"].astype(str)
    df["utilizacao_pct"] = df["vazao_msg_s"] * df["tempo_proc_medio_ms"] / 1000 / df["consumidores_ativos"] * 100
    df["extra"] = df["num_particoes"].notna() & (df["num_particoes"] != df["num_consumidores"])
    return df


def fmt(v, casas=2):
    return f"{v:.{casas}f}".replace(".", ",")


def fmt_int(v):
    return f"{v:,.0f}".replace(",", ".")


def principais(df):
    """Só as configurações comparáveis (sem a extra 6c-4p do Kafka)."""
    return df[~df["extra"]]


def v1(df):
    return principais(df).loc[lambda d: d["num_consumidores"] == 1, "vazao_msg_s"].median()


def parcelas_relativas(linhas):
    vals = []
    for _, r in linhas.iterrows():
        p = [int(x) for x in str(r["msgs_por_consumidor"]).split("|")]
        ideal = r["mensagens"] / len(p)
        vals += [v / ideal * 100 for v in p]
    return np.array(vals)


def titulo(fig, texto, sub):
    fig.suptitle(texto, fontsize=11.5, y=0.975)
    fig.text(0.5, 0.925, sub, ha="center", fontsize=8.5, color="#555555")


def caixas_agrupadas(ax, dados, ns, ideal=None, pontos=26):
    """dados[tec] = lista de séries (uma por N). Caixas lado a lado por N."""
    largura = 0.34
    desloc = {"Kafka": -0.19, "RabbitMQ": 0.19}
    for tec, series in dados.items():
        _, _, cor_caixa, cor_ponto = TECNOLOGIAS[tec]
        pos = [i + 1 + desloc[tec] for i in range(len(ns))]
        validos = [(p, s) for p, s in zip(pos, series) if len(s)]
        if not validos:
            continue
        ax.boxplot([s for _, s in validos], positions=[p for p, _ in validos], widths=largura,
                   patch_artist=True, showfliers=False,
                   medianprops=dict(color="black", linewidth=1.5),
                   boxprops=dict(facecolor=cor_caixa, alpha=0.5, edgecolor=cor_caixa),
                   whiskerprops=dict(color=cor_caixa), capprops=dict(color=cor_caixa))
        for p, s in validos:
            ax.scatter(p + RNG.uniform(-0.05, 0.05, len(s)), s, s=pontos, color=cor_ponto,
                       edgecolor="white", linewidth=0.5, zorder=3)
        if ideal is not None and tec in ideal:
            ax.scatter(pos, ideal[tec], marker="_", s=380, color=COR_IDEAL, linewidth=1.6, zorder=4)
    ax.set_xticks(range(1, len(ns) + 1))
    ax.set_xticklabels([str(n) for n in ns])
    ax.set_xlim(0.4, len(ns) + 0.6)
    ax.grid(False, axis="x")


def legenda(ax, com_ideal=None, loc="upper left"):
    h = [Patch(facecolor=TECNOLOGIAS[t][2], alpha=0.5, edgecolor=TECNOLOGIAS[t][2], label=t)
         for t in TECNOLOGIAS]
    if com_ideal:
        h.append(Line2D([], [], marker="_", markersize=14, linestyle="", color=COR_IDEAL,
                        markeredgewidth=1.6, label=com_ideal))
    ax.legend(handles=h, frameon=False, fontsize=8.5, loc=loc)


# ----------------------------------------------------------------------
# Gráficos
# ----------------------------------------------------------------------
def grafico1_vazao(pdf, dfs, ns):
    dados, ideal = {}, {}
    for tec, df in dfs.items():
        p = principais(df)
        dados[tec] = [p.loc[p["num_consumidores"] == n, "vazao_msg_s"].values for n in ns]
        ideal[tec] = [v1(df) * n for n in ns]
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, ns, ideal)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Vazão de consumo (msg/s)")
    legenda(ax, "ideal: N × V(1)")
    titulo(fig, "Vazão por número de consumidores",
           "Cada ponto = 1 execução. Vazão = mensagens ÷ (último fim de processamento − primeiro envio)")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_eficiencia(pdf, dfs, ns):
    dados = {}
    for tec, df in dfs.items():
        p = principais(df)
        base = v1(df)
        dados[tec] = [p.loc[p["num_consumidores"] == n, "vazao_msg_s"].values / base / n * 100 for n in ns]
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, ns)
    ax.axhline(100, color=COR_IDEAL, linestyle="--", linewidth=1.2)
    todos = np.concatenate([s for series in dados.values() for s in series if len(s)])
    ax.set_ylim(max(0, min(80, todos.min() - 5)), max(105, todos.max() + 3))
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Eficiência E(N) = V(N) ÷ (N × V(1))  (%)")
    legenda(ax, loc="lower left")
    titulo(fig, "Eficiência por número de consumidores",
           "100% (linha tracejada) = cada consumidor a mais rende um consumidor inteiro. "
           "V(1) = mediana com 1 consumidor, de cada tecnologia")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2b_utilizacao(pdf, dfs, ns):
    dados = {}
    for tec, df in dfs.items():
        p = principais(df)
        dados[tec] = [p.loc[p["num_consumidores"] == n, "utilizacao_pct"].values for n in ns]
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, ns)
    ax.axhline(100, color=COR_IDEAL, linestyle="--", linewidth=1.2)
    todos = np.concatenate([s for series in dados.values() for s in series if len(s)])
    ax.set_ylim(max(0, min(80, todos.min() - 5)), max(105, todos.max() + 3))
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Utilização = vazão × tempo de processamento ÷ consumidores ativos (%)")
    legenda(ax, loc="lower left")
    titulo(fig, "Utilização dos consumidores",
           "100% = nenhum consumidor fica parado. Não usa V(1): confirma a eficiência sem depender da linha de base "
           "(no RabbitMQ, V(1) varia entre execuções)")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


def grafico3_divisao(pdf, dfs, ns):
    dados = {}
    for tec, df in dfs.items():
        p = principais(df)
        dados[tec] = [parcelas_relativas(p[p["num_consumidores"] == n]) for n in ns]
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, ns, pontos=12)
    ax.axhline(100, color=COR_IDEAL, linestyle="--", linewidth=1.2)
    todos = np.concatenate([s for series in dados.values() for s in series if len(s)])
    folga = max(2, (todos.max() - todos.min()) * 0.15)
    ax.set_ylim(todos.min() - folga, todos.max() + folga)
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Parcela do consumidor ÷ (mensagens ÷ N)  (%)")
    legenda(ax, loc="lower left")
    titulo(fig, "Divisão das mensagens entre os consumidores",
           "Cada ponto = 1 consumidor em 1 execução. 100% = recebeu exatamente a sua parte")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


def grafico4_espera(pdf, dfs, ns):
    dados = {}
    for tec, df in dfs.items():
        p = principais(df)
        dados[tec] = [p.loc[p["num_consumidores"] == n, "espera_mediana_ms"].values / 1000 for n in ns]
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, ns)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Tempo de espera mediano da execução (s)")
    legenda(ax, loc="upper right")
    titulo(fig, "Tempo de espera por número de consumidores",
           "Espera = início do processamento − envio; cada ponto = a mediana de 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


def grafico5_particoes(pdf, dfs):
    k = dfs.get("Kafka")
    if k is None or not k["extra"].any():
        return
    extra = k[k["extra"]]
    p_extra = int(extra["num_particoes"].iloc[0])
    n_extra = int(extra["num_consumidores"].iloc[0])
    grupos = [
        (f"Kafka\n{p_extra} consumidores\n{p_extra} partições",
         k[(~k["extra"]) & (k["num_consumidores"] == p_extra)], "Kafka"),
        (f"Kafka\n{n_extra} consumidores\n{p_extra} partições", extra, "Kafka"),
        (f"Kafka\n{n_extra} consumidores\n{n_extra} partições",
         k[(~k["extra"]) & (k["num_consumidores"] == n_extra)], "Kafka"),
    ]
    r = dfs.get("RabbitMQ")
    if r is not None:
        grupos.append((f"RabbitMQ\n{n_extra} consumidores\n(1 fila)", r[r["num_consumidores"] == n_extra], "RabbitMQ"))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5.6))
    for i, (_, d, tec) in enumerate(grupos, start=1):
        _, _, cor_caixa, cor_ponto = TECNOLOGIAS[tec]
        s = d["vazao_msg_s"].values
        if not len(s):
            continue
        ax1.boxplot([s], positions=[i], widths=0.5, patch_artist=True, showfliers=False,
                    medianprops=dict(color="black", linewidth=1.5),
                    boxprops=dict(facecolor=cor_caixa, alpha=0.5, edgecolor=cor_caixa),
                    whiskerprops=dict(color=cor_caixa), capprops=dict(color=cor_caixa))
        ax1.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=24, color=cor_ponto,
                    edgecolor="white", linewidth=0.5, zorder=3)
        ativos = d["consumidores_ativos"].median()
        ax2.bar(i, ativos, width=0.55, color=cor_caixa, alpha=0.6, edgecolor=cor_caixa)
        ax2.text(i, ativos + 0.1, f"{ativos:.0f} de {int(d['num_consumidores'].iloc[0])}",
                 ha="center", va="bottom", fontsize=9)
    for ax in (ax1, ax2):
        ax.set_xticks(range(1, len(grupos) + 1))
        ax.set_xticklabels([g[0] for g in grupos], fontsize=8.5)
        ax.grid(False, axis="x")
    ax1.set_ylim(bottom=0)
    ax1.set_ylabel("Vazão de consumo (msg/s)")
    ax1.set_title("Vazão", fontsize=10.5)
    ax2.set_ylim(0, n_extra + 1)
    ax2.set_ylabel("Consumidores que receberam mensagens (mediana)")
    ax2.set_title("Consumidores com mensagens", fontsize=10.5)
    titulo(fig, "Kafka — limite das partições",
           f"Com {n_extra} consumidores e {p_extra} partições, {n_extra - p_extra} consumidores ficam sem partição; "
           "no RabbitMQ todos os consumidores da fila recebem mensagens")
    fig.tight_layout(rect=[0, 0, 1, 0.91])
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Resumo
# ----------------------------------------------------------------------
def resumo(dfs):
    linhas = []
    for tec, df in dfs.items():
        base = v1(df)
        for cfg, d in df.groupby("config", sort=False):
            n = int(d["num_consumidores"].iloc[0])
            p = d["num_particoes"].iloc[0]
            vm = d["vazao_msg_s"].median()
            linhas.append({
                "tecnologia": tec, "config": cfg, "num_consumidores": n,
                "num_particoes": None if pd.isna(p) else int(p), "extra": bool(d["extra"].iloc[0]),
                "execucoes": len(d),
                "vazao_mediana_msg_s": vm, "vazao_min_msg_s": d["vazao_msg_s"].min(),
                "vazao_max_msg_s": d["vazao_msg_s"].max(),
                "ganho": vm / base, "eficiencia_pct": vm / base / n * 100,
                "utilizacao_pct": d["utilizacao_pct"].median(),
                "espera_mediana_s": d["espera_mediana_ms"].median() / 1000,
                "tempo_proc_medio_ms": d["tempo_proc_medio_ms"].median(),
                "menor_parcela": int(d["menor_parcela"].min()), "maior_parcela": int(d["maior_parcela"].max()),
                "consumidores_ativos": int(d["consumidores_ativos"].median()),
            })
    res = pd.DataFrame(linhas)
    return res.sort_values(["extra", "num_consumidores", "tecnologia"]).reset_index(drop=True)


def tabela_resumo(pdf, res):
    linhas = []
    for _, r in res.iterrows():
        cfg = str(r["num_consumidores"])
        if r["extra"]:
            cfg += f" ({int(r['num_particoes'])} part.)"
        linhas.append([cfg, r["tecnologia"], fmt(r["vazao_mediana_msg_s"], 1),
                       f"{fmt(r['vazao_min_msg_s'], 1)} a {fmt(r['vazao_max_msg_s'], 1)}",
                       fmt(r["ganho"]), f"{fmt(r['eficiencia_pct'], 1)}%",
                       f"{fmt(r['utilizacao_pct'], 1)}%", fmt(r["espera_mediana_s"]), fmt(r["tempo_proc_medio_ms"]),
                       f"{fmt_int(r['menor_parcela'])} a {fmt_int(r['maior_parcela'])}"])
    fig, ax = plt.subplots(figsize=(11.5, 2.2 + 0.36 * len(linhas)))
    ax.axis("off")
    t = ax.table(cellText=linhas,
                 colLabels=["Consumidores", "Tecnologia", "Vazão\n(mediana, msg/s)", "Vazão\n(menor a maior)",
                            "Ganho\nS(N)", "Eficiência\nE(N)", "Utilização\nconsumidores", "Espera\nmediana (s)", "Processamento\nmedido (ms)",
                            "Mensagens por\nconsumidor"],
                 loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.4)
    t.scale(1, 1.6)
    for (rr, _), c in t.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(f"{CENARIO} — resumo comparativo", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "'Mediana' = a mediana entre as execuções. Ganho e eficiência sobre a mediana de 1 consumidor "
             "da mesma tecnologia.\nUtilização = vazão × tempo de processamento ÷ consumidores ativos (não depende de V(1)). 'Mensagens por consumidor' = menor e maior parcela de um consumidor em todas as execuções.",
             ha="center", va="bottom", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    if len(sys.argv) >= 3:
        pastas = {"Kafka": sys.argv[1], "RabbitMQ": sys.argv[2]}
        saida = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, SAIDA)
    else:
        pastas = {t: localizar(v[0], v[1]) for t, v in TECNOLOGIAS.items()}
        saida = os.path.join(PASTA_SCRIPT, SAIDA)

    dfs = {}
    for tec, pasta in pastas.items():
        if pasta:
            df = ler(pasta, TECNOLOGIAS[tec][1])
            if df is not None and not df.empty:
                dfs[tec] = df
                print(f"{tec}: {pasta}")
    if not dfs:
        print("Nenhum CSV encontrado.")
        return

    ns = sorted(set().union(*[set(principais(d)["num_consumidores"]) for d in dfs.values()]))
    with PdfPages(saida) as pdf:
        grafico1_vazao(pdf, dfs, ns)
        grafico2_eficiencia(pdf, dfs, ns)
        grafico2b_utilizacao(pdf, dfs, ns)
        grafico3_divisao(pdf, dfs, ns)
        grafico4_espera(pdf, dfs, ns)
        grafico5_particoes(pdf, dfs)
        res = resumo(dfs)
        tabela_resumo(pdf, res)
    res.to_csv(os.path.join(PASTA_SCRIPT, ARQ_RESUMO), index=False, float_format="%.4f")
    print(f"Resumo gravado: {os.path.join(PASTA_SCRIPT, ARQ_RESUMO)}")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()