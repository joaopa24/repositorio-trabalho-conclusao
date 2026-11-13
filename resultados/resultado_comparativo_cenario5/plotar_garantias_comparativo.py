#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 1: Comparação de garantias — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — latência MEDIANA por execução, por nível, as duas tecnologias
                lado a lado (Parte A). Mediana, e não média, como na Tabela 5:
                a média é dominada pelos episódios de mensagens retidas
                (grupos de ~40 ms no at most once do RabbitMQ).
    Gráfico 2 — distribuição acumulada da latência de todas as mensagens,
                um painel por nível (Parte A)
    Gráfico 3 — quanto a latência típica AUMENTA em relação ao at most once,
                em cada tecnologia (Parte A)
    Gráfico 4 — vazão máxima por nível (Parte B)
    Tabela    — resumo das duas tecnologias, com os mesmos valores das
                Tabelas 5 e 6 do TCC (também gravado em CSV)

Também grava as duas figuras do TCC em PNG, sem título (a legenda fica no
Word), 7,2 × 4,2 pol a 200 dpi:
    garantias-figura-latencia.png  → Figura 24
    garantias-figura-vazao.png     → Figura 25

Confira, na saída, as linhas "Kafka: <pasta>" e "RabbitMQ: <pasta>": são as
pastas de onde os CSVs foram lidos.

Organização esperada (o script numa pasta própria, ao lado das pastas de
resultados):

    TCC\\
    ├── comparativo_cenario2_1\\
    │   └── plotar_garantias_comparativo.py
    ├── resultado_cenario_2_1\\              (Kafka)
    └── resultado_rabbitmq_cenario_2_1\\     (RabbitMQ)

    cd comparativo_cenario2_1
    python plotar_garantias_comparativo.py

Também aceita os caminhos como argumentos:
    python plotar_garantias_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

Depende de: pandas, matplotlib, numpy
"""

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA_ACIMA = os.path.dirname(PASTA_SCRIPT)

GARANTIAS = ["at-most-once", "at-least-once", "exactly-once"]
NOMES = {"at-most-once": "at most once", "at-least-once": "at least once", "exactly-once": "exactly once"}

TECNOLOGIAS = {
    # nome: (pasta, prefixo dos arquivos, cor da caixa, cor dos pontos)
    "Kafka":    ("resultado_cenario_2_1",          "resultados-kafka-garantias",    "#185FA5", "#D85A30"),
    "RabbitMQ": ("resultado_rabbitmq_cenario_2_1", "resultados-rabbitmq-garantias", "#D85A30", "#1B6B2F"),
}

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

SAIDA = "garantias-comparativo.pdf"
FIG_LATENCIA = "garantias-figura-latencia.png"
FIG_VAZAO = "garantias-figura-vazao.png"
ARQ_RESUMO = "garantias-comparativo-resumo.csv"


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
    return df[df["execucao"] > 0].copy()


def carregar():
    global SAIDA
    args = sys.argv[1:]
    dados = {}
    for i, (tec, (pasta_nome, prefixo, _, _)) in enumerate(TECNOLOGIAS.items()):
        if len(args) >= 2:
            pasta = args[i]
        else:
            pasta = localizar(pasta_nome, f"{prefixo}-latencia.csv")
        if pasta is None:
            dados[tec] = None
            continue
        print(f"{tec}: {os.path.abspath(pasta)}")
        dados[tec] = {
            "lat": ler(pasta, f"{prefixo}-latencia.csv"),
            "det": ler(pasta, f"{prefixo}-latencia-detalhado.csv"),
            "vaz": ler(pasta, f"{prefixo}-vazao.csv"),
        }
    if len(args) >= 3:
        SAIDA = args[2]
    return dados


def fmt(v, casas=2):
    return f"{v:.{casas}f}".replace(".", ",")


def fmt_int(v):
    return f"{v:,.0f}".replace(",", ".")


def eixo_log(ax, eixo="y"):
    """Escala log com rótulos em valores redondos; usa mais marcas quando a
    faixa de valores é estreita (menos de ~1,5 década)."""
    a = ax.yaxis if eixo == "y" else ax.xaxis
    (ax.set_yscale if eixo == "y" else ax.set_xscale)("log")
    ax.figure.canvas.draw_idle()
    lo, hi = (ax.get_ylim() if eixo == "y" else ax.get_xlim())
    estreito = lo > 0 and np.log10(hi / lo) < 1.5
    subs = [1, 1.5, 2, 3, 5, 7] if estreito else [1, 2, 5]
    a.set_major_locator(mticker.LogLocator(base=10, subs=subs))
    a.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    a.set_minor_locator(mticker.NullLocator())


def titulo_fig(fig, texto, sub, y=0.975):
    fig.suptitle(texto, fontsize=11.5, y=y)
    # distância fixa em polegadas, para não encostar no título em figuras baixas
    fig.text(0.5, y - 0.34 / fig.get_figheight(), sub, ha="center", fontsize=8.5, color="#555555")


def caixas_agrupadas(ax, dados, chave, coluna):
    """Para cada nível, uma caixa por tecnologia (lado a lado); pontos = execuções."""
    largura = 0.34
    desloc = {tec: (-0.2 if i == 0 else 0.2) for i, tec in enumerate(TECNOLOGIAS)}
    for tec, (_, _, cor, cor_ponto) in TECNOLOGIAS.items():
        if dados.get(tec) is None or dados[tec][chave] is None:
            continue
        df = dados[tec][chave]
        for j, t in enumerate(GARANTIAS, start=1):
            v = df.loc[df["garantia"] == t, coluna].values
            if len(v) == 0:
                continue
            x = j + desloc[tec]
            ax.boxplot([v], positions=[x], widths=largura, patch_artist=True, showfliers=False,
                       medianprops=dict(color="black", linewidth=1.5),
                       boxprops=dict(facecolor=cor, alpha=0.5, edgecolor=cor),
                       whiskerprops=dict(color=cor), capprops=dict(color=cor))
            ax.scatter(x + RNG.uniform(-0.05, 0.05, len(v)), v, s=18, color=cor_ponto,
                       edgecolor="white", linewidth=0.5, zorder=3)
        ax.plot([], [], color=cor, linewidth=8, alpha=0.5, label=tec)
    ax.set_xticks(range(1, len(GARANTIAS) + 1))
    ax.set_xticklabels([NOMES[g] for g in GARANTIAS])
    ax.set_xlabel("Nível de garantia")
    ax.grid(False, axis="x")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="upper left")


def grafico1_latencia(pdf, dados):
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, "lat", "latencia_mediana_ms")
    eixo_log(ax)
    ax.set_ylabel("Latência mediana da execução (ms) — escala log")
    titulo_fig(fig, "Garantias — latência mediana por execução, por nível (Parte A)",
               "1 caixa = as execuções de uma tecnologia naquele nível; cada ponto = 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_ecdf(pdf, dados):
    fig, eixos = plt.subplots(1, 3, figsize=(12, 4.8), sharey=True)
    for ax, t in zip(eixos, GARANTIAS):
        for tec, (_, _, cor, _) in TECNOLOGIAS.items():
            if dados.get(tec) is None or dados[tec]["det"] is None:
                continue
            v = np.sort(dados[tec]["det"].loc[dados[tec]["det"]["garantia"] == t, "latencia_ms"].values)
            if len(v) == 0:
                continue
            ax.plot(v, np.arange(1, len(v) + 1) / len(v), color=cor, linewidth=1.6, label=tec)
        eixo_log(ax, "x")
        ax.set_title(NOMES[t], fontsize=10.5)
        ax.set_xlabel("Latência (ms) — escala log")
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    eixos[0].set_ylabel("Mensagens que chegaram em até X ms")
    eixos[-1].legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="lower right")
    titulo_fig(fig, "Garantias — distribuição acumulada da latência por mensagem (Parte A)",
               "Curva mais à esquerda = mensagens chegando mais rápido. Todas as mensagens das execuções.")
    fig.tight_layout(rect=[0, 0, 1, 0.86])
    pdf.savefig(fig)
    plt.close(fig)


def acrescimos(dados):
    """Acréscimo da latência TÍPICA em relação ao at most once: mediana, entre as
    execuções, da latência mediana de cada execução. Usa a mediana (e não a
    média) para que poucos episódios lentos não dominem o resultado."""
    out = {}
    for tec in TECNOLOGIAS:
        if dados.get(tec) is None or dados[tec]["lat"] is None:
            continue
        lat = dados[tec]["lat"]
        med = {t: lat.loc[lat["garantia"] == t, "latencia_mediana_ms"].median() for t in GARANTIAS}
        out[tec] = {t: med[t] - med[GARANTIAS[0]] for t in GARANTIAS[1:]}
    return out


def grafico3_acrescimo(pdf, dados):
    acr = acrescimos(dados)
    if not acr:
        return
    fig, ax = plt.subplots(figsize=(7.8, 5.2))
    x = np.arange(len(GARANTIAS) - 1)
    larg = 0.36
    for i, (tec, v) in enumerate(acr.items()):
        cor = TECNOLOGIAS[tec][2]
        vals = [v[t] for t in GARANTIAS[1:]]
        pos = x + (i - 0.5) * larg
        ax.bar(pos, vals, width=larg, color=cor, alpha=0.75, label=tec)
        for p, val in zip(pos, vals):
            ax.text(p, val, f"{'+' if val >= 0 else ''}{fmt(val)} ms", ha="center",
                    va="bottom" if val >= 0 else "top", fontsize=8.5)
    ax.axhline(0, color="#555555", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([f"de at most once\npara {NOMES[t]}" for t in GARANTIAS[1:]])
    ax.set_ylabel("Aumento da latência típica (ms)")
    ax.grid(False, axis="x")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="upper left")
    titulo_fig(fig, "Garantias — quanto a latência aumenta em relação ao at most once (Parte A)",
               "Latência típica = mediana das execuções da latência mediana de cada uma; (nível) − (at most once)")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico4_vazao(pdf, dados):
    fig, ax = plt.subplots(figsize=(8.4, 5.8))
    caixas_agrupadas(ax, dados, "vaz", "vazao_msg_s")
    ax.set_ylim(bottom=0)
    ax.set_ylabel("Vazão de ponta a ponta (msg/s)")
    titulo_fig(fig, "Garantias — vazão máxima por nível (Parte B)",
               "Sem controle de taxa; vazão = mensagens ÷ (última chegada − primeiro envio). Cada ponto = 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def figuras_tcc(dados, pasta):
    """Figuras 24 e 25 do TCC: mesmas caixas do PDF, sem título."""
    for nome, chave, coluna, rotulo, log in (
            (FIG_LATENCIA, "lat", "latencia_mediana_ms", "Latência mediana da execução (ms) — escala log", True),
            (FIG_VAZAO, "vaz", "vazao_msg_s", "Vazão de ponta a ponta (msg/s)", False)):
        fig, ax = plt.subplots(figsize=(7.2, 4.2))
        caixas_agrupadas(ax, dados, chave, coluna)
        if log:
            eixo_log(ax)
        else:
            ax.set_ylim(bottom=0)
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: fmt_int(v)))
        ax.set_ylabel(rotulo)
        fig.tight_layout()
        caminho = os.path.join(pasta, nome)
        fig.savefig(caminho, dpi=200)
        plt.close(fig)
        print(f"Figura para o TCC: {caminho}")


def resumo(dados):
    linhas = []
    for t in GARANTIAS:
        for tec in TECNOLOGIAS:
            d = dados.get(tec)
            if d is None:
                continue
            r = {"garantia": t, "tecnologia": tec}
            if d["lat"] is not None:
                m = d["lat"].loc[d["lat"]["garantia"] == t, "latencia_media_ms"]
                md = d["lat"].loc[d["lat"]["garantia"] == t, "latencia_mediana_ms"]
                r.update(latencia_tipica_ms=md.median(),
                         latencia_mediana_min_ms=md.min(), latencia_mediana_max_ms=md.max(),
                         latencia_media_mediana_ms=m.median(), latencia_media_min_ms=m.min(),
                         latencia_media_max_ms=m.max())
            if d["vaz"] is not None:
                v = d["vaz"].loc[d["vaz"]["garantia"] == t, "vazao_msg_s"]
                vp = d["vaz"].loc[d["vaz"]["garantia"] == t, "vazao_produtor_msg_s"]
                r.update(vazao_msg_s_mediana=v.median(), vazao_msg_s_min=v.min(), vazao_msg_s_max=v.max(),
                         vazao_produtor_msg_s_mediana=vp.median())
            linhas.append(r)
    return pd.DataFrame(linhas)


def tabela_resumo(pdf, res):
    """Mesmas colunas das Tabelas 5 e 6 do TCC."""
    def tem(r, c):
        return c in r and pd.notna(r[c])
    celulas = []
    for _, r in res.iterrows():
        celulas.append([
            NOMES[r["garantia"]], r["tecnologia"],
            fmt(r["latencia_tipica_ms"]) if tem(r, "latencia_tipica_ms") else "—",
            f"{fmt(r['latencia_mediana_min_ms'])} a {fmt(r['latencia_mediana_max_ms'])}"
            if tem(r, "latencia_mediana_min_ms") else "—",
            fmt_int(r["vazao_msg_s_mediana"]) if tem(r, "vazao_msg_s_mediana") else "—",
            f"{fmt_int(r['vazao_msg_s_min'])} a {fmt_int(r['vazao_msg_s_max'])}"
            if tem(r, "vazao_msg_s_min") else "—",
            fmt_int(r["vazao_produtor_msg_s_mediana"]) if tem(r, "vazao_produtor_msg_s_mediana") else "—",
        ])
    fig, ax = plt.subplots(figsize=(11.5, 2.2 + 0.36 * len(celulas)))
    ax.axis("off")
    tab = ax.table(cellText=celulas,
                   colLabels=["Nível", "Tecnologia", "Latência típica\n(ms) — Tab. 5",
                              "Faixa das medianas\n(ms) — Tab. 5", "Vazão de ponta a ponta\n(mediana, msg/s) — Tab. 6",
                              "Faixa da vazão\n(msg/s) — Tab. 6", "Vazão do produtor\n(mediana, msg/s) — Tab. 6"],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(8.8)
    tab.scale(1, 1.55)
    for (rr, _), c in tab.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Garantias — resumo Kafka × RabbitMQ", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Latência: Parte A (200 msg/s, igual nas duas tecnologias). "
             "Vazão: Parte B (sem controle de taxa).\nLatência típica = mediana entre as execuções da "
             "latência mediana de cada execução (pouco afetada por episódios lentos).",
             ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def main():
    dados = carregar()
    if all(v is None for v in dados.values()):
        print("Nenhuma pasta de resultados encontrada. Nada a fazer.")
        return
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA_SCRIPT, SAIDA)
    with PdfPages(saida) as pdf:
        grafico1_latencia(pdf, dados)
        grafico2_ecdf(pdf, dados)
        grafico3_acrescimo(pdf, dados)
        grafico4_vazao(pdf, dados)
        res = resumo(dados)
        tabela_resumo(pdf, res)
    figuras_tcc(dados, os.path.dirname(saida))
    res.to_csv(os.path.join(os.path.dirname(saida), ARQ_RESUMO), index=False, float_format="%.4f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()