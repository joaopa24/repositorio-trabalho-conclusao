#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Nível intermediário — Cenário 1: Comparação de garantias — gráficos do KAFKA.

Lê os CSVs gerados pelo orquestrador Cenario1ComparacaoGarantias:
    resultados-kafka-garantias-latencia.csv            (Parte A, 1 linha por execução e nível)
    resultados-kafka-garantias-latencia-detalhado.csv  (Parte A, 1 linha por mensagem)
    resultados-kafka-garantias-vazao.csv               (Parte B, 1 linha por execução e nível)

Gera um PDF com:
    Gráfico 1 — latência média por execução, por nível  (1 caixa = 10 execuções)
    Gráfico 2 — latência por mensagem, por nível         (escala log)
    Gráfico 3 — latência ao longo da execução            (verifica se formou fila)
    Tabela 1  — verificação: taxa de envio, início × fim, duplicatas
    Gráfico 4 — vazão máxima por nível (Parte B)
    Tabela 2  — resumo por nível (também gravado em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_garantias_kafka.py
ou:
    python plotar_garantias_kafka.py <pasta_dos_csvs> [saida.pdf]

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

# ----------------------------------------------------------------------
# Configuração específica desta tecnologia
# ----------------------------------------------------------------------
TECNOLOGIA    = "Kafka"
ARQ_LATENCIA  = "resultados-kafka-garantias-latencia.csv"
ARQ_DETALHADO = "resultados-kafka-garantias-latencia-detalhado.csv"
ARQ_VAZAO     = "resultados-kafka-garantias-vazao.csv"
ARQ_RESUMO    = "garantias-kafka-resumo.csv"
SAIDA_PADRAO  = "garantias-kafka-boxplot.pdf"
COR_CAIXA     = "#185FA5"   # azul
COR_PONTO     = "#D85A30"   # laranja
NOME_PONTO    = "laranja"
# ----------------------------------------------------------------------

CENARIO = "Intermediário — Cenário 1: Comparação de garantias"
GARANTIAS = ["at-most-once", "at-least-once", "exactly-once"]
NOMES = {"at-most-once": "at most once", "at-least-once": "at least once", "exactly-once": "exactly once"}
CORES_NIVEL = {"at-most-once": "#7FB3D5", "at-least-once": "#2E86C1", "exactly-once": "#1B2631"}
BLOCO = 250   # mensagens por bloco no gráfico 3 e na tabela 1

plt.rcParams.update({
    "font.size": 10,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 120,
})

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA = sys.argv[1] if len(sys.argv) > 1 else PASTA_SCRIPT
SAIDA = sys.argv[2] if len(sys.argv) > 2 else SAIDA_PADRAO

RNG = np.random.default_rng(42)  # jitter fixo: o gráfico sai igual sempre


def caminho(nome):
    return os.path.join(PASTA, nome)


def ler_csv(nome):
    if not os.path.isfile(caminho(nome)):
        print(f"[aviso] {nome} não encontrado em {os.path.abspath(PASTA)}")
        return None
    df = pd.read_csv(caminho(nome), encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df[df["execucao"] > 0].copy()


def fmt(v, casas=2):
    return f"{v:.{casas}f}".replace(".", ",")


def fmt_int(v):
    return f"{v:,.0f}".replace(",", ".")


def rotulos():
    return [NOMES[g] for g in GARANTIAS]


# ----------------------------------------------------------------------
# Auxiliares de desenho
# ----------------------------------------------------------------------
def titulo(ax, texto, sub):
    ax.set_title(texto, fontsize=11.5, pad=24)
    ax.text(0.5, 1.02, sub, transform=ax.transAxes, ha="center", fontsize=8.5, color="#555555")


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


def caixas_com_pontos(ax, series, rot):
    """1 caixa por nível; cada ponto = 1 execução."""
    pos = list(range(1, len(series) + 1))
    ax.boxplot(series, positions=pos, widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    for i, s in zip(pos, series):
        ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=26, color=COR_PONTO,
                   edgecolor="white", linewidth=0.6, zorder=3)
    ax.set_xticks(pos)
    ax.set_xticklabels(rot)
    ax.grid(False, axis="x")


def tabela(pdf, linhas, colunas, texto, nota=None):
    fig, ax = plt.subplots(figsize=(9.0, 1.9 + 0.38 * len(linhas) + (0.5 if nota else 0)))
    ax.axis("off")
    t = ax.table(cellText=linhas, colLabels=colunas, loc="center", cellLoc="center")
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.6)
    for (r, _), c in t.get_celld().items():
        if r == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(texto, fontsize=11, pad=10)
    if nota:
        fig.text(0.5, 0.03, nota, ha="center", va="bottom", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.09 if nota else 0, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def aviso_pagina(pdf, titulo_txt, texto):
    fig = plt.figure(figsize=(8.27, 3))
    fig.text(0.08, 0.75, titulo_txt, fontsize=12, weight="bold")
    fig.text(0.08, 0.55, texto, fontsize=9.5, va="top", color="#333333")
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Parte A — latência
# ----------------------------------------------------------------------
def grafico1_latencia_media(pdf, lat):
    series = [lat.loc[lat["garantia"] == g, "latencia_media_ms"].values for g in GARANTIAS]
    n = int(np.median([len(s) for s in series]))
    fig, ax = plt.subplots(figsize=(7.2, 5.6))
    caixas_com_pontos(ax, series, rotulos())
    ax.set_xlabel("Nível de garantia")
    ax.set_ylabel("Latência média da execução (ms) — escala log")
    eixo_log(ax)
    taxa = int(lat["taxa_alvo_msg_s"].iloc[0])
    titulo(ax, f"{TECNOLOGIA} — latência média por execução, por nível",
           f"1 caixa = {n} execuções; cada ponto {NOME_PONTO} = a latência média de 1 execução ({taxa} msg/s)")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_por_mensagem(pdf, det):
    dados = [det.loc[det["garantia"] == g, "latencia_ms"].values for g in GARANTIAS]
    fig, ax = plt.subplots(figsize=(7.2, 6.0))
    bp = ax.boxplot(dados, positions=[1, 2, 3], widths=0.5, patch_artist=True, showfliers=True,
                    flierprops=dict(marker="o", markersize=2.5, markerfacecolor=COR_PONTO,
                                    markeredgecolor="none", alpha=0.5),
                    medianprops=dict(color="black", linewidth=1.5),
                    boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
                    whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    trans = ax.get_xaxis_transform()
    ax.tick_params(axis="x", pad=10)
    for i, serie in enumerate(dados):
        n_out = len(bp["fliers"][i].get_ydata())
        ax.text(i + 1, -0.095, f"n={n_out}", transform=trans, ha="center", va="top",
                fontsize=7.5, color="#333333", clip_on=False)
        ax.text(i + 1, -0.135, f"N={len(serie) - n_out}", transform=trans, ha="center",
                va="top", fontsize=7.5, color="#777777", clip_on=False)
    eixo_log(ax)
    ax.set_xticks([1, 2, 3])
    ax.set_xticklabels(rotulos())
    ax.set_xlabel("Nível de garantia", labelpad=34)
    ax.set_ylabel("Latência fim a fim (ms) — escala log")
    ax.grid(False, axis="x")
    titulo(ax, f"{TECNOLOGIA} — latência por mensagem, por nível",
           "Cada caixa = todas as mensagens das execuções daquele nível; pontos = mensagens outliers")
    fig.text(0.5, 0.012,
             "Outliers: mensagens acima de Q3 + 1,5×IQR (critério de Tukey). "
             "n = mensagens outliers; N = mensagens dentro dos bigodes.",
             ha="center", va="bottom", fontsize=7.8, color="#555555")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def grafico3_ao_longo(pdf, det):
    fig, ax = plt.subplots(figsize=(8.4, 5.2))
    for g in GARANTIAS:
        d = det[det["garantia"] == g].copy()
        if d.empty:
            continue
        d["bloco"] = (d["mensagem"] - 1) // BLOCO
        s = d.groupby("bloco")["latencia_ms"].median()
        ax.plot(s.index * BLOCO + BLOCO / 2, s.values, marker="o", markersize=3, linewidth=1.3,
                color=CORES_NIVEL[g], label=NOMES[g])
    eixo_log(ax)
    ax.set_xlabel("Posição da mensagem na execução (ordem de chegada)")
    ax.set_ylabel("Latência mediana do bloco (ms) — escala log")
    ax.legend(title="Nível", frameon=False, fontsize=8.5, loc="upper left", bbox_to_anchor=(1.01, 1.0))
    titulo(ax, f"{TECNOLOGIA} — latência ao longo da execução",
           f"Mediana de cada bloco de {BLOCO} mensagens, juntando as execuções. Linha reta = não formou fila")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def tabela1_verificacao(pdf, lat, det):
    linhas = []
    for g in GARANTIAS:
        a = lat[lat["garantia"] == g]
        d = det[det["garantia"] == g]
        if a.empty or d.empty:
            continue
        n_max = d["mensagem"].max()
        ini = d[d["mensagem"] <= BLOCO]["latencia_ms"].median()
        fim = d[d["mensagem"] > n_max - BLOCO]["latencia_ms"].median()
        lin = [NOMES[g], str(len(a)),
               f"{fmt(a['taxa_envio_msg_s'].min(), 1)} a {fmt(a['taxa_envio_msg_s'].max(), 1)}",
               fmt(ini), fmt(fim), fmt(fim / ini), str(int(a["duplicatas"].sum()))]
        if TECNOLOGIA == "Kafka":
            t = a["transacoes"].median() if "transacoes" in a.columns else 0
            lin.append(fmt_int(t) if t > 0 else "—")
        linhas.append(lin)
    colunas = ["Nível", "Execuções", "Taxa de envio\nreal (msg/s)", f"Latência mediana\n{BLOCO} primeiras (ms)",
               f"Latência mediana\n{BLOCO} últimas (ms)", "Fim ÷ início", "Duplicatas"]
    if TECNOLOGIA == "Kafka":
        colunas.append("Transações por\nexecução")
    tabela(pdf, linhas, colunas, f"{TECNOLOGIA} — verificação da Parte A",
           nota="Taxa real igual à alvo e 'fim ÷ início' perto de 1 = não houve acúmulo de fila.\n"
                "Duplicatas devem ser 0: o cenário não provoca falhas (isso é avaliado no Cenário 2).")


# ----------------------------------------------------------------------
# Parte B — vazão máxima
# ----------------------------------------------------------------------
def grafico4_vazao(pdf, vaz):
    series = [vaz.loc[vaz["garantia"] == g, "vazao_msg_s"].values for g in GARANTIAS]
    n = int(vaz.groupby("garantia").size().median())
    fig, ax = plt.subplots(figsize=(7.2, 5.6))
    caixas_com_pontos(ax, series, rotulos())
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Nível de garantia")
    ax.set_ylabel("Vazão de ponta a ponta (msg/s)")
    titulo(ax, f"{TECNOLOGIA} — vazão máxima por nível (Parte B)",
           f"1 caixa = {n} execuções de {fmt_int(vaz['num_msgs'].iloc[0])} mensagens sem controle de taxa; "
           "do primeiro envio até a última chegada")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Resumo
# ----------------------------------------------------------------------
def resumo(lat, vaz):
    linhas = []
    base = None
    if lat is not None:
        base = lat.loc[lat["garantia"] == GARANTIAS[0], "latencia_mediana_ms"].median()
    for g in GARANTIAS:
        r = {"tecnologia": TECNOLOGIA, "garantia": g}
        if lat is not None:
            m = lat.loc[lat["garantia"] == g, "latencia_media_ms"]
            md = lat.loc[lat["garantia"] == g, "latencia_mediana_ms"]
            r.update(execucoes_latencia=len(m), latencia_tipica_ms=md.median(),
                     aumento_tipica_vs_amo_ms=md.median() - base,
                     latencia_media_mediana_ms=m.median(), latencia_media_min_ms=m.min(),
                     latencia_media_max_ms=m.max())
        if vaz is not None:
            v = vaz.loc[vaz["garantia"] == g, "vazao_msg_s"]
            r.update(execucoes_vazao=len(v), vazao_mediana_msg_s=v.median(),
                     vazao_min_msg_s=v.min(), vazao_max_msg_s=v.max())
        linhas.append(r)
    return pd.DataFrame(linhas)


def tabela2_resumo(pdf, res):
    linhas = []
    for _, r in res.iterrows():
        lin = [NOMES[r["garantia"]]]
        if "latencia_tipica_ms" in r:
            lin += [fmt(r["latencia_tipica_ms"]),
                    ("+" if r["aumento_tipica_vs_amo_ms"] >= 0 else "") + fmt(r["aumento_tipica_vs_amo_ms"]),
                    fmt(r["latencia_media_mediana_ms"]),
                    f"{fmt(r['latencia_media_min_ms'])} a {fmt(r['latencia_media_max_ms'])}"]
        else:
            lin += ["—"] * 4
        if "vazao_mediana_msg_s" in r:
            lin += [fmt_int(r["vazao_mediana_msg_s"]),
                    f"{fmt_int(r['vazao_min_msg_s'])} a {fmt_int(r['vazao_max_msg_s'])}"]
        else:
            lin += ["—", "—"]
        linhas.append(lin)
    tabela(pdf, linhas,
           ["Nível", "Latência típica\n(ms)", "Aumento vs.\nat most once (ms)", "Latência média\n(mediana, ms)",
            "Latência média\n(menor a maior, ms)", "Vazão máx.\n(mediana, msg/s)", "Vazão máx.\n(menor a maior)"],
           f"{TECNOLOGIA} — {CENARIO} — resumo",
           nota="Latência: Parte A (taxa fixa). Vazão: Parte B (sem controle de taxa).\n"
                "Latência típica = mediana entre as execuções da latência mediana de cada execução.")


def pagina_rosto(pdf):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.92, f"{CENARIO}", ha="center", fontsize=15, weight="bold")
    fig.text(0.5, 0.89, f"{TECNOLOGIA} — Parte A: latência a taxa fixa  |  Parte B: vazão máxima",
             ha="center", fontsize=11, color="#555555")
    linhas = ["Arquivos:"]
    for nome in (ARQ_LATENCIA, ARQ_DETALHADO, ARQ_VAZAO):
        linhas.append(f"  [{'OK' if os.path.isfile(caminho(nome)) else '--'}] {nome}")
    fig.text(0.1, 0.83, "\n".join(linhas), ha="left", va="top", fontsize=8.5, family="monospace")
    nota = (
        "Como ler:\n\n"
        "- Gráfico 1: 1 caixa por nível; cada ponto = 1 execução inteira (sua\n"
        "  latência média). Mensagens enviadas a taxa fixa, igual nos três níveis.\n\n"
        "- Gráfico 2: todas as mensagens de cada nível. Um outlier é uma MENSAGEM\n"
        "  atípica (critério de Tukey), não uma execução.\n\n"
        "- Gráfico 3 e Tabela 1: verificação. Latência estável do início ao fim =\n"
        "  não formou fila, e a Parte A mede o custo da garantia.\n\n"
        "- Gráfico 4: vazão máxima (Parte B), do primeiro envio até a última chegada.\n\n"
        "- Nenhuma falha é provocada: o que se mede é o CUSTO de cada nível.\n"
        "  O comportamento sob falha é avaliado no Cenário 2 (Falha do Consumidor).\n\n"
        "- Os níveis rodam intercalados (ordem rotativa) e o aquecimento é descartado."
    )
    fig.text(0.1, 0.70, nota, ha="left", va="top", fontsize=9.5, color="#333333")
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    print(f"Lendo CSVs de: {os.path.abspath(PASTA)}")
    lat = ler_csv(ARQ_LATENCIA)
    det = ler_csv(ARQ_DETALHADO)
    vaz = ler_csv(ARQ_VAZAO)

    with PdfPages(saida) as pdf:
        pagina_rosto(pdf)
        if lat is not None:
            grafico1_latencia_media(pdf, lat)
        if det is not None:
            grafico2_por_mensagem(pdf, det)
            grafico3_ao_longo(pdf, det)
        if lat is not None and det is not None:
            tabela1_verificacao(pdf, lat, det)
        if vaz is not None:
            grafico4_vazao(pdf, vaz)
        if lat is not None or vaz is not None:
            res = resumo(lat, vaz)
            tabela2_resumo(pdf, res)
            res.to_csv(caminho(ARQ_RESUMO), index=False, float_format="%.4f")
            print(f"Resumo gravado: {caminho(ARQ_RESUMO)}")
        else:
            aviso_pagina(pdf, "Nenhum CSV encontrado",
                         f"Rode o script dentro da pasta de resultados ou passe a pasta como argumento.\n"
                         f"Pasta lida: {os.path.abspath(PASTA)}")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
