#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 3 — Variação do tamanho da mensagem — gráficos do KAFKA.

Lê os CSVs gerados pelo orquestrador Cenario3TamanhoMensagem:
    resultados-kafka-cenario3-latencia.csv            (Parte A, 1 linha por execução e tamanho)
    resultados-kafka-cenario3-latencia-detalhado.csv  (Parte A, 1 linha por mensagem)
    resultados-kafka-cenario3-vazao.csv               (Parte B, 1 linha por execução e tamanho)

Gera um PDF com:
    Gráfico 1 — latência mediana por execução, por tamanho  (1 caixa = 10 execuções)
    Gráfico 2 — latência por mensagem, por tamanho         (escala log)
    Gráfico 3 — latência ao longo da execução, por tamanho  (verifica se formou fila)
    Tabela 1  — verificação: taxa de envio e início × fim da execução
    Gráfico 4 — vazão máxima por tamanho, em MB/s e msg/s  (Parte B)
    Gráfico 5 — FORMA DA CURVA: latência × tamanho (log-log) com o ajuste
                latência = a + b × tamanho, e vazão × tamanho
    Tabela 2  — resumo por tamanho (também gravado em CSV)
    Tabela 3  — ajuste da curva: a (custo fixo), b (custo por KB), R², expoente
                da lei de potência e resíduo por tamanho (também gravado em CSV).
                O ajuste usa a latência típica de cada tamanho (mediana das
                medianas) e só os tamanhos enviados à taxa predominante.

Execuções lentas: uma execução cuja latência mediana passa de LIMIAR_LENTA_MS
(20 ms) é marcada como LENTA (todas as mensagens dela atrasam ~45 ms). O
RESULTADO PRINCIPAL usa TODAS as execuções: caixas, latência típica, ajuste da
curva, gráfico 3 e tabela 1. As lentas aparecem como pontos vazados no
Gráfico 1. A análise só com as execuções normais é uma ANÁLISE DE
SENSIBILIDADE, exploratória: aparece à parte (Tabela 3b e coluna própria na
Tabela 2), sempre com o n de execuções que sobrou em cada tamanho.

Os tamanhos são lidos dos próprios CSVs (funciona com 3 ou com 9 tamanhos).

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_cenario3_kafka.py
ou:
    python plotar_cenario3_kafka.py <pasta_dos_csvs> [saida.pdf]

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
ARQ_LATENCIA  = "resultados-kafka-cenario3-latencia.csv"
ARQ_DETALHADO = "resultados-kafka-cenario3-latencia-detalhado.csv"
ARQ_VAZAO     = "resultados-kafka-cenario3-vazao.csv"
ARQ_RESUMO    = "cenario3-kafka-resumo.csv"
ARQ_AJUSTE    = "cenario3-kafka-ajuste-curva.csv"
SAIDA_PADRAO  = "cenario3-kafka-boxplot.pdf"
COR_CAIXA     = "#185FA5"   # azul
COR_PONTO     = "#0B3C6B"   # azul escuro (mesma família da caixa)
NOME_PONTO    = "azul-escuro"
MAPA_CORES    = "Blues"     # linhas do gráfico 3: do claro (menor) ao escuro (maior)
# ----------------------------------------------------------------------

CENARIO = "Cenário 3 — Tamanho da mensagem"
TAMANHOS = []        # preenchido em main() a partir dos CSVs
CORES_TAMANHO = {}
BLOCO = 50   # mensagens por bloco no gráfico 3 e na tabela 1
LIMIAR_LENTA_MS = 20.0   # execução com latência mediana acima disto = regime lento

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


def largura():
    """Largura das figuras de caixas: cresce com o número de tamanhos."""
    return 7.2 if len(TAMANHOS) <= 4 else 7.2 + 0.55 * (len(TAMANHOS) - 4)


def texto_taxas(lat):
    """Ex.: '50 msg/s; 200 KB a 25 msg/s, 500 KB a 10 msg/s'."""
    taxas = lat.groupby("tamanho_kb")["taxa_alvo_msg_s"].first()
    base = int(taxas.mode().iloc[0])
    excecoes = [f"{int(t)} KB a {int(v)} msg/s" for t, v in taxas.items() if int(v) != base]
    return f"{base} msg/s" + (f"; {', '.join(excecoes)} (teto de carga)" if excecoes else "")


def definir_tamanhos(*dfs):
    global TAMANHOS, CORES_TAMANHO
    ts = set()
    for d in dfs:
        if d is not None and "tamanho_kb" in d:
            ts |= set(int(x) for x in d["tamanho_kb"].unique())
    TAMANHOS = sorted(ts)
    cmap = plt.get_cmap(MAPA_CORES)
    n = max(len(TAMANHOS) - 1, 1)
    CORES_TAMANHO = {t: cmap(0.35 + 0.65 * i / n) for i, t in enumerate(TAMANHOS)}


def marcar_lentas(lat):
    """Coluna 'lenta': a execução inteira ficou no regime lento."""
    lat["lenta"] = lat["latencia_mediana_ms"] > LIMIAR_LENTA_MS
    return lat


def normais(lat):
    return lat[~lat["lenta"]]


def det_normais(det, lat):
    """Só as mensagens das execuções normais (mesmo par execução × tamanho)."""
    if lat is None:
        return det
    lentas = set(map(tuple, lat.loc[lat["lenta"], ["execucao", "tamanho_kb"]].values.tolist()))
    chave = list(zip(det["execucao"], det["tamanho_kb"]))
    return det[[c not in lentas for c in chave]]


def n_lentas(lat, t):
    return int(lat.loc[lat["tamanho_kb"] == t, "lenta"].sum())


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


def caixas_com_pontos(ax, series, rotulos):
    """1 caixa por tamanho; cada ponto = 1 execução."""
    pos = range(1, len(series) + 1)
    ax.boxplot(series, positions=list(pos), widths=0.5, patch_artist=True,
               showfliers=False,  # os pontos já mostram todas as execuções
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    for i, s in zip(pos, series):
        ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=26, color=COR_PONTO,
                   edgecolor="white", linewidth=0.6, zorder=3)
    ax.set_xticks(list(pos))
    ax.set_xticklabels(rotulos)
    ax.grid(False, axis="x")


def tabela(pdf, linhas, colunas, texto, nota=None, larguras=None):
    larg_fig = max(8.6, 1.5 * len(colunas))   # mais colunas = figura mais larga
    fig, ax = plt.subplots(figsize=(larg_fig, 1.9 + 0.36 * len(linhas) + (0.9 if nota else 0)))
    ax.axis("off")
    t = ax.table(cellText=linhas, colLabels=colunas, loc="center", cellLoc="center",
                 colWidths=larguras)
    t.auto_set_font_size(False)
    t.set_fontsize(8.8)
    t.scale(1, 1.9)
    for (r, _), c in t.get_celld().items():
        if r == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title(texto, fontsize=11, pad=10)
    if nota:
        fig.text(0.5, 0.03, nota, ha="center", va="bottom", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.12 if nota else 0, 1, 1])
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
    """Caixa = todas as execuções; pontos cheios = normais, vazados = lentas."""
    nor = normais(lat)
    series = [lat.loc[lat["tamanho_kb"] == t, "latencia_mediana_ms"].values for t in TAMANHOS]
    pontos = [nor.loc[nor["tamanho_kb"] == t, "latencia_mediana_ms"].values for t in TAMANHOS]
    lentas = [lat.loc[(lat["tamanho_kb"] == t) & lat["lenta"], "latencia_mediana_ms"].values
              for t in TAMANHOS]
    n = int(np.median([len(lat[lat["tamanho_kb"] == t]) for t in TAMANHOS]))
    fig, ax = plt.subplots(figsize=(largura(), 5.6))
    rot = [f"{t} KB" + (f"\n({n_lentas(lat, t)} lenta{'s' if n_lentas(lat, t) > 1 else ''})"
                       if n_lentas(lat, t) else "") for t in TAMANHOS]
    for i, s in enumerate(series, start=1):
        if not len(s):
            continue
        ax.boxplot([s], positions=[i], widths=0.5, patch_artist=True, showfliers=False,
                   medianprops=dict(color="black", linewidth=1.6),
                   boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
                   whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    for i, s in enumerate(pontos, start=1):
        if len(s):
            ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=26, color=COR_PONTO,
                       edgecolor="white", linewidth=0.6, zorder=3)
    for i, s in enumerate(lentas, start=1):
        if len(s):
            ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=30, facecolors="none",
                       edgecolors=COR_PONTO, linewidth=1.1, zorder=3)
    ax.set_xticks(range(1, len(TAMANHOS) + 1))
    ax.set_xticklabels(rot, fontsize=8.5)
    ax.set_xlim(0.4, len(TAMANHOS) + 0.6)
    ax.grid(False, axis="x")
    if lat["lenta"].any():   # só mostra o limiar e a legenda se houve execução lenta
        ax.axhline(LIMIAR_LENTA_MS, color="#777777", linestyle=":", linewidth=1)
        ax.scatter([], [], s=26, color=COR_PONTO, label="execução normal")
        ax.scatter([], [], s=30, facecolors="none", edgecolors=COR_PONTO,
                   label=f"execução lenta (mediana > {LIMIAR_LENTA_MS:g} ms)")
        ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9,
                  fontsize=8, loc="upper left")
    ax.set_xlabel("Tamanho da mensagem")
    ax.set_ylabel("Latência mediana da execução (ms) — escala log")
    # escala log: uma única execução atípica (ex.: 250 ms) não achata as demais caixas
    eixo_log(ax)
    titulo(ax, f"{TECNOLOGIA} — latência mediana por execução, por tamanho",
           f"{n} execuções por tamanho; caixa = todas as execuções; cada ponto = a latência mediana de "
           f"1 execução ({texto_taxas(lat)})")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_por_mensagem(pdf, det):
    dados = [det.loc[det["tamanho_kb"] == t, "latencia_ms"].values for t in TAMANHOS]
    pos = list(range(1, len(dados) + 1))
    fig, ax = plt.subplots(figsize=(largura(), 6.0))
    bp = ax.boxplot(dados, positions=pos, widths=0.5, patch_artist=True, showfliers=True,
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
    ax.set_xticks(pos)
    ax.set_xticklabels([f"{t} KB" for t in TAMANHOS])
    ax.set_xlabel("Tamanho da mensagem", labelpad=34)
    ax.set_ylabel("Latência fim a fim (ms) — escala log")
    ax.grid(False, axis="x")
    titulo(ax, f"{TECNOLOGIA} — latência por mensagem, por tamanho",
           "Cada caixa = todas as mensagens das execuções daquele tamanho; pontos = mensagens outliers")
    fig.text(0.5, 0.012,
             "Outliers: mensagens acima de Q3 + 1,5×IQR (critério de Tukey). "
             "n = mensagens outliers; N = mensagens dentro dos bigodes.",
             ha="center", va="bottom", fontsize=7.8, color="#555555")
    fig.tight_layout(rect=[0, 0.04, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def grafico3_ao_longo(pdf, det):
    fig, ax = plt.subplots(figsize=(8.4, 5.2))
    for t in TAMANHOS:
        d = det[det["tamanho_kb"] == t].copy()
        if d.empty:
            continue
        d["bloco"] = (d["mensagem"] - 1) // BLOCO
        g = d.groupby("bloco")["latencia_ms"].median()
        x = g.index * BLOCO + BLOCO / 2
        ax.plot(x, g.values, marker="o", markersize=3, linewidth=1.3, color=CORES_TAMANHO[t],
                label=f"{t} KB")
    eixo_log(ax)
    ax.set_xlabel("Posição da mensagem na execução (ordem de chegada)")
    ax.set_ylabel("Latência mediana do bloco (ms) — escala log")
    ax.legend(title="Tamanho", frameon=False, fontsize=8.5,
              loc="upper left", bbox_to_anchor=(1.01, 1.0))
    titulo(ax, f"{TECNOLOGIA} — latência ao longo da execução",
           f"Mediana de cada bloco de {BLOCO} mensagens, juntando todas as execuções. "
           "Linha sem subida = sem crescimento sustentado da latência")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def tabela1_verificacao(pdf, lat, det):
    linhas = []
    for t in TAMANHOS:
        a = lat[lat["tamanho_kb"] == t]
        d = det[det["tamanho_kb"] == t]
        if a.empty or d.empty:
            continue
        n_max = d["mensagem"].max()
        ini = d[d["mensagem"] <= BLOCO]["latencia_ms"].median()
        fim = d[d["mensagem"] > n_max - BLOCO]["latencia_ms"].median()
        linhas.append([f"{t} KB", f"{int(a['taxa_alvo_msg_s'].iloc[0])}",
                       f"{fmt(a['taxa_envio_msg_s'].min(), 1)} a {fmt(a['taxa_envio_msg_s'].max(), 1)}",
                       fmt(ini), fmt(fim), fmt(fim / ini)])
    tabela(pdf, linhas,
           ["Tamanho", "Taxa alvo\n(msg/s)", "Taxa de envio\nreal (msg/s)",
            f"Latência mediana\n{BLOCO} primeiras (ms)", f"Latência mediana\n{BLOCO} últimas (ms)",
            "Fim ÷ início"],
           f"{TECNOLOGIA} — verificação da Parte A (formou fila?)",
           nota="Todas as execuções. Se a taxa real bate com a alvo e 'fim ÷ início' fica perto de 1, não se observou\n"
                "crescimento sustentado da latência. Valores bem acima de 1 indicam acúmulo progressivo.")


# ----------------------------------------------------------------------
# Parte B — vazão máxima
# ----------------------------------------------------------------------
def grafico4_vazao(pdf, vaz):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10.5 if len(TAMANHOS) <= 4 else 13.5, 5.4))
    rot = [f"{t} KB" for t in TAMANHOS]
    caixas_com_pontos(ax1, [vaz.loc[vaz["tamanho_kb"] == t, "vazao_mb_s"].values for t in TAMANHOS], rot)
    ax1.tick_params(axis="x", labelsize=8 if len(TAMANHOS) > 5 else 10)
    ax1.set_ylabel("Vazão de ponta a ponta (MB/s)")
    ax1.set_xlabel("Tamanho da mensagem")
    ax1.set_ylim(bottom=0)
    ax1.set_title("Em MB/s (volume)", fontsize=10.5)
    caixas_com_pontos(ax2, [vaz.loc[vaz["tamanho_kb"] == t, "vazao_msg_s"].values for t in TAMANHOS], rot)
    eixo_log(ax2)
    ax2.tick_params(axis="x", labelsize=8 if len(TAMANHOS) > 5 else 10)
    ax2.set_ylabel("Vazão de ponta a ponta (msg/s) — escala log")
    ax2.set_xlabel("Tamanho da mensagem")
    ax2.set_title("Em mensagens por segundo", fontsize=10.5)
    n = int(vaz.groupby("tamanho_kb").size().median())
    vol = vaz["volume_mb"].median()
    fig.suptitle(f"{TECNOLOGIA} — vazão máxima por tamanho (Parte B)", fontsize=11.5, y=0.98)
    fig.text(0.5, 0.925,
             f"1 caixa = {n} execuções; cada ponto {NOME_PONTO} = 1 execução de ~{vol:.0f} MB sem controle de taxa. "
             "Vazão = do primeiro envio até a última mensagem chegar ao consumidor",
             ha="center", fontsize=8.3, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Forma da curva
# ----------------------------------------------------------------------
def ajuste_linear(lat, so_normais=False):
    """Ajusta latência = a + b × tamanho (mínimos quadrados) sobre a latência
    TÍPICA de cada tamanho: a mediana, entre as execuções, da latência mediana
    de cada execução (1 ponto por tamanho). Assim, uma execução atípica não
    arrasta a reta, e o ajuste usa o mesmo resumo do texto.

    Só entram no ajuste os tamanhos enviados à taxa predominante (a moda de
    taxa_alvo_msg_s), para que a taxa de envio não se confunda com o efeito do
    tamanho. Se todos os tamanhos usaram a mesma taxa, todos entram.

    Também ajusta a lei de potência log(lat) = log(c) + k·log(tamanho), sobre
    os mesmos pontos, para comparar a forma.

    so_normais=False (padrão, resultado principal): usa todas as execuções.
    so_normais=True: análise de sensibilidade, só as execuções normais."""
    taxa = lat.groupby("tamanho_kb")["taxa_alvo_msg_s"].first()
    base = normais(lat) if so_normais else lat
    tip_todos = base.groupby("tamanho_kb")["latencia_mediana_ms"].median()
    tams = [t for t in taxa[taxa == taxa.mode().iloc[0]].index if t in tip_todos.index]
    tip = tip_todos.loc[tams]
    x, y = tip.index.values.astype(float), tip.values.astype(float)
    b, a = np.polyfit(x, y, 1)
    pred = a + b * x
    r2 = 1 - np.sum((y - pred) ** 2) / np.sum((y - y.mean()) ** 2)
    k, logc = np.polyfit(np.log10(x), np.log10(y), 1)
    por_tam = []
    for t in TAMANHOS:
        obs = tip_todos.get(t, np.nan)
        fit = a + b * t
        por_tam.append(dict(tamanho_kb=t, latencia_mediana_ms=obs, ajuste_ms=fit,
                            residuo_ms=obs - fit, residuo_pct=100 * (obs - fit) / obs,
                            no_ajuste=bool(t in tams), execucoes_lentas=n_lentas(lat, t)))
    return dict(a=a, b=b, r2=r2, k=k, c=10 ** logc, n=len(x),
                fora=[int(t) for t in TAMANHOS if t not in tams]), pd.DataFrame(por_tam)


def grafico5_forma(pdf, lat, vaz):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5.6))

    if lat is not None:
        g = lat.groupby("tamanho_kb")["latencia_mediana_ms"]
        med, lo, hi = g.median(), g.min(), g.max()
        x = med.index.values.astype(float)
        ax1.errorbar(x, med.values, yerr=[med.values - lo.values, hi.values - med.values],
                     fmt="o", color=COR_CAIXA, ecolor=COR_CAIXA, elinewidth=1, capsize=3,
                     markersize=6, label="mediana entre todas as execuções\n(barra = menor a maior)", zorder=3)
        aj, _ = ajuste_linear(lat)
        if lat["lenta"].any():
            gn = normais(lat).groupby("tamanho_kb")["latencia_mediana_ms"].median()
            ajn, _ = ajuste_linear(lat, so_normais=True)
            ax1.scatter(gn.index.values.astype(float), gn.values, s=40, facecolors="none",
                        edgecolors=COR_PONTO, linewidths=1.1, zorder=4,
                        label="sensibilidade: só execuções normais")
            xsn = np.logspace(np.log10(x.min()), np.log10(x.max()), 200)
            ax1.plot(xsn, ajn["a"] + ajn["b"] * xsn, color=COR_PONTO, linewidth=1.0, linestyle=":",
                     label=f"ajuste só das normais: R² = {fmt(ajn['r2'], 3)}")
        xs = np.logspace(np.log10(x.min()), np.log10(x.max()), 200)
        if aj["fora"]:
            fora = med.loc[aj["fora"]]
            ax1.scatter(fora.index.values.astype(float), fora.values, s=90, facecolors="none",
                        edgecolors="#555555", linewidths=1.2, zorder=4,
                        label="fora do ajuste (outra taxa de envio)")
        ax1.plot(xs, aj["a"] + aj["b"] * xs, color=COR_PONTO, linewidth=1.6,
                 label=f"a + b × tamanho ({aj['n']} tamanhos)\na = {fmt(aj['a'])} ms; "
                       f"b = {fmt(aj['b'], 4)} ms/KB; R² = {fmt(aj['r2'], 3)}")
        ax1.plot(xs, aj["c"] * xs ** aj["k"], color="#777777", linewidth=1.1, linestyle="--",
                 label=f"lei de potência (expoente k = {fmt(aj['k'], 2)})")
        ax1.set_xscale("log")
        ax1.set_yscale("log")
        ax1.set_xticks(TAMANHOS)
        ax1.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5]))
        for eixo in (ax1.xaxis, ax1.yaxis):
            eixo.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
            eixo.set_minor_locator(mticker.NullLocator())
        ax1.set_xlabel("Tamanho da mensagem (KB) — escala log")
        ax1.set_ylabel("Latência mediana da execução (ms) — escala log")
        ax1.legend(frameon=False, fontsize=7.8, loc="upper left")
        ax1.set_title("Latência × tamanho (Parte A)", fontsize=10.5)

    if vaz is not None:
        gm = vaz.groupby("tamanho_kb")["vazao_mb_s"].median()
        gn = vaz.groupby("tamanho_kb")["vazao_msg_s"].median()
        x = gm.index.values.astype(float)
        ax2.plot(x, gm.values, marker="o", color=COR_CAIXA, label="MB/s (eixo da esquerda)")
        ax2.set_xscale("log")
        ax2.set_xticks(TAMANHOS)
        ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax2.xaxis.set_minor_locator(mticker.NullLocator())
        ax2.set_ylim(bottom=0)
        ax2.set_xlabel("Tamanho da mensagem (KB) — escala log")
        ax2.set_ylabel("Vazão mediana (MB/s)", color=COR_CAIXA)
        ax3 = ax2.twinx()
        ax3.plot(x, gn.values, marker="s", color=COR_PONTO, label="msg/s (eixo da direita)")
        ax3.set_yscale("log")
        ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}".replace(",", ".")))
        ax3.yaxis.set_minor_formatter(mticker.NullFormatter())
        ax3.set_ylabel("Vazão mediana (msg/s) — escala log", color=COR_PONTO)
        ax3.grid(False)
        ax3.spines["right"].set_visible(True)
        h1, l1 = ax2.get_legend_handles_labels()
        h2, l2 = ax3.get_legend_handles_labels()
        ax2.legend(h1 + h2, l1 + l2, frameon=False, fontsize=8, loc="center left")
        ax2.set_title("Vazão × tamanho (Parte B)", fontsize=10.5)

    fig.suptitle(f"{TECNOLOGIA} — forma da curva", fontsize=11.5, y=0.98)
    fig.text(0.5, 0.925,
             "Esquerda: reta tracejada = lei de potência; curva colorida = custo fixo + custo por KB "
             "(fica plana nas pequenas e sobe nas grandes). Direita: MB/s estabilizando = teto de banda.",
             ha="center", fontsize=8.2, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def tabela3_ajuste(pdf, lat, so_normais=False):
    aj, por_tam = ajuste_linear(lat, so_normais)
    linhas = [[f"{int(r.tamanho_kb)} KB", fmt(r.latencia_mediana_ms), fmt(r.ajuste_ms),
               fmt(r.residuo_ms), fmt(r.residuo_pct, 1) + " %", "Sim" if r.no_ajuste else "Não",
               str(r.execucoes_lentas)]
              for r in por_tam.itertuples()]
    tabela(pdf, linhas,
           ["Tamanho", "Latência típica\nobservada (ms)", "Ajuste\na + b·tamanho (ms)",
            "Resíduo (ms)", "Resíduo (%)", "Entra no\najuste", "Execuções\nlentas"],
           f"{TECNOLOGIA} — ajuste da curva de latência"
           + (" — sensibilidade: só execuções normais" if so_normais else " — todas as execuções"),
           nota=f"a = {fmt(aj['a'])} ms (custo fixo por mensagem); b = {fmt(aj['b'], 4)} ms por KB; "
                f"R² = {fmt(aj['r2'], 3)}; lei de potência k = {fmt(aj['k'], 2)} ({aj['n']} tamanhos).\n"
                + ("Latência típica = mediana das medianas das execuções normais (análise de sensibilidade).\n"
                 if so_normais else "Latência típica = mediana das medianas de todas as execuções.\n") +
                "Linear com custo fixo se os resíduos forem pequenos e sem padrão.\n"
                "k perto de 1 = proporcional; k < 1 = cresce mais devagar que o tamanho.")
    out = por_tam.copy()
    out.insert(0, "tecnologia", TECNOLOGIA)
    out["a_ms"], out["b_ms_por_kb"], out["r2"], out["expoente_k"] = aj["a"], aj["b"], aj["r2"], aj["k"]
    arq = ARQ_AJUSTE.replace(".csv", "-so-normais.csv") if so_normais else ARQ_AJUSTE
    out.to_csv(caminho(arq), index=False, float_format="%.6f")
    print(f"Ajuste gravado: {caminho(arq)}")


# ----------------------------------------------------------------------
# Resumo
# ----------------------------------------------------------------------
def resumo(lat, vaz):
    linhas = []
    for t in TAMANHOS:
        r = {"tecnologia": TECNOLOGIA, "tamanho_kb": t}
        if lat is not None:
            sel = lat[lat["tamanho_kb"] == t]
            m, md = sel["latencia_media_ms"], sel["latencia_mediana_ms"]
            mdn = sel.loc[~sel["lenta"], "latencia_mediana_ms"]
            r.update(execucoes_latencia=len(m), execucoes_lentas=int(sel["lenta"].sum()),
                     latencia_tipica_ms=md.median(),
                     latencia_tipica_q1_ms=md.quantile(0.25), latencia_tipica_q3_ms=md.quantile(0.75),
                     latencia_tipica_min_ms=md.min(), latencia_tipica_max_ms=md.max(),
                     execucoes_normais=len(mdn),
                     latencia_tipica_normal_ms=mdn.median(),
                     latencia_tipica_normal_min_ms=mdn.min(), latencia_tipica_normal_max_ms=mdn.max(),
                     latencia_mediana_mediana_ms=md.median(),
                     latencia_media_mediana_ms=m.median(),
                     latencia_media_min_ms=m.min(), latencia_media_max_ms=m.max())
        if vaz is not None:
            v = vaz.loc[vaz["tamanho_kb"] == t]
            r.update(execucoes_vazao=len(v), vazao_mb_s_mediana=v["vazao_mb_s"].median(),
                     vazao_mb_s_min=v["vazao_mb_s"].min(), vazao_mb_s_max=v["vazao_mb_s"].max(),
                     vazao_msg_s_mediana=v["vazao_msg_s"].median())
        linhas.append(r)
    return pd.DataFrame(linhas)


def tabela2_resumo(pdf, res):
    linhas = []
    for _, r in res.iterrows():
        lin = [f"{int(r['tamanho_kb'])} KB"]
        if "latencia_tipica_ms" in r:
            lin += [fmt(r["latencia_tipica_ms"]),
                    f"{fmt(r['latencia_tipica_min_ms'])} a {fmt(r['latencia_tipica_max_ms'])}",
                    f"{int(r['execucoes_lentas'])} de {int(r['execucoes_latencia'])}",
                    f"{fmt(r['latencia_tipica_normal_ms'])} (n={int(r['execucoes_normais'])})"]
        else:
            lin += ["—", "—", "—", "—"]
        if "vazao_mb_s_mediana" in r:
            lin += [fmt(r["vazao_mb_s_mediana"], 1),
                    f"{fmt(r['vazao_mb_s_min'], 1)} a {fmt(r['vazao_mb_s_max'], 1)}",
                    f"{r['vazao_msg_s_mediana']:,.0f}".replace(",", ".")]
        else:
            lin += ["—", "—", "—"]
        linhas.append(lin)
    tabela(pdf, linhas,
           ["Tamanho", "Latência típica\n(todas, ms)", "Faixa das\nexecuções (ms)",
            "Execuções\nlentas", "Sensibilidade:\nsó normais (ms)", "Vazão máx.\n(mediana, MB/s)", "Vazão máx.\n(faixa, MB/s)", "Vazão máx.\n(mediana, msg/s)"],
           f"{TECNOLOGIA} — {CENARIO} — resumo por tamanho",
           nota="Latência: Parte A (taxa fixa), todas as execuções; lenta = latência mediana da "
                f"execução acima de {LIMIAR_LENTA_MS:g} ms. 'Só normais' = análise de sensibilidade (n = execuções restantes).\n"
                "Vazão: Parte B (sem controle de taxa). 'Mediana' = a mediana entre as execuções.")


def pagina_rosto(pdf):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.92, f"{CENARIO} ({TECNOLOGIA})", ha="center", fontsize=17, weight="bold")
    fig.text(0.5, 0.885, "Parte A: latência a taxa fixa  |  Parte B: vazão máxima",
             ha="center", fontsize=11, color="#555555")
    linhas = ["Arquivos:"]
    for nome in (ARQ_LATENCIA, ARQ_DETALHADO, ARQ_VAZAO):
        linhas.append(f"  [{'OK' if os.path.isfile(caminho(nome)) else '--'}] {nome}")
    fig.text(0.1, 0.83, "\n".join(linhas), ha="left", va="top", fontsize=8.5, family="monospace")
    nota = (
        "Como ler:\n\n"
        "- Gráfico 1: 1 caixa por tamanho; cada ponto = 1 execução inteira (sua\n"
        "  latência mediana). Mensagens a taxa fixa (a taxa usada aparece no\n"
        "  subtítulo do gráfico).\n\n"
        "- Gráfico 2: todas as mensagens de cada tamanho. Aqui um outlier é uma\n"
        "  MENSAGEM atípica (critério de Tukey), não uma execução.\n\n"
        "- Gráfico 3 e Tabela 1: verificação. Se a latência fica estável do início\n"
        "  ao fim da execução, não houve crescimento sustentado da latência.\n\n"
        "- Gráfico 4: vazão máxima (Parte B), do primeiro envio até a última\n"
        "  mensagem chegar. Aqui a fila é esperada, por isso não se olha latência.\n\n"
        "- Gráfico 5 e Tabela 3: forma da curva. Latência × tamanho em log-log com o\n"
        "  ajuste a + b × tamanho (a = custo fixo, b = custo por KB) e a vazão × tamanho.\n"
        "  O ajuste usa a latência típica de cada tamanho e só os tamanhos enviados\n"
        "  à taxa predominante.\n\n"
        f"- Execução lenta = latência mediana acima de {LIMIAR_LENTA_MS:g} ms. Ela entra na\n"
        "  caixa, na latência típica e no ajuste (resultado principal). A análise só\n"
        "  com as normais é de sensibilidade e aparece à parte, com o n restante.\n\n"
        "- Os tamanhos rodam intercalados (ordem rotativa a cada execução) e a\n"
        "  execução de aquecimento de cada tamanho é descartada."
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
    if lat is not None:
        marcar_lentas(lat)
    definir_tamanhos(lat, det, vaz)
    print(f"Tamanhos encontrados: {TAMANHOS} KB")

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
        if (lat is not None or vaz is not None) and len(TAMANHOS) >= 3:
            grafico5_forma(pdf, lat, vaz)
        if lat is not None and len(TAMANHOS) >= 3:
            tabela3_ajuste(pdf, lat)
            if lat["lenta"].any():
                tabela3_ajuste(pdf, lat, so_normais=True)
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