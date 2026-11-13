#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 3 — Tamanho da mensagem — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — latência mediana por execução, por tamanho, as duas
                tecnologias lado a lado (Parte A)
    Gráfico 2 — distribuição acumulada da latência de todas as mensagens,
                um painel por tamanho (Parte A)
    Gráfico 3 — quanto a latência AUMENTA em relação a 1 KB, em cada
                tecnologia, para cada tamanho (Parte A)
    Gráfico 4 — vazão máxima em MB/s, por tamanho (Parte B)
    Gráfico 5 — FORMA DA CURVA, as duas tecnologias juntas: latência × tamanho
                (log-log, com o ajuste a + b × tamanho) e vazão × tamanho
    Tabela    — resumo das duas tecnologias (também gravado em CSV)
    Tabela 2  — ajuste da curva de latência das duas tecnologias (a, b, R², k)
    Figuras do TCC (PNG, sem título): cenario3-figura-latencia.png (Figura 18)
                e cenario3-figura-forma.png (Figura 19)
    Tabela 3  — critério das caixas (Seção 3.2), por tamanho: latência (Parte A,
                todas as execuções) e vazão em msg/s (Parte B). Caixas que não se
                sobrepõem = diferentes nas execuções observadas; que se sobrepõem =
                valores próximos, sem demonstração de equivalência.

Execuções lentas: latência mediana acima de LIMIAR_LENTA_MS (20 ms). O RESULTADO
PRINCIPAL usa TODAS as execuções (caixas, latência típica, acréscimos, ajuste e
figuras do TCC); as lentas aparecem como pontos vazados. A análise só com as
execuções normais é uma ANÁLISE DE SENSIBILIDADE, exploratória, mostrada à
parte e sempre com o n de execuções restantes.

Os tamanhos são lidos dos próprios CSVs (funciona com 3 ou com 9 tamanhos).

Organização esperada (o script numa pasta própria, ao lado das pastas de
resultados):

    TCC\\
    ├── comparativo_cenario3\\
    │   └── plotar_cenario3_comparativo.py
    ├── resultado_cenario_1_3\\              (Kafka)
    └── resultado_rabbitmq_cenario_1_3\\     (RabbitMQ)

    cd comparativo_cenario3
    python plotar_cenario3_comparativo.py

Também aceita os caminhos como argumentos:
    python plotar_cenario3_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

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

TAMANHOS = []   # preenchido em main() a partir dos CSVs

TECNOLOGIAS = {
    # nome: (pasta, prefixo dos arquivos, cor da caixa, cor dos pontos)
    "Kafka":    ("resultado_cenario_1_3",          "resultados-kafka-cenario3",    "#185FA5", "#0B3C6B"),
    "RabbitMQ": ("resultado_rabbitmq_cenario_1_3", "resultados-rabbitmq-cenario3", "#D85A30", "#8A3313"),
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

LIMIAR_LENTA_MS = 20.0   # execução com latência mediana acima disto = regime lento

SAIDA = "cenario3-comparativo.pdf"
ARQ_RESUMO = "cenario3-comparativo-resumo.csv"
ARQ_AJUSTE = "cenario3-comparativo-ajuste-curva.csv"
ARQ_CAIXAS = "cenario3-comparativo-criterio-caixas.csv"
FIG_TCC_LAT = "cenario3-figura-latencia.png"    # Figura 18 do TCC
FIG_TCC_FORMA = "cenario3-figura-forma.png"     # Figura 19 do TCC


def definir_tamanhos(dados):
    global TAMANHOS
    ts = set()
    for d in dados.values():
        if d is None:
            continue
        for chave in ("lat", "det", "vaz"):
            if d[chave] is not None:
                ts |= set(int(x) for x in d[chave]["tamanho_kb"].unique())
    TAMANHOS = sorted(ts)


def largura(base=8.4):
    return base if len(TAMANHOS) <= 4 else base + 0.65 * (len(TAMANHOS) - 4)


def texto_taxas(dados):
    for tec in TECNOLOGIAS:
        d = dados.get(tec)
        if d is not None and d["lat"] is not None:
            taxas = d["lat"].groupby("tamanho_kb")["taxa_alvo_msg_s"].first()
            base = int(taxas.mode().iloc[0])
            exc = [f"{int(t)} KB a {int(v)} msg/s" for t, v in taxas.items() if int(v) != base]
            return f"{base} msg/s" + (f"; {', '.join(exc)} (teto de 5 MB/s)" if exc else "")
    return ""


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
    if "latencia_mediana_ms" in df.columns:
        df["lenta"] = df["latencia_mediana_ms"] > LIMIAR_LENTA_MS
    return df


def normais(lat):
    return lat[~lat["lenta"]]


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


def caixas_agrupadas(ax, dados, chave, coluna, separar_lentas=False):
    """Para cada tamanho, uma caixa por tecnologia (lado a lado); pontos = execuções.
    A caixa usa sempre todas as execuções. Com separar_lentas, as lentas viram
    pontos vazados (e as normais, pontos cheios)."""
    largura = 0.34
    desloc = {tec: (-0.2 if i == 0 else 0.2) for i, tec in enumerate(TECNOLOGIAS)}
    for tec, (_, _, cor, cor_ponto) in TECNOLOGIAS.items():
        if dados.get(tec) is None or dados[tec][chave] is None:
            continue
        df = dados[tec][chave]
        for j, t in enumerate(TAMANHOS, start=1):
            sel = df[df["tamanho_kb"] == t]
            x = j + desloc[tec]
            v = sel[coluna].values
            if len(v) == 0:
                continue
            ax.boxplot([v], positions=[x], widths=largura, patch_artist=True, showfliers=False,
                       medianprops=dict(color="black", linewidth=1.5),
                       boxprops=dict(facecolor=cor, alpha=0.5, edgecolor=cor),
                       whiskerprops=dict(color=cor), capprops=dict(color=cor))
            cheios = sel[~sel["lenta"]][coluna].values if separar_lentas else v
            ax.scatter(x + RNG.uniform(-0.05, 0.05, len(cheios)), cheios, s=18, color=cor_ponto,
                       edgecolor="white", linewidth=0.5, zorder=3)
            if separar_lentas:
                vl = sel.loc[sel["lenta"], coluna].values
                if len(vl):
                    ax.scatter(x + RNG.uniform(-0.05, 0.05, len(vl)), vl, s=20, facecolors="none",
                               edgecolors=cor_ponto, linewidth=1.0, zorder=3)
        ax.plot([], [], color=cor, linewidth=8, alpha=0.5, label=tec)
    ax.set_xticks(range(1, len(TAMANHOS) + 1))
    ax.set_xticklabels([f"{t} KB" for t in TAMANHOS])
    ax.set_xlabel("Tamanho da mensagem")
    ax.grid(False, axis="x")
    if separar_lentas:
        ax.scatter([], [], s=20, facecolors="none", edgecolors="#555555",
                   label=f"execução lenta (mediana > {LIMIAR_LENTA_MS:g} ms)")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="upper left",
              fontsize=8.5)


def grafico1_latencia(pdf, dados):
    fig, ax = plt.subplots(figsize=(largura(), 5.8))
    caixas_agrupadas(ax, dados, "lat", "latencia_mediana_ms", separar_lentas=True)
    ax.axhline(LIMIAR_LENTA_MS, color="#777777", linestyle=":", linewidth=1)
    eixo_log(ax)
    ax.set_ylabel("Latência mediana da execução (ms) — escala log")
    titulo_fig(fig, "Cenário 3 — latência mediana por execução, por tamanho (Parte A)",
               "Caixa = todas as execuções de uma tecnologia naquele tamanho; cada ponto = 1 execução; "
               f"vazado = lenta. Taxa: {texto_taxas(dados)}")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_ecdf(pdf, dados):
    n = len(TAMANHOS)
    ncol = min(n, 3)
    nlin = int(np.ceil(n / ncol))
    fig, eixos = plt.subplots(nlin, ncol, figsize=(12, 4.2 * nlin + 0.6), sharey=True, squeeze=False)
    eixos = eixos.ravel()
    for ax in eixos[n:]:
        ax.set_visible(False)
    eixos = eixos[:n]
    for ax, t in zip(eixos, TAMANHOS):
        for tec, (_, _, cor, _) in TECNOLOGIAS.items():
            if dados.get(tec) is None or dados[tec]["det"] is None:
                continue
            v = np.sort(dados[tec]["det"].loc[dados[tec]["det"]["tamanho_kb"] == t, "latencia_ms"].values)
            if len(v) == 0:
                continue
            ax.plot(v, np.arange(1, len(v) + 1) / len(v), color=cor, linewidth=1.6, label=tec)
        eixo_log(ax, "x")
        ax.set_title(f"{t} KB", fontsize=10.5)
        ax.set_xlabel("Latência (ms) — escala log")
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(1.0))
    for ax in eixos[::ncol]:
        ax.set_ylabel("Mensagens que chegaram em até X ms")
    eixos[-1].legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="lower right")
    titulo_fig(fig, "Cenário 3 — distribuição acumulada da latência por mensagem (Parte A)",
               "Curva mais à esquerda = mensagens chegando mais rápido. Todas as mensagens das execuções.")
    fig.tight_layout(rect=[0, 0, 1, 1 - 0.9 / fig.get_figheight()])
    pdf.savefig(fig)
    plt.close(fig)


def acrescimos(dados):
    """Acréscimo da latência TÍPICA em relação a 1 KB: mediana, entre as
    execuções, da latência mediana de cada execução. Usa a mediana (e não a
    média) para que poucos episódios lentos não dominem o resultado."""
    out = {}
    for tec in TECNOLOGIAS:
        if dados.get(tec) is None or dados[tec]["lat"] is None:
            continue
        lat = dados[tec]["lat"]   # todas as execuções (resultado principal)
        med = {t: lat.loc[lat["tamanho_kb"] == t, "latencia_mediana_ms"].median() for t in TAMANHOS}
        out[tec] = {t: med[t] - med[TAMANHOS[0]] for t in TAMANHOS[1:]}
    return out


def grafico3_acrescimo(pdf, dados):
    acr = acrescimos(dados)
    if not acr:
        return
    fig, ax = plt.subplots(figsize=(largura(7.8), 5.2))
    x = np.arange(len(TAMANHOS) - 1)
    larg = 0.36
    for i, (tec, v) in enumerate(acr.items()):
        cor = TECNOLOGIAS[tec][2]
        vals = [v[t] for t in TAMANHOS[1:]]
        pos = x + (i - 0.5) * larg
        ax.bar(pos, vals, width=larg, color=cor, alpha=0.75, label=tec)
        for p, val in zip(pos, vals):
            ax.text(p, val, f"{'+' if val >= 0 else ''}{fmt(val)}", ha="center",
                    va="bottom" if val >= 0 else "top", fontsize=8.5 if len(TAMANHOS) <= 4 else 7)
    ax.axhline(0, color="#555555", linewidth=0.8)
    ax.set_xticks(x)
    base = TAMANHOS[0]
    ax.set_xticklabels([f"de {base} KB\npara {t} KB" for t in TAMANHOS[1:]],
                       fontsize=10 if len(TAMANHOS) <= 4 else 8)
    ax.set_ylabel("Aumento da latência típica (ms)")
    ax.grid(False, axis="x")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="upper left")
    titulo_fig(fig, "Cenário 3 — quanto a latência aumenta com o tamanho (Parte A)",
               "Latência típica = mediana entre todas as execuções da latência mediana de cada uma; "
               "(tamanho maior) − (menor tamanho), em ms")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico4_vazao(pdf, dados):
    fig, ax = plt.subplots(figsize=(largura(), 5.8))
    caixas_agrupadas(ax, dados, "vaz", "vazao_mb_s")
    ax.set_ylim(bottom=0)
    ax.set_ylabel("Vazão de ponta a ponta (MB/s)")
    titulo_fig(fig, "Cenário 3 — vazão máxima por tamanho (Parte B)",
               "Sem controle de taxa; vazão = volume ÷ (última chegada − primeiro envio). Cada ponto = 1 execução")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def ajuste_linear(lat, so_normais=False):
    """latência = a + b × tamanho, sobre a latência TÍPICA de cada tamanho
    (mediana, entre as execuções, da latência mediana de cada uma; 1 ponto por
    tamanho), usando só os tamanhos enviados à taxa predominante. Também a lei
    de potência log(lat) = log(c) + k·log(tamanho) sobre os mesmos pontos.
    Mesmo cálculo dos scripts individuais. so_normais=True = análise de
    sensibilidade (sem as execuções lentas)."""
    taxa = lat.groupby("tamanho_kb")["taxa_alvo_msg_s"].first()
    base = normais(lat) if so_normais else lat
    tip = base.groupby("tamanho_kb")["latencia_mediana_ms"].median()
    tams = [t for t in taxa[taxa == taxa.mode().iloc[0]].index if t in tip.index]
    tip = tip.loc[tams]
    x, y = tip.index.values.astype(float), tip.values.astype(float)
    b, a = np.polyfit(x, y, 1)
    pred = a + b * x
    r2 = 1 - np.sum((y - pred) ** 2) / np.sum((y - y.mean()) ** 2)
    k, logc = np.polyfit(np.log10(x), np.log10(y), 1)
    return dict(a=a, b=b, r2=r2, k=k, c=10 ** logc, n=len(x),
                fora=[int(t) for t in sorted(taxa.index) if t not in tams],
                lentas=int(lat["lenta"].sum()), execucoes=len(lat))


def grafico5_forma(pdf, dados):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5.6))
    for tec, (_, _, cor, _) in TECNOLOGIAS.items():
        d = dados.get(tec)
        if d is None:
            continue
        if d["lat"] is not None:
            g = d["lat"].groupby("tamanho_kb")["latencia_mediana_ms"]
            med, lo, hi = g.median(), g.min(), g.max()
            x = med.index.values.astype(float)
            ax1.errorbar(x, med.values, yerr=[med.values - lo.values, hi.values - med.values], fmt="o",
                         color=cor, ecolor=cor, elinewidth=1, capsize=3, markersize=5, zorder=3)
            aj = ajuste_linear(d["lat"])
            if d["lat"]["lenta"].any():
                gn = normais(d["lat"]).groupby("tamanho_kb")["latencia_mediana_ms"].median()
                ajn = ajuste_linear(d["lat"], so_normais=True)
                xsn = np.logspace(np.log10(x.min()), np.log10(x.max()), 200)
                ax1.scatter(gn.index.values.astype(float), gn.values, s=36, facecolors="none",
                            edgecolors=cor, linewidths=1.0, zorder=4)
                ax1.plot(xsn, ajn["a"] + ajn["b"] * xsn, color=cor, linewidth=1.0, linestyle=":",
                         label=f"{tec}, sensibilidade (só normais): R² = {fmt(ajn['r2'], 3)}")
            xs = np.logspace(np.log10(x.min()), np.log10(x.max()), 200)
            if aj["fora"]:
                fora = med.loc[aj["fora"]]
                ax1.scatter(fora.index.values.astype(float), fora.values, s=80, facecolors="none",
                            edgecolors="#555555", linewidths=1.1, zorder=4)
            ax1.plot(xs, aj["a"] + aj["b"] * xs, color=cor, linewidth=1.5,
                     label=f"{tec}: a = {fmt(aj['a'])} ms; b = {fmt(aj['b'], 4)} ms/KB; "
                           f"R² = {fmt(aj['r2'], 3)}")
        if d["vaz"] is not None:
            gm = d["vaz"].groupby("tamanho_kb")["vazao_mb_s"]
            med, lo, hi = gm.median(), gm.min(), gm.max()
            x = med.index.values.astype(float)
            ax2.errorbar(x, med.values, yerr=[med.values - lo.values, hi.values - med.values], fmt="-o",
                         color=cor, ecolor=cor, elinewidth=1, capsize=3, markersize=5, label=tec)
    for ax in (ax1, ax2):
        ax.set_xscale("log")
        ax.set_xticks(TAMANHOS)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_locator(mticker.NullLocator())
        ax.set_xlabel("Tamanho da mensagem (KB) — escala log")
    ax1.set_yscale("log")
    ax1.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5]))
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    ax1.yaxis.set_minor_locator(mticker.NullLocator())
    ax1.set_ylabel("Latência mediana da execução (ms) — escala log")
    ax1.set_title("Latência × tamanho (Parte A)", fontsize=10.5)
    ax1.legend(frameon=False, fontsize=8, loc="upper left")
    ax2.set_ylim(bottom=0)
    ax2.set_ylabel("Vazão de ponta a ponta (MB/s)")
    ax2.set_title("Vazão × tamanho (Parte B)", fontsize=10.5)
    ax2.legend(frameon=False, fontsize=8.5, loc="upper left")
    titulo_fig(fig, "Cenário 3 — forma da curva, Kafka × RabbitMQ",
               "Pontos = mediana entre todas as execuções (barra = menor a maior); vazados e pontilhado = só normais. "
               "Esquerda: curva = ajuste a + b × tamanho. Direita: estabilizar = teto de banda.")
    fig.tight_layout(rect=[0, 0, 1, 0.88])
    pdf.savefig(fig)
    plt.close(fig)


def tabela_ajuste(pdf, dados):
    linhas, saida = [], []
    for tec in TECNOLOGIAS:
        d = dados.get(tec)
        if d is None or d["lat"] is None:
            continue
        variantes = [(tec, False)]
        if d["lat"]["lenta"].any():
            variantes.append((f"{tec} (sensib.: só normais)", True))
        for nome, so_n in variantes:
            aj = ajuste_linear(d["lat"], so_normais=so_n)
            linhas.append([nome, fmt(aj["a"]), fmt(aj["b"], 4), fmt(aj["b"] * 100), fmt(aj["r2"], 3),
                           fmt(aj["k"], 2), str(aj["n"]), f"{aj['lentas']} de {aj['execucoes']}"])
            saida.append(dict(tecnologia=tec, analise="so_normais" if so_n else "todas",
                              a_ms=aj["a"], b_ms_por_kb=aj["b"], r2=aj["r2"],
                              expoente_k=aj["k"], tamanhos_no_ajuste=aj["n"],
                              tamanhos_fora=" ".join(str(t) for t in aj["fora"]),
                              execucoes_lentas=aj["lentas"], execucoes=aj["execucoes"]))
    if not linhas:
        return
    fig, ax = plt.subplots(figsize=(10.2, 2.6 + 0.36 * len(linhas)))
    ax.axis("off")
    tab = ax.table(cellText=linhas,
                   colLabels=["Tecnologia", "a — custo fixo\n(ms)", "b — custo por KB\n(ms/KB)",
                              "Custo de 100 KB\na mais (ms)", "R²", "Expoente k\n(lei de potência)",
                              "Tamanhos\nno ajuste", "Execuções\nlentas"],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(8.8)
    tab.scale(1, 1.6)
    for (rr, _), c in tab.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Cenário 3 — ajuste da curva de latência: latência = a + b × tamanho", fontsize=11, pad=10)
    fig.text(0.5, 0.04, "Ajuste por mínimos quadrados sobre a latência típica de cada tamanho (mediana, entre TODAS as "
             "execuções, da latência mediana de cada uma),\nsó com os tamanhos enviados à taxa predominante. "
             "'Sensib.' = mesma conta sem as execuções lentas (exploratória). "
             "a menor = menos custo fixo por mensagem; b menor = mensagens grandes pesam menos. "
             "k perto de 1 = proporcional ao tamanho.", ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.1, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)
    return pd.DataFrame(saida)


def resumo(dados):
    linhas = []
    for t in TAMANHOS:
        for tec in TECNOLOGIAS:
            d = dados.get(tec)
            if d is None:
                continue
            r = {"tamanho_kb": t, "tecnologia": tec}
            if d["lat"] is not None:
                sel = d["lat"][d["lat"]["tamanho_kb"] == t]
                m, md = sel["latencia_media_ms"], sel["latencia_mediana_ms"]
                r.update(latencia_tipica_ms=md.median(),
                         execucoes_lentas=int(sel["lenta"].sum()), execucoes=len(sel),
                         latencia_tipica_normais_ms=sel.loc[~sel["lenta"], "latencia_mediana_ms"].median(),
                         execucoes_normais=int((~sel["lenta"]).sum()),
                         latencia_media_mediana_ms=m.median(), latencia_media_min_ms=m.min(),
                         latencia_media_max_ms=m.max())
            if d["vaz"] is not None:
                v = d["vaz"].loc[d["vaz"]["tamanho_kb"] == t, "vazao_mb_s"]
                vm = d["vaz"].loc[d["vaz"]["tamanho_kb"] == t, "vazao_msg_s"]
                r.update(vazao_mb_s_mediana=v.median(), vazao_mb_s_min=v.min(), vazao_mb_s_max=v.max(),
                         vazao_msg_s_mediana=vm.median())
            linhas.append(r)
    return pd.DataFrame(linhas)


def tabela_resumo(pdf, res):
    celulas = []
    for _, r in res.iterrows():
        celulas.append([
            f"{int(r['tamanho_kb'])} KB", r["tecnologia"],
            fmt(r["latencia_tipica_ms"]) if "latencia_tipica_ms" in r else "—",
            f"{int(r['execucoes_lentas'])} de {int(r['execucoes'])}" if "execucoes_lentas" in r else "—",
            (f"{fmt(r['latencia_tipica_normais_ms'])} (n={int(r['execucoes_normais'])})"
             if "latencia_tipica_normais_ms" in r else "—"),
            fmt(r["vazao_mb_s_mediana"], 1) if "vazao_mb_s_mediana" in r else "—",
            f"{r['vazao_msg_s_mediana']:,.0f}".replace(",", ".") if "vazao_msg_s_mediana" in r else "—",
        ])
    fig, ax = plt.subplots(figsize=(10.2, 2.2 + 0.36 * len(celulas)))
    ax.axis("off")
    tab = ax.table(cellText=celulas,
                   colLabels=["Tamanho", "Tecnologia", "Latência típica\n(todas, ms)", "Execuções\nlentas",
                              "Sensibilidade:\nsó normais (ms)", "Vazão máx.\n(mediana, MB/s)",
                              "Vazão máx.\n(mediana, msg/s)"],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(8.8)
    tab.scale(1, 1.55)
    for (rr, _), c in tab.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Cenário 3 — resumo Kafka × RabbitMQ", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Latência: Parte A (taxa fixa, igual nas duas tecnologias; menor nos maiores tamanhos). "
             "Vazão: Parte B (sem controle de taxa).\nLatência típica = mediana entre as execuções da "
             f"latência mediana de cada uma; lenta = latência mediana acima de {LIMIAR_LENTA_MS:g} ms.",
             ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def iqr(v):
    return float(np.percentile(v, 25)), float(np.percentile(v, 75))


def comparar_caixas(va, vb, maior_melhor):
    """Critério da Seção 3.2: caixas (Q1–Q3) separadas = diferença."""
    if len(va) == 0 or len(vb) == 0:
        return "—"
    (a1, a3), (b1, b3) = iqr(va), iqr(vb)
    if a3 < b1:
        return "Kafka" if not maior_melhor else "RabbitMQ"
    if b3 < a1:
        return "RabbitMQ" if not maior_melhor else "Kafka"
    return "sobrepostas"


def criterio_caixas(dados):
    k, r = dados.get("Kafka"), dados.get("RabbitMQ")
    if k is None or r is None:
        return None
    linhas = []
    for t in TAMANHOS:
        lin = {"tamanho_kb": t}
        if k["lat"] is not None and r["lat"] is not None:
            lk = k["lat"]; lr = r["lat"]   # todas as execuções
            lin["latencia_melhor"] = comparar_caixas(
                lk.loc[lk["tamanho_kb"] == t, "latencia_mediana_ms"].values,
                lr.loc[lr["tamanho_kb"] == t, "latencia_mediana_ms"].values, maior_melhor=False)
        if k["vaz"] is not None and r["vaz"] is not None:
            lin["vazao_melhor"] = comparar_caixas(
                k["vaz"].loc[k["vaz"]["tamanho_kb"] == t, "vazao_msg_s"].values,
                r["vaz"].loc[r["vaz"]["tamanho_kb"] == t, "vazao_msg_s"].values, maior_melhor=True)
        linhas.append(lin)
    return pd.DataFrame(linhas)


def tabela_caixas(pdf, cx):
    celulas = [[f"{int(r['tamanho_kb'])} KB", r.get("latencia_melhor", "—"), r.get("vazao_melhor", "—")]
               for _, r in cx.iterrows()]
    fig, ax = plt.subplots(figsize=(7.5, 2.2 + 0.36 * len(celulas)))
    ax.axis("off")
    tab = ax.table(cellText=celulas,
                   colLabels=["Tamanho", "Menor latência\n(Parte A, todas as execuções)",
                              "Maior vazão\n(Parte B, msg/s)"],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(8.8)
    tab.scale(1, 1.55)
    for (rr, _), c in tab.get_celld().items():
        if rr == 0:
            c.set_facecolor("#E8EEF5")
            c.set_text_props(weight="bold")
    ax.set_title("Cenário 3 — critério das caixas (Seção 3.2)", fontsize=11, pad=10)
    fig.text(0.5, 0.03, "Caixa = Q1 a Q3 das execuções. Sem sobreposição = a tecnologia indicada foi melhor nas execuções\n"
             "observadas (evidência descritiva); 'sobrepostas' = valores próximos, sem demonstração de equivalência.",
             ha="center", fontsize=8, color="#555555")
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def figuras_tcc(dados, pasta):
    """Figuras 18 e 19 do TCC: sem título (a legenda fica no Word), 200 dpi."""
    # Figura 18 — latência mediana por execução (caixas = todas as execuções; lentas vazadas)
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    caixas_agrupadas(ax, dados, "lat", "latencia_mediana_ms", separar_lentas=True)
    ax.axhline(LIMIAR_LENTA_MS, color="#777777", linestyle=":", linewidth=1)
    eixo_log(ax)
    ax.tick_params(axis="x", labelsize=8)
    ax.set_ylabel("Latência mediana da execução (ms)")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(pasta, FIG_TCC_LAT), dpi=200)
    plt.close(fig)

    # Figura 19 — forma da curva: latência (com a reta) e vazão em MB/s
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7.6, 3.7))
    for tec, (_, _, cor, _) in TECNOLOGIAS.items():
        d = dados.get(tec)
        if d is None:
            continue
        if d["lat"] is not None:
            g = d["lat"].groupby("tamanho_kb")["latencia_mediana_ms"]   # todas as execuções
            med, lo, hi = g.median(), g.quantile(0.25), g.quantile(0.75)   # barra = 1º ao 3º quartil
            x = med.index.values.astype(float)
            ax1.errorbar(x, med.values, yerr=[med.values - lo.values, hi.values - med.values], fmt="o",
                         color=cor, ecolor=cor, elinewidth=1, capsize=2.5, markersize=4, zorder=3)
            aj = ajuste_linear(d["lat"])
            xs = np.logspace(np.log10(x.min()), np.log10(x.max()), 200)
            ax1.plot(xs, aj["a"] + aj["b"] * xs, color=cor, linewidth=1.4,
                     label=f"{tec}: R² = {fmt(aj['r2'], 3)}")
            if d["lat"]["lenta"].any():
                gn = normais(d["lat"]).groupby("tamanho_kb")["latencia_mediana_ms"].median()
                ajn = ajuste_linear(d["lat"], so_normais=True)
                ax1.scatter(gn.index.values.astype(float), gn.values, s=22, facecolors="none",
                            edgecolors=cor, linewidths=0.9, zorder=4)
                ax1.plot(xs, ajn["a"] + ajn["b"] * xs, color=cor, linewidth=0.9, linestyle=":",
                         label=f"{tec}, só normais: R² = {fmt(ajn['r2'], 3)}")
        if d["vaz"] is not None:
            gm = d["vaz"].groupby("tamanho_kb")["vazao_mb_s"]
            med, lo, hi = gm.median(), gm.quantile(0.25), gm.quantile(0.75)
            x = med.index.values.astype(float)
            ax2.errorbar(x, med.values, yerr=[med.values - lo.values, hi.values - med.values], fmt="-o",
                         color=cor, ecolor=cor, elinewidth=1, capsize=2.5, markersize=4, label=tec)
    for ax in (ax1, ax2):
        ax.set_xscale("log")
        ax.set_xticks(TAMANHOS)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_locator(mticker.NullLocator())
        ax.tick_params(labelsize=8)
        ax.set_xlabel("Tamanho da mensagem (KB)")
    ax1.set_yscale("log")
    ax1.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 1.5, 2, 3, 5, 7]))
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    ax1.yaxis.set_minor_locator(mticker.NullLocator())
    ax1.set_ylabel("Latência típica (ms)")
    ax1.set_title("(a) Latência (Parte A)", fontsize=9)
    ax1.legend(frameon=False, fontsize=7, loc="upper left")
    ax2.set_ylim(bottom=0)
    ax2.set_ylabel("Vazão de ponta a ponta (MB/s)")
    ax2.set_title("(b) Vazão máxima (Parte B)", fontsize=9)
    ax2.legend(frameon=False, fontsize=7.5, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(pasta, FIG_TCC_FORMA), dpi=200)
    plt.close(fig)


def main():
    dados = carregar()
    if all(v is None for v in dados.values()):
        print("Nenhuma pasta de resultados encontrada. Nada a fazer.")
        return
    definir_tamanhos(dados)
    print(f"Tamanhos encontrados: {TAMANHOS} KB")
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA_SCRIPT, SAIDA)
    with PdfPages(saida) as pdf:
        grafico1_latencia(pdf, dados)
        grafico2_ecdf(pdf, dados)
        grafico3_acrescimo(pdf, dados)
        grafico4_vazao(pdf, dados)
        aj = None
        if len(TAMANHOS) >= 3:
            grafico5_forma(pdf, dados)
            aj = tabela_ajuste(pdf, dados)
        res = resumo(dados)
        tabela_resumo(pdf, res)
        cx = criterio_caixas(dados)
        if cx is not None:
            tabela_caixas(pdf, cx)
    res.to_csv(os.path.join(os.path.dirname(saida), ARQ_RESUMO), index=False, float_format="%.4f")
    figuras_tcc(dados, os.path.dirname(saida))
    print(f"Figuras do TCC: {FIG_TCC_LAT} e {FIG_TCC_FORMA}")
    if cx is not None:
        cx.to_csv(os.path.join(os.path.dirname(saida), ARQ_CAIXAS), index=False)
    if aj is not None:
        aj.to_csv(os.path.join(os.path.dirname(saida), ARQ_AJUSTE), index=False, float_format="%.6f")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()