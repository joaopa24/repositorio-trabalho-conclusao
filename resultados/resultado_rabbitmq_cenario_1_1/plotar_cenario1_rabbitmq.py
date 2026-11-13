#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 1 — Baseline — gráficos do RABBITMQ.

Lê os CSVs gerados pelo orquestrador Cenario1BaselineJava:
    resultados-rabbitmq-cenario1-java.csv             (1 linha por execução)
    resultados-rabbitmq-cenario1-java-detalhado.csv   (1 linha por mensagem)

Gera um PDF com:
    Gráfico 1 — latência mediana por execução    (1 caixa = 10 execuções)
    Gráfico 2 — vazão do produtor por execução    (1 caixa = 10 execuções)
    Gráfico 3 — latência por mensagem, uma caixa por execução (escala log)
    Gráfico 4 — latência ao longo da execução (média das execuções em cada
                posição) — mostra QUANDO as mensagens lentas aparecem

    Gráfico 6 — atraso do gerador (envio efetivo - instante planejado):
                distribuição e as 50 primeiras mensagens
    Gráfico 7 — latência medida x latência a partir do instante planejado,
                com tabela por execução
    (6 e 7 só aparecem se o CSV detalhado tiver a coluna atraso_gerador_ms)

Nos gráficos 1 e 2 cada ponto é uma execução inteira; no gráfico 3 cada
ponto fora dos bigodes é uma mensagem (critério de Tukey).

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_cenario1_rabbitmq.py
ou:
    python plotar_cenario1_rabbitmq.py <pasta_dos_csvs> [saida.pdf]

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
TECNOLOGIA    = "RabbitMQ"
ARQ_AGREGADO  = "resultados-rabbitmq-cenario1-java.csv"
ARQ_DETALHADO = "resultados-rabbitmq-cenario1-java-detalhado.csv"
ARQ_INGESTAO  = None   # o RabbitMQ não grava latência de ingestão
SAIDA_PADRAO  = "cenario1-rabbitmq-boxplot.pdf"
COR_CAIXA     = "#D85A30"   # laranja
COR_PONTO     = "#1B6B2F"   # verde
NOME_PONTO    = "verde"
NOME_PONTOS   = "verdes"
# ----------------------------------------------------------------------

CENARIO = "Cenário 1 — Baseline"

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


# ----------------------------------------------------------------------
# Leitura
# ----------------------------------------------------------------------
def ler_csv(nome, pular=0):
    """Lê CSV com ',' ou ';' como separador; devolve None se não existir."""
    if not nome or not os.path.isfile(caminho(nome)):
        return None
    with open(caminho(nome), "r", encoding="utf-8-sig", errors="replace") as f:
        for _ in range(pular):
            f.readline()
        cab = f.readline()
    sep = ";" if cab.count(";") > cab.count(",") else ","
    dec = "," if sep == ";" else "."
    df = pd.read_csv(caminho(nome), sep=sep, decimal=dec, skiprows=pular, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def ler_agregado():
    # a 1ª linha do agregado é um título (ex.: "cenario1-baseline-java")
    return ler_csv(ARQ_AGREGADO, pular=1)


def ler_detalhado():
    df = ler_csv(ARQ_DETALHADO)
    if df is None:
        return None
    df = df[df["execucao"] > 0].copy()  # nunca usa o aquecimento
    return df


def ler_ingestao():
    """Ingestão (só Kafka). Se o arquivo tiver mais de uma campanha colada
    (versão antiga do código), fica só com a ÚLTIMA ocorrência de cada
    (execucao, sequencia); a execução 0 (aquecimento) é descartada."""
    df = ler_csv(ARQ_INGESTAO)
    if df is None:
        return None
    df = df[df["execucao"] > 0]
    df = df.drop_duplicates(subset=["execucao", "sequencia"], keep="last")
    return df.sort_values(["execucao", "sequencia"])


def mediana_por_execucao(det):
    """Latência mediana de cada execução, calculada a partir do CSV detalhado
    (1 linha por mensagem). Índice = número da execução."""
    return det.groupby("execucao")["latencia_ms"].median().sort_index()


def conferir_execucoes(med, agg, nome=""):
    """Avisa se o detalhado e o agregado não têm as mesmas execuções
    (ex.: uma execução inválida que ficou só em um dos dois arquivos)."""
    if agg is None:
        return
    prefixo = f"[aviso] {nome}: " if nome else "[aviso] "
    if "execucao" in agg.columns:
        a = set(pd.to_numeric(agg["execucao"], errors="coerce").dropna().astype(int)) - {0}
        d = set(med.index.astype(int))
        if a != d:
            print(f"{prefixo}execuções diferentes entre agregado {sorted(a)} e detalhado {sorted(d)}")
    elif len(agg) != len(med):
        print(f"{prefixo}agregado tem {len(agg)} execuções e o detalhado tem {len(med)}")


def esta_ordenado(df):
    """True se as latências de TODAS as execuções estão em ordem crescente:
    sinal de que o CSV foi gravado ordenado (versão antiga do consumidor do
    RabbitMQ) e a coluna 'mensagem' não é a ordem de chegada."""
    return all((g["latencia_ms"].diff().dropna() >= 0).all() for _, g in df.groupby("execucao"))


# ----------------------------------------------------------------------
# Gráficos
# ----------------------------------------------------------------------
def titulo(ax, fig, texto, sub):
    ax.set_title(texto, fontsize=11.5, pad=24)
    ax.text(0.5, 1.02, sub, transform=ax.transAxes, ha="center", fontsize=8.5, color="#555555")


def boxplot_execucoes(pdf, valores, ylabel, texto, sub):
    """1 caixa = N execuções; cada ponto = 1 execução."""
    fig, ax = plt.subplots(figsize=(5.8, 5.6))
    ax.boxplot([valores], positions=[1], widths=0.5, patch_artist=True,
               showfliers=False,  # os pontos abaixo já mostram TODAS as execuções
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    ax.scatter(1 + RNG.uniform(-0.08, 0.08, len(valores)), valores, s=28,
               color=COR_PONTO, edgecolor="white", linewidth=0.6, zorder=3)
    ax.set_xticks([1])
    ax.set_xticklabels([f"{TECNOLOGIA} — {CENARIO}"])
    ax.set_ylabel(ylabel)
    ax.ticklabel_format(axis="y", style="plain", useOffset=False)  # mostra o valor real
    ax.grid(True, axis="y")
    ax.grid(False, axis="x")
    titulo(ax, fig, texto, sub)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def eixo_log(ax):
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5]))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_locator(mticker.NullLocator())


def grafico3_por_mensagem(pdf, det):
    execucoes = sorted(det["execucao"].unique())
    dados = [det.loc[det["execucao"] == e, "latencia_ms"].values for e in execucoes]

    fig, ax = plt.subplots(figsize=(max(8, 0.8 * len(execucoes)), 6.3))
    bp = ax.boxplot(dados, positions=range(1, len(execucoes) + 1), widths=0.6,
                    patch_artist=True, showfliers=True,
                    flierprops=dict(marker="o", markersize=2.5, markerfacecolor=COR_PONTO,
                                    markeredgecolor="none", alpha=0.5),
                    medianprops=dict(color="black", linewidth=1.5),
                    boxprops=dict(facecolor=COR_CAIXA, alpha=0.5, edgecolor=COR_CAIXA),
                    whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))

    # n = mensagens outliers; N = mensagens dentro dos bigodes (lidos do próprio boxplot)
    ax.tick_params(axis="x", pad=10)
    trans = ax.get_xaxis_transform()
    for i, serie in enumerate(dados):
        n_out = len(bp["fliers"][i].get_ydata())
        ax.text(i + 1, -0.095, f"n={n_out}", transform=trans, ha="center", va="top",
                fontsize=7, color="#333333", clip_on=False)
        ax.text(i + 1, -0.140, f"N={len(serie) - n_out}", transform=trans, ha="center",
                va="top", fontsize=7, color="#777777", clip_on=False)

    eixo_log(ax)
    ax.set_xticks(range(1, len(execucoes) + 1))
    ax.set_xticklabels([str(int(e)) for e in execucoes])
    ax.set_xlabel("Execução", labelpad=38)
    ax.set_ylabel("Latência fim a fim (ms) — escala log")
    ax.grid(False, axis="x")
    n_msg = int(det.groupby("execucao").size().median())
    titulo(ax, fig, f"{TECNOLOGIA} — {CENARIO} — latência por mensagem, por execução",
           f"Cada caixa = as ~{n_msg} mensagens de uma execução; pontos {NOME_PONTOS} = mensagens outliers")
    fig.text(0.5, 0.015,
             "Outliers: mensagens acima de Q3 + 1,5×IQR (critério de Tukey).\n"
             "Abaixo de cada caixa: n = mensagens outliers; N = mensagens dentro dos bigodes "
             "(n + N = total da execução).",
             ha="center", va="bottom", fontsize=7.8, color="#555555")
    fig.tight_layout(rect=[0, 0.07, 1, 1])
    pdf.savefig(fig)
    plt.close(fig)


def grafico4_ao_longo_da_execucao(pdf, det):
    if esta_ordenado(det):
        aviso_pagina(pdf, "Gráfico 4 não gerado",
                     "O CSV detalhado está gravado em ordem crescente de latência (versão antiga\n"
                     "do consumidor), então a coluna 'mensagem' não é a ordem de chegada.\n"
                     "Rode a versão nova do Java para obter este gráfico.")
        return

    g = det.groupby("mensagem")["latencia_ms"]
    mediana = g.median()
    media = g.mean()

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.plot(media.index, media.values, color=COR_PONTO, linewidth=0.9,
            label="Média das execuções em cada posição")
    ax.plot(mediana.index, mediana.values, color=COR_CAIXA, linewidth=0.9,
            label="Mediana das execuções em cada posição")
    eixo_log(ax)
    ax.set_xlabel("Posição da mensagem na execução (ordem de chegada)")
    ax.set_ylabel("Latência fim a fim (ms) — escala log")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, fontsize=8.5, loc="upper right")
    titulo(ax, fig, f"{TECNOLOGIA} — {CENARIO} — latência ao longo da execução",
           "Mostra QUANDO as mensagens lentas acontecem: no início da execução ou espalhadas por ela")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico5_ingestao(pdf, det, ing):
    """Fim a fim x ingestão (envio -> confirmação do broker), por mensagem."""
    if ing is None:
        return
    junto = det.merge(ing.rename(columns={"sequencia": "mensagem"}),
                      on=["execucao", "mensagem"], how="inner")
    if junto.empty:
        aviso_pagina(pdf, "Gráfico 5 não gerado",
                     "Não foi possível casar as mensagens do CSV detalhado com as do CSV de ingestão.")
        return

    linhas = []
    for e, g in junto.groupby("execucao"):
        r = np.corrcoef(g["latencia_ms"], g["latencia_ingestao_ms"])[0, 1]
        resto = (g["latencia_ms"] - g["latencia_ingestao_ms"]).median()
        linhas.append((int(e), r, resto, g["latencia_ms"].mean()))
    pior = max(linhas, key=lambda x: x[3])[0]  # execução com maior latência média
    g = junto[junto["execucao"] == pior]

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.plot(g["mensagem"], g["latencia_ms"], color=COR_PONTO, linewidth=0.9,
            label="Fim a fim (produtor → consumidor)")
    ax.plot(g["mensagem"], g["latencia_ingestao_ms"], color=COR_CAIXA, linewidth=0.9,
            label="Ingestão (produtor → confirmação do broker)")
    eixo_log(ax)
    ax.set_xlabel("Posição da mensagem na execução")
    ax.set_ylabel("Latência (ms) — escala log")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, fontsize=8.5, loc="upper right")
    titulo(ax, fig, f"{TECNOLOGIA} — {CENARIO} — fim a fim × ingestão (execução {pior})",
           "Se as duas linhas sobem juntas, o atraso acontece ANTES de a mensagem chegar ao broker")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)

    # tabela por execução
    fig, ax = plt.subplots(figsize=(7.5, 1.4 + 0.33 * len(linhas)))
    ax.axis("off")
    tab = ax.table(cellText=[[e, f"{r:.2f}", f"{resto:.2f}"] for e, r, resto, _ in linhas],
                   colLabels=["Execução", "Correlação fim a fim × ingestão",
                              "Broker → consumidor (mediana, ms)"],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(9)
    tab.scale(1, 1.5)
    ax.set_title(f"{TECNOLOGIA} — {CENARIO} — decomposição da latência por execução", fontsize=11)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Atraso do gerador (coluna atraso_gerador_ms, gravada pela versão do Java
# que envia o header "ts-planejado"). Atraso = envio efetivo - instante
# planejado. A latência "a partir do planejado" = latencia_ms + atraso; é a
# latência que a mensagem teria se o relógio começasse no instante em que
# ela DEVERIA ter saído (correção para omissão coordenada).
# ----------------------------------------------------------------------
COL_ATRASO = "atraso_gerador_ms"
PISO_LOG_MS = 0.001  # 1 µs: valores menores aparecem neste piso no eixo log


def tem_atraso(det):
    return det is not None and COL_ATRASO in det.columns


def ccdf(valores):
    """Pontos da curva 'fração de mensagens ACIMA de x' (em %)."""
    v = np.sort(np.asarray(valores, dtype=float))
    n = len(v)
    acima = (n - np.arange(1, n + 1)) / n * 100
    return v, acima


def eixo_ccdf(ax):
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_ylim(bottom=0.001, top=100)


def resumo_atraso(det):
    """Uma linha por execução: atraso do gerador e efeito sobre a latência."""
    linhas = []
    for e, g in det.groupby("execucao"):
        a = g[COL_ATRASO]
        lat = g["latencia_ms"]
        plan = lat + a
        linhas.append(dict(
            execucao=int(e),
            atraso_mediano=a.median(),
            atraso_p99=a.quantile(0.99),
            atraso_max=a.max(),
            msgs_atraso_1ms=int((a > 1).sum()),
            lat_mediana=lat.median(),
            plan_mediana=plan.median(),
            lat_10=(lat > 10).mean() * 100,
            plan_10=(plan > 10).mean() * 100,
            lat_max=lat.max(),
            plan_max=plan.max(),
        ))
    return pd.DataFrame(linhas)


def grafico6_atraso_gerador(pdf, det):
    if not tem_atraso(det):
        aviso_pagina(pdf, "Gráfico 6 não gerado",
                     f"O CSV detalhado não tem a coluna '{COL_ATRASO}'.\n"
                     "Rode a versão do Java que envia o header 'ts-planejado'.")
        return

    a = det[COL_ATRASO].clip(lower=PISO_LOG_MS)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.9), gridspec_kw=dict(width_ratios=[1.2, 1]))

    x, y = ccdf(a)
    ax1.plot(x, y, color=COR_CAIXA, linewidth=1.8, drawstyle="steps-post")
    for ref in (1, 10):
        ax1.axvline(ref, color="#888888", linestyle=":", linewidth=1)
        pct = (det[COL_ATRASO] > ref).mean() * 100
        ax1.text(ref, 40, f" {ref} ms\n {pct:.2f}% acima", fontsize=8, color="#555555", va="top")
    eixo_ccdf(ax1)
    ax1.set_xlabel("Atraso do gerador (ms) — escala log")
    ax1.set_ylabel("Mensagens com atraso acima de X (%) — escala log")
    ax1.set_title("(a) distribuição do atraso, todas as mensagens", fontsize=10.5)

    ini = det[det["mensagem"] <= 50].groupby("mensagem")[COL_ATRASO].median()
    ax2.plot(ini.index, ini.values, color=COR_PONTO, linewidth=1.6, marker="o", markersize=2.5)
    ax2.set_ylim(bottom=0)
    ax2.set_xlabel("Posição da mensagem")
    ax2.set_ylabel("Atraso mediano entre as execuções (ms)")
    ax2.set_title("(b) as 50 primeiras mensagens", fontsize=10.5)

    fig.suptitle(f"{TECNOLOGIA} — {CENARIO} — atraso do gerador (envio efetivo − instante planejado)",
                 fontsize=11.5, y=0.99)
    fig.text(0.5, 0.925, f"Mediana {det[COL_ATRASO].median() * 1000:.1f} µs; "
             f"99% das mensagens abaixo de {det[COL_ATRASO].quantile(0.99):.3f} ms; "
             f"maior atraso {det[COL_ATRASO].max():.1f} ms",
             ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico7_latencia_planejada(pdf, det):
    """Latência medida x latência contada a partir do instante planejado."""
    if not tem_atraso(det):
        return
    lat = det["latencia_ms"]
    plan = lat + det[COL_ATRASO]

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    for serie, rotulo, estilo in ((lat, "Medida (a partir do envio efetivo)", "-"),
                                  (plan, "A partir do instante planejado", "--")):
        x, y = ccdf(serie.clip(lower=PISO_LOG_MS))
        ax.plot(x, y, color=COR_CAIXA if estilo == "-" else COR_PONTO, linestyle=estilo,
                linewidth=1.6, drawstyle="steps-post", label=rotulo)
    for ref in (10, 100):
        ax.axvline(ref, color="#888888", linestyle=":", linewidth=1)
        ax.text(ref, 40, f" {ref} ms", fontsize=8, color="#666666", va="top")
    eixo_ccdf(ax)
    ax.set_xlabel("Latência fim a fim (ms) — escala log")
    ax.set_ylabel("Mensagens acima de X (%) — escala log")
    ax.legend(frameon=True, facecolor="white", edgecolor="none", framealpha=0.9, fontsize=8.5, loc="lower left")
    titulo(ax, fig, f"{TECNOLOGIA} — {CENARIO} — efeito do atraso do gerador na latência",
           "Onde as curvas se separam, a latência medida subestima a espera real da mensagem")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)

    # tabela por execução
    r = resumo_atraso(det)
    linhas = [[f"{int(x.execucao)}",
               f"{x.atraso_mediano * 1000:.1f}",
               f"{x.atraso_p99:.3f}",
               f"{x.atraso_max:.1f}",
               f"{x.msgs_atraso_1ms}",
               f"{x.lat_mediana:.3f} / {x.plan_mediana:.3f}",
               f"{x.lat_10:.2f} / {x.plan_10:.2f}",
               f"{x.lat_max:.1f} / {x.plan_max:.1f}"] for x in r.itertuples()]
    fig, ax = plt.subplots(figsize=(11.0, 1.6 + 0.33 * len(linhas)))
    ax.axis("off")
    tab = ax.table(cellText=linhas,
                   colLabels=["Exec.", "Atraso\nmediano (µs)", "Atraso\np99 (ms)", "Atraso\nmáx. (ms)",
                              "Msgs com\natraso > 1 ms", "Latência mediana (ms)\nmedida / planejado",
                              "Msgs > 10 ms (%)\nmedida / planejado", "Latência máx. (ms)\nmedida / planejado"],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(8.5)
    tab.scale(1, 1.6)
    for (lin, _), cel in tab.get_celld().items():
        if lin == 0:
            cel.set_height(cel.get_height() * 1.6)
    ax.set_title(f"{TECNOLOGIA} — {CENARIO} — atraso do gerador por execução", fontsize=11)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def aviso_pagina(pdf, titulo_txt, texto):
    fig = plt.figure(figsize=(8.27, 3))
    fig.text(0.08, 0.75, titulo_txt, fontsize=12, weight="bold")
    fig.text(0.08, 0.55, texto, fontsize=9.5, va="top", color="#333333")
    pdf.savefig(fig)
    plt.close(fig)


def pagina_rosto(pdf):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.92, f"{CENARIO} ({TECNOLOGIA})", ha="center", fontsize=17, weight="bold")
    fig.text(0.5, 0.885, "Latência e vazão — 10 execuções de 10.000 mensagens a 200 msg/s",
             ha="center", fontsize=11, color="#555555")
    linhas = ["Arquivos:"]
    for nome in (ARQ_AGREGADO, ARQ_DETALHADO, ARQ_INGESTAO):
        if nome:
            linhas.append(f"  [{'OK' if os.path.isfile(caminho(nome)) else '--'}] {nome}")
    fig.text(0.1, 0.83, "\n".join(linhas), ha="left", va="top", fontsize=8.5, family="monospace")

    nota = (
        "Como ler:\n\n"
        "- Gráficos 1 e 2: 1 caixa = 10 execuções; cada ponto = 1 execução inteira\n"
        "  (sua latência mediana ou sua vazão). Um ponto isolado = execução atípica.\n\n"
        "- Gráfico 3: uma caixa por execução, com todas as mensagens dela. Aqui um\n"
        "  outlier é uma MENSAGEM atípica (critério de Tukey), não uma execução.\n\n"
        "- Gráfico 4: latência por posição na execução. Mostra se as mensagens\n"
        "  lentas se concentram no início ou aparecem espalhadas.\n\n"
        "- Gráfico 5 (só Kafka): compara a latência fim a fim com a de ingestão\n"
        "  (envio até a confirmação do broker). Se andam juntas, o atraso acontece\n"
        "  antes de a mensagem chegar ao broker.\n\n"
        "- Gráfico 6: atraso do gerador = envio efetivo - instante planejado.\n"
        "  Mostra se o produtor saiu do ritmo de 200 msg/s em algum momento.\n\n"
        "- Gráfico 7: compara a latência medida com a contada a partir do\n"
        "  instante planejado. A diferença é a espera que ficaria fora da\n"
        "  medição (omissão coordenada).\n\n"
        "- A execução de aquecimento é descartada e nunca aparece nos gráficos."
    )
    fig.text(0.1, 0.70, nota, ha="left", va="top", fontsize=9.5, color="#333333")
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    print(f"Lendo CSVs de: {os.path.abspath(PASTA)}")
    agg = ler_agregado()
    det = ler_detalhado()
    ing = ler_ingestao() if ARQ_INGESTAO else None

    with PdfPages(saida) as pdf:
        pagina_rosto(pdf)
        if det is not None:
            med = mediana_por_execucao(det)
            conferir_execucoes(med, agg, TECNOLOGIA)
            boxplot_execucoes(pdf, med.values, "Latência mediana (ms)",
                              f"{TECNOLOGIA} — latência mediana por execução",
                              f"1 caixa = {len(med)} execuções; cada ponto {NOME_PONTO} = a latência mediana de 1 execução")
        if agg is not None:
            n = len(agg)
            boxplot_execucoes(pdf, agg["vazao_msg_s"].values, "Vazão do produtor (msg/s)",
                              f"{TECNOLOGIA} — vazão do produtor por execução",
                              f"1 caixa = {n} execuções; taxa configurada: 200 msg/s")
        else:
            print(f"[aviso] {ARQ_AGREGADO} não encontrado")
        if det is not None:
            grafico3_por_mensagem(pdf, det)
            grafico4_ao_longo_da_execucao(pdf, det)
            grafico5_ingestao(pdf, det, ing)
            grafico6_atraso_gerador(pdf, det)
            grafico7_latencia_planejada(pdf, det)
        else:
            print(f"[aviso] {ARQ_DETALHADO} não encontrado")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()