#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 1 — Baseline — COMPARATIVO Kafka x RabbitMQ.

Gera um PDF com:
    Gráfico 1 — latência mediana por execução, as duas tecnologias no mesmo
                eixo (escala log, para as duas caberem)
    Gráfico 2 — distribuição acumulada da latência de TODAS as mensagens:
                que fração das mensagens chegou em até X ms
    Gráfico 3 — latência ao longo da execução: (a) as 50 primeiras mensagens,
                posição a posição; (b) a execução inteira, em blocos de 200
                mensagens. Mostra se a latência cresce (fila) ou fica estável
    Gráfico 4 — vazão do produtor por execução (eixo único, 195 a 205 msg/s)
    Gráfico 5 — atraso do gerador (envio efetivo - instante planejado) e
                latência medida x latência a partir do instante planejado
                (só se os CSVs detalhados tiverem a coluna atraso_gerador_ms)
    Tabela    — resumo dos boxplots, das mensagens lentas e do atraso do gerador

Organização esperada (o script numa pasta própria, ao lado das pastas de
resultados):

    TCC\\
    ├── comparativo_cenario1\\
    │   └── plotar_cenario1_comparativo.py
    ├── resultado_cenario_1_1\\              (Kafka)
    └── resultado_rabbitmq_cenario_1_1\\     (RabbitMQ)

    cd comparativo_cenario1
    python plotar_cenario1_comparativo.py

Também aceita os caminhos como argumentos:
    python plotar_cenario1_comparativo.py <pasta_kafka> <pasta_rabbitmq> [saida.pdf]

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

TECNOLOGIAS = {
    # nome: (pasta, agregado, detalhado, cor da caixa, cor dos pontos)
    "RabbitMQ": ("resultado_rabbitmq_cenario_1_1", "resultados-rabbitmq-cenario1-java.csv",
                 "resultados-rabbitmq-cenario1-java-detalhado.csv", "#D85A30", "#1B6B2F"),
    "Kafka": ("resultado_cenario_1_1", "resultados-kafka-cenario1-java.csv",
              "resultados-kafka-cenario1-java-detalhado.csv", "#185FA5", "#D85A30"),
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

# Atraso do gerador: coluna gravada pela versão do Java que envia o header
# "ts-planejado". Latência a partir do planejado = latencia_ms + atraso.
COL_ATRASO = "atraso_gerador_ms"
PISO_LOG_MS = 0.001  # 1 µs: valores menores aparecem neste piso no eixo log


def localizar(nome_pasta, nome_csv):
    """Procura a pasta de resultados ao lado da pasta do script e, se não
    achar, dentro dela."""
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        pasta = os.path.join(base, nome_pasta)
        if os.path.isfile(os.path.join(pasta, nome_csv)):
            return pasta
    print(f"[aviso] '{nome_csv}' não encontrado em:")
    for base in (PASTA_ACIMA, PASTA_SCRIPT):
        print(f"         {os.path.join(base, nome_pasta)}")
    return None


def ler_csv(caminho, pular=0):
    with open(caminho, "r", encoding="utf-8-sig", errors="replace") as f:
        for _ in range(pular):
            f.readline()
        cab = f.readline()
    sep = ";" if cab.count(";") > cab.count(",") else ","
    df = pd.read_csv(caminho, sep=sep, decimal="," if sep == ";" else ".",
                     skiprows=pular, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def carregar():
    pastas_arg = sys.argv[1:3]
    dados = {}
    for i, (nome, (pasta_padrao, arq_agg, arq_det, cor, cor_ponto)) in enumerate(
            # ordem dos argumentos: Kafka primeiro, depois RabbitMQ
            sorted(TECNOLOGIAS.items(), key=lambda kv: kv[0] != "Kafka")):
        pasta = pastas_arg[i] if len(pastas_arg) > i else localizar(pasta_padrao, arq_agg)
        if pasta is None:
            continue
        agg = ler_csv(os.path.join(pasta, arq_agg), pular=1)
        det = ler_csv(os.path.join(pasta, arq_det))
        det = det[det["execucao"] > 0]
        med = mediana_por_execucao(det)
        conferir_execucoes(med, agg, nome)
        dados[nome] = dict(agg=agg, det=det, med=med, cor=cor, cor_ponto=cor_ponto)
    # ordem fixa nos gráficos: RabbitMQ, Kafka
    return {k: dados[k] for k in ("RabbitMQ", "Kafka") if k in dados}


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


def eixo_log(ax):
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5]))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_minor_locator(mticker.NullLocator())


def titulo(ax, texto, sub):
    ax.set_title(texto, fontsize=11.5, pad=24)
    ax.text(0.5, 1.02, sub, transform=ax.transAxes, ha="center", fontsize=8.5, color="#555555")


def caixa_com_pontos(ax, pos, valores, cor, cor_ponto):
    ax.boxplot([valores], positions=[pos], widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=cor, alpha=0.5, edgecolor=cor),
               whiskerprops=dict(color=cor), capprops=dict(color=cor))
    ax.scatter(pos + RNG.uniform(-0.08, 0.08, len(valores)), valores, s=28,
               color=cor_ponto, edgecolor="white", linewidth=0.6, zorder=3)


def grafico1_latencia_media(pdf, dados):
    fig, ax = plt.subplots(figsize=(6.6, 5.6))
    nomes = list(dados)
    for i, nome in enumerate(nomes, start=1):
        d = dados[nome]
        caixa_com_pontos(ax, i, d["med"].values, d["cor"], d["cor_ponto"])
        med = d["med"].median()
        ax.annotate(f"mediana {med:.2f} ms", xy=(i + 0.3, med), va="center", fontsize=8.5,
                    color="#333333")
    ax.set_xticks(range(1, len(nomes) + 1))
    ax.set_xticklabels(nomes)
    ax.set_xlim(0.4, len(nomes) + 0.9)
    eixo_log(ax)
    ax.grid(False, axis="x")
    ax.set_ylabel("Latência mediana da execução (ms) — escala log")
    titulo(ax, "Cenário 1 — latência mediana por execução",
           "1 caixa = 10 execuções; cada ponto = a latência mediana de 1 execução")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_acumulada(pdf, dados):
    fig, ax = plt.subplots(figsize=(7.6, 5.4))
    for nome, d in dados.items():
        lat = np.sort(d["det"]["latencia_ms"].values)
        frac = np.arange(1, len(lat) + 1) / len(lat) * 100
        ax.plot(lat, frac, color=d["cor"], linewidth=2, label=nome)
    for ref in (10, 100):
        ax.axvline(ref, color="#888888", linestyle=":", linewidth=1)
        ax.text(ref, 2, f" {ref} ms", fontsize=8, color="#666666")
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_xlabel("Latência fim a fim (ms) — escala log")
    ax.set_ylabel("Mensagens que chegaram em até X ms (%)")
    ax.set_ylim(0, 101)
    ax.legend(frameon=False, loc="lower right")
    titulo(ax, "Cenário 1 — distribuição da latência de todas as mensagens",
           "Curva mais à esquerda = mais rápida; curva que demora a chegar em 100% = mais mensagens lentas")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico3_ao_longo(pdf, dados, bloco=200, inicio=50):
    """(a) mediana, entre as execuções, de cada uma das primeiras posições;
    (b) mediana de cada bloco de mensagens ao longo da execução inteira.
    Se houvesse acúmulo progressivo, a linha de (b) subiria. Uma linha que
    não sobe NÃO prova ausência de fila (uma fila estável não cresce)."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.9),
                                   gridspec_kw=dict(width_ratios=[1, 1.6]))
    for nome, d in dados.items():
        det = d["det"]
        ini = det[det["mensagem"] <= inicio].groupby("mensagem")["latencia_ms"].median()
        ax1.plot(ini.index, ini.values, color=d["cor"], linewidth=1.6, marker="o",
                 markersize=2.5, label=nome)
        b = ((det["mensagem"] - 1) // bloco)
        g = det.groupby(b)["latencia_ms"].median()
        ax2.plot(g.index * bloco + bloco / 2, g.values, color=d["cor"], linewidth=1.6, label=nome)
    eixo_log(ax1)
    ax1.set_xlabel("Posição da mensagem")
    ax1.set_ylabel("Latência mediana entre as execuções (ms) — escala log")
    ax1.set_title(f"(a) as {inicio} primeiras mensagens", fontsize=10.5)
    ax2.set_ylim(bottom=0)
    ax2.set_xlabel("Posição da mensagem na execução")
    ax2.set_ylabel(f"Latência mediana do bloco de {bloco} mensagens (ms)")
    ax2.set_title("(b) a execução inteira", fontsize=10.5)
    ax2.legend(frameon=False, loc="upper right")
    fig.suptitle("Cenário 1 — latência ao longo da execução", fontsize=11.5, y=0.99)
    fig.text(0.5, 0.925, "Juntando as 10 execuções de cada tecnologia. "
             "Linha que não sobe em (b) = sem crescimento sustentado da latência", ha="center",
             fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico4_vazao(pdf, dados):
    """Eixo único de 195 a 205 msg/s: mostra que as duas ficaram coladas na
    taxa configurada, sem exagerar diferenças de centésimos."""
    fig, ax = plt.subplots(figsize=(6.6, 5.2))
    nomes = list(dados)
    for i, nome in enumerate(nomes, start=1):
        d = dados[nome]
        caixa_com_pontos(ax, i, d["agg"]["vazao_msg_s"].values, d["cor"], d["cor_ponto"])
    ax.axhline(200, color="#888888", linestyle="--", linewidth=1, zorder=1)
    ax.text(len(nomes) + 0.45, 200.25, "taxa configurada", fontsize=8, color="#666666", ha="right")
    ax.set_ylim(195, 205)
    ax.set_xticks(range(1, len(nomes) + 1))
    ax.set_xticklabels(nomes)
    ax.ticklabel_format(axis="y", style="plain", useOffset=False)
    ax.grid(False, axis="x")
    ax.set_ylabel("Vazão do produtor (msg/s)")
    titulo(ax, "Cenário 1 — vazão do produtor por execução",
           "1 caixa = 10 execuções; linha tracejada = 200 msg/s configurados")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def tem_atraso(dados):
    return all(COL_ATRASO in d["det"].columns for d in dados.values())


def ccdf(valores):
    """Pontos da curva 'fração de mensagens ACIMA de x' (em %)."""
    v = np.sort(np.asarray(valores, dtype=float))
    n = len(v)
    return v, (n - np.arange(1, n + 1)) / n * 100


def eixo_ccdf(ax):
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}"))
    ax.set_ylim(bottom=0.001, top=100)


def grafico5_atraso(pdf, dados):
    """(a) atraso do gerador das duas tecnologias; (b) latência medida
    (linha cheia) x latência a partir do instante planejado (tracejada)."""
    if not tem_atraso(dados):
        print(f"[aviso] coluna '{COL_ATRASO}' ausente — gráfico 5 não gerado "
              "(rode a versão do Java que envia o header 'ts-planejado')")
        return
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 5.0))
    for nome, d in dados.items():
        det = d["det"]
        x, y = ccdf(det[COL_ATRASO].clip(lower=PISO_LOG_MS))
        ax1.plot(x, y, color=d["cor"], linewidth=1.8, drawstyle="steps-post", label=nome)
        lat = det["latencia_ms"]
        x, y = ccdf(lat.clip(lower=PISO_LOG_MS))
        ax2.plot(x, y, color=d["cor"], linewidth=1.6, drawstyle="steps-post",
                 label=f"{nome} — medida")
        x, y = ccdf((lat + det[COL_ATRASO]).clip(lower=PISO_LOG_MS))
        ax2.plot(x, y, color=d["cor"], linewidth=1.6, linestyle="--", drawstyle="steps-post",
                 label=f"{nome} — a partir do planejado")
    for ax, refs in ((ax1, (1, 10)), (ax2, (10, 100))):
        for ref in refs:
            ax.axvline(ref, color="#888888", linestyle=":", linewidth=1)
            ax.text(ref, 40, f" {ref} ms", fontsize=8, color="#666666", va="top")
        eixo_ccdf(ax)
    ax1.set_xlabel("Atraso do gerador (ms) — escala log")
    ax1.set_ylabel("Mensagens acima de X (%) — escala log")
    ax1.set_title("(a) atraso do gerador: envio efetivo − instante planejado", fontsize=10.5)
    ax1.legend(frameon=False, loc="lower left")
    ax2.set_xlabel("Latência fim a fim (ms) — escala log")
    ax2.set_title("(b) latência medida × a partir do instante planejado", fontsize=10.5)
    ax2.legend(frameon=False, loc="lower left", fontsize=8)
    fig.suptitle("Cenário 1 — atraso do gerador e efeito na latência", fontsize=11.5, y=0.99)
    fig.text(0.5, 0.925, "Juntando as 10 execuções de cada tecnologia. Onde as linhas cheia e "
             "tracejada se separam, a latência medida subestima a espera real", ha="center",
             fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def resumo_atraso(d):
    """Linhas extras da tabela: atraso do gerador e latência a partir do
    instante planejado. '—' se o CSV não tiver a coluna."""
    det = d["det"]
    if COL_ATRASO not in det.columns:
        return ["—"] * 7
    a = det[COL_ATRASO]
    plan = det["latencia_ms"] + a
    plan_exec = plan.groupby(det["execucao"])
    acima10 = plan_exec.apply(lambda s: (s > 10).mean() * 100)
    return [
        f"{a.median() * 1000:.1f} µs",
        f"{a.quantile(0.99):.3f} ms",
        f"{(a > 1).mean() * 100:.2f}%",
        f"{a.max():.1f} ms",
        f"{plan_exec.median().median():.2f} ms",
        f"{acima10.min():.1f}% – {acima10.max():.1f}%",
        f"{plan.max():.1f} ms",
    ]


def resumo(d):
    med_exec = d["med"]
    q1, med, q3 = np.percentile(med_exec, [25, 50, 75])
    iqr = q3 - q1
    dentro = med_exec[(med_exec >= q1 - 1.5 * iqr) & (med_exec <= q3 + 1.5 * iqr)]
    outl = med_exec[(med_exec < q1 - 1.5 * iqr) | (med_exec > q3 + 1.5 * iqr)]
    por_exec = d["det"].groupby("execucao")["latencia_ms"]
    acima10 = por_exec.apply(lambda s: (s > 10).mean() * 100)
    acima100 = por_exec.apply(lambda s: (s > 100).mean() * 100)
    vaz = d["agg"]["vazao_msg_s"]
    return [
        f"{med:.2f} ms",
        f"{q1:.2f} – {q3:.2f} ms",
        f"{dentro.min():.2f} – {dentro.max():.2f} ms",
        ", ".join(f"{v:.2f}" for v in sorted(outl)) or "nenhum",
        f"{d['det']['latencia_ms'].median():.2f} ms",
        f"{med_exec.min():.2f} – {med_exec.max():.2f} ms",
        f"{acima10.min():.1f}% – {acima10.max():.1f}%",
        f"{acima100.min():.2f}% – {acima100.max():.2f}%",
        f"{d['det']['latencia_ms'].max():.1f} ms",
        f"{vaz.median():.2f} msg/s",
        f"{vaz.min():.2f} – {vaz.max():.2f} msg/s",
    ] + resumo_atraso(d)


def tabela(pdf, dados):
    linhas_nome = [
        "Latência mediana/execução: mediana",
        "Latência mediana/execução: caixa (Q1–Q3)",
        "Latência mediana/execução: bigodes",
        "Latência mediana/execução: outliers (ms)",
        "Latência de todas as mensagens: mediana",
        "Latência mediana/execução: mín–máx",
        "Mensagens > 10 ms, por execução",
        "Mensagens > 100 ms, por execução",
        "Maior latência registrada",
        "Vazão do produtor: mediana",
        "Vazão do produtor (mín–máx)",
        "Atraso do gerador: mediana",
        "Atraso do gerador: 99% das mensagens abaixo de",
        "Mensagens com atraso do gerador > 1 ms",
        "Maior atraso do gerador",
        "A partir do planejado: latência mediana/execução (mediana)",
        "A partir do planejado: mensagens > 10 ms, por execução",
        "A partir do planejado: maior latência",
    ]
    colunas = {nome: resumo(d) for nome, d in dados.items()}
    texto = [[ln] + [colunas[n][i] for n in dados] for i, ln in enumerate(linhas_nome)]

    fig, ax = plt.subplots(figsize=(10.5, 7.4))
    ax.axis("off")
    tab = ax.table(cellText=texto, colLabels=[""] + list(dados), loc="center",
                   cellLoc="center", colWidths=[0.48] + [0.26] * len(dados))
    tab.auto_set_font_size(False)
    tab.set_fontsize(9)
    tab.scale(1, 1.55)
    for (r, c), cel in tab.get_celld().items():
        if c == 0:
            cel.set_text_props(ha="left")
        if r == 0:
            cel.set_text_props(weight="bold")
    ax.set_title("Cenário 1 — resumo Kafka × RabbitMQ (10 execuções de 10.000 mensagens)", fontsize=11)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)

    # também em CSV, para copiar para o TCC
    pd.DataFrame(texto, columns=["Métrica"] + list(dados)).to_csv(
        os.path.join(PASTA_SCRIPT, "cenario1-comparativo-resumo.csv"), index=False, encoding="utf-8-sig")


def main():
    dados = carregar()
    if not dados:
        print("Nenhum resultado encontrado — nada a plotar.")
        return
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.join(PASTA_SCRIPT, "cenario1-comparativo.pdf")
    with PdfPages(saida) as pdf:
        grafico1_latencia_media(pdf, dados)
        grafico2_acumulada(pdf, dados)
        grafico3_ao_longo(pdf, dados)
        grafico4_vazao(pdf, dados)
        grafico5_atraso(pdf, dados)
        tabela(pdf, dados)
    print(f"PDF gerado: {saida}")
    print(f"Resumo em CSV: {os.path.join(PASTA_SCRIPT, 'cenario1-comparativo-resumo.csv')}")


if __name__ == "__main__":
    main()