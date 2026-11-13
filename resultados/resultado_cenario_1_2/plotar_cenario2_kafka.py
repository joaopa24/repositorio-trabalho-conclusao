#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 2 — Consumidor Lento — gráficos do KAFKA.

Lê os dois CSVs gerados pelo orquestrador Cenario2ConsumidorLento:
    resultados-kafka-cenario2.csv            (1 linha por execução)
    resultados-kafka-cenario2-detalhado.csv  (1 linha por mensagem)

Gera um PDF com:
    Gráfico 1 — utilização do consumidor (%) por max.poll.records
    Gráfico 2 — tempo ocioso médio por mensagem (ms) por max.poll.records
    Gráfico 3 — vazão efetiva do consumidor (msg/s), com o teórico de 200 msg/s
    Gráfico 4 — latência fim a fim x ordem de chegada (validação: cresce
                ~5 ms por posição na fila, em qualquer configuração)
    Gráfico 5 — vazão do produtor (verificação: não deve depender do
                parâmetro do consumidor)

Nos gráficos 1, 2, 3 e 5: 1 caixa = 10 execuções; cada ponto = 1 execução.

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_cenario2_kafka.py
ou:
    python plotar_cenario2_kafka.py <pasta_dos_csvs> [saida.pdf]

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
TECNOLOGIA    = "Kafka"
COL_PARAM     = "max_poll_records"
NOME_PARAM    = "max.poll.records"
ARQ_AGREGADO  = "resultados-kafka-cenario2.csv"
ARQ_DETALHADO = "resultados-kafka-cenario2-detalhado.csv"
SAIDA_PADRAO  = "cenario2-kafka.pdf"
COR_CAIXA     = "#185FA5"   # azul (mesmo do Cenário 1)
COR_PONTO     = "#D85A30"   # laranja (mesmo do Cenário 1)
CMAP_LINHAS   = "Blues"     # tons de uma cor só para os valores do parâmetro

DELAY_MS = 5
VAZAO_TEORICA = 1000.0 / DELAY_MS  # 200 msg/s

# ----------------------------------------------------------------------
plt.rcParams.update({
    "font.size": 10,
    "axes.grid": True,
    "axes.grid.axis": "y",
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 120,
})

PASTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PASTA = sys.argv[1] if len(sys.argv) > 1 else PASTA_SCRIPT
SAIDA = sys.argv[2] if len(sys.argv) > 2 else SAIDA_PADRAO


def caminho(nome):
    return os.path.join(PASTA, nome)


def ler(nome):
    path = caminho(nome)
    if not os.path.isfile(path):
        print(f"[aviso] arquivo não encontrado: {path}")
        return None
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    return df


# ----------------------------------------------------------------------
# Boxplot por valor do parâmetro: 1 caixa por valor, pontos = execuções
# ----------------------------------------------------------------------
def boxplot_por_parametro(pdf, agregado, coluna, ylabel, titulo, subtitulo,
                          referencia=None, rotulo_ref=None):
    if agregado is None or coluna not in agregado.columns:
        print(f"[aviso] coluna '{coluna}' ausente — gráfico '{titulo}' não gerado")
        return

    valores = sorted(agregado[COL_PARAM].unique())
    dados = [pd.to_numeric(agregado.loc[agregado[COL_PARAM] == v, coluna],
                           errors="coerce").dropna().values for v in valores]
    posicoes = np.arange(1, len(valores) + 1)

    fig, ax = plt.subplots(figsize=(7.2, 5.0))
    ax.boxplot(dados, positions=posicoes, widths=0.55, patch_artist=True,
               showfliers=False,  # os pontos abaixo já mostram todas as execuções
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.45, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))

    rng = np.random.default_rng(42)  # jitter fixo: o gráfico sai igual sempre
    for p, d in zip(posicoes, dados):
        ax.scatter(p + rng.uniform(-0.1, 0.1, len(d)), d, s=22, color=COR_PONTO,
                   edgecolor="white", linewidth=0.6, zorder=3)

    if referencia is not None:
        ax.axhline(referencia, color="#555555", linestyle="--", linewidth=1)
        ax.annotate(rotulo_ref, xy=(posicoes[-1] + 0.35, referencia),
                    xytext=(0, -4), textcoords="offset points",
                    ha="right", va="top", fontsize=8.5, color="#555555")

    ax.set_xticks(posicoes)
    ax.set_xticklabels([str(int(v)) for v in valores])
    ax.set_xlabel(NOME_PARAM)
    ax.set_ylabel(ylabel)
    ax.set_title(f"{TECNOLOGIA} — {titulo}", fontsize=11.5, pad=22)
    ax.text(0.5, 1.02, subtitulo, transform=ax.transAxes, ha="center",
            fontsize=8.5, color="#555555")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Gráfico 4 — latência x ordem de chegada
# ----------------------------------------------------------------------
def grafico_latencia_sequencia(pdf, detalhado):
    if detalhado is None:
        print("[aviso] sem CSV detalhado — gráfico 4 não gerado")
        return

    valores = sorted(detalhado[COL_PARAM].unique())
    cores = plt.get_cmap(CMAP_LINHAS)(np.linspace(0.45, 0.95, len(valores)))

    fig, ax = plt.subplots(figsize=(7.2, 5.0))
    xmax = 0
    for cor, v in zip(cores, valores):
        sub = detalhado[detalhado[COL_PARAM] == v]
        media = sub.groupby("ordem_chegada")["latencia_ms"].mean().sort_index()
        xmax = max(xmax, int(media.index.max()))
        ax.plot(media.index, media.values / 1000.0, color=cor, linewidth=1.6,
                label=f"{NOME_PARAM} = {int(v)}")

    xs = np.arange(1, xmax + 1)
    ax.plot(xs, xs * DELAY_MS / 1000.0, color="black", linestyle="--", linewidth=1,
            label=f"Referência: posição × {DELAY_MS} ms")

    ax.set_xlabel("Ordem de chegada da mensagem ao consumidor")
    ax.set_ylabel("Latência fim a fim (s)")
    ax.set_title(f"{TECNOLOGIA} — latência fim a fim × posição na fila",
                 fontsize=11.5, pad=32)
    ax.text(0.5, 1.02, "Média das 10 execuções em cada posição. Se as linhas acompanham a\n"
            "referência, a latência mede a espera na fila, e não o broker.",
            transform=ax.transAxes, ha="center", fontsize=8.5, color="#555555")
    ax.grid(True, axis="both", alpha=0.3, linestyle="--")
    ax.legend(fontsize=8.5, frameon=False, loc="upper left")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Tabela-resumo (mediana de cada métrica por valor do parâmetro)
# ----------------------------------------------------------------------
def tabela_resumo(pdf, agregado):
    if agregado is None:
        return
    colunas = [
        ("utilizacao_consumidor_pct", "Utilização (%)", "{:.1f}"),
        ("ocioso_medio_ms", "Ocioso médio (ms)", "{:.3f}"),
        ("vazao_consumidor_msg_s", "Consumidor (msg/s)", "{:.1f}"),
        ("vazao_produtor_msg_s", "Produtor (msg/s)", "{:.0f}"),
    ]
    linhas = []
    for v in sorted(agregado[COL_PARAM].unique()):
        sub = agregado[agregado[COL_PARAM] == v]
        linha = [str(int(v)), str(len(sub))]
        for col, _, fmt in colunas:
            linha.append(fmt.format(sub[col].median()) if col in sub else "—")
        linhas.append(linha)

    fig, ax = plt.subplots(figsize=(8.0, 1.6 + 0.4 * len(linhas)))
    ax.axis("off")
    tab = ax.table(cellText=linhas,
                   colLabels=[NOME_PARAM, "Execuções"] + [c[1] for c in colunas],
                   loc="center", cellLoc="center")
    tab.auto_set_font_size(False)
    tab.set_fontsize(9)
    tab.scale(1, 1.6)
    ax.set_title(f"{TECNOLOGIA} — Cenário 2 — medianas das 10 execuções", fontsize=11)
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
def pagina_rosto(pdf):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.92, f"Cenário 2 — Consumidor Lento ({TECNOLOGIA})",
             ha="center", fontsize=17, weight="bold")
    fig.text(0.5, 0.885, f"Custo de entrega ao consumidor por {NOME_PARAM}",
             ha="center", fontsize=11, color="#555555")
    fig.text(0.1, 0.82, f"Pasta lida: {os.path.abspath(PASTA)}", fontsize=8.5, family="monospace")

    linhas = ["Arquivos:"]
    for nome in (ARQ_AGREGADO, ARQ_DETALHADO):
        marca = "OK" if os.path.isfile(caminho(nome)) else "--"
        linhas.append(f"  [{marca}] {nome}")
    fig.text(0.1, 0.79, "\n".join(linhas), ha="left", va="top", fontsize=8.5, family="monospace")

    nota = (
        "Como ler:\n\n"
        f"- O produtor envia as 1.000 mensagens de uma vez, então sempre há mensagens\n"
        f"  esperando. O consumidor leva {DELAY_MS} ms por mensagem ({VAZAO_TEORICA:.0f} msg/s no máximo).\n\n"
        "- Utilização do consumidor: fração do tempo em que ele estava processando.\n"
        "  100% = nunca ficou parado esperando a próxima mensagem.\n\n"
        "- Tempo ocioso: intervalo entre terminar uma mensagem e receber a próxima.\n"
        "  É o custo da entrega. Ocioso perto de 0 = a próxima já estava no cliente.\n\n"
        "- Gráficos 1, 2, 3 e 5: 1 caixa = 10 execuções; cada ponto = 1 execução.\n\n"
        "- Gráfico 4 (validação): a latência fim a fim cresce cerca de 5 ms por\n"
        "  posição na fila. Ela mede a espera na fila, por isso não é usada para\n"
        "  comparar as tecnologias neste cenário.\n\n"
        f"- Gráfico 5 (verificação): {NOME_PARAM} é do consumidor; o gráfico mostra\n"
        "  se ele afeta a vazão do produtor."
    )
    fig.text(0.1, 0.70, nota, ha="left", va="top", fontsize=9.5, color="#333333")
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    agregado = ler(ARQ_AGREGADO)
    detalhado = ler(ARQ_DETALHADO)

    with PdfPages(saida) as pdf:
        pagina_rosto(pdf)
        boxplot_por_parametro(
            pdf, agregado, "utilizacao_consumidor_pct", "Utilização do consumidor (%)",
            "utilização do consumidor",
            "Fração do tempo em que o consumidor estava processando (1 ponto = 1 execução)",
            referencia=100, rotulo_ref="100% = nunca ficou parado")
        boxplot_por_parametro(
            pdf, agregado, "ocioso_medio_ms", "Tempo ocioso médio por mensagem (ms)",
            "tempo ocioso por mensagem",
            "Espera entre terminar uma mensagem e receber a próxima (1 ponto = 1 execução)")
        boxplot_por_parametro(
            pdf, agregado, "vazao_consumidor_msg_s", "Vazão do consumidor (msg/s)",
            "vazão efetiva do consumidor",
            "Mensagens processadas por segundo (1 ponto = 1 execução)",
            referencia=VAZAO_TEORICA, rotulo_ref=f"teórico: {VAZAO_TEORICA:.0f} msg/s")
        grafico_latencia_sequencia(pdf, detalhado)
        boxplot_por_parametro(
            pdf, agregado, "vazao_produtor_msg_s", "Vazão do produtor (msg/s)",
            "vazão do produtor (verificação)",
            f"Verifica se {NOME_PARAM}, que é do consumidor, afeta o produtor")
        tabela_resumo(pdf, agregado)

    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()
