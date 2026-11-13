"""
Avançado — Cenário 4: Replay — gráficos do KAFKA.

Uso:
    python replay_kafka.py [pasta_resultados] [pasta_saida]

    pasta_resultados  pasta com resultados-kafka-replay.csv; sem argumento, procura
                      na pasta atual, na do script e em PASTA_PADRAO
                      (padrão: resultado_cenario_3_4)
    pasta_saida       padrão: a mesma pasta dos resultados

Gera replay-kafka.pdf, com 3 páginas:
    1  o que cada modo releu: intervalo de sequências recebido em cada execução,
       com o intervalo esperado ao fundo
    2  tempo até a 1ª mensagem, por modo
    3  vazão de releitura, por modo (sem o modo offset: 1.000 mensagens são
       poucas para estimar vazão)

Os tempos e as vazões servem para comparar os modos DESTA tecnologia entre si.
Entre as tecnologias, os valores absolutos não são comparáveis (janelas de
leitura e protocolos diferentes); ver replay_comparativo.py.
"""

import csv
import os
import statistics as st
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.transforms as mtransforms  # noqa: E402
from matplotlib.backends.backend_pdf import PdfPages  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

# ---------------------------------------------------------------- o que muda entre Kafka e RabbitMQ
TECNOLOGIA = "Kafka"
ARQUIVO = "kafka"
PASTA_PADRAO = "resultado_cenario_3_4"
COR = "#2a78d6"
MODOS = [   # (valor no CSV, rótulo, como lê)
    ("normal", "consumo normal", "grupo G, lê tudo e faz commit"),
    ("grupo-existente", "mesmo grupo", "volta com o grupo G"),
    ("inicio", "início", "seekToBeginning"),
    ("timestamp", "timestamp", "offsetsForTimes, meio da pausa"),
    ("offset", "offset", "seek(49.000)"),
    ("novo-grupo", "grupo novo", "grupo novo, earliest"),
]
# modos em que o tempo até a 1ª mensagem inclui a entrada no grupo
COM_GRUPO = {"normal", "novo-grupo"}
# por que um modo recebe 0 mensagens
MOTIVO_ZERO = {"grupo-existente": "o broker guardou a posição do grupo"}
DESCRICAO = ("50.000 mensagens de 1 KB numa partição, com pausa de 5 s no meio do envio; "
             "10 execuções, modos em ordem rotativa")

# ---------------------------------------------------------------- aparência (igual ao comparativo)
TEXTO, TEXTO_2 = "#0b0b0b", "#52514e"
GRADE, EIXO = "#e1e0d9", "#c3c2b7"
ESPERADO = "#e6e5df"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 9.5, "axes.labelsize": 9,
    "axes.edgecolor": EIXO, "axes.labelcolor": TEXTO_2, "axes.titlecolor": TEXTO,
    "xtick.color": TEXTO_2, "ytick.color": TEXTO_2, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRADE,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.facecolor": "white", "axes.facecolor": "white",
    "savefig.dpi": 300, "pdf.fonttype": 42, "legend.frameon": False, "legend.fontsize": 8,
})

N = 50_000


def virgula(v, casas=1):
    return f"{v:.{casas}f}".replace(".", ",")


def milhar(v):
    return f"{int(round(v)):,}".replace(",", ".")


# ---------------------------------------------------------------- dados
def carregar(pasta):
    """{modo: [execução, ...]}. No RabbitMQ conta as ACEITAS (depois do filtro do consumidor)."""
    caminho = os.path.join(pasta, f"resultados-{ARQUIVO}-replay.csv")
    dados = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            dados[r["modo"]].append({
                "execucao": int(r["execucao"]),
                "esperadas": int(r["esperadas"]),
                "recebidas": int(r["aceitas"] if "aceitas" in r else r["recebidas"]),
                "descartadas": int(r["descartadas"]) if "descartadas" in r else None,
                "primeira": int(r["primeira_seq"]),
                "ultima": int(r["ultima_seq"]),
                "correto": r["correto"].strip().lower() == "true",
                "t_primeira_ms": float(r["t_primeira_ms"]),
                "vazao": float(r["vazao_msg_s"]),
            })
    for v in dados.values():
        v.sort(key=lambda d: d["execucao"])
    faltando = [m for m, _, _ in MODOS if m not in dados]
    if faltando:
        raise SystemExit(f"Modos sem dados no CSV: {faltando}")
    return dados


# ---------------------------------------------------------------- auxiliares de desenho
def cabecalho(fig, subtitulo, detalhe):
    """Nome da tecnologia (na cor dela), o assunto da página ao lado e uma linha de detalhe."""
    nome = fig.text(0.01, 0.995, TECNOLOGIA, fontsize=12, fontweight="bold", color=COR, va="top", ha="left")
    fig.canvas.draw()
    fim = nome.get_window_extent().transformed(fig.transFigure.inverted()).x1
    fig.text(fim + 0.012, 0.995, subtitulo, fontsize=11, color=TEXTO, va="top", ha="left")
    fig.text(0.01, 0.95, detalhe, fontsize=8, color=TEXTO_2, va="top", ha="left")


def pontos(ax, itens, fmt, ylabel, titulo, log=False):
    """itens = [(rótulo do eixo x, [valores por execução])]; traço = mediana, ponto = execução."""
    for x, (_, vals) in enumerate(itens):
        m = st.median(vals)
        ax.plot([x - 0.28, x + 0.28], [m, m], color=COR, lw=2.2, solid_capstyle="round", zorder=3)
        ax.scatter([x] * len(vals), vals, s=34, color=COR, edgecolor="white", linewidth=1.2, zorder=4)
        ax.annotate(fmt(m), xy=(x + 0.32, m), va="center", fontsize=8, color=TEXTO)
    ax.set_xticks(range(len(itens)))
    ax.set_xticklabels([rot for rot, _ in itens], fontsize=7.5)
    ax.set_xlim(-0.6, len(itens) - 0.2)
    ax.set_ylabel(ylabel)
    ax.set_title(titulo, loc="left", color=TEXTO)
    ax.grid(axis="x", visible=False)
    if log:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=6))
        ax.yaxis.set_minor_formatter(NullFormatter())


def rotulo(m, rot, como):
    return f"{rot}\n{como}"


# ---------------------------------------------------------------- páginas
def pagina_releitura(dados):
    n = len(MODOS)
    fig, ax = plt.subplots(figsize=(12.5, 0.62 * n + 1.8))
    lado = mtransforms.blended_transform_factory(ax.transAxes, ax.transData)
    for y, (m, rot, como) in enumerate(MODOS):
        execs = dados[m]
        esp = execs[0]["esperadas"]
        corretas = sum(d["correto"] for d in execs)
        if esp > 0:   # o que devia chegar: as últimas "esp" sequências
            ax.barh(y, esp, left=N - esp + 0.5, height=0.64, color=ESPERADO, lw=0, zorder=1)
        for d in execs:
            if d["recebidas"] > 0:
                ax.plot([d["primeira"] - 0.5, d["ultima"] + 0.5], [y, y], color=COR, lw=7, alpha=0.35,
                        solid_capstyle="butt", zorder=3)
        if esp > 0:
            p = st.median(d["primeira"] for d in execs)
            u = st.median(d["ultima"] for d in execs)
            txt = f"{milhar(st.median(d['recebidas'] for d in execs))} de {milhar(esp)}  ·  seq {milhar(p)} a {milhar(u)}"
        else:
            txt = "0 mensagens" + (f" ({MOTIVO_ZERO[m]})" if m in MOTIVO_ZERO else "")
        txt += f"  ·  {corretas}/{len(execs)} corretas"
        desc = [d["descartadas"] for d in execs if d["descartadas"] is not None]
        if desc:
            txt += f"  ·  {sum(desc)} descartadas"
        ax.text(1.01, y, txt, transform=lado, va="center", ha="left", fontsize=7.5,
                color=TEXTO if corretas == len(execs) else "#d03b3b")
    ax.axvline(N / 2 + 0.5, color=TEXTO_2, lw=0.8, ls=(0, (3, 3)), zorder=2)
    ax.text(N / 2 + 400, -0.75, "pausa de 5 s", fontsize=7.5, color=TEXTO_2, va="bottom", ha="left")
    ax.set_yticks(range(n))
    ax.set_yticklabels([rotulo(m, r, c) for m, r, c in MODOS], fontsize=7.5)
    ax.set_ylim(n - 0.5, -0.9)
    ax.set_xlim(0, N + 1)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: milhar(v)))
    ax.set_xlabel("Número de sequência da mensagem")
    ax.grid(axis="y", visible=False)
    fig.subplots_adjust(left=0.17, right=0.6, top=0.8, bottom=0.12)
    cabecalho(fig, "o que cada modo releu",
              DESCRICAO + ".\nFaixa cinza = esperado; faixa colorida = recebido em cada execução")
    return fig


def pagina_tempo(dados):
    itens = []
    for m, rot, _ in MODOS:
        vals = [d["t_primeira_ms"] for d in dados[m] if d["t_primeira_ms"] >= 0]
        if vals:
            itens.append((rot + ("\n(inclui entrar no grupo)" if m in COM_GRUPO else ""), vals))
    todos = [v for _, vals in itens for v in vals]
    log = max(todos) / min(todos) > 8
    fig, ax = plt.subplots(figsize=(8.5, 3.9))
    pontos(ax, itens, lambda v: f"{milhar(v)} ms", "Milissegundos" + (" (escala log)" if log else ""),
           "Tempo do pedido de leitura até a 1ª mensagem", log=log)
    if log:
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: milhar(v)))
    else:
        ax.set_ylim(0, max(todos) * 1.25)
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    cabecalho(fig, "quanto tempo até começar a reler",
              "Inclui achar a posição pedida. Traço = mediana; cada ponto = 1 execução. "
              "Modos que recebem 0 mensagens ficam de fora")
    return fig


def pagina_vazao(dados):
    itens = []
    for m, rot, _ in MODOS:
        if m == "offset":
            continue
        vals = [d["vazao"] for d in dados[m] if d["recebidas"] > 1]
        if vals:
            itens.append((rot, vals))
    fig, ax = plt.subplots(figsize=(8.5, 3.9))
    pontos(ax, itens, lambda v: f"{milhar(v)} msg/s", "Mensagens por segundo",
           "Vazão de leitura, da 1ª à última mensagem recebida")
    ax.set_ylim(0, max(v for _, vals in itens for v in vals) * 1.2)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: milhar(v)))
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    extra = "; nos modos com grupo, não inclui a entrada no grupo" if COM_GRUPO else ""
    cabecalho(fig, "velocidade de releitura",
              "Sem o modo offset (1.000 mensagens: poucas para estimar vazão)" + extra
              + ". Traço = mediana; cada ponto = 1 execução")
    return fig


def achar_pasta(indicada=None):
    """
    Pasta com o CSV. Sem argumento, procura na pasta atual, na pasta do
    script e numa subpasta (ou pasta vizinha) chamada PASTA_PADRAO, de modo
    que o script funcione rodando de dentro ou de fora da pasta de resultados.
    """
    nome = f"resultados-{ARQUIVO}-replay.csv"
    if indicada:
        candidatas = [indicada]
    else:
        aqui = os.path.dirname(os.path.abspath(__file__))
        candidatas = [os.getcwd(), aqui, os.path.join(os.getcwd(), PASTA_PADRAO), os.path.join(aqui, PASTA_PADRAO),
                      os.path.join(os.getcwd(), os.pardir, PASTA_PADRAO), os.path.join(aqui, os.pardir, PASTA_PADRAO)]
    for p in candidatas:
        if os.path.isfile(os.path.join(p, nome)):
            return os.path.normpath(p)
    raise SystemExit(f"Não encontrei {nome} em: " + "; ".join(os.path.abspath(p) for p in candidatas)
                     + "\nPasse a pasta como argumento: py replay_" + ARQUIVO + ".py <pasta_dos_resultados>")


def main():
    pasta = achar_pasta(sys.argv[1] if len(sys.argv) > 1 else None)
    saida = sys.argv[2] if len(sys.argv) > 2 else pasta
    print("Lendo de:", os.path.abspath(pasta))
    os.makedirs(saida, exist_ok=True)
    dados = carregar(pasta)
    pdf = os.path.join(saida, f"replay-{ARQUIVO}.pdf")
    with PdfPages(pdf) as pp:
        for gerar in (pagina_releitura, pagina_tempo, pagina_vazao):
            fig = gerar(dados)
            pp.savefig(fig)
            plt.close(fig)
    print("Gerado:", os.path.abspath(pdf))


if __name__ == "__main__":
    main()
