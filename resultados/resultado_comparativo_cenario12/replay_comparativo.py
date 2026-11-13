"""
Avançado — Cenário 4: Replay — gráficos comparativos Kafka × RabbitMQ.

Uso:
    python replay_comparativo.py [pasta_kafka] [pasta_rabbitmq] [pasta_saida]

    Sem argumentos, procura resultado_cenario_3_4 e resultado_rabbitmq_cenario_3_4
    na pasta atual, na pasta do script e nas pastas acima delas.

    <pasta_kafka>    contém resultados-kafka-replay.csv
    <pasta_rabbitmq> contém resultados-rabbitmq-replay.csv
    [pasta_saida]    padrão: a pasta em que o script está

Gera:
    replay-comparativo.pdf, com 2 páginas:
        1  o que cada modo releu, com os modos pareados pela função
           (consumo normal, voltar de onde parou, início, instante, offset)
        2  razões DENTRO de cada tecnologia: o custo de achar a posição
           (tempo até a 1ª mensagem ÷ o do modo início) e o custo de reler
           (vazão do início ÷ vazão do consumo normal, na mesma execução)
    replay-comparativo-resumo.csv  medianas e faixas por tecnologia e modo

Os valores absolutos de tempo e de vazão NÃO são comparados entre as
tecnologias: os clientes leem com janelas diferentes (até 1 MB por busca no
Kafka, 500 mensagens no RabbitMQ) e o stream é lido pelo AMQP 0.9.1, e não
pelo protocolo nativo de streams. Entre as tecnologias, comparam-se a
possibilidade de reler, a correção das mensagens e a precisão da posição.
"""

import csv
import os
import statistics as st
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.backends.backend_pdf import PdfPages  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

# ---------------------------------------------------------------- aparência
COR = {"Kafka": "#2a78d6", "RabbitMQ": "#eb6834"}   # categórica, validada (CVD ΔE 24,7)
TEXTO, TEXTO_2 = "#0b0b0b", "#52514e"
GRADE, EIXO = "#e1e0d9", "#c3c2b7"
ESPERADO = "#e6e5df"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 9.5, "axes.labelsize": 9,
    "axes.edgecolor": EIXO, "axes.labelcolor": TEXTO_2, "axes.titlecolor": TEXTO,
    "xtick.color": TEXTO_2, "ytick.color": TEXTO_2, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRADE,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.facecolor": "white", "axes.facecolor": "white",
    "savefig.dpi": 300, "pdf.fonttype": 42,
})

N = 50_000

# linhas da página 1, pareadas pela função: (função, (modo Kafka, como), (modo RabbitMQ, como))
LINHAS = [
    ("Consumo normal", ("normal", "grupo, commit no fim"), ("normal-stream", "stream, grava a posição")),
    ("Voltar de onde parou", ("grupo-existente", "posição guardada pelo broker"),
     ("posicao-salva", "posição guardada pela aplicação")),
    ("Reler desde o início", ("inicio", "seekToBeginning"), ("inicio", 'x-stream-offset = "first"')),
    ("A partir de um instante", ("timestamp", "offsetsForTimes (ms)"), ("timestamp", "x-stream-offset = data (s)")),
    ("A partir de um offset", ("offset", "seek(49.000)"), ("offset", "x-stream-offset = 49.000")),
    ("Grupo novo", ("novo-grupo", "grupo novo, earliest"), None),
    ("Fila clássica: consumo normal", None, ("classica", "ack apaga a mensagem")),
    ("Fila clássica: ler de novo", None, ("classica-de-novo", "nada a reler")),
]
# modo do consumo normal usado na razão "reler ÷ ler" de cada tecnologia
NORMAL = {"Kafka": "normal", "RabbitMQ": "normal-stream"}


def virgula(v, casas=1):
    return f"{v:.{casas}f}".replace(".", ",")


def milhar(v):
    return f"{int(round(v)):,}".replace(",", ".")


# ---------------------------------------------------------------- leitura
def ler(caminho):
    """{modo: {execução: linha}}. No RabbitMQ conta as ACEITAS (depois do filtro do consumidor)."""
    dados = defaultdict(dict)
    with open(caminho, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            dados[r["modo"]][int(r["execucao"])] = {
                "esperadas": int(r["esperadas"]),
                "recebidas": int(r["aceitas"] if "aceitas" in r else r["recebidas"]),
                "descartadas": int(r["descartadas"]) if "descartadas" in r else None,
                "primeira": int(r["primeira_seq"]),
                "ultima": int(r["ultima_seq"]),
                "correto": r["correto"].strip().lower() == "true",
                "t_primeira_ms": float(r["t_primeira_ms"]),
                "vazao": float(r["vazao_msg_s"]),
            }
    return dados


def carregar(pasta_k, pasta_r):
    dados = {"Kafka": ler(os.path.join(pasta_k, "resultados-kafka-replay.csv")),
             "RabbitMQ": ler(os.path.join(pasta_r, "resultados-rabbitmq-replay.csv"))}
    for _, k, r in LINHAS:
        for tec, par in (("Kafka", k), ("RabbitMQ", r)):
            if par and par[0] not in dados[tec]:
                raise SystemExit(f"{tec}: modo {par[0]} sem dados no CSV")
    return dados


# ---------------------------------------------------------------- figuras
_PDF = None   # PdfPages aberto em main(); cada figura vira uma página


def salvar(fig):
    _PDF.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def fig_releitura(dados):
    """O que cada modo releu, lado a lado, com os modos pareados pela função."""
    n = len(LINHAS)
    fig, eixos = plt.subplots(1, 2, figsize=(11, 0.78 * n + 1.2), sharey=True)
    for col, (ax, tec) in enumerate(zip(eixos, ("Kafka", "RabbitMQ"))):
        for y, linha in enumerate(LINHAS):
            par = linha[1 + col]
            if par is None:
                ax.text(N / 2, y, "sem par nesta tecnologia", ha="center", va="center", fontsize=7.5,
                        color=TEXTO_2, style="italic")
                continue
            modo, como = par
            execs = list(dados[tec][modo].values())
            esp = execs[0]["esperadas"]
            corretas = sum(d["correto"] for d in execs)
            if esp > 0:
                ax.barh(y - 0.12, esp, left=N - esp + 0.5, height=0.42, color=ESPERADO, lw=0, zorder=1)
            for d in execs:
                if d["recebidas"] > 0:
                    ax.plot([d["primeira"] - 0.5, d["ultima"] + 0.5], [y - 0.12, y - 0.12], color=COR[tec], lw=6,
                            alpha=0.35, solid_capstyle="butt", zorder=3)
            if esp > 0:
                txt = (f"{milhar(st.median(d['recebidas'] for d in execs))} msgs, seq "
                       f"{milhar(st.median(d['primeira'] for d in execs))} a {milhar(st.median(d['ultima'] for d in execs))}")
            else:
                txt = "0 mensagens"
            txt += f"  ·  {corretas}/{len(execs)} corretas  ·  {como}"
            ax.text(0, y + 0.3, txt, ha="left", va="center", fontsize=7, color=TEXTO)
        ax.axvline(N / 2 + 0.5, color=TEXTO_2, lw=0.8, ls=(0, (3, 3)), zorder=2)
        ax.text(N / 2 + 400, -0.72, "pausa de 5 s", fontsize=7.5, color=TEXTO_2, va="bottom", ha="left")
        ax.set_xlim(0, N + 1)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: milhar(v)))
        ax.set_xlabel("Número de sequência da mensagem")
        ax.set_title(tec, loc="left", color=COR[tec], fontweight="bold", fontsize=11)
        ax.grid(axis="y", visible=False)
    eixos[0].set_yticks(range(n))
    eixos[0].set_yticklabels([f for f, _, _ in LINHAS], fontsize=8)
    eixos[0].set_ylim(n - 0.45, -0.85)
    fig.suptitle("O que cada modo releu (faixa cinza = esperado; faixa colorida = recebido em cada execução)",
                 x=0.01, ha="left", fontsize=9.5, color=TEXTO)
    fig.tight_layout()
    salvar(fig)


def _razoes(ax, itens, titulo, nota_ref):
    """itens = [(tecnologia, rótulo, [razões por execução])]."""
    for x, (tec, rot, vals) in enumerate(itens):
        m = st.median(vals)
        ax.plot([x - 0.28, x + 0.28], [m, m], color=COR[tec], lw=2.2, solid_capstyle="round", zorder=3)
        ax.scatter([x] * len(vals), vals, s=34, color=COR[tec], edgecolor="white", linewidth=1.2, zorder=4)
        ax.annotate(virgula(m, 2), xy=(x + 0.32, m), va="center", fontsize=8, color=TEXTO)
    ax.axhline(1, color=TEXTO_2, lw=0.8, ls=(0, (3, 3)), zorder=2)
    ax.text(-0.55, 1.02, nota_ref, fontsize=7.5, color=TEXTO_2, va="bottom", ha="left")
    ax.set_xticks(range(len(itens)))
    ax.set_xticklabels([f"{rot}\n{tec}" for tec, rot, _ in itens], fontsize=7.5)
    ax.set_xlim(-0.6, len(itens) - 0.2)
    topo = max(v for _, _, vals in itens for v in vals)
    ax.set_ylim(0, max(1.6, topo * 1.15))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    ax.set_title(titulo, loc="left", color=TEXTO)
    ax.grid(axis="x", visible=False)


def fig_razoes(dados):
    # custo de achar a posição: tempo até a 1ª mensagem ÷ mediana do modo início, na mesma tecnologia
    pos = []
    for tec in ("Kafka", "RabbitMQ"):
        base = st.median(d["t_primeira_ms"] for d in dados[tec]["inicio"].values())
        for modo in ("timestamp", "offset"):
            pos.append((tec, modo, [d["t_primeira_ms"] / base for d in dados[tec][modo].values()]))
    # custo de reler: vazão do início ÷ vazão do consumo normal, na mesma execução
    rel = []
    for tec in ("Kafka", "RabbitMQ"):
        ini, nor = dados[tec]["inicio"], dados[tec][NORMAL[tec]]
        rel.append((tec, "início ÷ consumo normal",
                    [ini[e]["vazao"] / nor[e]["vazao"] for e in sorted(ini) if e in nor and nor[e]["vazao"] > 0]))
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 3.7), gridspec_kw={"width_ratios": [2, 1.2]})
    _razoes(a1, pos, "Achar a posição: tempo até a 1ª mensagem ÷ o do modo início", "igual ao início")
    _razoes(a2, rel, "Reler × ler: vazão do início ÷ a do consumo normal", "igual ao consumo normal")
    fig.suptitle("Razões dentro de cada tecnologia (traço = mediana; cada ponto = 1 execução). Os valores absolutos "
                 "não são comparados entre as tecnologias:\njanelas de leitura diferentes e stream lido pelo AMQP 0.9.1",
                 x=0.01, ha="left", fontsize=8.5, color=TEXTO_2)
    fig.tight_layout()
    salvar(fig)


def resumo_csv(dados, pasta):
    caminho = os.path.join(pasta, "replay-comparativo-resumo.csv")
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tecnologia", "modo", "execucoes", "corretas", "esperadas", "recebidas_mediana",
                    "primeira_seq", "ultima_seq", "descartadas_total", "t_primeira_mediana_ms", "t_primeira_min_ms",
                    "t_primeira_max_ms", "vazao_mediana_msg_s", "vazao_min_msg_s", "vazao_max_msg_s"])
        for tec in ("Kafka", "RabbitMQ"):
            for modo, ex in dados[tec].items():
                v = list(ex.values())
                tp = [d["t_primeira_ms"] for d in v if d["t_primeira_ms"] >= 0]
                vz = [d["vazao"] for d in v if d["recebidas"] > 1]
                desc = [d["descartadas"] for d in v if d["descartadas"] is not None]
                w.writerow([tec, modo, len(v), sum(d["correto"] for d in v), v[0]["esperadas"],
                            f"{st.median(d['recebidas'] for d in v):g}",
                            f"{st.median(d['primeira'] for d in v):g}", f"{st.median(d['ultima'] for d in v):g}",
                            sum(desc) if desc else "-",
                            f"{st.median(tp):.2f}" if tp else "-1", f"{min(tp):.2f}" if tp else "-1",
                            f"{max(tp):.2f}" if tp else "-1",
                            f"{st.median(vz):.0f}" if vz else "-1", f"{min(vz):.0f}" if vz else "-1",
                            f"{max(vz):.0f}" if vz else "-1"])
    return caminho


def achar_pasta(indicada, nome_csv, padrao):
    """Pasta com nome_csv: a indicada, ou procura perto da pasta atual e da pasta do script."""
    if indicada:
        candidatas = [indicada]
    else:
        aqui = os.path.dirname(os.path.abspath(__file__))
        candidatas = []
        for base in (os.getcwd(), aqui):
            candidatas += [os.path.join(base, padrao), os.path.join(base, os.pardir, padrao), base]
    for p in candidatas:
        if os.path.isfile(os.path.join(p, nome_csv)):
            return os.path.normpath(p)
    raise SystemExit(f"Não encontrei {nome_csv} em: " + "; ".join(os.path.abspath(p) for p in candidatas)
                     + "\nPasse as pastas como argumentos: py replay_comparativo.py <pasta_kafka> <pasta_rabbitmq>")


def main():
    pasta_k = achar_pasta(sys.argv[1] if len(sys.argv) > 1 else None, "resultados-kafka-replay.csv",
                          "resultado_cenario_3_4")
    pasta_r = achar_pasta(sys.argv[2] if len(sys.argv) > 2 else None, "resultados-rabbitmq-replay.csv",
                          "resultado_rabbitmq_cenario_3_4")
    print("Kafka:", os.path.abspath(pasta_k), "| RabbitMQ:", os.path.abspath(pasta_r))
    # sem pasta de saída indicada, grava na pasta em que o script está
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.dirname(os.path.abspath(__file__))
    os.makedirs(saida, exist_ok=True)
    global _PDF
    dados = carregar(pasta_k, pasta_r)
    pdf = os.path.join(saida, "replay-comparativo.pdf")
    with PdfPages(pdf) as _PDF:
        fig_releitura(dados)
        fig_razoes(dados)
    print("Gerado:", os.path.abspath(pdf))
    print("Resumo:", resumo_csv(dados, saida))


if __name__ == "__main__":
    main()
