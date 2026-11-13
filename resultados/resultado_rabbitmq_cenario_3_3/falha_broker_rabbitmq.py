"""
Avançado — Cenário 3: Falha de broker — gráficos do RABBITMQ.

Uso:
    python falha_broker_rabbitmq.py [pasta_resultados] [pasta_saida]

    pasta_resultados  pasta com os CSVs; sem argumento, procura na pasta atual,
                      na do script e em PASTA_PADRAO. Contém resultados-rabbitmq-falha-broker.csv e ...-serie.csv
                      (padrão: resultado_rabbitmq_cenario_3_3)
    pasta_saida       padrão: a mesma pasta dos resultados

Gera falha-broker-rabbitmq.pdf, com 4 páginas:
    1  latência de cada mensagem pelo instante de envio, todas as execuções
    2  zoom na queda do líder: mensagens enviadas e recebidas (acumulado)
    3  interrupção da entrega e maior intervalo sem confirmação, por execução
    4  mensagens não entregues, perdidas e duplicadas, por execução

Interrupção da entrega: soma das lacunas de mais de 1 s entre chegadas
consecutivas (se nenhuma passa de 1 s, vale a maior lacuna), calculada da
série temporal. A coluna maior_pausa_entrega_ms subestima a interrupção
quando a parada vem em dois pedaços, e a eleicao_ms não é usada.
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
from matplotlib.ticker import FuncFormatter, LogLocator, MaxNLocator, NullFormatter  # noqa: E402

# ---------------------------------------------------------------- o que muda entre Kafka e RabbitMQ
TECNOLOGIA = "RabbitMQ"
ARQUIVO = "rabbitmq"
PASTA_PADRAO = "resultado_rabbitmq_cenario_3_3"
COR = "#eb6834"
CONFIGS = [   # (valor no CSV, rótulo, nível de garantia)
    ("confirms", "confirms", "confirmação após as réplicas"),
    ("sem-confirms", "sem confirms", "sem confirmação"),
]
DESCRICAO = ("200 msg/s por 60 s; o nó líder da quorum queue cai aos 20 s e volta aos 40 s. "
             "Linha forte = 1ª execução")

# ---------------------------------------------------------------- aparência (igual ao comparativo)
CRITICO = "#d03b3b"
TEXTO, TEXTO_2 = "#0b0b0b", "#52514e"
GRADE, EIXO = "#e1e0d9", "#c3c2b7"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 9.5, "axes.labelsize": 9,
    "axes.edgecolor": EIXO, "axes.labelcolor": TEXTO_2, "axes.titlecolor": TEXTO,
    "xtick.color": TEXTO_2, "ytick.color": TEXTO_2, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRADE,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.facecolor": "white", "axes.facecolor": "white",
    "savefig.dpi": 300, "pdf.fonttype": 42, "legend.frameon": False, "legend.fontsize": 8,
})

QUEDA_S, VOLTA_S, MSGS, TAXA = 20.0, 40.0, 12_000, 200


def virgula(v, casas=1):
    return f"{v:.{casas}f}".replace(".", ",")


# ---------------------------------------------------------------- dados
def interrupcao_s(chegadas):
    ch = sorted(chegadas)
    lac = [b - a for a, b in zip(ch, ch[1:])]
    grandes = sum(x for x in lac if x > 1.0)
    return grandes if grandes > 0 else max(lac)


def carregar(pasta):
    resumo = os.path.join(pasta, f"resultados-{ARQUIVO}-falha-broker.csv")
    serie = os.path.join(pasta, f"resultados-{ARQUIVO}-falha-broker-serie.csv")
    linhas = defaultdict(list)
    with open(serie, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            linhas[(r["configuracao"], int(r["execucao"]))].append(
                (int(r["sequencia"]), float(r["envio_s"]), float(r["chegada_s"]), float(r["latencia_ms"])))
    dados = defaultdict(list)
    with open(resumo, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            e = int(r["execucao"])
            s = linhas[(r["configuracao"], e)]
            recebidas = {x[0] for x in s}
            dados[r["configuracao"]].append({
                "execucao": e,
                "serie": sorted(s, key=lambda x: x[1]),
                "faltam": [q for q in range(1, MSGS + 1) if q not in recebidas],
                "interrupcao_s": interrupcao_s([x[2] for x in s]),
                "sem_confirmacao_s": float(r["maior_intervalo_sem_confirmacao_ms"]) / 1000,
                "nao_entregues": int(r["nao_entregues"]),
                "perdidas": int(r["perdidas"]),
                "duplicadas": int(r["duplicadas"]),
            })
    for v in dados.values():
        v.sort(key=lambda d: d["execucao"])
    faltando = [c for c, _, _ in CONFIGS if c not in dados]
    if faltando:
        raise SystemExit(f"Configurações sem dados no CSV: {faltando}")
    return dados


# ---------------------------------------------------------------- auxiliares de desenho
def cabecalho(fig, subtitulo, detalhe):
    """Nome da tecnologia (na cor dela), o assunto da página ao lado e uma linha de detalhe."""
    nome = fig.text(0.01, 0.995, TECNOLOGIA, fontsize=12, fontweight="bold", color=COR, va="top", ha="left")
    fig.canvas.draw()
    fim = nome.get_window_extent().transformed(fig.transFigure.inverted()).x1
    fig.text(fim + 0.012, 0.995, subtitulo, fontsize=11, color=TEXTO, va="top", ha="left")
    fig.text(0.01, 0.95, detalhe, fontsize=8, color=TEXTO_2, va="top", ha="left")


def titulo_painel(ax, rot, garantia, extra=""):
    ax.set_title(f"{rot}  ·  {garantia}" + (f"\n{extra}" if extra else ""), loc="left", color=TEXTO)


def marcar_queda_volta(ax, rotulos=False, y=None):
    for t, txt in ((QUEDA_S, "queda do líder"), (VOLTA_S, "volta do nó")):
        ax.axvline(t, color=TEXTO_2, lw=0.8, ls=(0, (3, 3)), zorder=1)
        if rotulos and ax.get_xlim()[0] <= t <= ax.get_xlim()[1]:
            ax.text(t, y if y is not None else ax.get_ylim()[1], f" {txt}", fontsize=7.5, color=TEXTO_2,
                    va="top", ha="left")


def faixa_nao_entregues(ax, faltam):
    if faltam:
        ini, fim = (faltam[0] - 1) / TAXA, (faltam[-1] - 1) / TAXA
        ax.axvspan(ini, max(fim, ini + 0.12), color=CRITICO, alpha=0.3, lw=0, zorder=0)


def pontos_por_execucao(ax, valores_por_cfg, fmt, ylabel, titulo, nao_se_aplica=lambda v: v < 0, curto=False):
    for x, (cfg, rot, gar) in enumerate(CONFIGS):
        vals = valores_por_cfg[cfg]
        if all(nao_se_aplica(v) for v in vals):
            ax.text(x, 0.5, "não se aplica\n(sem confirmação)", transform=ax.get_xaxis_transform(),
                    ha="center", va="center", fontsize=7.5, color=TEXTO_2)
            continue
        m = st.median(vals)
        ax.plot([x - 0.28, x + 0.28], [m, m], color=COR, lw=2.2, solid_capstyle="round", zorder=3)
        ax.scatter([x] * len(vals), vals, s=34, color=COR, edgecolor="white", linewidth=1.2, zorder=4)
        ax.annotate(fmt(m), xy=(x + 0.32, m), va="center", fontsize=8, color=TEXTO)
    ax.set_xticks(range(len(CONFIGS)))
    ax.set_xticklabels([rot if curto else f"{rot}\n{gar}" for _, rot, gar in CONFIGS], fontsize=7.5)
    ax.set_xlim(-0.6, len(CONFIGS) - 0.3)
    ax.set_ylabel(ylabel)
    ax.set_title(titulo, loc="left", color=TEXTO)
    ax.grid(axis="x", visible=False)


# ---------------------------------------------------------------- páginas
def pagina_latencia(dados):
    n = len(CONFIGS)
    fig, eixos = plt.subplots(1, n, figsize=(3.9 * n, 3.9), sharey=True, squeeze=False)
    eixos = eixos[0]
    for ax, (cfg, rot, gar) in zip(eixos, CONFIGS):
        execs = dados[cfg]
        for i, d in enumerate(execs):
            faixa_nao_entregues(ax, d["faltam"])
            forte = i == 0
            ax.plot([x[1] for x in d["serie"]], [max(x[3], 0.5) for x in d["serie"]], color=COR,
                    lw=1.0 if forte else 0.6, alpha=1.0 if forte else 0.35, zorder=3 if forte else 2,
                    label="1ª execução" if forte else ("demais execuções" if i == 1 else None))
        ax.set_yscale("log")
        ax.set_ylim(1, 80_000)
        ax.set_xlim(0, 61)
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=6))
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: {1: "1 ms", 10: "10 ms", 100: "100 ms", 1000: "1 s",
                                                                 10000: "10 s"}.get(int(round(v)), "")))
        marcar_queda_volta(ax, rotulos=(ax is eixos[0]), y=50_000)
        tot = [len(d["faltam"]) for d in execs]
        titulo_painel(ax, rot, gar, ("não entregues: " + ", ".join(str(t) for t in tot[:-1]) + f" e {tot[-1]}"
                                     + " (faixas vermelhas)") if any(tot) else "")
        ax.set_xlabel("Instante de envio da mensagem (s)")
        ax.grid(axis="x", visible=False)
    eixos[0].set_ylabel("Latência de ponta a ponta (escala log)")
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    fig.legend(*eixos[0].get_legend_handles_labels(), loc="upper right", bbox_to_anchor=(0.995, 0.995), ncol=2)
    cabecalho(fig, "latência de cada mensagem ao longo do envio", DESCRICAO)
    return fig


def pagina_zoom(dados):
    n = len(CONFIGS)
    maior = max(d["interrupcao_s"] for v in dados.values() for d in v)
    x0, x1 = QUEDA_S - 2, QUEDA_S + max(maior + 3, 4)
    fig, eixos = plt.subplots(1, n, figsize=(3.9 * n, 3.6), sharey=True, squeeze=False)
    eixos = eixos[0]
    for ax, (cfg, rot, gar) in zip(eixos, CONFIGS):
        d = dados[cfg][0]
        # enviadas: o produtor envia a sequência q no instante (q - 1) / taxa
        env_t = [(q - 1) / TAXA for q in range(1, MSGS + 1)]
        ax.plot(env_t, range(1, MSGS + 1), color=TEXTO_2, lw=1.0, ls=(0, (4, 2)), label="enviadas", zorder=2)
        primeiras = {}
        for q, _, c, _ in d["serie"]:
            primeiras[q] = min(c, primeiras.get(q, c))
        cheg = sorted(primeiras.values())
        ax.step(cheg, range(1, len(cheg) + 1), where="post", color=COR, lw=1.4, label="recebidas", zorder=3)
        faixa_nao_entregues(ax, d["faltam"])
        ax.set_xlim(x0, x1)
        lo, hi = int((x0 - 0.5) * TAXA), int((x1 + 0.5) * TAXA)
        ax.set_ylim(max(lo, 0), hi)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{int(v):,}".replace(",", ".")))
        marcar_queda_volta(ax)
        ax.text(QUEDA_S, hi, " queda do líder", fontsize=7.5, color=TEXTO_2, va="top", ha="left")
        titulo_painel(ax, rot, gar)
        ax.text(0.98, 0.04, f"interrupção da entrega: {virgula(d['interrupcao_s'], 2)} s"
                + (f"\n{len(d['faltam'])} não entregues (faixa vermelha)" if d["faltam"] else ""),
                transform=ax.transAxes, ha="right", va="bottom", fontsize=7.5, color=TEXTO)
        ax.set_xlabel("Tempo desde o início do envio (s)")
    eixos[0].set_ylabel("Mensagens (acumulado)")
    eixos[0].legend(loc="upper left", bbox_to_anchor=(0.0, 0.9))
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    cabecalho(fig, "envio e recebimento em torno da queda (1ª execução)",
              "A distância horizontal entre as linhas é o atraso; o trecho plano da linha das recebidas é a interrupção")
    return fig


def pagina_tempos(dados):
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 3.7))
    pontos_por_execucao(a1, {c: [d["interrupcao_s"] for d in v] for c, v in dados.items()},
                        lambda v: f"{virgula(v, 2)} s", "Segundos", "Interrupção da entrega ao consumidor")
    pontos_por_execucao(a2, {c: [d["sem_confirmacao_s"] for d in v] for c, v in dados.items()},
                        lambda v: f"{virgula(v, 2)} s", "Segundos", "Maior intervalo sem confirmação ao produtor")
    topo = max(max(d["interrupcao_s"], d["sem_confirmacao_s"]) for v in dados.values() for d in v)
    for ax in (a1, a2):
        ax.set_ylim(0, topo * 1.2 + 0.05)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}".replace(".", ",")))
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    cabecalho(fig, "quanto tempo o fluxo ficou parado", "Traço = mediana das execuções; cada ponto = 1 execução")
    return fig


def pagina_mensagens(dados):
    fig, eixos = plt.subplots(1, 3, figsize=(12, 3.7))
    paineis = [("nao_entregues", "Não entregues (enviadas que nunca chegaram)", lambda v: False),
               ("perdidas", "Perdidas (confirmadas que nunca chegaram)", lambda v: v < 0),
               ("duplicadas", "Duplicadas (entregas repetidas)", lambda v: False)]
    topo = max(max(d[k] for v in dados.values() for d in v) for k, _, _ in paineis)
    for ax, (k, titulo, nsa) in zip(eixos, paineis):
        pontos_por_execucao(ax, {c: [d[k] for d in v] for c, v in dados.items()}, lambda v: f"{v:g}",
                            "Mensagens", titulo, nao_se_aplica=nsa, curto=True)
        ax.set_ylim(0, max(5, topo * 1.25))
        ax.yaxis.set_major_locator(MaxNLocator(integer=True, steps=[1, 2, 5, 10]))
    fig.tight_layout(rect=(0, 0, 1, 0.88))
    cabecalho(fig, "o que aconteceu com as mensagens", "De 12.000 enviadas por execução. "
              "Traço = mediana; cada ponto = 1 execução")
    return fig


def achar_pasta(indicada=None):
    """
    Pasta com os CSVs. Sem argumento, procura na pasta atual, na pasta do
    script e numa subpasta (ou pasta vizinha) chamada PASTA_PADRAO, de modo
    que o script funcione rodando de dentro ou de fora da pasta de resultados.
    """
    nome = f"resultados-{ARQUIVO}-falha-broker.csv"
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
                     + "\nPasse a pasta como argumento: py falha_broker_" + ARQUIVO + ".py <pasta_dos_resultados>")


def main():
    pasta = achar_pasta(sys.argv[1] if len(sys.argv) > 1 else None)
    saida = sys.argv[2] if len(sys.argv) > 2 else pasta
    print("Lendo de:", os.path.abspath(pasta))
    os.makedirs(saida, exist_ok=True)
    dados = carregar(pasta)
    paginas = [("latencia", pagina_latencia), ("zoom-queda", pagina_zoom), ("tempos", pagina_tempos),
               ("mensagens", pagina_mensagens)]
    pdf = os.path.join(saida, f"falha-broker-{ARQUIVO}.pdf")
    with PdfPages(pdf) as pp:
        for _, gerar in paginas:
            fig = gerar(dados)
            pp.savefig(fig)
            plt.close(fig)
    print("Gerado:", os.path.abspath(pdf))


if __name__ == "__main__":
    main()