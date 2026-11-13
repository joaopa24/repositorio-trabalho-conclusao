"""
Avançado — Cenário 3: Falha de broker — gráficos comparativos Kafka × RabbitMQ.

Uso:
    python falha_broker_comparativo.py [pasta_kafka] [pasta_rabbitmq] [pasta_saida]

    Sem argumentos, procura resultado_cenario_3_3 e resultado_rabbitmq_cenario_3_3
    na pasta atual, na pasta do script e nas pastas acima delas.

    <pasta_kafka>    contém resultados-kafka-falha-broker.csv e ...-serie.csv
    <pasta_rabbitmq> contém resultados-rabbitmq-falha-broker.csv e ...-serie.csv
    [pasta_saida]    padrão: a pasta em que o script está

Gera:
    falha-broker-comparativo.pdf, com 3 páginas:
        1  latência de cada mensagem pelo instante de envio, uma execução por
           configuração (a de parada mediana, a mesma da versão anterior)
        2  interrupção da entrega por configuração, em três medidas
           (3 execuções + mediana)
        3  mensagens enviadas que nunca chegaram, por configuração
    falha-broker-comparativo-resumo.csv  medianas e faixas por configuração

Interrupção da entrega: calculada da série temporal, e não da coluna
maior_pausa_entrega_ms, em três medidas separadas, iguais nas duas tecnologias.
Pausa = lacuna entre chegadas consecutivas ao consumidor (ordenadas pela chegada).
    maior pausa         a maior lacuna na janela de JANELA_ANTES s antes a
                        JANELA_DEPOIS s depois da queda
    pausas acima de 1 s soma das lacunas maiores que LIMITE_PAUSA_S na mesma
                        janela; quando a parada vem em dois pedaços (como em
                        algumas execuções do Kafka), a maior pausa sozinha a
                        subestima. Sem lacunas acima do limite, vale 0.
    retomada            da queda até o início do primeiro trecho de
                        RETOMADA_MSGS mensagens seguidas (1 s a 200 msg/s) com
                        latência até o percentil 99 da latência antes da queda,
                        sem os INSTAVEL_S s iniciais.
O script também imprime o maior intervalo antes da queda e depois da janela, para
conferir que as pausas contadas vêm da falha.

A coluna eleicao_ms não é usada: no RabbitMQ ela reflete o atraso com que a
management API mostra o novo líder, e não o tempo da eleição.
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
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

# ---------------------------------------------------------------- aparência
COR = {"Kafka": "#2a78d6", "RabbitMQ": "#eb6834"}   # categórica, validada (CVD ΔE 24,7)
CRITICO = "#d03b3b"                                 # status: mensagens perdidas
TEXTO, TEXTO_2 = "#0b0b0b", "#52514e"
GRADE, EIXO = "#e1e0d9", "#c3c2b7"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 9.5, "axes.labelsize": 9,
    "axes.edgecolor": EIXO, "axes.labelcolor": TEXTO_2, "axes.titlecolor": TEXTO,
    "xtick.color": TEXTO_2, "ytick.color": TEXTO_2, "xtick.labelsize": 8, "ytick.labelsize": 8,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRADE,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.facecolor": "white", "axes.facecolor": "white",
    "savefig.dpi": 300, "pdf.fonttype": 42,
})

# ordem e rótulos das configurações (pareadas pela coluna "garantia")
CONFIGS = [
    ("Kafka", "acks-all", "acks=all"),
    ("RabbitMQ", "confirms", "confirms"),
    ("Kafka", "acks-1", "acks=1"),
    ("Kafka", "acks-0", "acks=0"),
    ("RabbitMQ", "sem-confirms", "sem confirms"),
]
GARANTIA = {"acks-all": "Confirmação após as réplicas", "confirms": "Confirmação após as réplicas",
            "acks-1": "Confirmação só do líder", "acks-0": "Sem confirmação", "sem-confirms": "Sem confirmação"}

QUEDA_S, VOLTA_S, MSGS = 20.0, 40.0, 12_000

# medidas da interrupção (ver o cabeçalho)
JANELA_ANTES, JANELA_DEPOIS = 0.5, 15.0   # s, em torno da queda registrada (t_queda_s)
LIMITE_PAUSA_S = 1.0                      # 200 × o intervalo normal de 5 ms
RETOMADA_MSGS = 200                       # 1 s de fluxo a 200 msg/s
INSTAVEL_S = 5.0                          # início da execução, fora da referência


def virgula(v, casas=1):
    return f"{v:.{casas}f}".replace(".", ",")


def milhar(v):
    return f"{int(round(v)):,}".replace(",", ".")


# ---------------------------------------------------------------- leitura
def ler_resumo(caminho):
    with open(caminho, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def ler_serie(caminho):
    """{(configuracao, execucao): [(sequencia, envio_s, chegada_s, latencia_ms)]}"""
    s = defaultdict(list)
    with open(caminho, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            s[(r["configuracao"], int(r["execucao"]))].append(
                (int(r["sequencia"]), float(r["envio_s"]), float(r["chegada_s"]), float(r["latencia_ms"])))
    return s


def percentil(valores, p):
    """Percentil p (0–100) com interpolação linear, como o numpy."""
    v = sorted(valores)
    k = (len(v) - 1) * p / 100
    i = int(k)
    return v[i] if i + 1 >= len(v) else v[i] + (v[i + 1] - v[i]) * (k - i)


def medidas_interrupcao(linhas, queda):
    """Maior pausa, soma das pausas > LIMITE_PAUSA_S e retomada, em segundos.
    Também devolve o maior intervalo antes da queda e depois da janela (conferência)."""
    vistas, chegadas = set(), []
    for seq, _, ch, lat in sorted(linhas, key=lambda x: x[2]):
        if seq not in vistas:              # só a primeira chegada de cada mensagem
            vistas.add(seq)
            chegadas.append((ch, lat))
    ch = [c for c, _ in chegadas]
    lac = [(a, b - a) for a, b in zip(ch, ch[1:])]          # (início, duração)
    ini_j, fim_j = queda - JANELA_ANTES, queda + JANELA_DEPOIS
    na_janela = [g for a, g in lac if ini_j <= a <= fim_j]
    antes = [g for a, g in lac if a + g < queda and a > INSTAVEL_S]
    depois = [g for a, g in lac if a > fim_j]

    ref = percentil([lat for c, lat in chegadas if INSTAVEL_S < c < queda], 99)
    normal = [lat <= ref for _, lat in chegadas]
    retomada, seguidas = float("nan"), 0
    for i in range(len(chegadas) - 1, -1, -1):              # de trás para a frente
        seguidas = seguidas + 1 if normal[i] else 0
        if seguidas >= RETOMADA_MSGS and ch[i] > queda:
            retomada = ch[i] - queda                         # fica com o primeiro trecho
    return {
        "maior_pausa_s": max(na_janela),
        "pausas_acima_s": sum(g for g in na_janela if g > LIMITE_PAUSA_S),
        "retomada_s": retomada,
        "maior_antes_s": max(antes),
        "maior_depois_s": max(depois) if depois else 0.0,
    }


def carregar(pasta_k, pasta_r):
    dados = {}
    for tec, pasta, nome in [("Kafka", pasta_k, "kafka"), ("RabbitMQ", pasta_r, "rabbitmq")]:
        resumo = ler_resumo(os.path.join(pasta, f"resultados-{nome}-falha-broker.csv"))
        serie = ler_serie(os.path.join(pasta, f"resultados-{nome}-falha-broker-serie.csv"))
        for r in resumo:
            chave = (tec, r["configuracao"])
            e = int(r["execucao"])
            linhas = serie[(r["configuracao"], e)]
            dados.setdefault(chave, []).append({
                "execucao": e,
                **medidas_interrupcao(linhas, float(r["t_queda_s"])),
                "nao_entregues": int(r["nao_entregues"]),
                "perdidas": int(r["perdidas"]),
                "duplicadas": int(r["duplicadas"]),
                "redeliveries": int(r["redeliveries"]),
                "sem_confirmacao_ms": float(r["maior_intervalo_sem_confirmacao_ms"]),
                "serie": linhas,
            })
    return dados


# ---------------------------------------------------------------- figuras
_PDF = None   # PdfPages aberto em main(); cada figura vira uma página


def salvar(fig, pasta, nome):
    _PDF.savefig(fig, bbox_inches="tight")
    plt.close(fig)


def fig_latencia(dados, pasta):
    """Latência de cada mensagem pelo instante de envio (execução de parada mediana)."""
    ordem = [("Kafka", "acks-all", "acks=all"), ("Kafka", "acks-1", "acks=1"), ("Kafka", "acks-0", "acks=0"),
             ("RabbitMQ", "confirms", "confirms"), ("RabbitMQ", "sem-confirms", "sem confirms")]
    fig, eixos = plt.subplots(len(ordem), 1, figsize=(6.3, 8.2), sharex=True, sharey=True)
    for ax, (tec, cfg, rot) in zip(eixos, ordem):
        # mesma execução da versão anterior: a de parada mediana (soma das pausas
        # acima do limite ou, sem nenhuma, a maior pausa); serve só para escolher
        execs = sorted(dados[(tec, cfg)], key=lambda d: d["pausas_acima_s"] or d["maior_pausa_s"])
        d = execs[len(execs) // 2]
        linhas = sorted(d["serie"], key=lambda x: x[1])
        ax.plot([x[1] for x in linhas], [max(x[3], 0.5) for x in linhas], color=COR[tec], lw=0.9)
        ax.set_yscale("log")
        ax.set_ylim(1, 80_000)
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=6))
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: {1: "1 ms", 10: "10 ms", 100: "100 ms",
                                                                 1000: "1 s", 10000: "10 s"}.get(int(round(v)), "")))
        for t in (QUEDA_S, VOLTA_S):
            ax.axvline(t, color=TEXTO_2, lw=0.8, ls=(0, (3, 3)))
        # mensagens não entregues: marcadas no instante de envio
        recebidas = {x[0] for x in linhas}
        faltam = [s for s in range(1, MSGS + 1) if s not in recebidas]
        if faltam:
            ini, fim = (faltam[0] - 1) / 200, (faltam[-1] - 1) / 200
            ax.axvspan(ini, max(fim, ini + 0.15), color=CRITICO, alpha=0.35, lw=0)
        extra = (f"\n{len(faltam)} mensagens não entregues, enviadas entre {virgula(ini, 2)} e {virgula(fim, 2)} s "
                 f"(faixa vermelha)") if faltam else ""
        ax.set_title(f"{tec} — {rot}  ·  maior pausa de {virgula(d['maior_pausa_s'], 2)} s, "
                     f"retomada em {virgula(d['retomada_s'], 2)} s (execução {d['execucao']}){extra}",
                     loc="left", color=TEXTO)
        ax.grid(axis="x", visible=False)
    eixos[0].text(QUEDA_S + 0.4, 35_000, "queda do líder", va="center", ha="left", fontsize=7.5, color=TEXTO_2)
    eixos[0].text(VOLTA_S + 0.4, 35_000, "volta do nó", va="center", ha="left", fontsize=7.5, color=TEXTO_2)
    eixos[-1].set_xlabel("Instante de envio da mensagem (s)")
    eixos[-1].set_xlim(0, 61)
    fig.supylabel("Latência de ponta a ponta (escala log)", color=TEXTO_2, fontsize=9)
    fig.tight_layout(h_pad=1.2)
    salvar(fig, pasta, "falha-broker-latencia")


def _categorias(ax, valores_por_cfg, fmt, ylabel, titulo):
    xs = range(len(CONFIGS))
    for x, (tec, cfg, rot) in zip(xs, CONFIGS):
        vals = valores_por_cfg[(tec, cfg)]
        m = st.median(vals)
        ax.plot([x - 0.28, x + 0.28], [m, m], color=COR[tec], lw=2.2, solid_capstyle="round", zorder=3)
        ax.scatter([x] * len(vals), vals, s=34, color=COR[tec], edgecolor="white", linewidth=1.2, zorder=4)
        ax.annotate(fmt(m), xy=(x + 0.32, m), va="center", fontsize=8, color=TEXTO)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([f"{rot}\n{tec}" for tec, _, rot in CONFIGS])
    ax.set_xlim(-0.6, len(CONFIGS) - 0.2)
    ax.set_ylabel(ylabel)
    ax.set_title(titulo, loc="left", color=TEXTO, pad=6)
    ax.grid(axis="x", visible=False)
    # separa os níveis de garantia
    for xsep in (1.5, 2.5):
        ax.axvline(xsep, color=GRADE, lw=0.8)
    # nível de garantia de cada grupo, dentro do gráfico, no alto
    for x0, x1, txt in [(-0.5, 1.5, "Confirmação após as réplicas"), (1.5, 2.5, "Só do líder"),
                        (2.5, 4.5, "Sem confirmação")]:
        ax.text((x0 + x1) / 2, 0.985, txt, transform=ax.get_xaxis_transform(), ha="center", va="top",
                fontsize=7.5, color=TEXTO_2)


def fig_interrupcao(dados, pasta):
    medidas = [("maior_pausa_s", "Maior pausa entre chegadas"),
               ("pausas_acima_s", f"Soma das pausas acima de {virgula(LIMITE_PAUSA_S, 0)} s"),
               ("retomada_s", f"Retomada ({RETOMADA_MSGS} mensagens seguidas com latência normal)")]
    fig, eixos = plt.subplots(len(medidas), 1, figsize=(6.3, 8.6), sharex=True)
    for i, (ax, (chave, titulo)) in enumerate(zip(eixos, medidas)):
        vals = {k: [d[chave] for d in v] for k, v in dados.items()}
        ax.set_ylim(0, 12)
        _categorias(ax, vals, lambda v: f"{virgula(v, 2)} s", "Tempo (s)",
                    f"{chr(97 + i)}) {titulo} (3 execuções; traço = mediana)")
    fig.tight_layout(h_pad=1.4)
    salvar(fig, pasta, "falha-broker-interrupcao")


def fig_nao_entregues(dados, pasta):
    fig, ax = plt.subplots(figsize=(6.3, 3.2))
    vals = {k: [d["nao_entregues"] for d in v] for k, v in dados.items()}
    ax.set_ylim(0, 66)
    _categorias(ax, vals, lambda v: milhar(v), "Mensagens não entregues",
                "Mensagens enviadas que nunca chegaram ao consumidor (de 12.000)")
    fig.tight_layout()
    salvar(fig, pasta, "falha-broker-nao-entregues")


def resumo_csv(dados, pasta):
    caminho = os.path.join(pasta, "falha-broker-comparativo-resumo.csv")
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        medidas = ["maior_pausa_s", "pausas_acima_s", "retomada_s"]
        cab = ["tecnologia", "configuracao", "garantia", "execucoes"]
        for m in medidas:
            base = m[:-2]
            cab += [f"{base}_mediana_s", f"{base}_min_s", f"{base}_max_s"]
        cab += ["maior_intervalo_antes_da_queda_s", "maior_intervalo_depois_da_janela_s",
                "maior_intervalo_sem_confirmacao_mediana_ms", "perdidas_mediana", "nao_entregues_mediana",
                "nao_entregues_min", "nao_entregues_max", "duplicadas_total", "redeliveries_total"]
        w.writerow(cab)
        for tec, cfg, _ in CONFIGS:
            v = dados[(tec, cfg)]
            ne = [d["nao_entregues"] for d in v]
            sc = [d["sem_confirmacao_ms"] for d in v]
            pe = [d["perdidas"] for d in v]
            linha = [tec, cfg, GARANTIA[cfg], len(v)]
            for m in medidas:
                x = [d[m] for d in v]
                linha += [f"{st.median(x):.3f}", f"{min(x):.3f}", f"{max(x):.3f}"]
            linha += [f"{max(d['maior_antes_s'] for d in v):.3f}", f"{max(d['maior_depois_s'] for d in v):.3f}",
                      f"{st.median(sc):.1f}" if min(sc) >= 0 else "-1", f"{st.median(pe):g}" if min(pe) >= 0 else "-1",
                      f"{st.median(ne):g}", min(ne), max(ne), sum(d["duplicadas"] for d in v),
                      sum(d["redeliveries"] for d in v)]
            w.writerow(linha)
    return caminho


def imprimir(dados):
    """Mediana e faixa de cada medida, no formato do texto."""
    fx = lambda x: f"{virgula(st.median(x), 2)} ({virgula(min(x), 2)} a {virgula(max(x), 2)})"
    print(f"\n{'Configuração':26s} {'Maior pausa (s)':22s} {'Pausas > 1 s (s)':22s} {'Retomada (s)':22s}")
    for tec, cfg, rot in CONFIGS:
        v = dados[(tec, cfg)]
        print(f"{tec + ' – ' + rot:26s} " + " ".join(f"{fx([d[m] for d in v]):22s}"
              for m in ("maior_pausa_s", "pausas_acima_s", "retomada_s")))
    todas = [d for v in dados.values() for d in v]
    print(f"\nMaior intervalo antes da queda (sem os {virgula(INSTAVEL_S, 0)} s iniciais): "
          f"{virgula(max(d['maior_antes_s'] for d in todas), 3)} s; "
          f"depois da janela: {virgula(max(d['maior_depois_s'] for d in todas), 3)} s")


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
                     + "\nPasse as pastas como argumentos: py falha_broker_comparativo.py <pasta_kafka> <pasta_rabbitmq>")


def main():
    pasta_k = achar_pasta(sys.argv[1] if len(sys.argv) > 1 else None, "resultados-kafka-falha-broker.csv",
                          "resultado_cenario_3_3")
    pasta_r = achar_pasta(sys.argv[2] if len(sys.argv) > 2 else None, "resultados-rabbitmq-falha-broker.csv",
                          "resultado_rabbitmq_cenario_3_3")
    print("Kafka:", os.path.abspath(pasta_k), "| RabbitMQ:", os.path.abspath(pasta_r))
    # sem pasta de saída indicada, grava na pasta em que o script está
    saida = sys.argv[3] if len(sys.argv) > 3 else os.path.dirname(os.path.abspath(__file__))
    os.makedirs(saida, exist_ok=True)
    global _PDF
    dados = carregar(pasta_k, pasta_r)
    pdf = os.path.join(saida, "falha-broker-comparativo.pdf")
    with PdfPages(pdf) as _PDF:
        fig_latencia(dados, saida)
        fig_interrupcao(dados, saida)
        fig_nao_entregues(dados, saida)
    print("Gerado:", os.path.abspath(pdf))
    print("Resumo:", resumo_csv(dados, saida))
    imprimir(dados)


if __name__ == "__main__":
    main()