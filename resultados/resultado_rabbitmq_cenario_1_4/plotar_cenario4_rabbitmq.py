#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cenário 4 — Múltiplos consumidores — gráficos do RABBITMQ.

Lê os CSVs gerados pelo orquestrador Cenario4MultiplosConsumidores:
    resultados-rabbitmq-cenario4.csv            (1 linha por execução e configuração)
    resultados-rabbitmq-cenario4-detalhado.csv  (1 linha por mensagem; usado no gráfico 5)

Gera um PDF com:
    Gráfico 1 — vazão por configuração, com a linha ideal N × V(1)
    Gráfico 2 — ganho de velocidade e eficiência
    Gráfico 2b — utilização dos consumidores (tempo ocupado ÷ tempo disponível)
    Gráfico 3 — divisão das mensagens entre os consumidores
    Gráfico 4 — tempo de espera (início do processamento − envio)
    Gráfico 5 — mensagens tratadas ao longo do tempo, por consumidor (1 execução)
    Tabela 1  — verificação (contagem, tempo de processamento medido, rebalanceamentos)
    Tabela 2  — resumo por configuração (também gravado em CSV)

Uso (dentro da pasta de resultados, sem argumentos):
    python plotar_cenario4_rabbitmq.py
ou:
    python plotar_cenario4_rabbitmq.py <pasta_dos_csvs> [saida.pdf]

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
TECNOLOGIA    = "RabbitMQ"
ARQ_AGREGADO  = "resultados-rabbitmq-cenario4.csv"
ARQ_DETALHADO = "resultados-rabbitmq-cenario4-detalhado.csv"
ARQ_RESUMO    = "cenario4-rabbitmq-resumo.csv"
SAIDA_PADRAO  = "cenario4-rabbitmq-boxplot.pdf"
COR_CAIXA     = "#D85A30"   # laranja
COR_PONTO     = "#1B6B2F"   # verde
NOME_PONTO    = "verde"
# ----------------------------------------------------------------------

CENARIO = "Cenário 4 — Múltiplos consumidores"
COR_IDEAL = "#555555"

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


def preparar(ag):
    """Completa colunas que só o Kafka tem e ordena as configurações."""
    if "num_particoes" not in ag.columns:
        ag["num_particoes"] = np.nan
    if "revogacoes" not in ag.columns:
        ag["revogacoes"] = 0
    ag["msgs_por_consumidor"] = ag["msgs_por_consumidor"].astype(str)
    # Utilização = tempo que os consumidores ativos passam processando ÷ tempo disponível.
    # Descontada a variação do tempo real de processamento (Thread.sleep(5) leva ~5,5 a 5,9 ms).
    ag["utilizacao_pct"] = (ag["vazao_msg_s"] * ag["tempo_proc_medio_ms"] / 1000
                            / ag["consumidores_ativos"] * 100)
    return ag


def ordem_configs(ag):
    """Principais (consumidores = partições, ou RabbitMQ) primeiro, por N; extras no fim."""
    cfg = ag.drop_duplicates("config")[["config", "num_consumidores", "num_particoes"]]
    extra = cfg["num_particoes"].notna() & (cfg["num_particoes"] != cfg["num_consumidores"])
    cfg = cfg.assign(extra=extra).sort_values(["extra", "num_consumidores", "num_particoes"])
    return list(cfg["config"])


def rotulo(ag, config):
    r = ag[ag["config"] == config].iloc[0]
    n = int(r["num_consumidores"])
    p = r["num_particoes"]
    if pd.notna(p) and int(p) != n:
        return f"{n}\n({int(p)} partições)"
    return str(n)


def nome_config(ag, config):
    r = ag[ag["config"] == config].iloc[0]
    n = int(r["num_consumidores"])
    txt = f"{n} consumidor" + ("" if n == 1 else "es")
    if eh_extra(ag, config):
        txt += f", {int(r['num_particoes'])} partições"
    return txt


def limite_particoes(ag, configs, ax, pos, fator):
    """Marca, nas configurações com menos partições que consumidores, o limite
    partições × fator (o máximo possível, já que os consumidores a mais ficam parados)."""
    marcou = False
    for x, c in zip(pos, configs):
        if eh_extra(ag, c):
            p = int(ag.loc[ag["config"] == c, "num_particoes"].iloc[0])
            ax.scatter([x], [p * fator], marker="_", s=700, color="#B8860B", linewidth=1.6, zorder=4,
                       linestyle=":", label=None if marcou else "limite: partições")
            marcou = True


def eh_extra(ag, config):
    r = ag[ag["config"] == config].iloc[0]
    return pd.notna(r["num_particoes"]) and int(r["num_particoes"]) != int(r["num_consumidores"])


def fmt(v, casas=2):
    return f"{v:.{casas}f}".replace(".", ",")


def fmt_int(v):
    return f"{v:,.0f}".replace(",", ".")


def v1_mediana(ag):
    base = ag[ag["num_consumidores"] == 1]["vazao_msg_s"]
    return base.median() if len(base) else np.nan


# ----------------------------------------------------------------------
# Auxiliares de desenho
# ----------------------------------------------------------------------
def titulo(ax, texto, sub):
    ax.set_title(texto, fontsize=11.5, pad=24)
    ax.text(0.5, 1.02, sub, transform=ax.transAxes, ha="center", fontsize=8.5, color="#555555")


def caixas_com_pontos(ax, series, rotulos, cor_caixa=COR_CAIXA, cor_ponto=COR_PONTO):
    """1 caixa por configuração; cada ponto = 1 execução."""
    pos = list(range(1, len(series) + 1))
    ax.boxplot(series, positions=pos, widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=cor_caixa, alpha=0.5, edgecolor=cor_caixa),
               whiskerprops=dict(color=cor_caixa), capprops=dict(color=cor_caixa))
    for i, s in zip(pos, series):
        ax.scatter(i + RNG.uniform(-0.08, 0.08, len(s)), s, s=26, color=cor_ponto,
                   edgecolor="white", linewidth=0.6, zorder=3)
    ax.set_xticks(pos)
    ax.set_xticklabels(rotulos)
    ax.grid(False, axis="x")
    return pos


def tabela(pdf, linhas, colunas, texto, nota=None, larguras=None):
    fig, ax = plt.subplots(figsize=(9.2, 1.9 + 0.40 * len(linhas) + (0.5 if nota else 0)))
    ax.axis("off")
    t = ax.table(cellText=linhas, colLabels=colunas, loc="center", cellLoc="center",
                 colWidths=larguras)
    t.auto_set_font_size(False)
    t.set_fontsize(8.6)
    t.scale(1, 1.7)
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
# Gráficos
# ----------------------------------------------------------------------
def grafico1_vazao(pdf, ag, configs):
    v1 = v1_mediana(ag)
    series = [ag.loc[ag["config"] == c, "vazao_msg_s"].values for c in configs]
    fig, ax = plt.subplots(figsize=(7.6, 5.6))
    pos = caixas_com_pontos(ax, series, [rotulo(ag, c) for c in configs])
    if np.isfinite(v1):
        ideal = [v1 * ag.loc[ag["config"] == c, "num_consumidores"].iloc[0] for c in configs]
        ax.scatter(pos, ideal, marker="_", s=700, color=COR_IDEAL, linewidth=1.6, zorder=4,
                   label="ideal: N × V(1)")
        limite_particoes(ag, configs, ax, pos, v1)
        ax.legend(frameon=False, fontsize=8.5, loc="upper left")
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Vazão de consumo (msg/s)")
    n = int(ag.groupby("config").size().median())
    titulo(ax, f"{TECNOLOGIA} — vazão por número de consumidores",
           f"1 caixa = {n} execuções; cada ponto {NOME_PONTO} = 1 execução. "
           "Vazão = mensagens ÷ (último fim de processamento − primeiro envio)")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico2_ganho(pdf, ag, configs):
    v1 = v1_mediana(ag)
    if not np.isfinite(v1):
        return
    ns = [int(ag.loc[ag["config"] == c, "num_consumidores"].iloc[0]) for c in configs]
    ganho = [ag.loc[ag["config"] == c, "vazao_msg_s"].values / v1 for c in configs]
    efic = [g / n * 100 for g, n in zip(ganho, ns)]
    rot = [rotulo(ag, c) for c in configs]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5.4))
    pos = caixas_com_pontos(ax1, ganho, rot)
    ax1.scatter(pos, ns, marker="_", s=700, color=COR_IDEAL, linewidth=1.6, zorder=4, label="ideal = N")
    limite_particoes(ag, configs, ax1, pos, 1)
    ax1.set_ylim(bottom=0)
    ax1.set_ylabel("Ganho de velocidade S(N) = V(N) ÷ V(1)")
    ax1.set_xlabel("Número de consumidores")
    ax1.legend(frameon=False, fontsize=8.5, loc="upper left")
    ax1.set_title("Ganho de velocidade", fontsize=10.5)

    caixas_com_pontos(ax2, efic, rot)
    ax2.axhline(100, color=COR_IDEAL, linestyle="--", linewidth=1.2, label="ideal = 100%")
    ax2.set_ylim(0, max(110, max(np.max(e) for e in efic) + 5))
    ax2.set_ylabel("Eficiência E(N) = S(N) ÷ N (%)")
    ax2.set_xlabel("Número de consumidores")
    ax2.legend(frameon=False, fontsize=8.5, loc="lower left")
    ax2.set_title("Eficiência", fontsize=10.5)

    fig.suptitle(f"{TECNOLOGIA} — ganho de velocidade e eficiência", fontsize=11.5, y=0.98)
    fig.text(0.5, 0.925, f"V(1) = mediana das execuções com 1 consumidor ({fmt(v1, 1)} msg/s); "
             f"cada ponto {NOME_PONTO} = 1 execução", ha="center", fontsize=8.5, color="#555555")
    fig.tight_layout(rect=[0, 0, 1, 0.9])
    pdf.savefig(fig)
    plt.close(fig)


def grafico2b_utilizacao(pdf, ag, configs):
    """Fatia do tempo em que os consumidores ativos estão de fato processando.
    Não depende de V(1), então não herda a incerteza da linha de base."""
    series = [ag.loc[ag["config"] == c, "utilizacao_pct"].values for c in configs]
    fig, ax = plt.subplots(figsize=(7.6, 5.6))
    caixas_com_pontos(ax, series, [rotulo(ag, c) for c in configs])
    ax.axhline(100, color=COR_IDEAL, linestyle="--", linewidth=1.2, label="ideal = 100%")
    topo = max(np.max(s) for s in series)
    ax.set_ylim(max(0, min(np.min(s) for s in series) - 10), max(105, topo + 3))
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Utilização = vazão × tempo de processamento ÷ consumidores ativos (%)")
    ax.legend(frameon=False, fontsize=8.5, loc="lower left")
    titulo(ax, f"{TECNOLOGIA} — utilização dos consumidores",
           f"Cada ponto {NOME_PONTO} = 1 execução. 100% = nenhum consumidor ativo fica parado; "
           "o que falta é custo de partida, espera por mensagem e coordenação")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def parcelas_relativas(ag, config):
    """Parcela de cada consumidor ÷ parcela ideal (mensagens ÷ N), em %."""
    linhas = ag[ag["config"] == config]
    vals = []
    for _, r in linhas.iterrows():
        p = [int(x) for x in str(r["msgs_por_consumidor"]).split("|")]
        ideal = r["mensagens"] / len(p)
        vals += [v / ideal * 100 for v in p]
    return np.array(vals)


def grafico3_divisao(pdf, ag, configs):
    series = [parcelas_relativas(ag, c) for c in configs]
    fig, ax = plt.subplots(figsize=(7.6, 5.6))
    pos = list(range(1, len(series) + 1))
    ax.boxplot(series, positions=pos, widths=0.5, patch_artist=True, showfliers=False,
               medianprops=dict(color="black", linewidth=1.6),
               boxprops=dict(facecolor=COR_CAIXA, alpha=0.35, edgecolor=COR_CAIXA),
               whiskerprops=dict(color=COR_CAIXA), capprops=dict(color=COR_CAIXA))
    for i, s in zip(pos, series):
        ax.scatter(i + RNG.uniform(-0.15, 0.15, len(s)), s, s=14, color=COR_PONTO,
                   edgecolor="none", alpha=0.7, zorder=3)
    ax.axhline(100, color=COR_IDEAL, linestyle="--", linewidth=1.2, label="divisão igual = 100%")
    ax.set_xticks(pos)
    ax.set_xticklabels([rotulo(ag, c) for c in configs])
    ax.grid(False, axis="x")
    topo = max([np.max(x) for x in series if len(x)] + [100])
    ax.set_ylim(-5, max(115, topo + 10))
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Parcela do consumidor ÷ (mensagens ÷ N)  (%)")
    ax.legend(frameon=False, fontsize=8.5, loc="upper left")
    titulo(ax, f"{TECNOLOGIA} — divisão das mensagens entre os consumidores",
           f"Cada ponto {NOME_PONTO} = 1 consumidor em 1 execução. 100% = recebeu exatamente a sua parte")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico4_espera(pdf, ag, configs):
    series = [ag.loc[ag["config"] == c, "espera_mediana_ms"].values / 1000 for c in configs]
    fig, ax = plt.subplots(figsize=(7.6, 5.6))
    caixas_com_pontos(ax, series, [rotulo(ag, c) for c in configs])
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Número de consumidores")
    ax.set_ylabel("Tempo de espera mediano da execução (s)")
    titulo(ax, f"{TECNOLOGIA} — tempo de espera por número de consumidores",
           f"Espera = início do processamento − envio; cada ponto {NOME_PONTO} = a mediana de 1 execução")
    fig.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)


def grafico5_ao_longo(pdf, det, ag, configs):
    """Mensagens acumuladas por consumidor ao longo do tempo, numa execução de cada configuração."""
    if det is None or det.empty:
        return
    exec_ref = int(det["execucao"].min())
    k = len(configs)
    cols = min(k, 3)
    lins = int(np.ceil(k / cols))
    fig, axes = plt.subplots(lins, cols, figsize=(11, 3.6 * lins), squeeze=False)
    cmap = plt.get_cmap("tab10")
    for idx, c in enumerate(configs):
        ax = axes[idx // cols][idx % cols]
        d = det[(det["execucao"] == exec_ref) & (det["config"] == c)]
        n = int(ag.loc[ag["config"] == c, "num_consumidores"].iloc[0])
        for cid in range(1, n + 1):
            dc = d[d["consumidor"] == cid].sort_values("fim_ms")
            if dc.empty:
                ax.plot([0], [0], color=cmap((cid - 1) % 10), linewidth=1.3, label=f"C{cid} (0 msgs)")
                continue
            ax.plot(dc["fim_ms"].values / 1000, np.arange(1, len(dc) + 1),
                    color=cmap((cid - 1) % 10), linewidth=1.3, label=f"C{cid}")
        ax.set_title(nome_config(ag, c), fontsize=10)
        ax.set_xlabel("Tempo desde o primeiro envio (s)")
        ax.set_ylabel("Mensagens tratadas (acumulado)")
        ax.legend(frameon=False, fontsize=7, ncol=2, loc="upper left")
    for idx in range(k, lins * cols):
        axes[idx // cols][idx % cols].axis("off")
    fig.suptitle(f"{TECNOLOGIA} — mensagens tratadas ao longo do tempo, por consumidor "
                 f"(execução {exec_ref})", fontsize=11.5)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    pdf.savefig(fig)
    plt.close(fig)


# ----------------------------------------------------------------------
# Tabelas e resumo
# ----------------------------------------------------------------------
def tabela1_verificacao(pdf, ag, configs):
    tem_particoes = ag["num_particoes"].notna().any()
    linhas = []
    for c in configs:
        a = ag[ag["config"] == c]
        lin = [rotulo(ag, c).replace("\n", " "), str(len(a)),
               fmt_int(a["mensagens"].iloc[0]),
               f"{fmt(a['tempo_proc_medio_ms'].min())} a {fmt(a['tempo_proc_medio_ms'].max())}",
               f"{int(a['consumidores_ativos'].min())} a {int(a['consumidores_ativos'].max())}"]
        if tem_particoes:
            lin.append(str(int(a["revogacoes"].fillna(0).sum())))
        lin.append(fmt_int(a["vazao_produtor_msg_s"].median()))
        linhas.append(lin)
    colunas = ["Consumidores", "Execuções", "Mensagens\npor execução", "Tempo de processamento\nmedido (ms)",
               "Consumidores\ncom mensagens"]
    if tem_particoes:
        colunas.append("Partições\nrevogadas")
    colunas.append("Vazão do produtor\n(mediana, msg/s)")
    nota = ("A contagem e a ausência de duplicatas já são conferidas pelo orquestrador (a campanha para se falhar).\n"
            "O tempo de processamento medido fica um pouco acima dos 5 ms do Thread.sleep (sobrecarga do agendador do SO);\n"
            "o que importa é ser parecido entre as configurações"
            + ("; partições revogadas deve ser 0." if tem_particoes else "."))
    tabela(pdf, linhas, colunas, f"{TECNOLOGIA} — verificação", nota=nota)


def resumo(ag, configs):
    v1 = v1_mediana(ag)
    linhas = []
    for c in configs:
        a = ag[ag["config"] == c]
        n = int(a["num_consumidores"].iloc[0])
        vm = a["vazao_msg_s"].median()
        p = a["num_particoes"].iloc[0]
        linhas.append({
            "tecnologia": TECNOLOGIA, "config": c, "num_consumidores": n,
            "num_particoes": None if pd.isna(p) else int(p),
            "execucoes": len(a),
            "vazao_mediana_msg_s": vm, "vazao_min_msg_s": a["vazao_msg_s"].min(),
            "vazao_max_msg_s": a["vazao_msg_s"].max(),
            "ganho": vm / v1 if np.isfinite(v1) else np.nan,
            "eficiencia_pct": vm / v1 / n * 100 if np.isfinite(v1) else np.nan,
            "utilizacao_pct": a["utilizacao_pct"].median(),
            "espera_mediana_s": a["espera_mediana_ms"].median() / 1000,
            "tempo_proc_medio_ms": a["tempo_proc_medio_ms"].median(),
            "menor_parcela": int(a["menor_parcela"].min()), "maior_parcela": int(a["maior_parcela"].max()),
            "consumidores_ativos": int(a["consumidores_ativos"].median()),
        })
    return pd.DataFrame(linhas)


def tabela2_resumo(pdf, res, ag):
    linhas = []
    for _, r in res.iterrows():
        linhas.append([rotulo(ag, r["config"]).replace("\n", " "),
                       fmt(r["vazao_mediana_msg_s"], 1),
                       f"{fmt(r['vazao_min_msg_s'], 1)} a {fmt(r['vazao_max_msg_s'], 1)}",
                       fmt(r["ganho"]), f"{fmt(r['eficiencia_pct'], 1)}%",
                       f"{fmt(r['utilizacao_pct'], 1)}%", fmt(r["espera_mediana_s"]),
                       f"{fmt_int(r['menor_parcela'])} a {fmt_int(r['maior_parcela'])}"])
    tabela(pdf, linhas,
           ["Consumidores", "Vazão\n(mediana, msg/s)", "Vazão\n(menor a maior)", "Ganho\nS(N)",
            "Eficiência\nE(N)", "Utilização\nconsumidores", "Espera mediana\n(s)", "Mensagens por\nconsumidor"],
           f"{TECNOLOGIA} — {CENARIO} — resumo",
           nota="'Mediana' = a mediana entre as execuções. Ganho e eficiência calculados sobre a mediana de 1 consumidor.\n"
                "Utilização = vazão × tempo de processamento ÷ consumidores ativos (não depende de V(1)).\n"
                "'Mensagens por consumidor' = menor e maior parcela de um consumidor em todas as execuções.")


def pagina_rosto(pdf):
    fig = plt.figure(figsize=(8.27, 11.69))
    fig.text(0.5, 0.92, f"{CENARIO} ({TECNOLOGIA})", ha="center", fontsize=17, weight="bold")
    fig.text(0.5, 0.885, "1 produtor, 1 a 6 consumidores, 5 ms de processamento por mensagem",
             ha="center", fontsize=11, color="#555555")
    linhas = ["Arquivos:"]
    for nome in (ARQ_AGREGADO, ARQ_DETALHADO):
        linhas.append(f"  [{'OK' if os.path.isfile(caminho(nome)) else '--'}] {nome}")
    fig.text(0.1, 0.83, "\n".join(linhas), ha="left", va="top", fontsize=8.5, family="monospace")
    nota = (
        "Como ler:\n\n"
        "- Gráfico 1: vazão de consumo de cada execução. O traço cinza é o ideal,\n"
        "  N vezes a vazão com 1 consumidor.\n\n"
        "- Gráfico 2: ganho de velocidade S(N) = V(N) ÷ V(1) e eficiência S(N) ÷ N.\n"
        "  Eficiência de 100% = cada consumidor a mais rende um consumidor inteiro.\n\n"
        "- Gráfico 2b: utilização = fatia do tempo em que os consumidores ativos\n"
        "  estão processando. Não usa V(1), então serve de conferência da eficiência.\n\n"
        "- Gráfico 3: quanto cada consumidor recebeu, em relação à sua parte\n"
        "  (mensagens ÷ N). 100% = divisão exatamente igual.\n\n"
        "- Gráfico 4: tempo que a mensagem espera até começar a ser processada.\n"
        "  Como tudo é enviado no início, é sobretudo o tempo de fila.\n\n"
        "- Gráfico 5: ritmo de cada consumidor numa execução de exemplo.\n\n"
        "- As configurações rodam intercaladas (ordem rotativa a cada execução) e a\n"
        "  rodada de aquecimento é descartada."
    )
    fig.text(0.1, 0.74, nota, ha="left", va="top", fontsize=9.5, color="#333333")
    pdf.savefig(fig)
    plt.close(fig)


def main():
    saida = SAIDA if os.path.isabs(SAIDA) else os.path.join(PASTA, SAIDA)
    print(f"Lendo CSVs de: {os.path.abspath(PASTA)}")
    ag = ler_csv(ARQ_AGREGADO)
    det = ler_csv(ARQ_DETALHADO)

    with PdfPages(saida) as pdf:
        pagina_rosto(pdf)
        if ag is None or ag.empty:
            aviso_pagina(pdf, "Nenhum CSV encontrado",
                         f"Rode o script dentro da pasta de resultados ou passe a pasta como argumento.\n"
                         f"Pasta lida: {os.path.abspath(PASTA)}")
        else:
            ag = preparar(ag)
            configs = ordem_configs(ag)
            grafico1_vazao(pdf, ag, configs)
            grafico2_ganho(pdf, ag, configs)
            grafico2b_utilizacao(pdf, ag, configs)
            grafico3_divisao(pdf, ag, configs)
            grafico4_espera(pdf, ag, configs)
            grafico5_ao_longo(pdf, det, ag, configs)
            tabela1_verificacao(pdf, ag, configs)
            res = resumo(ag, configs)
            tabela2_resumo(pdf, res, ag)
            res.to_csv(caminho(ARQ_RESUMO), index=False, float_format="%.4f")
            print(f"Resumo gravado: {caminho(ARQ_RESUMO)}")
    print(f"PDF gerado: {saida}")


if __name__ == "__main__":
    main()