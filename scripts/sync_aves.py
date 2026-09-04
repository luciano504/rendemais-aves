# -*- coding: utf-8 -*-
"""
Sync Aves Resfriadas — VR -> Supabase
Roda todo dia às 06h de Recife (GitHub Actions, repo público).

Passos:
 1. Lê o cadastro aves_itens do Supabase
 2. Busca no VR as vendas e entradas (NF) desde o último dia sincronizado
 3. Grava aves_diario e recalcula o estoque virtual desde o último minibalanço
 4. Recalcula os fatores do DDV (dia da semana + semana do mês) e o VMD base
 5. Gera a sugestão do sistema para o pedido de hoje em aves_sugestoes

Secrets necessários: VR_URL, SUPABASE_URL, SUPABASE_KEY (service role)
"""
import os, sys, io, json, math, datetime as dt
from collections import defaultdict

import requests
import pandas as pd

VR_URL       = os.environ["VR_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

LOJAS = {1: "L01", 2: "L02", 3: "L03", 5: "L05", 8: "L08", 9: "L09"}
FORNECEDORES_VR = [12300, 622, 12580, 12688]   # BLJ (Natto) + Mauricea (3 CNPJs)

# vendas destes códigos alimentam o estoque de outro item (galeto)
VENDA_MAP = {3420: (371, 0.5), 3421: (371, 1.0), 3422: (371, 0.5)}

HIST_DIAS   = 90     # bootstrap de histórico na primeira execução
JANELA_DOW  = 56     # 8 semanas completas p/ fator dia-da-semana
JANELA_SEM  = 84     # p/ fator semana-do-mês
JANELA_VMD  = 28     # p/ VMD base
COBERTURA   = 2.5    # dias de venda garantidos após a entrega

HOJE = dt.date.today()   # runner em UTC; às 06h de Recife (09h UTC) a data bate
RESUMO = []

# ---------------------------------------------------------------- VR
def query_vr(sql):
    r = requests.post(VR_URL, data={"sql_query": sql, "export_type": "csv"}, timeout=600)
    r.raise_for_status()
    txt = r.text
    if "<html" in txt[:400].lower() or "Fatal error" in txt[:800]:
        if "streamCsv" in txt:          # resultado vazio quebra o endpoint
            return pd.DataFrame()
        raise RuntimeError("VR devolveu HTML/erro: " + txt[:300])
    if not txt.strip():
        return pd.DataFrame()
    return pd.read_csv(io.StringIO(txt))

# ---------------------------------------------------------------- Supabase REST
def sb_headers(extra=None):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json"}
    if extra: h.update(extra)
    return h

def sb_get(table, params):
    out, offset = [], 0
    while True:
        p = dict(params); p["offset"] = offset; p["limit"] = 1000
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}", headers=sb_headers(), params=p, timeout=120)
        if r.status_code >= 300:
            raise RuntimeError(f"GET {table}: {r.status_code} {r.text[:300]}")
        rows = r.json(); out += rows
        if len(rows) < 1000: return out
        offset += 1000

def sb_patch(table, filtro, campos):
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/{table}?{filtro}",
                       headers=sb_headers(), data=json.dumps(campos), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"Patch {table}: {r.status_code} {r.text[:200]}")

def sb_upsert(table, rows, on_conflict):
    if not rows: return
    for i in range(0, len(rows), 500):
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}",
            headers=sb_headers({"Prefer": "resolution=merge-duplicates"}),
            data=json.dumps(rows[i:i+500]), timeout=120)
        if r.status_code >= 300:
            raise RuntimeError(f"Upsert {table}: {r.status_code} {r.text[:300]}")

# ---------------------------------------------------------------- 1. cadastro
itens = sb_get("aves_itens", {"select": "*", "ativo": "eq.true"})
if not itens:
    sys.exit("aves_itens vazio — rodar setup_aves.sql primeiro")
POR_ID   = {it["id_produto"]: it for it in itens}
IDS_APP  = list(POR_ID.keys())
IDS_VENDA = IDS_APP + list(VENDA_MAP.keys())
RESUMO.append(f"{len(itens)} itens ativos no cadastro")

# ---------------------------------------------------------------- 2. janela a sincronizar
ult = sb_get("aves_diario", {"select": "data", "order": "data.desc", "limit": "1"})
if ult:
    ini = dt.date.fromisoformat(ult[0]["data"]) - dt.timedelta(days=2)  # refaz 2 dias (NF atrasada)
else:
    ini = HOJE - dt.timedelta(days=HIST_DIAS)
fim = HOJE - dt.timedelta(days=1)   # até ontem (dia fechado)
RESUMO.append(f"Sincronizando {ini} a {fim}")

def chunks(d0, d1, passo=28):
    a = d0
    while a <= d1:
        b = min(a + dt.timedelta(days=passo - 1), d1)
        yield a, b
        a = b + dt.timedelta(days=1)

ids_sql   = ",".join(map(str, IDS_VENDA))
ids_app_sql = ",".join(map(str, IDS_APP))
lojas_sql = ",".join(map(str, LOJAS.keys()))

vendas, entradas = [], []
for a, b in chunks(ini, fim):
    v = query_vr(f"""
        SELECT v.data, v.id_loja, vi.id_produto, sum(vi.quantidade) qtd
        FROM pdv.venda v JOIN pdv.vendaitem vi ON vi.id_venda = v.id
        WHERE v.data BETWEEN '{a}' AND '{b}' AND v.cancelado = false
          AND v.id_loja IN ({lojas_sql}) AND vi.id_produto IN ({ids_sql})
        GROUP BY 1,2,3""")
    if len(v): vendas.append(v)
    e = query_vr(f"""
        SELECT n.dataentrada AS data, n.id_loja, i.id_produto,
               sum(i.quantidade) qtd, sum(i.quantidade * i.qtdembalagem) unidades
        FROM public.notaentrada n JOIN public.notaentradaitem i ON i.id_notaentrada = n.id
        WHERE n.dataentrada BETWEEN '{a}' AND '{b}'
          AND n.id_fornecedor IN ({",".join(map(str, FORNECEDORES_VR))})
          AND n.id_loja IN ({lojas_sql}) AND i.id_produto IN ({ids_app_sql})
        GROUP BY 1,2,3""")
    if len(e): entradas.append(e)

vendas   = pd.concat(vendas)   if vendas   else pd.DataFrame(columns=["data","id_loja","id_produto","qtd"])
entradas = pd.concat(entradas) if entradas else pd.DataFrame(columns=["data","id_loja","id_produto","qtd","unidades"])
RESUMO.append(f"{len(vendas)} linhas de venda, {len(entradas)} de entrada no VR")

# ---------------------------------------------------------------- 3. montar aves_diario
# venda na unidade do app (bandeja=bandejas, granel=kg, galeto=un; meio galeto -> 0.5 do 371)
mov = defaultdict(lambda: {"venda": 0.0, "entrada": 0.0, "entrada_caixas": 0.0})
for _, r in vendas.iterrows():
    pid, fator = int(r.id_produto), 1.0
    if pid in VENDA_MAP: pid, fator = VENDA_MAP[pid]
    if pid not in POR_ID: continue
    loja = LOJAS.get(int(r.id_loja))
    if not loja: continue
    mov[(str(r.data)[:10], loja, pid)]["venda"] += float(r.qtd) * fator

for _, r in entradas.iterrows():
    pid = int(r.id_produto)
    it = POR_ID.get(pid)
    loja = LOJAS.get(int(r.id_loja))
    if not it or not loja: continue
    m = mov[(str(r.data)[:10], loja, pid)]
    if it["tipo"] == "bandeja":
        m["entrada"] += float(r.unidades)                       # bandejas
        m["entrada_caixas"] += float(r.qtd)                     # caixas mãe
    else:                                                       # granel/galeto: kg ou un
        m["entrada"] += float(r.qtd) * float(it["entrada_fator"])
        m["entrada_caixas"] += float(r.qtd)

linhas = [{"data": d, "loja": l, "id_produto": p,
           "venda": round(v["venda"], 3), "entrada": round(v["entrada"], 3),
           "entrada_caixas": round(v["entrada_caixas"], 3)}
          for (d, l, p), v in mov.items()]
sb_upsert("aves_diario", linhas, "data,loja,id_produto")
RESUMO.append(f"{len(linhas)} linhas gravadas em aves_diario")

# ---------------------------------------------------------------- 4. estoque virtual
# recomeça no último minibalanço de cada loja+item e anda dia a dia
d0_hist = (HOJE - dt.timedelta(days=HIST_DIAS)).isoformat()
diario = sb_get("aves_diario", {"select": "*", "data": f"gte.{d0_hist}", "order": "data.asc"})
balancos = sb_get("aves_minibalanco", {"select": "*", "order": "data.asc"})

base = {}   # (loja,pid) -> (data, contagem)
for b in balancos:
    base[(b["loja"], b["id_produto"])] = (b["data"], float(b["contagem"]))

por_chave = defaultdict(list)
for r in diario:
    por_chave[(r["loja"], r["id_produto"])].append(r)

atualiza = []
estoque_atual = {}                       # (loja,pid) -> estoque de ontem
for chave, rows in por_chave.items():
    bal = base.get(chave)
    est = None
    for r in rows:                       # rows já em ordem de data
        if bal and r["data"] >= bal[0]:
            if r["data"] == bal[0]:
                est = bal[1]             # contagem é o ponto zero do dia do balanço
            elif est is not None:
                est = est + float(r["entrada"]) - float(r["venda"])
            if est is not None:
                est = max(est, 0.0)
                atualiza.append({"data": r["data"], "loja": chave[0], "id_produto": chave[1],
                                 "venda": r["venda"], "entrada": r["entrada"],
                                 "entrada_caixas": r["entrada_caixas"],
                                 "estoque_virtual": round(est, 3)})
    if est is not None:
        estoque_atual[chave] = est
sb_upsert("aves_diario", atualiza, "data,loja,id_produto")
RESUMO.append(f"Estoque virtual recalculado para {len(estoque_atual)} loja x item "
              f"({len(base)} com minibalanço)")

# ---- fallback: sem minibalanço, assume o estoque atual do VR (produtocomplemento)
# CONSOLIDA os códigos irmãos: no VR o galeto entra no 371 e sai pelo 3420/3421/3422,
# então o código de entrada fica inflado e os de venda ficam negativos. Somando os dois
# (o meio galeto valendo 0,5) chega-se perto do estoque físico enquanto o vínculo de
# baixa não é ajustado no VR.
vr_est = query_vr(f"""
    SELECT id_loja, id_produto, estoque FROM public.produtocomplemento
    WHERE id_loja IN ({lojas_sql}) AND id_produto IN ({ids_sql})""")
cons = defaultdict(float)
for _, r in vr_est.iterrows():
    loja = LOJAS.get(int(r.id_loja)); pid = int(r.id_produto)
    if not loja: continue
    fator = 1.0
    if pid in VENDA_MAP: pid, fator = VENDA_MAP[pid]
    if pid not in POR_ID: continue
    cons[(loja, pid)] += float(r.estoque or 0) * fator
n_fb = 0
for chave, val in cons.items():
    if chave in base: continue                  # já tem minibalanço: a contagem manda
    estoque_atual[chave] = max(val, 0.0)
    n_fb += 1
RESUMO.append(f"Sem minibalanço: estoque do VR (códigos irmãos consolidados) para {n_fb} loja x item")

# ---- último custo unitário de cada item (vai no TXT de importação do VR)
custos = query_vr(f"""
    SELECT i.id_produto, max(n.dataentrada) AS dt, max(i.custocomimposto) AS custo
    FROM public.notaentradaitem i JOIN public.notaentrada n ON n.id = i.id_notaentrada
    WHERE i.id_produto IN ({ids_app_sql}) AND n.dataentrada >= current_date - 60
    GROUP BY i.id_produto, n.dataentrada
    ORDER BY i.id_produto, n.dataentrada DESC""")
n_cst = 0
vistos = set()
for _, r in (custos.iterrows() if len(custos) else []):
    pid = int(r.id_produto)                       # a 1ª linha de cada item é a entrada mais recente
    if pid in vistos or pid not in POR_ID: continue
    vistos.add(pid)
    try:
        sb_patch("aves_itens", f"id_produto=eq.{pid}", {"ultimo_custo": round(float(r.custo), 4)})
        n_cst += 1
    except Exception as e:
        print(f"custo {pid}: {e}")
RESUMO.append(f"Último custo atualizado em {n_cst} itens")

# ---------------------------------------------------------------- 5. fatores do DDV
df = pd.DataFrame(diario)
if len(df):
    df["data"] = pd.to_datetime(df["data"])
    df["venda"] = df["venda"].astype(float)
    df["dow"] = df["data"].dt.dayofweek.map({6:0,0:1,1:2,2:3,3:4,4:5,5:6})  # dom=0..sab=6
    df["semana"] = ((df["data"].dt.day - 1) // 7 + 1).clip(upper=5)

fatores_rows = []
prev_por_chave = {}
for loja in LOJAS.values():
    dfl = df[df["loja"] == loja] if len(df) else df
    # fator dia-da-semana da loja (todos os itens de aves juntos — mix estável)
    c1 = dfl[dfl["data"] >= pd.Timestamp(HOJE - dt.timedelta(days=JANELA_DOW))]
    tot_dow = c1.groupby("dow")["venda"].sum()
    f_dow = {str(d): round(float(tot_dow.get(d, 0)) / tot_dow.mean(), 3) if len(tot_dow) and tot_dow.mean() > 0 else 1.0
             for d in range(7)}
    # fator semana-do-mês (normalizado por dias de cada faixa)
    c2 = dfl[dfl["data"] >= pd.Timestamp(HOJE - dt.timedelta(days=JANELA_SEM))]
    som = c2.groupby("semana")["venda"].sum()
    dias = c2.groupby("semana")["data"].nunique()
    med = (som / dias).dropna()
    f_sem = {str(s): round(float(med.get(s, med.mean())) / med.mean(), 3) if len(med) and med.mean() > 0 else 1.0
             for s in range(1, 6)}
    # VMD base por item, dessazonalizado
    c3 = dfl[dfl["data"] >= pd.Timestamp(HOJE - dt.timedelta(days=JANELA_VMD))].copy()
    if len(c3):
        c3["fator"] = c3.apply(lambda r: max(float(f_dow.get(str(int(r["dow"])), 1)) *
                                             float(f_sem.get(str(int(r["semana"])), 1)), 0.2), axis=1)
        c3["dessaz"] = c3["venda"] / c3["fator"]
        vmds = c3.groupby("id_produto")["dessaz"].sum() / JANELA_VMD   # soma/28: dias sem venda contam 0
    else:
        vmds = pd.Series(dtype=float)
    for pid in IDS_APP:
        vmd = round(float(vmds.get(pid, 0.0)), 3)
        fatores_rows.append({"loja": loja, "id_produto": pid, "vmd_base": vmd,
                             "f_dow": f_dow, "f_semana": f_sem,
                             "atualizado_em": dt.datetime.utcnow().isoformat()})
        prev_por_chave[(loja, pid)] = (vmd, f_dow, f_sem)
sb_upsert("aves_fatores", fatores_rows, "loja,id_produto")
RESUMO.append(f"Fatores recalculados: {len(fatores_rows)} loja x item")

# --------------------------------------------------- 5b. MERCADORIA EM TRÂNSITO
# NF-e que o fornecedor já emitiu (o VR baixou da SEFAZ) e que ainda não virou
# nota de entrada em nenhuma loja. É o que evita o pedido dobrado: a loja acha
# que nada chegou, pede de novo, e depois chegam os dois.
#
# O XML da NF-e fica em notaentradanfe.xml; os itens saem dele por regex e o
# código do fornecedor (cProd) vira id_produto pela tabela produtofornecedor.
# A quantidade no XML SEMPRE vem em kg -> convertemos pela kg_por_unidade.
TRANSITO_DIAS = 10

transito_rows, transito_por_chave = [], defaultdict(lambda: {"qtd": 0.0, "cx": 0.0, "nfs": []})
try:
    lojas_ids = ",".join(str(k) for k in LOJAS)
    forn_ids  = ",".join(str(f) for f in FORNECEDORES_VR)
    ids_tr    = ",".join(str(i) for i in IDS_APP)
    # CTEs MATERIALIZED: sem isso o Postgres empurra o join de texto para dentro
    # do parse do XML e a consulta passa de 60 s.
    tr = query_vr(f"""
WITH pf AS MATERIALIZED (
  SELECT id_fornecedor, ltrim(codigoexterno,'0') cod, min(id_produto) id_produto
  FROM public.produtofornecedor
  WHERE id_produto IN ({ids_tr}) AND id_fornecedor IN ({forn_ids})
  GROUP BY 1,2
),
nf AS MATERIALIZED (
  SELECT nfe.id_loja, nfe.id_fornecedor, nfe.numeronota, nfe.chavenfe,
         nfe.dataentrada::date dt, (current_date - nfe.dataentrada::date) dias,
         nfe.valortotal, nfe.xml
  FROM public.notaentradanfe nfe
  WHERE nfe.dataentrada >= current_date - {TRANSITO_DIAS}
    AND nfe.id_fornecedor IN ({forn_ids})
    AND nfe.id_loja IN ({lojas_ids})
    AND length(coalesce(nfe.xml,'')) > 500
    AND NOT EXISTS (SELECT 1 FROM public.notaentrada n WHERE n.chavenfe = nfe.chavenfe)
),
det AS MATERIALIZED (
  SELECT nf.id_loja, nf.id_fornecedor, nf.numeronota, nf.chavenfe, nf.dt, nf.dias,
         nf.valortotal,
         ltrim(substring(d from '<cProd>([^<]*)'),'0') cod,
         substring(d from '<qCom>([^<]*)')::numeric kg
  FROM nf, regexp_split_to_table(nf.xml, '<det[ >]') d
  WHERE d LIKE '%<cProd>%'
)
SELECT det.id_loja, det.id_fornecedor, det.numeronota, det.chavenfe, det.dt, det.dias,
       round(det.valortotal,2) valornf, pf.id_produto, sum(det.kg) kg
FROM det JOIN pf ON pf.id_fornecedor = det.id_fornecedor AND pf.cod = det.cod
GROUP BY 1,2,3,4,5,6,7,8""")

    # ---- a mesma carga chega em duas séries ----
    # A Mauricea emite a mesma entrega em duas séries (10 e 11): mesmo CNPJ,
    # mesmo valor, um dia de diferença, itens idênticos. Contar as duas dobraria
    # o trânsito e faria a loja deixar de pedir o que precisa. Mantemos só a
    # nota mais recente de cada (loja, fornecedor, valor) dentro de 3 dias.
    vistos, descartadas = {}, 0
    manter = set()
    for _, r in tr.sort_values("dias").iterrows():
        k = (int(r.id_loja), int(r.id_fornecedor), float(r.valornf or 0))
        ch = str(r.chavenfe)
        if k in vistos:
            if abs(int(r.dias) - vistos[k][1]) <= 3 and ch != vistos[k][0]:
                descartadas += 1
                continue                      # duplicata da mesma carga
        vistos[k] = (ch, int(r.dias))
        manter.add(ch)

    for _, r in tr.iterrows():
        if str(r.chavenfe) not in manter:
            continue
        loja = LOJAS.get(int(r.id_loja))
        pid  = int(r.id_produto)
        if not loja or pid not in POR_ID:
            continue
        it = POR_ID[pid]
        kg_un = float(it.get("kg_por_unidade") or it.get("peso_bandeja_kg") or 1.0)
        if kg_un <= 0:
            continue
        # mesma conversão da entrada: congelado que vira resfriado perde peso
        qtd = float(r.kg) / kg_un * float(it.get("entrada_fator") or 1.0)
        # galeto se conta em unidade inteira, qualquer que seja o peso da ave;
        # se a nota vier com peso quebrado, arredonda para a unidade mais próxima
        if it["tipo"] == "galeto":
            qtd = float(round(qtd))
        cx  = qtd / float(it.get("unidades_caixa") or 1)
        transito_rows.append({
            "loja": loja, "id_produto": pid, "numeronota": str(r.numeronota),
            "emitida": str(r.dt)[:10], "dias": int(r.dias),
            "qtd": round(qtd, 3), "caixas": round(cx, 2),
            "fornecedor": it.get("fornecedor"),
            "atualizado_em": dt.datetime.utcnow().isoformat()})
        g = transito_por_chave[(loja, pid)]
        g["qtd"] += qtd
        g["cx"]  += cx
        g["nfs"].append((str(r.numeronota), int(r.dias)))

    # a tabela é um retrato do momento: apaga tudo e regrava
    requests.delete(f"{SUPABASE_URL}/rest/v1/aves_transito?loja=neq.__nada__",
                    headers=sb_headers(), timeout=60)
    sb_upsert("aves_transito", transito_rows, "loja,id_produto,numeronota")
    RESUMO.append(f"Em trânsito: {len(transito_rows)} NF x item em "
                  f"{len(set(r['loja'] for r in transito_rows))} lojas"
                  + (f" ({descartadas} linhas de nota duplicada em 2ª série descartadas)"
                     if descartadas else ""))
except Exception as e:
    # o trânsito é um aviso a mais: se falhar, o app continua funcionando sem ele
    RESUMO.append(f"Em trânsito: FALHOU ({str(e)[:120]})")

# ---------------------------------------------------------------- 6. sugestão do dia
def prev(vmd, f_dow, f_sem, d):
    dow = (d.weekday() + 1) % 7                     # dom=0..sab=6
    sem = min((d.day - 1) // 7 + 1, 5)
    return vmd * float(f_dow.get(str(dow), 1)) * float(f_sem.get(str(sem), 1))

sug_rows = []
for (loja, pid), (vmd, f_dow, f_sem) in prev_por_chave.items():
    it = POR_ID[pid]
    est = estoque_atual.get((loja, pid))            # estoque ao fim de ontem
    p0 = prev(vmd, f_dow, f_sem, HOJE)
    p1 = prev(vmd, f_dow, f_sem, HOJE + dt.timedelta(days=1))
    p2 = prev(vmd, f_dow, f_sem, HOJE + dt.timedelta(days=2))
    p3 = prev(vmd, f_dow, f_sem, HOJE + dt.timedelta(days=3))
    # pedido de hoje chega amanhã e precisa cobrir até entrega+2,5 dias
    necessidade = p0 + p1 + p2 + (COBERTURA - 2.0) * p3
    falta = necessidade - (est or 0.0)
    # teto de validade: estoque após a chegada não pode passar de validade_dias de venda
    teto = max(float(it["validade_dias"]) * vmd - max((est or 0) - p0, 0), 0)
    bruto = max(min(falta, teto), 0.0)
    # o fornecedor só fatura caixa fechada: TODA sugestão sai em nº de caixas mãe
    cx = float(it.get("unidades_caixa") or 1)
    sug = math.floor(bruto / cx + 0.5)              # arredonda para a caixa mais próxima
    if sug == 0 and falta > 0.6 * cx: sug = 1       # falta relevante garante 1 caixa
    # o que já está faturado e a caminho desta loja
    t = transito_por_chave.get((loja, pid))
    t_qtd = round(t["qtd"], 3) if t else 0
    t_cx  = round(t["cx"], 2) if t else 0
    t_nf  = ", ".join(f"{n} ({d}d)" for n, d in sorted(t["nfs"], key=lambda x: x[1])) if t else None
    t_dias = min(d for _, d in t["nfs"]) if t else None
    # sugestão alternativa, já descontando o que vem a caminho.
    # NÃO substitui a sugestão do sistema: uma NF pode ter sido cancelada ou já
    # ter chegado sem casar a chave, e descontar sozinho causaria ruptura.
    # Quem decide é o operador, vendo os dois números na tela.
    sug_liq = sug
    if t_cx > 0:
        bruto_liq = max(bruto - t["qtd"], 0.0)
        sug_liq = math.floor(bruto_liq / cx + 0.5)
        if sug_liq == 0 and bruto_liq > 0.6 * cx: sug_liq = 1
    sug_rows.append({"data": HOJE.isoformat(), "loja": loja, "id_produto": pid,
                     "estoque_virtual": round(est, 3) if est is not None else None,
                     "prev_hoje": round(p0, 2), "prev_d1": round(p1, 2), "prev_d2": round(p2, 2),
                     "sug_sistema": sug,
                     "transito_qtd": t_qtd, "transito_cx": t_cx,
                     "transito_nf": t_nf, "transito_dias": t_dias,
                     "sug_liquida": sug_liq})
sb_upsert("aves_sugestoes", sug_rows, "data,loja,id_produto")
RESUMO.append(f"Sugestões do dia {HOJE}: {len(sug_rows)} loja x item")

# ---------------------------------------------------------------- resumo
resumo = "\n".join("- " + l for l in RESUMO)
print(resumo)
sm = os.environ.get("GITHUB_STEP_SUMMARY")
if sm:
    with open(sm, "a") as f:
        f.write("## Sync Aves\n" + resumo + "\n")
