"""
RADAR ALTO CUSTO - Monitor de Licitações Farmacêuticas no PNCP
================================================================
Busca licitações publicadas no Portal Nacional de Contratações Públicas
que contenham termos relacionados a medicamentos de alto custo no objeto da compra.

Estratégia:
- A API do PNCP NÃO tem busca por texto. É necessário buscar por data + modalidade.
- O script busca contratações dos últimos N dias, varrendo todas as modalidades relevantes.
- Filtra pelo campo "objetoCompra" usando as keywords definidas.
- Salva no Supabase (upsert) e envia alerta no Telegram apenas para novos registros.

Credenciais via variáveis de ambiente (GitHub Secrets).
"""

import os
import sys
import requests
import datetime
import time
import logging

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("radar_altocusto")

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_ALTOCUSTO", "")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Fallback para rodar local com valores hardcoded (remover em produção)
if not TELEGRAM_BOT_TOKEN:
    TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
if not CHAT_ID:
    CHAT_ID = "-5180338942"
if not SUPABASE_URL:
    SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
if not SUPABASE_KEY:
    SUPABASE_KEY = "sb_publishable_4gTDfatSOwa5X4CJSnPRIQ_vBUJXb99"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# ================= TABELAS SUPABASE =================
TABELA_EDITAIS = "editais_pncp_altocusto"
TABELA_ITENS = "itens_pncp_altocusto"

# ================= KEYWORDS ALTO CUSTO =================
KEYWORDS = [
    "77vf", "Proteina C Composicao Humana", "Proteinas C Composicoes Humanas",
    "Abilify", "Acalabrutinib", "Acalabrutinibe", "Acamprosate", "Acamprosato",
    "Acido Quenodesoxicolico", "Adeek", "Adek", "Adenuric", "Alafenamida",
    "Alectinibe", "Alfapeginterferona", "Alfassebelipase", "Ammonaps", "Amvuttra",
    "Anacinra", "Anagrelida", "Anakinra", "Aquadeks", "Aquae", "Asenapine",
    "Atomoxetina", "Atomoxetine", "Atriance", "Axitinibe", "Balsalazide",
    "Benzatropina", "Benztropine", "Betaciclodextrina", "Betanecol", "Biktarvy",
    "Braftovi", "Brentuximab", "Brevactid", "Brivaracetam", "Briviact", "Campral",
    "Camzyos", "Canabidiol", "Canabis", "Cannabis", "Cbd", "Cenobamato",
    "Cevimelina", "Cevimeline", "Champix", "Charlotte", "Chenodal", "Chenodiol",
    "Cibinqo", "Cidofovir", "Cisteamina", "Cortef", "Cortrosyn", "Cosintropina",
    "Cosmegen", "Cosyntropin", "Cystagon", "Cytomel", "Dactinomicina", "Dantrium",
    "Dantroleno", "Daratumumabe", "Dayvigo", "Dekas", "Diacomit", "Diamox",
    "Diazoxido", "Dietilpropiona", "Dificlir", "Dissulfiram", "Edastar",
    "Eflornithine", "Elixinol", "Elmiron", "Enjaymo", "Epipen",
    "Escopolamina Adesivo", "Estiripentol", "Ethosuximide", "Etossuximida", "Evr",
    "Farmausa", "Febuxostat", "Febuxostato", "Felbamate", "Felbamato",
    "Fenilbutirato Sodio", "Fidaxomicin", "Firdapse", "Flecainide", "Flucitosina",
    "Fomepizol", "Foscarnet", "Foscavir", "Gamainterferona", "Givinostat",
    "Glanatec", "Gonadorelin", "Gonadotropina", "Guanfacina", "Guanfacine",
    "Gutron", "Hemina", "Hepazec", "Hialuronato", "Hidroxocobalamin", "Hioscina",
    "Hisone", "Hodpro", "Ibrutinib", "Idebenone", "Imbruvica", "Imukin",
    "Inbrija", "Increlex", "Inotersena", "Inovelon", "Intralipid", "Intuniv",
    "Isodiolex", "Jaypirca", "Kaliumbromid", "Kayexalate", "Kenalog", "Kimmtrak",
    "Kineret", "Lecanemab", "Lemborexant", "Leqembi", "Leukine", "Liotironina",
    "Lokelma", "Lomoother", "Lomustina", "Ludiomil", "Macitentana", "Maprotilina",
    "Mechlorethamine", "Medrosan", "Megamilbedoce", "Melphalan Mesylate",
    "Mexiletina", "Mexiletine", "Midodrina", "Midodrine", "Mirvetuximabe",
    "Misintu", "Mitomycin", "Mounjaro", "Nabix", "Nadolol", "Natulan",
    "Nelarabina", "Nintedanibe", "Nitisinona", "Nivolumabe", "Ocrelizumabe",
    "Omalizumabe", "Ontozry", "Opdualag", "Ospolot", "Pembrolizumabe", "Penepin",
    "Penfluridol", "Penridol", "Pentosana", "Perfenazina", "Perphenazine",
    "Pimozida", "Pimozide", "Pirtobrutinibe", "Pluvicto", "Pomalid",
    "Procarbazina", "Proglicem", "Proglycem", "Purodiol", "Pyrukynd",
    "Quenodesoxicolico", "Quenodiol", "Quinidina", "Quinidine", "Quvivq",
    "Rapaflo", "Relyvrio", "Revocon", "Ripasudil", "Rsho", "Rufinamida",
    "Ruxolitinibe", "Saphris", "Scopoderm", "Sidovis", "Smoflipid",
    "Soravtansina", "Strattera", "Sulindaco", "Sulthiame", "Synacthen", "Taloxa",
    "Tanganil", "Tegsedi", "Tepadina", "Tetrabenazine", "Tetracosactina", "Thc",
    "Thiola", "Thiotepa", "Tiopronina", "Tirzepatide", "Trabec",
    "Tranilcipromina", "Trastuzumabe Deruxtecana", "Trientina", "Trientine",
    "Trikafta", "Tryprine", "Tudca", "Vabysmo", "Vaniqa", "Vareniclina",
    "Varenicline", "Vepesid", "Vimizim", "Vivjoa", "Xagrid", "Xenazine",
    "Zanubrutinib", "Zonisamida", "Zonisamide", "Ztalmy",
]

# API do PNCP - endpoint correto de consulta pública
PNCP_API_BASE = "https://pncp.gov.br/api/consulta/v1"

# Quantos dias para trás buscar
DIAS_RETROATIVOS = 15

# Códigos de modalidade de contratação no PNCP:
# 4=Concorrência Eletrônica, 5=Concorrência Presencial, 6=Pregão Eletrônico,
# 7=Pregão Presencial, 8=Dispensa de Licitação, 9=Inexigibilidade,
# 11=Credenciamento, 13=IRP
MODALIDADES = [4, 5, 6, 7, 8, 9, 11, 13]

# Tamanho da página de resultados
TAMANHO_PAGINA = 50

# =================================================


def formatar_data_br(data_iso):
    """Converte ISO datetime para formato brasileiro dd/mm/yyyy HH:MM"""
    if not data_iso:
        return "Não informada"
    try:
        dt = datetime.datetime.fromisoformat(data_iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(data_iso)


def keyword_match(texto):
    """Verifica se alguma keyword aparece no texto (case insensitive)"""
    if not texto:
        return False
    texto_lower = texto.lower()
    return any(kw.lower() in texto_lower for kw in KEYWORDS)


def classificar_modalidade(modalidade_nome):
    """Classifica a modalidade para a tag do Telegram"""
    if not modalidade_nome:
        return "📋 <b>LICITAÇÃO</b>"
    mod_lower = str(modalidade_nome).lower()
    if "dispensa" in mod_lower or "inexigibilidade" in mod_lower:
        return "⚖️ <b>JUDICIALIZAÇÃO / COMPRA DIRETA</b>"
    elif "pregão" in mod_lower:
        return "📈 <b>GRANDE VOLUME / PREGÃO</b>"
    elif "concorrência" in mod_lower:
        return "🏗️ <b>CONCORRÊNCIA</b>"
    elif "credenciamento" in mod_lower:
        return "📝 <b>CREDENCIAMENTO</b>"
    return "📋 <b>LICITAÇÃO</b>"


def buscar_itens_relevantes(cnpj, ano, sequencial):
    """
    Busca os itens de uma compra específica e filtra os relevantes por keyword.
    Retorna: (lista_texto_telegram, lista_banco, valor_total)
    """
    url = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/itens"
    apresentacoes_texto = []
    apresentacoes_banco = []
    valor_total = 0.0

    try:
        r = requests.get(url, params={"pagina": 1, "tamanhoPagina": 500}, timeout=15,
                         headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
        if r.status_code == 200:
            dados = r.json()
            itens = dados if isinstance(dados, list) else dados.get("items", dados.get("data", []))
            for item in itens:
                desc = item.get("descricao", item.get("materialOuServicoNome", ""))
                if keyword_match(desc):
                    num = item.get("numeroItem", "-")
                    qtd = item.get("quantidade", 0)
                    vlr = item.get("valorTotalEstimado")
                    if vlr:
                        valor_total += float(vlr)
                    apresentacoes_texto.append(f"Item {num}: {desc}\n↳ Qtd: {qtd}")
                    apresentacoes_banco.append({
                        "numero_item": str(num),
                        "descricao": str(desc)[:500],
                        "quantidade": float(qtd) if qtd else 0.0,
                        "valor_unitario": float(item.get("valorUnitarioEstimado")) if item.get("valorUnitarioEstimado") else None,
                        "valor_total": float(vlr) if vlr else None,
                    })
    except Exception as e:
        log.warning(f"Erro ao buscar itens {cnpj}/{ano}/{sequencial}: {e}")

    return apresentacoes_texto, apresentacoes_banco, valor_total


def check_and_save_supabase(dados_edital, dados_itens):
    """
    Faz upsert do edital no Supabase.
    Retorna True se o registro é NOVO (para disparar Telegram), False se já existia.
    """
    url_id = dados_edital["url_id"]
    endpoint_editais = f"{SUPABASE_URL}/rest/v1/{TABELA_EDITAIS}"
    endpoint_itens = f"{SUPABASE_URL}/rest/v1/{TABELA_ITENS}"

    is_new = True
    try:
        check_response = requests.get(
            f"{endpoint_editais}?url_id=eq.{requests.utils.quote(url_id)}&select=url_id",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        if check_response.status_code == 200 and len(check_response.json()) > 0:
            is_new = False
    except Exception as e:
        log.warning(f"Aviso ao verificar existência no Supabase: {e}")

    # UPSERT
    headers_upsert = SUPABASE_HEADERS.copy()
    headers_upsert["Prefer"] = "resolution=merge-duplicates"

    try:
        res = requests.post(
            f"{endpoint_editais}?on_conflict=url_id",
            headers=headers_upsert,
            json=dados_edital,
            timeout=10
        )
        if res.status_code not in [200, 201, 204]:
            log.error(f"ERRO SUPABASE upsert ({url_id}): {res.status_code} - {res.text}")
            return False
        else:
            status = "NOVO" if is_new else "ATUALIZADO"
            log.info(f"✅ Supabase {status}: {url_id}")
    except Exception as e:
        log.error(f"ERRO SUPABASE request ({url_id}): {e}")
        return False

    # Só insere itens se for registro novo
    if is_new and dados_itens:
        try:
            for item in dados_itens:
                item["edital_url_id"] = url_id
            requests.post(endpoint_itens, headers=SUPABASE_HEADERS, json=dados_itens, timeout=10)
        except Exception as e:
            log.warning(f"Erro ao inserir itens: {e}")

    return is_new


def enviar_telegram(edital, apresentacoes_texto, tag):
    """Envia mensagem formatada para o grupo do Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    dt_inicio = formatar_data_br(edital.get("data_inicio"))
    dt_fim = formatar_data_br(edital.get("data_fim"))

    # Monta bloco de itens se houver
    bloco_itens = ""
    if apresentacoes_texto:
        itens_str = "\n".join(apresentacoes_texto[:5])  # máx 5 itens no alerta
        bloco_itens = f"\n\n📦 <b>ITENS RELEVANTES:</b>\n{itens_str}"

    # Valor estimado
    valor = edital.get("valor_total_estimado", 0)
    bloco_valor = ""
    if valor and valor > 0:
        bloco_valor = f"\n💰 <b>Valor estimado:</b> R$ {valor:,.2f}"

    link_pncp = f"https://pncp.gov.br/app/editais{edital.get('url_id', '')}"

    msg = f"""🏛️ <b>NOVA LICITAÇÃO - ALTO CUSTO</b>
{tag}

📋 <b>Objeto:</b> {edital.get('titulo', 'N/A')[:300]}
🏢 <b>Órgão:</b> {edital.get('orgao', 'N/A')}
📍 <b>UF:</b> {edital.get('uf', 'N/A')}
📑 <b>Modalidade:</b> {edital.get('modalidade', 'N/A')}{bloco_valor}

⏳ <b>CRONOGRAMA:</b>
🟢 Abertura: {dt_inicio}
🔴 Encerramento: {dt_fim}{bloco_itens}

🔗 <a href="{link_pncp}">Acessar no PNCP</a>
<i>Alerta automático - Radar Alto Custo</i>"""

    try:
        resp = requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            log.info(f"🚀 Telegram enviado: {edital.get('titulo', '')[:60]}")
        else:
            log.error(f"Telegram erro {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        log.error(f"Telegram request falhou: {e}")


def buscar_contratacoes_pagina(data_inicial, data_final, modalidade, uf=None, pagina=1):
    """
    Consulta a API pública do PNCP: /v1/contratacoes/publicacao
    Retorna a lista de contratações ou lista vazia.
    """
    url = f"{PNCP_API_BASE}/contratacoes/publicacao"
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "tamanhoPagina": TAMANHO_PAGINA,
        "pagina": pagina,
    }
    if uf:
        params["uf"] = uf

    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            dados = r.json()
            return dados.get("data", [])
        elif r.status_code == 204:
            return []  # sem dados
        else:
            log.warning(f"API PNCP retornou {r.status_code} para modalidade={modalidade}, pagina={pagina}")
            return []
    except requests.exceptions.Timeout:
        log.warning(f"Timeout na API PNCP (mod={modalidade}, pag={pagina})")
        return []
    except Exception as e:
        log.error(f"Erro na API PNCP: {e}")
        return []


def buscar_todas_contratacoes(data_inicial, data_final, modalidade):
    """
    Busca todas as páginas de uma modalidade em um intervalo de datas.
    Retorna lista consolidada.
    """
    todas = []
    pagina = 1
    while True:
        resultados = buscar_contratacoes_pagina(data_inicial, data_final, modalidade, pagina=pagina)
        if not resultados:
            break
        todas.extend(resultados)
        if len(resultados) < TAMANHO_PAGINA:
            break  # última página
        pagina += 1
        time.sleep(0.3)  # rate limiting gentil
        if pagina > 20:  # safety: máx ~1000 resultados por modalidade/período
            log.warning(f"Limite de páginas atingido (mod={modalidade})")
            break
    return todas


def processar_contratacao(item):
    """
    Processa uma contratação retornada pela API.
    Verifica keywords no objetoCompra, busca itens, salva e notifica.
    """
    objeto = item.get("objetoCompra", "")
    if not keyword_match(objeto):
        return False

    cnpj = item.get("orgaoEntidade", {}).get("cnpj", "")
    ano = item.get("anoCompra")
    seq = item.get("sequencialCompra")

    if not cnpj or not ano or not seq:
        return False

    url_id = f"/compras/{cnpj}/{ano}/{seq}"

    # Buscar itens relevantes
    apres_texto, apres_banco, valor_itens = buscar_itens_relevantes(cnpj, ano, seq)

    # Usar valor dos itens ou o valor total estimado da compra
    valor_total = valor_itens if valor_itens > 0 else (
        float(item.get("valorTotalEstimado", 0)) if item.get("valorTotalEstimado") else 0.0
    )

    # Título: combinar modalidade + número ou usar objeto resumido
    modalidade_nome = item.get("modalidadeNome", "")
    numero_edital = item.get("numeroCompra", item.get("sequencialCompra", ""))
    titulo = f"{modalidade_nome} {numero_edital}" if modalidade_nome else objeto[:200]

    dados_edital = {
        "url_id": url_id,
        "titulo": titulo,
        "objeto": objeto[:500],
        "orgao": item.get("orgaoEntidade", {}).get("razaoSocial", ""),
        "uf": item.get("unidadeOrgao", {}).get("ufSigla", "") or item.get("orgaoEntidade", {}).get("ufSigla", ""),
        "modalidade": modalidade_nome,
        "data_publicacao": item.get("dataPublicacaoPncp"),
        "valor_total_estimado": valor_total,
        "data_inicio": item.get("dataAberturaProposta"),
        "data_fim": item.get("dataEncerramentoProposta"),
        "numero_controle_pncp": item.get("numeroControlePNCP", ""),
        "link_sistema_origem": item.get("linkSistemaOrigem", ""),
    }

    # Salvar e verificar se é novo
    if check_and_save_supabase(dados_edital, apres_banco):
        tag = classificar_modalidade(modalidade_nome)
        enviar_telegram(dados_edital, apres_texto, tag)
        return True

    return False


def main():
    """Fluxo principal do radar"""
    log.info("=" * 60)
    log.info("🔍 RADAR ALTO CUSTO - Iniciando busca de licitações")
    log.info("=" * 60)

    # Validar configurações
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("❌ SUPABASE_URL ou SUPABASE_KEY não configurados!")
        sys.exit(1)
    if not TELEGRAM_BOT_TOKEN or not CHAT_ID:
        log.warning("⚠️ Telegram não configurado - alertas não serão enviados")

    # Calcular intervalo de datas
    hoje = datetime.date.today()
    data_inicio = hoje - datetime.timedelta(days=DIAS_RETROATIVOS)
    data_inicial_str = data_inicio.strftime("%Y%m%d")
    data_final_str = hoje.strftime("%Y%m%d")

    log.info(f"📅 Período: {data_inicio.strftime('%d/%m/%Y')} a {hoje.strftime('%d/%m/%Y')}")
    log.info(f"🔑 Keywords: {len(KEYWORDS)} termos configurados")
    log.info(f"📊 Modalidades: {MODALIDADES}")

    total_encontradas = 0
    total_novas = 0
    total_analisadas = 0

    for modalidade in MODALIDADES:
        log.info(f"--- Buscando modalidade {modalidade} ---")
        contratacoes = buscar_todas_contratacoes(data_inicial_str, data_final_str, modalidade)
        log.info(f"   📄 {len(contratacoes)} contratações retornadas")
        total_analisadas += len(contratacoes)

        for item in contratacoes:
            objeto = item.get("objetoCompra", "")
            if keyword_match(objeto):
                total_encontradas += 1
                log.info(f"   🎯 Match: {objeto[:80]}...")
                if processar_contratacao(item):
                    total_novas += 1

        time.sleep(0.5)  # pausa entre modalidades

    log.info("=" * 60)
    log.info(f"📊 RESUMO:")
    log.info(f"   Contratações analisadas: {total_analisadas}")
    log.info(f"   Com keywords alto custo: {total_encontradas}")
    log.info(f"   Novas (Telegram enviado): {total_novas}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
