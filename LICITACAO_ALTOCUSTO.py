"""
RADAR ALTO CUSTO - Monitor de Licitações Farmacêuticas no PNCP (v3)
====================================================================
Busca licitações do Portal Nacional de Contratações Públicas com itens de
medicamentos de alto custo, grava no Supabase (editais_pncp_altocusto /
itens_pncp_altocusto) e avisa no Telegram. A lógica comum aos radares está em
radar_comum.py.

v3:
- só grava edital com ITEM do radar confirmado no PNCP;
- lê todas as páginas de itens (antes parava no 500º);
- casa grafia em inglês e variações ("DANTROLENE", "DARATUMUMAB", "TIRZEPATIDA")
  e só no começo da palavra ("Evr" não casa mais com "Mionevrix");
- varre os editais abertos com TODOS os termos (antes a busca usava 40 dos 230);
- revisa datas e itens dos editais abertos e lê os documentos (impugnação, remarcação).

Uso: python LICITACAO_ALTOCUSTO.py   execução normal
     RADAR_SIMULAR=1 python ...      sem gravar no banco e sem Telegram
"""

import os

import radar_comum
from termos import Termos

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID_ALTOCUSTO", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
if not CHAT_ID:
    CHAT_ID = "-5180338942"
if not SUPABASE_URL:
    SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
if not SUPABASE_KEY:
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNsY2FveXJxaGt4aXJmZWtjeG90Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4NzUzNTUsImV4cCI6MjA4NzQ1MTM1NX0.vwTN5ACAylYO-FMQ5iEubJ2I-vZ9YMUHG-7pXk2YIOg"

# ================= KEYWORDS ALTO CUSTO =================
# Termos que confirmam um ITEM (regras de casamento em termos.py: sem acento,
# começo de palavra, grafia em inglês e plural casam sozinhos).
KEYWORDS = [
    "77vf", "Proteina C Composicao Humana", "Proteinas C Composicoes Humanas",
    "Abilify", "Acalabrutinib", "Acalabrutinibe", "Acamprosate", "Acamprosato",
    "Acido Quenodesoxicolico", "Adeek", "Adek", "Adenuric", "Alafenamida",
    "Alectinibe", "Alfapeginterferona", "Alfassebelipase", "Ammonaps", "Amvuttra",
    "Anacinra", "Anagrelida", "Anakinra", "Aquadeks", "Aquae", "Asenapine",
    "Atomoxetina", "Atomoxetine", "Atriance", "Axitinibe", "Balsalazide",
    "Benzatropina", "Benztropine", "Betaciclodextrina", "Betanecol", "Biktarvy",
    "Braftovi", "Brentuximab", "Brevactid", "Brivaracetam", "Briviact", "Campral",
    "Camzyos", "Cenobamato",
    "Cevimelina", "Cevimeline", "Champix", "Charlotte", "Chenodal", "Chenodiol",
    "Cibinqo", "Cidofovir", "Cisteamina", "Cortef", "Cortrosyn", "Cosintropina",
    "Cosmegen", "Cosyntropin", "Cystagon", "Cytomel", "Dactinomicina", "Dantrium",
    "Dantroleno", "Daratumumabe", "Dayvigo", "Dekas", "Diacomit", "Diamox",
    "Diazoxido", "Dietilpropiona", "Dificlir", "Dissulfiram", "Disulfiram", "Edastar",
    "Eflornithine", "Eflornitina", "Elixinol", "Elmiron", "Enjaymo", "Epipen",
    "Escopolamina Adesivo", "Estiripentol", "Ethosuximide", "Etossuximida", "Evr",
    "Farmausa", "Febuxostat", "Febuxostato", "Felbamate", "Felbamato",
    "Fenilbutirato Sodio", "Fidaxomicin", "Firdapse", "Flecainide", "Flucitosina",
    "Fomepizol", "Foscarnet", "Foscavir", "Gamainterferona", "Givinostat",
    "Glanatec", "Gonadorelin", "Gonadotropina", "Gonadotrofina", "Guanfacina", "Guanfacine",
    "Gutron", "Hemina", "Hepazec", "Hialuronato", "Hidroxocobalamin", "Hioscina",
    "Hisone", "Hodpro", "Ibrutinib", "Idebenone", "Imbruvica", "Imukin",
    "Inbrija", "Increlex", "Inotersena", "Inovelon", "Intralipid", "Intuniv",
    "Isodiolex", "Jaypirca", "Kaliumbromid", "Kayexalate", "Kenalog", "Kimmtrak",
    "Kineret", "Lecanemab", "Lemborexant", "Leqembi", "Leukine", "Liotironina",
    "Lokelma", "Lomoother", "Lomustina", "Ludiomil", "Macitentana", "Maprotilina",
    "Mechlorethamine", "Mecloretamina", "Medrosan", "Megamilbedoce", "Melphalan Mesylate",
    "Mexiletina", "Mexiletine", "Midodrina", "Midodrine", "Mirvetuximabe",
    "Misintu", "Mitomycin", "Mitomicina", "Mounjaro", "Nabix", "Nadolol", "Natulan",
    "Nelarabina", "Nintedanibe", "Nitisinona", "Nivolumabe", "Ocrelizumabe",
    "Omalizumabe", "Ontozry", "Opdualag", "Ospolot", "Pembrolizumabe", "Penepin",
    "Penfluridol", "Penridol", "Pentosana", "Perfenazina", "Perphenazine",
    "Pimozida", "Pimozide", "Pirtobrutinibe", "Pluvicto", "Pomalidomida",
    "Procarbazina", "Proglicem", "Proglycem", "Purodiol", "Pyrukynd",
    "Quenodesoxicolico", "Quenodiol", "Quinidina", "Quinidine", "Quvivq",
    "Rapaflo", "Relyvrio", "Revocon", "Ripasudil", "Rsho", "Rufinamida",
    "Ruxolitinibe", "Saphris", "Scopoderm", "Sidovis", "Smoflipid",
    "Soravtansina", "Strattera", "Sulindaco", "Sulthiame", "Sultiame", "Synacthen", "Taloxa",
    "Tanganil", "Tegsedi", "Tepadina", "Tetrabenazine", "Tetracosactina",
    "Thiola", "Thiotepa", "Tiotepa", "Tiopronina", "Tirzepatide", "Trabectedina",
    "Tranilcipromina", "Trastuzumabe Deruxtecana", "Trientina", "Trientine",
    "Trikafta", "Tryprine", "Tudca", "Vabysmo", "Vaniqa", "Vareniclina",
    "Varenicline", "Vepesid", "Vimizim", "Vivjoa", "Xagrid", "Xenazine",
    "Zanubrutinib", "Zonisamida", "Zonisamide", "Ztalmy",
]

# Busca dos publicados ontem e hoje (os termos que mais trazem edital).
KEYWORDS_BUSCA = [
    "pembrolizumabe", "nivolumabe", "ruxolitinibe", "ocrelizumabe",
    "daratumumabe", "trastuzumabe deruxtecana", "brentuximab",
    "ibrutinib", "zanubrutinib", "acalabrutinibe",
    "tirzepatide", "mounjaro", "omalizumabe",
    "nintedanibe", "axitinibe", "alectinibe",
    "trikafta", "vabysmo", "lecanemab",
    "dactinomicina", "lomustina", "nelarabina",
    "anakinra", "atomoxetina", "brivaracetam",
    "etossuximida", "rufinamida", "cenobamato",
    "dissulfiram", "vareniclina", "liotironina",
    "midodrina", "dantroleno", "cisteamina",
    "fomepizol", "cidofovir", "foscarnet",
    "pluvicto", "mirvetuximabe", "pirtobrutinibe",
]

# Varredura dos abertos: todos os termos, menos siglas curtas que a busca
# do PNCP confundiria com qualquer coisa.
KEYWORDS_ABERTOS = [k for k in KEYWORDS if len(k) > 4]

RADAR = radar_comum.Radar(
    nome="altocusto",
    titulo="💊 RADAR ALTO CUSTO v3 — Licitações Farmacêuticas",
    tabela_editais="editais_pncp_altocusto",
    tabela_itens="itens_pncp_altocusto",
    termos_itens=Termos(KEYWORDS),
    # Sem itens no PNCP, o objeto precisa citar o medicamento (sigla curta não basta).
    termos_objeto=Termos(KEYWORDS_ABERTOS),
    termos_busca=KEYWORDS_BUSCA,
    termos_abertos=KEYWORDS_ABERTOS,
    cabecalho_alerta="💊 <b>{tipo} — ALTO CUSTO</b>",
    nome_resumo="alto custo",
    emoji="💊",
    supabase_url=SUPABASE_URL,
    supabase_key=SUPABASE_KEY,
    telegram_token=TELEGRAM_BOT_TOKEN,
    chat_id=CHAT_ID,
)

if __name__ == "__main__":
    radar_comum.executar(RADAR)
