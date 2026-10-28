"""
RADAR ALTO CUSTO - Monitor de Licitações Farmacêuticas no PNCP (v3)
====================================================================
Busca licitações do Portal Nacional de Contratações Públicas com itens dos
medicamentos da lista abaixo, grava no Supabase (editais_pncp_altocusto /
itens_pncp_altocusto) e avisa no Telegram. A lógica comum aos radares está em
radar_comum.py.

v3:
- só grava edital com ITEM do radar confirmado no PNCP;
- lê todas as páginas de itens (antes parava no 500º);
- casa grafia em inglês, marca e princípio ativo, sempre no começo da palavra;
- varre os editais abertos com todos os termos, além dos publicados ontem e hoje;
- revisa datas e itens dos editais abertos e lê os documentos (impugnação, remarcação).
Lista de medicamentos trocada em 28/10/2026 pela lista do time de licitação.

Uso: python LICITACAO_ALTOCUSTO.py   execução normal
     RADAR_SIMULAR=1 python ...      sem gravar no banco e sem Telegram
"""

import os

import radar_comum
from termos import Termos, Trechos

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

# ================= MEDICAMENTOS ALTO CUSTO =================
# Lista do time de licitação (28/10/2026): nome mostrado -> marca, princípio
# ativo e grafias. Regras de casamento em termos.py (sem acento, começo de
# palavra, grafia em inglês e plural casam sozinhos). O painel tem a mesma
# lista em radar-licitacoes-app/lib/radares.php.
MEDICAMENTOS = {
    "Guanfacina": ["Guanfacina", "Guanfacine", "Intuniv"],
    "Fenilbutirato de sódio": ["Ammonaps", "Pheburane", "Buphenyl", "Fenilbutirato Sodio", "Phenylbutyrate"],
    "Vitaminas ADEK": ["Aquadeks", "Dekas", "Adek", "Adeks"],
    "Ácido quenodesoxicólico": ["Acido Quenodesoxicolico", "Quenodesoxicolico", "Chenodeoxycholic",
                                "Chenodiol", "Quenodiol", "Chenodal"],
    "Brevactid": ["Brevactid"],
    "Hidrocortisona comprimido": ["Cortef", "Hidrocortisona Comprimido", "Hidrocortisona Comp", "Hidrocortisona Cp"],
    "Cosintropina": ["Cortrosyn", "Cosintropina", "Cosyntropin", "Tetracosactida", "Tetracosactide",
                     "Tetracosactina", "Synacthen"],
    "Cisteamina": ["Cystagon", "Procysbi", "Cisteamina", "Cysteamine", "Mercaptamina"],
    "Liotironina": ["Cytomel", "Liotironina", "Liothyronine"],
    "Lemborexante": ["Dayvigo", "Lemborexante", "Lemborexant"],
    "Elixinol": ["Elixinol"],
    "Pentosana": ["Elmiron", "Pentosana", "Pentosan"],
    # Epinefrina só como caneta/autoinjetora: a ampola de adrenalina está em
    # quase toda lista de hospital e de prefeitura.
    "Epinefrina autoinjetora": ["Epipen", "Epi Pen", "Jext", "Epinefrina Autoinjetora", "Epinefrina Autoinjetor",
                                "Epinefrina Auto Injetora", "Adrenalina Autoinjetora", "Adrenalina Autoinjetor",
                                "Adrenalina Auto Injetora",
                                "Caneta Epinefrina", "Caneta Adrenalina", "Epinefrina Caneta", "Adrenalina Caneta",
                                "Penepin"],
    "Foscarnet": ["Foscavir", "Foscarnet", "Foscarnete"],
    "Ripasudil": ["Glanatec", "Ripasudil"],
    "Interferon gama": ["Imukin", "Actimmune", "Interferon Gama", "Interferona Gama", "Interferon Gamma",
                        "Gamainterferona"],
    "Anakinra": ["Kineret", "Anakinra", "Anacinra"],
    "Lomustina": ["Lomoother", "Lomustina", "Lomustine", "Ceenu", "Gleostine"],
    "Diazóxido": ["Proglicem", "Proglycem", "Diazoxido", "Diazoxide"],
    "Sultiame": ["Ospolot", "Sultiame", "Sultiamo", "Sulthiame"],
    # Escopolamina só como adesivo/transdérmica (o butilbrometo é o Buscopan).
    "Escopolamina adesivo": ["Scopoderm", "Escopolamina Adesivo", "Escopolamina Transdermica",
                             "Escopolamina Patch", "Transderm Scop", "Hioscina Adesivo", "Hioscina Transdermica"],
    "Tiopronina": ["Thiola", "Tiopronina", "Tiopronin"],
    "Tiotepa": ["Thiotepa", "Tiotepa", "Tepadina"],
    "Zonisamida": ["Zonisamida", "Zonisamide", "Zonegran"],
    "Mexiletina": ["Mexiletina", "Mexiletine", "Mexitil"],
    "Mitotano": ["Mitotano", "Mitotane", "Lysodren"],
    "Midodrina": ["Midodrina", "Midodrine", "Gutron", "Proamatine"],
}
KEYWORDS = [t for variacoes in MEDICAMENTOS.values() for t in variacoes]

# Confirmam item, mas não entram na busca do PNCP: "hidrocortisona" e
# "comprimido" juntos trariam qualquer lista de farmácia básica.
GENERICOS = {"Hidrocortisona Comprimido", "Hidrocortisona Comp", "Hidrocortisona Cp"}

# Busca dos publicados ontem/hoje e varredura dos abertos: todos os termos,
# menos os genéricos e as siglas curtas que a busca confundiria com qualquer coisa.
KEYWORDS_BUSCA = [k for k in KEYWORDS if len(k) > 4 and k not in GENERICOS]

# Item que cita o termo mas não é o produto: Solu-Cortef (hidrocortisona
# injetável), butilbrometo de escopolamina, teste de laboratório.
EXCLUI_ITEM = Trechos([
    "solu cortef", "solucortef", "butilbrometo", "butil brometo", "teste rapido", "testes rapidos",
    "gravidez", "beta hcg", "imunocromatograf", "reagente", "toxicolog", "drogas de abuso",
])

RADAR = radar_comum.Radar(
    nome="altocusto",
    titulo="💊 RADAR ALTO CUSTO v3 — Licitações Farmacêuticas",
    tabela_editais="editais_pncp_altocusto",
    tabela_itens="itens_pncp_altocusto",
    termos_itens=Termos(KEYWORDS),
    # Sem itens no PNCP, o objeto precisa citar o medicamento (sigla curta não basta).
    termos_objeto=Termos(KEYWORDS_BUSCA),
    termos_busca=KEYWORDS_BUSCA,
    termos_abertos=KEYWORDS_BUSCA,
    exclui_item=EXCLUI_ITEM,
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
