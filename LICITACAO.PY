"""
RADAR FARMAUSA - Monitor de Licitações de Cannabis no PNCP (v3)
===============================================================
Busca licitações do Portal Nacional de Contratações Públicas com itens de
cannabis/canabidiol, grava no Supabase (editais_pncp / itens_pncp) e avisa no
Telegram. A lógica comum aos radares está em radar_comum.py.

v3:
- só grava edital com ITEM do radar confirmado no PNCP (acabou o "sem item
  confirmado" de inscrição em congresso CBDA/CBDE, patrocínio, exame toxicológico);
- lê todas as páginas de itens (antes parava no 500º);
- varre todos os editais abertos a cada execução, além dos publicados ontem e hoje;
- revisa datas e itens dos editais abertos e lê os documentos (impugnação, remarcação).

Uso: python LICITACAO.PY            execução normal
     RADAR_SIMULAR=1 python ...     sem gravar no banco e sem Telegram
"""

import os

import radar_comum
from termos import Termos, CONTEXTO_EVENTO, CONTEXTO_EXAME, CONTEXTO_SERVICO_EXAME, Trechos

# ================= CONFIGURAÇÕES =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    TELEGRAM_BOT_TOKEN = "8388155318:AAGrSb4FwLvAS51PZG4tmnapkM2V7p0lTYk"
if not CHAT_ID:
    CHAT_ID = "-5236299203"
if not SUPABASE_URL:
    SUPABASE_URL = "https://clcaoyrqhkxirfekcxot.supabase.co"
if not SUPABASE_KEY:
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNsY2FveXJxaGt4aXJmZWtjeG90Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE4NzUzNTUsImV4cCI6MjA4NzQ1MTM1NX0.vwTN5ACAylYO-FMQ5iEubJ2I-vZ9YMUHG-7pXk2YIOg"

# Termos que confirmam um ITEM (regras de casamento em termos.py). Item de
# ação judicial costuma vir só com a marca do produto, por isso as marcas.
KEYWORDS = [
    "canabidiol", "cannabidiol", "cannabis", "canabis", "canabinoide",
    "cannabinoid", "tetrahidrocanabinol", "tetraidrocanabinol", "cbd", "thc",
    "cânhamo", "marijuana", "maconha medicinal", "full spectrum",
    "Mevatyl", "Sativex", "Nabix", "Purodiol", "Isodiolex", "Elixinol", "Rsho",
    "Neotivex", "Health Meds", "HempMeds", "Charlotte's Web",
]

# Termos da busca do PNCP (publicados ontem/hoje e varredura dos abertos).
KEYWORDS_BUSCA = [
    "canabidiol", "cannabis", "CBD", "cannabidiol", "canabinoide",
    "tetrahidrocanabinol", "canabis",
]

TERMOS_ITENS = Termos(KEYWORDS)
# Sem itens no PNCP, o objeto precisa citar o produto por extenso (CBD/THC soltos não bastam).
TERMOS_OBJETO = Termos([k for k in KEYWORDS if k not in ("cbd", "thc")])

RADAR = radar_comum.Radar(
    nome="cannabis",
    titulo="🔍 RADAR FARMAUSA v3 — Licitações Cannabis",
    tabela_editais="editais_pncp",
    tabela_itens="itens_pncp",
    termos_itens=TERMOS_ITENS,
    termos_objeto=TERMOS_OBJETO,
    termos_busca=KEYWORDS_BUSCA,
    termos_abertos=KEYWORDS_BUSCA,
    exclui_item=CONTEXTO_EXAME,
    exclui_objeto=Trechos(CONTEXTO_EVENTO.trechos + CONTEXTO_EXAME.trechos),
    # Inscrição em congresso CBDA, curso, credenciamento de exames laboratoriais
    # ("Canabinoides" no meio de 800 exames): fora, mesmo com item que cite o termo.
    veta_objeto=Trechos(CONTEXTO_EVENTO.trechos + CONTEXTO_SERVICO_EXAME.trechos),
    cabecalho_alerta="🌿 <b>{tipo} — CANNABIS MEDICINAL</b>",
    nome_resumo="cannabis",
    emoji="🌿",
    supabase_url=SUPABASE_URL,
    supabase_key=SUPABASE_KEY,
    telegram_token=TELEGRAM_BOT_TOKEN,
    chat_id=CHAT_ID,
)

if __name__ == "__main__":
    radar_comum.executar(RADAR)
