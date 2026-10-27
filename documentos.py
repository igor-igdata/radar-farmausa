"""
Leitura dos documentos dos editais abertos (edital, termo de referência,
impugnações, decisões, avisos), com o resumo gravado em documentos_edital.

- A lista de arquivos vem de /api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos.
- Só relê um edital quando o PNCP mudou alguma coisa nele (dataAtualizacaoGlobal).
- Do edital e do termo de referência tira trechos de validade, prazo de entrega,
  amostra, registro na ANVISA, exclusividade ME/EPP, ação judicial e sessão.
- Dos demais documentos (impugnação, decisão, errata, aviso) tira trechos de
  remarcação, suspensão, revogação e do resultado da impugnação: o órgão costuma
  remarcar a sessão no PDF sem mudar a data no cadastro do PNCP.
- Documento novo em edital já lido vira aviso no Telegram.
Sem a tabela documentos_edital no banco, ou sem pypdf, a leitura fica desligada.
"""

import datetime
import time
import io
import logging
import re
import unicodedata
import zipfile

import radar_comum as rc

log = logging.getLogger("radar")

TABELA = "documentos_edital"
MAX_PDFS_POR_EXECUCAO = 30
MAX_MB = 25
MAX_PAGINAS = 200

# (classe, padrão no título + tipo do documento). A ordem importa: a decisão
# sobre uma impugnação é "decisao", não "impugnacao".
CLASSES = [
    ("decisao", r"decis|julgament|parecer|resposta"),
    ("impugnacao", r"impugna|recurso"),
    ("esclarecimento", r"esclarec|pedido"),
    ("revogacao", r"revoga|anula|cancelad|deserta|fracassad"),
    ("suspensao", r"suspens|adiament|adiad|remarc|prorroga|nova data|reabertura"),
    ("errata", r"errat|retifica|alterac|aditament|adendo"),
    ("edital", r"^edital|edital$|aviso de contratacao|aviso de dispensa"),
    ("termo_referencia", r"termo de referencia|\btr\b|projeto basico|especificac"),
    ("ata", r"\bata\b|homologa|adjudica|resultado"),
]
PRINCIPAIS = {"edital", "termo_referencia"}
AVISOS = {"decisao", "impugnacao", "esclarecimento", "revogacao", "suspensao", "errata"}
NOMES_CLASSE = {
    "decisao": "Decisão", "impugnacao": "Impugnação", "esclarecimento": "Esclarecimento",
    "revogacao": "Revogação/anulação", "suspensao": "Suspensão/remarcação", "errata": "Errata/retificação",
    "edital": "Edital", "termo_referencia": "Termo de referência", "ata": "Ata/resultado", "outro": "Documento",
}

NUMERO = r"\d+ ?(\([^)]{0,30}\) ?)?"
# tema: (achado, detalhe que precisa estar por perto ou None, vale o primeiro
# achado mesmo sem o detalhe, o que logo depois do achado o desqualifica)
TEMAS = {
    "validade": (r"validade (minima|nao inferior|de no minimo|remanescente|igual ou superior|superior a|de pelo menos)"
                 r"|prazo de validade|validade dos? (medicamentos?|produtos?|itens?|objeto)",
                 NUMERO + r"(%|por cento|mes|meses|dias|anos)", False,
                 r"(validade|vigencia) (da|das|desta|de sua) (proposta|propostas|ata|registro)"),
    "entrega": (r"prazo (maximo )?(de|para) (a )?entrega|prazo de fornecimento"
                r"|entreg\w* (no prazo (maximo )?de |em |no maximo |dentro de |no prazo de )?(ate )?\d+",
                NUMERO + r"(dias|horas)", False, r"entrega (das|da) (proposta|propostas|documentacao)"),
    "amostra": (r"\bamostras?\b", r"exigid|exigencia|apresenta|prazo|dispensad|nao sera", True, None),
    "anvisa": (r"registro (valido )?(na|no|junto a|junto ao|perante a) ?(anvisa|agencia nacional de vigilancia|ministerio da saude)"
               r"|rdc (n\W{0,3})?327|autorizacao sanitaria|autorizacao de funcionamento", None, True, None),
    "me_epp": (r"exclusiv\w* (para |a |as |de |a participacao de )?(me\b|microempresa|empresas de pequeno)"
               r"|cota (reservada|de ate 25)|nao havendo exclusividade|ampla (participacao|concorrencia)",
               None, True, None),
    "judicial": (r"mandados? judic|mandado de seguranca|ordens? judic|setor judicial|judicializad|demandas? judic"
                 r"|(atend\w+|cumpri\w+|para) (a |as |o |os |de |da |do |das |dos )?(decis\w+|determina\w+|aco\w+|processos?) judic",
                 None, True, None),
    "marca": (r"marcas? (de referencia|referencia|especifica|exigida|padronizada|padrao)|referencia (de )?marca"
              r"|exclusivamente (a|da) marca|padronizacao (de|da) marca|(ou|e) (similar|equivalente)"
              r"|vedad\w* (a )?(indicacao|preferencia) (de|por) marca|nao (sera|serao) aceit\w* marca",
              None, True, None),
    "marca_proposta": (r"(indicar|informar|constar|cotar|especificar|mencionar|conter|declarar)\w* (a |as |obrigatoriamente (a |as )?)?marcas?"
                       r"|marcas?( e|,| ?/) ?(o |a )?(fabricante|modelo|procedencia)",
                       None, True, None),
    # Onde acontece a disputa: o PNCP só informa o sistema que publicou.
    "plataforma": (r"bll ?compras|bllcompras|bll\.org|bolsa de licitacoes e leiloes|bnc ?compras|bnccompras"
                   r"|bolsa nacional de compras|licitanet|portal de compras publicas|portaldecompraspublicas"
                   r"|compras\.gov\.br|comprasnet|gov\.br/compras|licitacoes-e|licitacoes e do banco do brasil"
                   r"|bec\.sp|bolsa eletronica de compras|compras\.mg|portal de compras do estado|licitar digital"
                   r"|licitardigital|ammlicita|comprasbr|compras br|bbmnet|bbm net|e-?licita|publinexo"
                   r"|banrisul|compraspublicas|portal ?de ?compras|siga\.|pregao ?online|plataforma (eletronica|digital|de compras)",
                   None, True, None),
    "sessao": (r"sessao publica|abertura da sessao|data da sessao|inicio da (sessao|disputa)|data de abertura"
               r"|abertura e julgamento das propostas|recebimento das propostas",
               r"\d{1,2}/\d{1,2}/\d{2,4}|\d{1,2} ?h ?\d{0,2}|\d{1,2}:\d{2}", False, None),
}
# Trechos procurados nos demais documentos.
AVISOS_TEXTO = (r"remarca|nova data|redesigna|fica(m)? (adiad|suspens|prorrogad|remarcad)|adiamento|"
                r"suspens(ao|a) (do|da|de|dos) (certame|sessao|pregao|licitacao|processo|prazo)|reabertura|revoga|anula|"
                r"(julg\w*|considera\w*|declara\w*) (parcialmente )?(procedente|improcedente)|"
                r"\b(deferid|indeferid)\w*|dar provimento|negar provimento|"
                r"alterad[ao] (a|o) (data|prazo)")
# Remarcação ou suspensão citada num aviso (não a "suspensão de licitar" das sanções).
REMARCACAO = (r"remarca|nova data|redesigna|adiad|adiament|fica(m)? suspens|"
              r"suspens(ao|a) (do|da|de|dos) (certame|sessao|pregao|licitacao|processo|prazo)")


def alertas_do_edital(arquivos, avisos, situacao):
    """Rótulos curtos para o painel marcar o edital na lista."""
    classes = {a["classe"] for a in arquivos}
    textos = " ".join(_sem_acento(x["trecho"]) for x in avisos)
    saida = []
    for classe, rotulo in (("impugnacao", "Impugnação"), ("decisao", "Decisão"),
                           ("esclarecimento", "Esclarecimento"), ("errata", "Errata")):
        if classe in classes:
            saida.append(rotulo)
    if "suspensao" in classes or re.search(REMARCACAO, textos):
        saida.append("Remarcação/suspensão")
    if "revogacao" in classes or re.search(r"revog|anul", _sem_acento(situacao or "")):
        saida.append("Revogação/anulação")
    return saida


def _sem_acento(t):
    """Mesmo tamanho do texto original, para recortar o trecho no texto com acento."""
    return "".join(unicodedata.normalize("NFKD", c)[:1] or c for c in t).lower()


def _titulo(a):
    t = re.sub(r"[_]+", " ", a.get("titulo") or "").strip()
    return re.sub(r"\s+", " ", t) or a.get("tipoDocumentoNome") or "Documento"


def classificar(a):
    base = _sem_acento(f"{_titulo(a)} {a.get('tipoDocumentoNome') or ''}")
    base = re.sub(r"[^a-z0-9]+", " ", base).strip()
    tipo = _sem_acento(a.get("tipoDocumentoNome") or "")
    if tipo in ("edital", "aviso de contratacao direta"):
        return "edital"
    if "termo de referencia" in tipo:
        return "termo_referencia"
    for classe, padrao in CLASSES:
        if re.search(padrao, base):
            return classe
    return "outro"


def _trecho(texto, inicio, fim, largura=260):
    """Recorta a frase em volta do achado."""
    a = max(0, inicio - largura // 2)
    b = min(len(texto), fim + largura)
    pedaco = texto[a:b]
    rel = inicio - a
    corte = max(pedaco.rfind(". ", 0, rel), pedaco.rfind("; ", 0, rel), pedaco.rfind("\n", 0, rel))
    if corte >= 0:
        pedaco, rel = pedaco[corte + 1:], rel - corte - 1
    fim_frase = re.search(r"[.;](\s|$)", pedaco[rel + (fim - inicio):])
    if fim_frase:
        pedaco = pedaco[: rel + (fim - inicio) + fim_frase.end()]
    return re.sub(r"\s+", " ", pedaco).strip()[:400]


def extrair_temas(texto):
    base = _sem_acento(texto)
    achados = {}
    for tema, (padrao, detalhe, vale_sem_detalhe, evitar) in TEMAS.items():
        escolhido = None
        for n, m in enumerate(re.finditer(padrao, base)):
            if n >= 60:
                break
            janela = base[m.start(): m.end() + 200]
            if re.search(r"\.{5,}", janela):   # linha de sumário
                continue
            if evitar and re.search(evitar, base[max(0, m.start() - 20): m.end() + 40]):
                continue
            if not detalhe or re.search(detalhe, janela):
                escolhido = m
                break
            if escolhido is None and vale_sem_detalhe:
                escolhido = m
        if escolhido:
            achados[tema] = _trecho(texto, escolhido.start(), escolhido.end())
    return achados


def extrair_avisos(texto):
    base = _sem_acento(texto)
    trechos = []
    for m in re.finditer(AVISOS_TEXTO, base):
        t = _trecho(texto, m.start(), m.end())
        if t and all(t[:80] not in x for x in trechos):
            trechos.append(t)
        if len(trechos) >= 3:
            break
    return trechos


def texto_do_pdf(conteudo):
    """Texto de um PDF (ou dos PDFs dentro de um ZIP). '' se não há texto."""
    import pypdf
    if conteudo[:2] == b"PK":
        partes = []
        try:
            with zipfile.ZipFile(io.BytesIO(conteudo)) as z:
                for nome in z.namelist()[:10]:
                    if nome.lower().endswith(".pdf") and z.getinfo(nome).file_size <= MAX_MB * 2**20:
                        partes.append(texto_do_pdf(z.read(nome)))
        except zipfile.BadZipFile:
            return ""
        return "\n".join(partes)
    if conteudo[:5] != b"%PDF-":
        return ""
    try:
        leitor = pypdf.PdfReader(io.BytesIO(conteudo))
        return "\n".join((p.extract_text() or "") for p in leitor.pages[:MAX_PAGINAS])
    except Exception as e:
        log.warning(f"      PDF ilegível: {e.__class__.__name__}")
        return ""


class Leitor:
    def __init__(self, radar, limite=MAX_PDFS_POR_EXECUCAO):
        self.radar = radar
        self.limite = limite
        self.pdfs = 0
        self.linhas = None
        self.ativo = True
        try:
            import pypdf  # noqa: F401
        except ImportError:
            log.warning("📄 pypdf não instalado: leitura de documentos desligada")
            self.ativo = False

    def _carregar(self):
        try:
            r = rc._sb(self.radar, "GET", TABELA, params={"select": "url_id", "limit": 1}, prefer="")
            status = r.status_code
        except Exception as e:
            status = e.__class__.__name__
        if status != 200:
            log.warning(f"📄 Tabela {TABELA} indisponível ({status}): leitura de documentos desligada")
            self.ativo = False
            return
        linhas = rc.sb_ler(self.radar, TABELA, "url_id,situacao,atualizado_pncp,arquivos,resumo", "url_id") or []
        self.linhas = {l["url_id"]: l for l in linhas}

    def _baixar(self, url):
        """Conteúdo do arquivo; b"" se não vale a pena ler (grande demais, não
        existe); None se o PNCP não entregou agora (tenta na próxima execução)."""
        for tentativa in range(3):
            if tentativa:
                time.sleep(5 * tentativa)
            # Com "Accept: application/json" o PNCP não entrega o arquivo.
            r = rc._sessao.get(url, headers={**rc.PNCP_HEADERS, "Accept": "*/*"}, timeout=90, stream=True)
            if r.status_code == 404:
                return b""
            if r.status_code != 200:   # 422/429/5xx: o PNCP limita o ritmo de downloads
                log.info(f"      download respondeu {r.status_code}")
                continue
            if int(r.headers.get("content-length") or 0) > MAX_MB * 2**20:
                log.info("      arquivo grande demais, não lido")
                return b""
            dados = b""
            for bloco in r.iter_content(2**16):
                dados += bloco
                if len(dados) > MAX_MB * 2**20:
                    log.info("      arquivo grande demais, não lido")
                    return b""
            return dados
        return None

    def atualizar(self, edital, compra, alertar=True):
        if not self.ativo:
            return
        if self.linhas is None:
            self._carregar()
            if not self.ativo:
                return
        url_id = edital["url_id"]
        cnpj, ano, seq = rc.partes_url_id(url_id)
        antigo = self.linhas.get(url_id)
        situacao = compra.get("situacaoCompraNome") or ""
        marca = compra.get("dataAtualizacaoGlobal") or compra.get("dataAtualizacao") or ""
        if antigo and antigo.get("atualizado_pncp") == marca and (antigo.get("situacao") or "") == situacao:
            return

        r = rc._get_pncp(f"{rc.PNCP}/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/arquivos")
        if r is None or r.status_code not in (200, 204):
            return
        lista = (rc._json(r) or []) if r.status_code == 200 else []
        arquivos = []
        for a in lista:
            if a.get("statusAtivo") is False:
                continue
            arquivos.append({
                "seq": a.get("sequencialDocumento"),
                "titulo": _titulo(a),
                "tipo": a.get("tipoDocumentoNome") or "",
                "classe": classificar(a),
                "data": a.get("dataPublicacaoPncp"),
                "url": a.get("url") or a.get("uri"),
            })
        arquivos.sort(key=lambda x: (x["data"] or "", x["seq"] or 0))

        resumo = dict((antigo or {}).get("resumo") or {})
        lidos = set(resumo.get("lidos") or [])
        exig = dict(resumo.get("exigencias") or {})
        avisos = list(resumo.get("avisos") or [])
        completo = True
        for a in arquivos:
            if a["seq"] in lidos:
                continue
            if a["classe"] not in PRINCIPAIS | AVISOS or not a["url"]:
                lidos.add(a["seq"])
                continue
            if self.pdfs >= self.limite:
                completo = False
                break
            self.pdfs += 1
            log.info(f"   📄 Lendo {NOMES_CLASSE[a['classe']].lower()}: {a['titulo'][:60]} ({url_id})")
            try:
                conteudo = self._baixar(a["url"])
            except Exception as e:
                log.warning(f"      download falhou: {e.__class__.__name__}")
                conteudo = None
            if conteudo is None:
                completo = False   # fica para a próxima execução
                continue
            time.sleep(1)
            texto = texto_do_pdf(conteudo) if conteudo else ""
            lidos.add(a["seq"])
            if not texto.strip():
                continue
            if a["classe"] in PRINCIPAIS:
                for tema, trecho in extrair_temas(texto).items():
                    exig.setdefault(tema, {"trecho": trecho, "doc": a["seq"]})
            else:
                for trecho in extrair_avisos(texto):
                    avisos.append({"doc": a["seq"], "classe": a["classe"], "titulo": a["titulo"],
                                   "data": a["data"], "trecho": trecho})

        linha = {
            "url_id": url_id,
            "situacao": situacao,
            "atualizado_pncp": marca if completo else None,   # incompleto: relê na próxima
            "arquivos": arquivos,
            "resumo": {"lidos": sorted(x for x in lidos if x is not None), "exigencias": exig, "avisos": avisos,
                       "alertas": alertas_do_edital(arquivos, avisos, situacao)},
            "atualizado_em": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        if not rc.sb_gravar(self.radar, "POST", TABELA, params={"on_conflict": "url_id"}, json=linha,
                            descricao=f"{url_id} ({len(arquivos)} arquivo(s))",
                            prefer="resolution=merge-duplicates,return=minimal"):
            return
        self.linhas[url_id] = linha

        if not (alertar and antigo):
            return
        conhecidos = {a.get("seq") for a in antigo.get("arquivos") or []}
        novos = [a for a in arquivos if a["seq"] not in conhecidos]
        if novos:
            textos = [x["trecho"] for x in avisos if x["doc"] in {a["seq"] for a in novos}]
            rotulados = [{**a, "tipo": NOMES_CLASSE.get(a["classe"], a["tipo"])} for a in novos]
            rc.alerta_documento(self.radar, edital, rotulados, textos)
        antes = antigo.get("situacao") or ""
        if situacao and antes and situacao != antes and re.search(r"revog|anul|suspens", _sem_acento(situacao)):
            rc.alerta_situacao(self.radar, edital, situacao)
