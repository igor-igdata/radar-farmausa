"""
Casamento de termos nas descrições do PNCP (itens e objeto das compras).

Regras, valendo para os dois radares:
- compara sem acento e sem diferença de maiúsculas;
- o termo tem que começar uma palavra ("evr" não casa mais com "mionevrix");
- termos de até 5 letras (CBD, THC, siglas) só casam a palavra inteira, mas
  aceitam número colado ("CBD200MG");
- nos demais, a última vogal vira opcional e a palavra pode ter até 2 letras a
  mais: "Dantroleno" casa "DANTROLENE", "Daratumumabe" casa "DARATUMUMAB",
  "Tirzepatide" casa "TIRZEPATIDA" e "Zonisamida" casa "ZONISAMIDAS";
- termo com mais de uma palavra casa com até 3 palavras no meio
  ("Fenilbutirato Sodio" casa "fenilbutirato de sódio").
"""

import re
import unicodedata


def normalizar(texto):
    """Minúsculas, sem acento, só letras e números separados por um espaço."""
    t = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode().lower()
    return " " + re.sub(r"[^a-z0-9]+", " ", t).strip() + " "


def _padrao_palavra(palavra):
    if len(palavra) <= 5 or not palavra.isalpha():
        return re.escape(palavra) + r"(?![a-z])"
    raiz = palavra[:-1] if palavra[-1] in "aeo" else palavra
    return re.escape(raiz) + r"[a-z]{0,2}(?![a-z])"


def _padrao_termo(termo):
    palavras = normalizar(termo).split()
    if not palavras:
        return None
    return r"(?<![a-z0-9])" + r"(?: [a-z0-9]+){0,3} ".join(_padrao_palavra(p) for p in palavras)


class Termos:
    """Lista de termos que sabe dizer se um texto cita algum deles, e quais."""

    def __init__(self, termos):
        self.termos = list(dict.fromkeys(t for t in termos if normalizar(t).strip()))
        self._um_a_um = [(t, re.compile(_padrao_termo(t))) for t in self.termos]
        self._todos = re.compile("|".join("(?:%s)" % _padrao_termo(t) for t in self.termos)) if self.termos else None

    def casa(self, texto):
        return bool(self._todos and self._todos.search(normalizar(texto)))

    def achados(self, texto):
        n = normalizar(texto)
        return [t for t, rx in self._um_a_um if rx.search(n)]


class Trechos:
    """Começos de palavra que bastam aparecer no texto (sem acento).
    "curso" casa "cursos", mas não "recursos"."""

    def __init__(self, trechos):
        self.trechos = [normalizar(t).rstrip() for t in trechos]

    def casa(self, texto):
        n = normalizar(texto)
        return any(t in n for t in self.trechos)

    def achados(self, texto):
        n = normalizar(texto)
        return [t.strip() for t in self.trechos if t in n]


# Objeto de compra que não é medicamento, embora cite o termo: inscrição em
# congresso (o "CBD" da busca casa CBDA/CBDE), curso, patrocínio etc.
CONTEXTO_EVENTO = Trechos([
    "inscricao", "inscricoes", "congresso", "seminario", "simposio", "curso",
    "capacitacao", "treinamento", "palestra", "evento", "patrocinio",
    "forum", "workshop", "conferencia", "jornada", "hospedagem",
    "passagem aerea", "passagens aereas", "diarias",
])

# Item de exame de laboratório que procura a substância (urina, teste de triagem).
CONTEXTO_EXAME = Trechos([
    "teste", "testes", "triagem", "drogas de abuso", "toxicolog",
    "imunocromatograf", "reagente", "exame", "exames", "deteccao", "urina",
    "laboratorial", "laboratoriais", "analises clinicas",
])

# Objeto que fala em compra de medicamento: derruba o veto por evento/exame
# ("medicamentos ... e exames para paciente de mandado", município de Patrocínio).
CONTEXTO_MEDICAMENTO = Trechos(["medicament", "farmac", "fitoterap"])

# Objeto de serviço de laboratório: credenciamento de exames com centenas de
# itens, um deles "Canabinoides" ou "CBD - Coombs direto".
CONTEXTO_SERVICO_EXAME = Trechos([
    "exame", "exames", "laboratorial", "laboratoriais", "analises clinicas",
    "toxicolog", "patologia clinica",
])
