"""
Trava das execuções agendadas dos radares (job "horario" dos workflows).

O agendamento do GitHub atrasa horas e pula execuções: com 6 horários por dia,
rodava 2 vezes, à tarde e à noite. Por isso os workflows tentam a cada 15
minutos e esta trava decide se a tentativa roda de verdade: só em dia útil,
das 7h às 18h de Brasília, e com pelo menos 50 minutos desde a última execução
de verdade. Assim sai mais ou menos uma rodada por hora, com 9h e 12h incluídas.
Disparo manual pelo GitHub roda sempre (o workflow nem chama a trava).

Uso: python horario.py <execucoes.json>
     (o arquivo é a saída de gh run list --json databaseId,status,createdAt,startedAt,updatedAt)
Escreve rodar=sim ou rodar=nao em $GITHUB_OUTPUT.
"""
import datetime
import json
import os
import sys
from zoneinfo import ZoneInfo

BRASILIA = ZoneInfo("America/Sao_Paulo")
INICIO, FIM = 7, 18           # horário comercial: das 7h até antes das 18h
INTERVALO_MIN = 50            # minutos entre duas execuções de verdade
SO_PASSOU_SEG = 120           # tentativa barrada aqui termina em segundos


def _hora(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))


def decidir(agora, execucoes, eu):
    """None se roda; senão o motivo de não rodar."""
    local = agora.astimezone(BRASILIA)
    if local.isoweekday() > 5 or not INICIO <= local.hour < FIM:
        return f"fora do horário comercial ({local:%d/%m %H:%M})"
    for r in execucoes:
        if r["databaseId"] == eu:
            continue
        # em andamento, ou que durou mais que uma passagem pela trava
        de_verdade = r["status"] != "completed" or (
            (_hora(r["updatedAt"]) - _hora(r["startedAt"])).total_seconds() > SO_PASSOU_SEG)
        if de_verdade and agora - _hora(r["createdAt"]) < datetime.timedelta(minutes=INTERVALO_MIN):
            return f"já rodou às {_hora(r['createdAt']).astimezone(BRASILIA):%H:%M}"
    return None


if __name__ == "__main__":
    execucoes = json.load(open(sys.argv[1], encoding="utf-8"))
    motivo = decidir(datetime.datetime.now(datetime.timezone.utc), execucoes, int(os.environ["GITHUB_RUN_ID"]))
    print(f"Não roda: {motivo}" if motivo else "Roda")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as saida:
        saida.write(f"rodar={'nao' if motivo else 'sim'}\n")
