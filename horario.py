"""
Trava das execuções agendadas pelo GitHub (job "horario" dos workflows).

Quem dispara os robôs no horário é o Supabase (sql/disparo_robos.sql), de hora em
hora das 8h às 17h de Brasília em dia útil, por workflow_dispatch. O agendamento do
GitHub atrasa horas (em 12/11/2026 a primeira execução do dia foi às 15h07) e fica
só de reserva: a tentativa roda se for dia útil, das 8h às 18h, e se não houve
execução de verdade nos últimos 70 minutos, isto é, se o Supabase perdeu uma hora.
Disparo pelo Supabase ou manual roda sempre (o workflow nem chama a trava).

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
INICIO, FIM = 8, 18           # horário comercial: das 8h até antes das 18h
INTERVALO_MIN = 70            # o Supabase dispara a cada 60 min; reserva só se ele falhar
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
