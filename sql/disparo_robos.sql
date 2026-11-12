-- Disparo dos robôs do radar pelo Supabase (pg_cron + pg_net).
--
-- O agendamento do GitHub não executa nada de manhã neste repositório (em 12/11/2026 a
-- primeira execução do dia foi às 15h07). Aqui o banco chama a API do GitHub
-- (workflow_dispatch) de hora em hora, das 8h às 17h de Brasília, em dia útil, e os
-- robôs rodam na hora. O agendamento do GitHub fica só de reserva (horario.py).
--
-- ANTES de rodar este arquivo: guardar a chave do GitHub no Vault do Supabase com o
-- nome github_radar_disparo (pelo painel, sem colar a chave em SQL). A chave é um
-- fine-grained token de igor-igdata, só do repositório radar-farmausa, com
-- "Actions: Read and write".
-- Para trocar a chave depois: atualizar o mesmo segredo no Vault; nada aqui muda.

create extension if not exists pg_cron with schema pg_catalog;
create extension if not exists pg_net with schema extensions;

-- schema fora da API (o PostgREST só expõe public): a chave anon não chega aqui
create schema if not exists radar_interno;
revoke all on schema radar_interno from public, anon, authenticated;

create or replace function radar_interno.disparar_robos()
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  chave text;
  arquivo text;
begin
  select decrypted_secret into chave from vault.decrypted_secrets where name = 'github_radar_disparo';
  if chave is null then
    raise exception 'Falta a chave github_radar_disparo no Vault do Supabase';
  end if;
  foreach arquivo in array array['automacao.yml', 'radar-altocusto.yml'] loop
    perform net.http_post(
      url := 'https://api.github.com/repos/igor-igdata/radar-farmausa/actions/workflows/' || arquivo || '/dispatches',
      body := jsonb_build_object('ref', 'main'),
      headers := jsonb_build_object(
        'Authorization', 'Bearer ' || chave,
        'Accept', 'application/vnd.github+json',
        'X-GitHub-Api-Version', '2022-11-28',
        'User-Agent', 'radar-farmausa-supabase',
        'Content-Type', 'application/json'));
  end loop;
end;
$$;

revoke all on function radar_interno.disparar_robos() from public, anon, authenticated;

-- De hora em hora, das 8h às 17h de Brasília, segunda a sexta. O pg_cron do Supabase
-- usa UTC (Brasília = UTC-3, sem horário de verão). Rodar de novo só atualiza o horário.
select cron.schedule('radar-disparo-robos', '0 11-20 * * 1-5', 'select radar_interno.disparar_robos()');

-- Conferência (rodar à parte quando precisar):
--   select radar_interno.disparar_robos();          -- dispara os dois robôs agora
--   select id, status_code, created from net._http_response order by id desc limit 2;
--                                                   -- 204 = o GitHub aceitou; 401 = chave vencida ou errada
--   select status, start_time, return_message from cron.job_run_details
--    where jobid = (select jobid from cron.job where jobname = 'radar-disparo-robos')
--    order by start_time desc limit 10;            -- histórico do agendamento
-- Para desligar: select cron.unschedule('radar-disparo-robos');
