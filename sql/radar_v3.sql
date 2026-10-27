-- Radar v3: rodar uma vez no SQL Editor do Supabase. Pode rodar de novo sem problema.
-- Os robôs funcionam sem isto; com isto passam a gravar unidade, exclusividade
-- ME/EPP, plataforma e o resumo dos documentos dos editais.

-- 1. Itens: unidade de fornecimento e benefício (exclusivo ME/EPP, cota reservada...)
alter table public.itens_pncp            add column if not exists unidade text;
alter table public.itens_pncp            add column if not exists beneficio text;
alter table public.itens_pncp_altocusto  add column if not exists unidade text;
alter table public.itens_pncp_altocusto  add column if not exists beneficio text;

-- 2. Editais: plataforma onde acontece a disputa (o link já fica em link_sistema_origem)
alter table public.editais_pncp            add column if not exists plataforma text;
alter table public.editais_pncp_altocusto  add column if not exists plataforma text;

-- 3. Documentos dos editais (documentos.py), lidos pelo painel farmausa.com/radar-licitacoes
create table if not exists public.documentos_edital (
  url_id          text primary key,                 -- /compras/{cnpj}/{ano}/{seq}
  situacao        text,                             -- situação da compra no PNCP
  atualizado_pncp text,                             -- dataAtualizacaoGlobal da última leitura completa
  arquivos        jsonb not null default '[]'::jsonb,
  resumo          jsonb not null default '{}'::jsonb,
  atualizado_em   timestamptz not null default now()
);

alter table public.documentos_edital enable row level security;

-- Os robôs gravam com a chave anon (como nas tabelas de editais e itens); o painel só lê.
drop policy if exists "documentos leitura" on public.documentos_edital;
drop policy if exists "documentos gravacao" on public.documentos_edital;
drop policy if exists "documentos atualizacao" on public.documentos_edital;
drop policy if exists "documentos remocao" on public.documentos_edital;
create policy "documentos leitura" on public.documentos_edital
  for select to anon, authenticated using (true);
create policy "documentos gravacao" on public.documentos_edital
  for insert to anon with check (true);
create policy "documentos atualizacao" on public.documentos_edital
  for update to anon using (true) with check (true);
create policy "documentos remocao" on public.documentos_edital
  for delete to anon using (true);

grant select, insert, update, delete on public.documentos_edital to anon, authenticated;

-- A API do Supabase passa a enxergar as colunas e a tabela novas na hora.
notify pgrst, 'reload schema';
