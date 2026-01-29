# Supabase (Local Development)

This repo uses Supabase Local for persistence and analytics (Postgres + PostgREST).

## Prereqs
- Docker running
- Node (for `npx`)

## Start Supabase Local
From repo root:

```bash
npx -y supabase start --yes
```

Default ports for this repo (see `supabase/config.toml`):
- API / PostgREST: `http://127.0.0.1:55421`
- Postgres: `postgresql://postgres:postgres@127.0.0.1:55422/postgres`
- Studio: `http://127.0.0.1:55423`

## Apply migrations
If you need to re-apply migrations from scratch:

```bash
npx -y supabase db reset --local --yes
```

## Configure backend writes
Get the local API URL + service role key:

```bash
npx -y supabase status -o env
```

Then set these in `backend/.env`:
- `SUPABASE_WRITE_ENABLED=1`
- `SUPABASE_URL=<API_URL>`
- `SUPABASE_SERVICE_ROLE_KEY=<SERVICE_ROLE_KEY>`

## Stop Supabase Local

```bash
npx -y supabase stop --yes
```

