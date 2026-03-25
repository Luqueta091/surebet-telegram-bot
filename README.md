# Surebet Telegram Bot

Bot do Telegram em Python com menu interativo para explicar surebets. A aplicação foi preparada para deploy no Render usando webhook HTTP.

## Funcionalidades

- Menu `/start` com botões inline
- Telas:
  - `📘 O que é Surebet?`
  - `🧮 Como calcular?`
  - `⚡ Como usar as entradas?`
  - `💰 Qual banca usar?`
- Botão `🏢 Casas recomendadas` disponível na navegação
- Endpoint `/health` para health check
- Configuração automática do webhook quando `WEBHOOK_BASE_URL` estiver definido

## Executar localmente

1. Crie um ambiente virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Instale as dependências:

```bash
pip install -r requirements.txt
```

3. Configure as variáveis:

```bash
cp .env.example .env
```

4. Rode a aplicação:

```bash
python app.py
```

## Deploy no Render

Use um **Web Service** com:

- Runtime: `python`
- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn --bind 0.0.0.0:$PORT app:app`

Variáveis obrigatórias:

- `TELEGRAM_BOT_TOKEN`
- `WEBHOOK_SECRET`
- `WEBHOOK_BASE_URL`

Exemplo de webhook final:

```text
https://seu-servico.onrender.com/telegram/webhook/seu-segredo
```
