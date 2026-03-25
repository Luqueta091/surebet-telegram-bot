# Surebet Telegram Bot

Bot completo para Telegram com:

- `python-telegram-bot` v20+
- explicações de Surebet via botões inline
- assinatura VIP com PIX do Mercado Pago
- webhook em Flask
- remoção automática de acesso com APScheduler
- persistência em SQLite

Toda a lógica principal está em `main.py`. O arquivo `app.py` existe apenas como ponte de compatibilidade para o start command atual do Render.

## Variáveis de ambiente

- `TELEGRAM_TOKEN`
- `MP_ACCESS_TOKEN`
- `GRUPO_VIP_ID`
- `WEBHOOK_URL`
- `DATABASE_PATH` (opcional, recomendado no Render para apontar para um disco persistente)

## Executar localmente

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

## Deploy no Render

Use um **Web Service** com:

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn --bind 0.0.0.0:$PORT app:app`

## Observações

- O bot precisa ser administrador do grupo VIP para criar links de convite e remover membros.
- `WEBHOOK_URL` deve ser uma URL HTTPS pública, por exemplo `https://seu-bot.onrender.com`.
- Para não perder o SQLite em deploys do Render, aponte `DATABASE_PATH` para o disco persistente, por exemplo `/var/data/assinantes.db`.
- O endpoint de health check é `/health` e retorna `OK`.
