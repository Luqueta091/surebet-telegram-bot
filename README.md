# Surebet Telegram Bot

Bot completo para Telegram com:

- `python-telegram-bot` v20+
- explicações de Surebet via botões inline
- assinatura VIP com PIX da SyncPay
- webhook em Flask
- remoção automática de acesso com APScheduler
- persistência em SQLite

Toda a lógica principal está em `main.py`. O arquivo `app.py` existe apenas como ponte de compatibilidade para o start command atual do Render.

## Variáveis de ambiente

- `TELEGRAM_TOKEN`
- `SYNCPAY_CLIENT_ID`
- `SYNCPAY_CLIENT_SECRET`
- `SYNCPAY_API_BASE_URL`
- `SYNCPAY_WEBHOOK_TOKEN` (opcional, para validar o `Authorization` enviado pela SyncPay)
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
- Na primeira assinatura, o bot pede `CPF; email; telefone` para gerar o PIX da SyncPay.
- A SyncPay envia webhooks para `WEBHOOK_URL/webhook`; se você criar o webhook via painel/API deles, salve o token retornado em `SYNCPAY_WEBHOOK_TOKEN`.
- `WEBHOOK_URL` deve ser uma URL HTTPS pública, por exemplo `https://seu-bot.onrender.com`.
- Para não perder o SQLite em deploys do Render, aponte `DATABASE_PATH` para o disco persistente, por exemplo `/var/data/assinantes.db`.
- O endpoint de health check é `/health` e retorna `OK`.
