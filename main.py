from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import mercadopago
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import Flask, request
from mercadopago.config import RequestOptions
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler, ContextTypes

load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("surebet-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

APP_TIMEZONE = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Sao_Paulo"))
DATABASE_PATH = Path(__file__).with_name("assinantes.db")
VIP_PRICE = 29.90
VIP_PRICE_TEXT = "R$29,90"
VIP_DURATION_DAYS = 30

TELEGRAM_TOKEN = (
    os.environ.get("TELEGRAM_TOKEN", "").strip()
    or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
)
MP_ACCESS_TOKEN = os.environ.get("MP_ACCESS_TOKEN", "").strip()
GRUPO_VIP_ID_RAW = os.environ.get("GRUPO_VIP_ID", "").strip()
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip()

START_TEXT = """Olá! Sou o assistente do grupo de Surebet 👋

Escolha o que você quer aprender:

📘 O que é Surebet?
🧮 Como calcular?
⚡ Como usar as entradas?
💰 Qual banca usar?
💳 Assinar VIP"""

SUREBET_TEXT = """📘 O QUE É SUREBET?

Surebet significa aposta segura. É quando você aposta em todos os resultados possíveis de um jogo em casas diferentes e garante lucro independente do resultado.

Como isso é possível? As casas competem entre si e às vezes oferecem odds em desacordo. Quando a soma das probabilidades fica abaixo de 100%, você lucra.

Exemplo:
- Casa A: Time 1 vence → odd 2.10
- Casa B: Time 2 vence → odd 2.10
Apostando R$100 em cada lado → recebe R$210 em qualquer resultado → lucro de R$10 ✅

Isso não é sorte. É matemática."""

CALCULATION_TEXT = """🧮 COMO CALCULAR UMA SUREBET?

Fórmula:
(1 ÷ odd1) + (1 ÷ odd2) menor que 1 = SUREBET ✅

Exemplo:
- Casa A: odd 2.10 → 1 ÷ 2.10 = 0.476
- Casa B: odd 2.10 → 1 ÷ 2.10 = 0.476
- Soma: 0.952 ✅

Como distribuir a banca:
Stake A = (Banca ÷ odd A) ÷ soma total

💡 Aqui no grupo as entradas já vêm com os valores calculados!"""

ENTRY_USAGE_TEXT = """⚡ COMO USAR AS ENTRADAS DO GRUPO?

1️⃣ Ative as notificações do grupo
2️⃣ Ao receber a entrada, abra as casas indicadas
3️⃣ Aposte o valor exato em cada casa
4️⃣ Lucro garantido!

⚠️ As odds mudam em minutos. Aja rápido e nunca aposte em apenas um lado."""

BANKROLL_TEXT = """💰 GESTÃO DE BANCA

Use entre 5% e 10% da banca por entrada.

Exemplo:
Banca: R$500 → Por entrada: R$25 a R$50

Casas recomendadas:
- Bet365
- Betano
- Sportingbet
- Betfair

💡 Tenha pelo menos 3 casas cadastradas."""

CALLBACK_SUREBET = "surebet_info"
CALLBACK_CALC = "surebet_calc"
CALLBACK_ENTRIES = "surebet_entries"
CALLBACK_BANKROLL = "surebet_bankroll"
CALLBACK_SUBSCRIBE = "vip_subscribe"
CALLBACK_MENU = "back_to_menu"

app = Flask(__name__)

BOOTSTRAP_LOCK = threading.Lock()
SERVICES_STARTED = False
BOT_READY = threading.Event()
BOT_THREAD: threading.Thread | None = None
TELEGRAM_APP: Application | None = None
TELEGRAM_LOOP: asyncio.AbstractEventLoop | None = None
SCHEDULER: BackgroundScheduler | None = None


def normalized_group_vip_id() -> int | None:
    if not GRUPO_VIP_ID_RAW:
        return None
    try:
        return int(GRUPO_VIP_ID_RAW)
    except ValueError:
        logger.error("GRUPO_VIP_ID inválido: %s", GRUPO_VIP_ID_RAW)
        return None


def notification_webhook_url() -> str:
    if not WEBHOOK_URL:
        return ""
    if WEBHOOK_URL.rstrip("/").endswith("/webhook"):
        return WEBHOOK_URL.rstrip("/")
    return f"{WEBHOOK_URL.rstrip('/')}/webhook"


def current_date() -> datetime.date:
    return datetime.now(APP_TIMEZONE).date()


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📘 O que é Surebet?", callback_data=CALLBACK_SUREBET)],
            [InlineKeyboardButton("🧮 Como calcular?", callback_data=CALLBACK_CALC)],
            [InlineKeyboardButton("⚡ Como usar as entradas?", callback_data=CALLBACK_ENTRIES)],
            [InlineKeyboardButton("💰 Qual banca usar?", callback_data=CALLBACK_BANKROLL)],
            [InlineKeyboardButton("💳 Assinar VIP", callback_data=CALLBACK_SUBSCRIBE)],
        ]
    )


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)]]
    )


def database_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_database() -> None:
    with database_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS assinantes (
                user_id INTEGER PRIMARY KEY,
                nome TEXT,
                status TEXT,
                data_pagamento DATE,
                vencimento DATE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pagamentos_processados (
                payment_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                processed_at TEXT NOT NULL
            )
            """
        )


def get_assinante(user_id: int) -> sqlite3.Row | None:
    with database_connection() as connection:
        return connection.execute(
            "SELECT * FROM assinantes WHERE user_id = ?",
            (user_id,),
        ).fetchone()


def save_pending_assinante(user_id: int, nome: str) -> None:
    with database_connection() as connection:
        connection.execute(
            """
            INSERT INTO assinantes (user_id, nome, status, data_pagamento, vencimento)
            VALUES (?, ?, 'pendente', NULL, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                nome = excluded.nome,
                status = 'pendente',
                data_pagamento = NULL,
                vencimento = NULL
            """,
            (user_id, nome),
        )


def activate_assinante(user_id: int, nome: str, data_pagamento: str, vencimento: str) -> None:
    with database_connection() as connection:
        connection.execute(
            """
            INSERT INTO assinantes (user_id, nome, status, data_pagamento, vencimento)
            VALUES (?, ?, 'ativo', ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                nome = excluded.nome,
                status = 'ativo',
                data_pagamento = excluded.data_pagamento,
                vencimento = excluded.vencimento
            """,
            (user_id, nome, data_pagamento, vencimento),
        )


def mark_assinante_expired(user_id: int) -> None:
    with database_connection() as connection:
        connection.execute(
            "UPDATE assinantes SET status = 'vencido' WHERE user_id = ?",
            (user_id,),
        )


def get_assinantes_expiring_on(vencimento: str) -> list[sqlite3.Row]:
    with database_connection() as connection:
        rows = connection.execute(
            """
            SELECT * FROM assinantes
            WHERE status = 'ativo' AND vencimento = ?
            """,
            (vencimento,),
        ).fetchall()
    return list(rows)


def payment_already_processed(payment_id: str) -> bool:
    with database_connection() as connection:
        row = connection.execute(
            "SELECT payment_id FROM pagamentos_processados WHERE payment_id = ?",
            (payment_id,),
        ).fetchone()
    return row is not None


def mark_payment_processed(payment_id: str, user_id: int) -> None:
    with database_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO pagamentos_processados (payment_id, user_id, processed_at)
            VALUES (?, ?, ?)
            """,
            (payment_id, user_id, datetime.now(APP_TIMEZONE).isoformat()),
        )


def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.strip().split(maxsplit=1)
    if not parts:
        return "Usuario", "Telegram"
    if len(parts) == 1:
        return parts[0], "Telegram"
    return parts[0], parts[1]


def missing_subscription_config() -> list[str]:
    missing: list[str] = []
    if not MP_ACCESS_TOKEN:
        missing.append("MP_ACCESS_TOKEN")
    if not normalized_group_vip_id():
        missing.append("GRUPO_VIP_ID")
    if not notification_webhook_url():
        missing.append("WEBHOOK_URL")
    return missing


def create_mercado_pago_sdk() -> mercadopago.SDK:
    if not MP_ACCESS_TOKEN:
        raise RuntimeError("MP_ACCESS_TOKEN não configurado.")
    return mercadopago.SDK(MP_ACCESS_TOKEN)


def fetch_payment(payment_id: str) -> dict[str, Any]:
    sdk = create_mercado_pago_sdk()
    result = sdk.payment().get(payment_id)
    response = result.get("response", {})
    if not response:
        raise RuntimeError(f"Pagamento {payment_id} não encontrado.")
    return response


def create_pix_charge(user_id: int, nome: str) -> str:
    webhook_url = notification_webhook_url()
    if not webhook_url:
        raise RuntimeError("WEBHOOK_URL não configurado.")

    sdk = create_mercado_pago_sdk()
    first_name, last_name = split_name(nome)

    payment_data = {
        "transaction_amount": VIP_PRICE,
        "description": "Assinatura VIP Surebet - 30 dias",
        "payment_method_id": "pix",
        "notification_url": webhook_url,
        "external_reference": str(user_id),
        "metadata": {
            "telegram_user_id": str(user_id),
            "telegram_nome": nome,
        },
        "payer": {
            "email": f"telegram-{user_id}@example.com",
            "first_name": first_name,
            "last_name": last_name,
        },
    }

    request_options = RequestOptions()
    request_options.custom_headers = {
        "x-idempotency-key": str(uuid.uuid4()),
    }

    result = sdk.payment().create(payment_data, request_options)
    response = result.get("response", {})
    qr_code = (
        response.get("point_of_interaction", {})
        .get("transaction_data", {})
        .get("qr_code")
    )

    if not qr_code:
        raise RuntimeError(f"Não foi possível gerar o código PIX: {response}")

    save_pending_assinante(user_id, nome)

    return (
        "💳 ASSINAR VIP\n\n"
        f"Valor: {VIP_PRICE_TEXT}\n\n"
        "Use o código PIX copia e cola abaixo para concluir sua assinatura:\n\n"
        f"`{qr_code}`\n\n"
        "Assim que o pagamento for aprovado, você receberá automaticamente o link exclusivo do grupo VIP."
    )


def extract_payment_id(payload: dict[str, Any]) -> str:
    return (
        str(payload.get("data", {}).get("id") or "").strip()
        or str(payload.get("id") or "").strip()
        or str(request.args.get("data.id") or "").strip()
        or str(request.args.get("id") or "").strip()
    )


def run_telegram_coroutine(coro: Any) -> Any:
    if not BOT_READY.wait(timeout=30):
        raise RuntimeError("Bot do Telegram ainda não está pronto.")
    if TELEGRAM_LOOP is None:
        raise RuntimeError("Loop do Telegram não inicializado.")
    future = asyncio.run_coroutine_threadsafe(coro, TELEGRAM_LOOP)
    return future.result(timeout=60)


async def create_unique_invite_link(user_id: int) -> str:
    if TELEGRAM_APP is None:
        raise RuntimeError("Bot do Telegram não inicializado.")
    invite_link = await TELEGRAM_APP.bot.create_chat_invite_link(
        chat_id=normalized_group_vip_id(),
        member_limit=1,
        name=f"vip-{user_id}-{uuid.uuid4().hex[:8]}",
    )
    return invite_link.invite_link


async def send_private_message(user_id: int, text: str) -> None:
    if TELEGRAM_APP is None:
        raise RuntimeError("Bot do Telegram não inicializado.")
    await TELEGRAM_APP.bot.send_message(chat_id=user_id, text=text)


async def remove_user_from_group(user_id: int) -> None:
    if TELEGRAM_APP is None:
        raise RuntimeError("Bot do Telegram não inicializado.")
    chat_id = normalized_group_vip_id()
    if chat_id is None:
        raise RuntimeError("GRUPO_VIP_ID não configurado.")
    await TELEGRAM_APP.bot.ban_chat_member(chat_id=chat_id, user_id=user_id)
    await TELEGRAM_APP.bot.unban_chat_member(
        chat_id=chat_id,
        user_id=user_id,
        only_if_banned=True,
    )


def process_approved_payment(payment_id: str) -> None:
    if payment_already_processed(payment_id):
        logger.info("Pagamento %s já processado anteriormente.", payment_id)
        return

    payment = fetch_payment(payment_id)
    if payment.get("status") != "approved":
        logger.info(
            "Pagamento %s ainda não aprovado. Status atual: %s",
            payment_id,
            payment.get("status"),
        )
        return

    metadata = payment.get("metadata") or {}
    user_id_raw = metadata.get("telegram_user_id") or payment.get("external_reference")
    if not user_id_raw:
        raise RuntimeError(f"Pagamento {payment_id} sem telegram_user_id nos metadados.")

    user_id = int(user_id_raw)
    nome = (
        str(metadata.get("telegram_nome") or "").strip()
        or str((payment.get("payer") or {}).get("first_name") or "").strip()
        or f"Usuario {user_id}"
    )

    data_pagamento = current_date()
    vencimento = data_pagamento + timedelta(days=VIP_DURATION_DAYS)
    invite_link = run_telegram_coroutine(create_unique_invite_link(user_id))
    welcome_text = (
        "✅ Pagamento aprovado!\n\n"
        "Seu acesso ao grupo VIP foi liberado por 30 dias.\n\n"
        f"Link exclusivo:\n{invite_link}\n\n"
        "Esse link aceita apenas 1 entrada. Se precisar renovar depois, use /assinar."
    )

    run_telegram_coroutine(send_private_message(user_id, welcome_text))
    activate_assinante(user_id, nome, data_pagamento.isoformat(), vencimento.isoformat())
    mark_payment_processed(payment_id, user_id)
    logger.info("Pagamento %s aprovado para user_id=%s.", payment_id, user_id)


def expire_due_subscribers() -> None:
    due_today = get_assinantes_expiring_on(current_date().isoformat())
    if not due_today:
        logger.info("Nenhum assinante vencendo hoje.")
        return

    for assinante in due_today:
        user_id = int(assinante["user_id"])
        try:
            run_telegram_coroutine(remove_user_from_group(user_id))
        except Exception as exc:
            logger.warning("Falha ao remover usuário %s do grupo VIP: %s", user_id, exc)

        try:
            run_telegram_coroutine(
                send_private_message(
                    user_id,
                    "⏰ Seu acesso ao grupo VIP venceu hoje.\n\n"
                    "Para renovar por mais 30 dias, use /assinar.",
                )
            )
        except Exception as exc:
            logger.warning("Falha ao avisar vencimento para %s: %s", user_id, exc)

        mark_assinante_expired(user_id)
        logger.info("Assinatura vencida processada para user_id=%s.", user_id)


async def present_callback_text(update: Update, text: str) -> None:
    query = update.callback_query
    if query is None:
        return
    await query.answer()
    try:
        await query.edit_message_text(text, reply_markup=back_to_menu_keyboard())
    except TelegramError:
        if query.message:
            await query.message.reply_text(text, reply_markup=back_to_menu_keyboard())


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(START_TEXT, reply_markup=main_menu_keyboard())


async def handle_subscription_request(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    via_callback: bool,
) -> None:
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return

    existing = await asyncio.to_thread(get_assinante, user.id)
    if existing and existing["status"] == "ativo" and existing["vencimento"]:
        try:
            vencimento = datetime.fromisoformat(existing["vencimento"]).date()
        except ValueError:
            vencimento = None
        if vencimento and vencimento >= current_date():
            active_text = (
                "✅ Sua assinatura VIP já está ativa.\n\n"
                f"Vencimento atual: {vencimento.strftime('%d/%m/%Y')}\n\n"
                "Quando precisar renovar, use /assinar."
            )
            if via_callback:
                await present_callback_text(update, active_text)
            else:
                await message.reply_text(active_text, reply_markup=back_to_menu_keyboard())
            return

    missing = missing_subscription_config()
    if missing:
        config_text = (
            "⚠️ O sistema de assinatura ainda não está totalmente configurado.\n\n"
            f"Variáveis pendentes: {', '.join(missing)}"
        )
        if via_callback:
            await present_callback_text(update, config_text)
        else:
            await message.reply_text(config_text, reply_markup=back_to_menu_keyboard())
        return

    if via_callback and update.callback_query is not None:
        await update.callback_query.answer()
        status_message = await message.reply_text("Gerando sua cobrança PIX...")
    else:
        status_message = await message.reply_text("Gerando sua cobrança PIX...")

    try:
        payment_text = await asyncio.to_thread(create_pix_charge, user.id, user.full_name)
    except Exception as exc:
        logger.exception("Falha ao gerar cobrança PIX: %s", exc)
        await status_message.edit_text(
            "❌ Não consegui gerar sua cobrança PIX agora. Tente novamente em alguns instantes.",
            reply_markup=back_to_menu_keyboard(),
        )
        return

    await status_message.edit_text(
        payment_text,
        reply_markup=back_to_menu_keyboard(),
        parse_mode="Markdown",
    )


async def assinar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await handle_subscription_request(update, context, via_callback=False)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data
    if data == CALLBACK_MENU:
        await query.answer()
        try:
            await query.edit_message_text(START_TEXT, reply_markup=main_menu_keyboard())
        except TelegramError:
            if query.message:
                await query.message.reply_text(START_TEXT, reply_markup=main_menu_keyboard())
        return

    if data == CALLBACK_SUREBET:
        await present_callback_text(update, SUREBET_TEXT)
        return

    if data == CALLBACK_CALC:
        await present_callback_text(update, CALCULATION_TEXT)
        return

    if data == CALLBACK_ENTRIES:
        await present_callback_text(update, ENTRY_USAGE_TEXT)
        return

    if data == CALLBACK_BANKROLL:
        await present_callback_text(update, BANKROLL_TEXT)
        return

    if data == CALLBACK_SUBSCRIBE:
        await handle_subscription_request(update, context, via_callback=True)
        return

    await query.answer("Opção inválida.")


async def telegram_bot_main() -> None:
    global TELEGRAM_APP, TELEGRAM_LOOP

    if not TELEGRAM_TOKEN:
        logger.warning("TELEGRAM_TOKEN não configurado. Bot não será iniciado.")
        return

    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("assinar", assinar_command))
    application.add_handler(CallbackQueryHandler(callback_handler))

    TELEGRAM_APP = application
    TELEGRAM_LOOP = asyncio.get_running_loop()

    await application.initialize()
    if application.updater is None:
        raise RuntimeError("Updater do Telegram não foi inicializado.")
    await application.updater.start_polling(drop_pending_updates=False)
    await application.start()
    BOT_READY.set()
    logger.info("Bot do Telegram iniciado com polling.")

    await asyncio.Event().wait()


def telegram_thread_target() -> None:
    try:
        asyncio.run(telegram_bot_main())
    except Exception as exc:
        logger.exception("Thread do Telegram finalizada com erro: %s", exc)


def start_scheduler() -> None:
    global SCHEDULER
    if SCHEDULER is not None:
        return

    scheduler = BackgroundScheduler(timezone=APP_TIMEZONE)
    scheduler.add_job(
        expire_due_subscribers,
        trigger="cron",
        hour=9,
        minute=0,
        id="expire_due_subscribers",
        replace_existing=True,
    )
    scheduler.start()
    SCHEDULER = scheduler
    logger.info("APScheduler iniciado para expirações diárias às 09:00.")


def ensure_services_started() -> None:
    global SERVICES_STARTED, BOT_THREAD

    with BOOTSTRAP_LOCK:
        if SERVICES_STARTED:
            return

        init_database()
        start_scheduler()

        if TELEGRAM_TOKEN:
            BOT_THREAD = threading.Thread(
                target=telegram_thread_target,
                name="telegram-bot-thread",
                daemon=True,
            )
            BOT_THREAD.start()
        else:
            logger.warning("TELEGRAM_TOKEN ausente. O bot não será iniciado.")

        SERVICES_STARTED = True


@app.get("/")
def index() -> tuple[dict[str, str], int]:
    return {"service": "surebet-telegram-bot", "status": "ok"}, 200


@app.get("/health")
def health() -> tuple[str, int]:
    return "OK", 200


@app.post("/webhook")
def mercadopago_webhook() -> tuple[dict[str, Any], int]:
    payload = request.get_json(silent=True) or {}
    payment_id = extract_payment_id(payload)
    notification_type = (
        str(
            payload.get("type")
            or payload.get("topic")
            or request.args.get("type")
            or request.args.get("topic")
            or ""
        )
        .strip()
        .lower()
    )

    if not payment_id:
        return {"status": "ignored", "reason": "missing_payment_id"}, 200

    if notification_type and notification_type != "payment":
        return {"status": "ignored", "reason": notification_type}, 200

    try:
        process_approved_payment(payment_id)
    except Exception as exc:
        logger.exception("Falha ao processar webhook do pagamento %s: %s", payment_id, exc)
        return {"status": "error"}, 500

    return {"status": "ok", "payment_id": payment_id}, 200


ensure_services_started()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "10000")),
        threaded=True,
        use_reloader=False,
    )
