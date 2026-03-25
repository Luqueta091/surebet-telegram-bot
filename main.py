from __future__ import annotations

import asyncio
import logging
import os
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from flask import Flask, request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("surebet-bot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

APP_TIMEZONE = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Sao_Paulo"))
DATABASE_PATH = Path(
    os.environ.get(
        "DATABASE_PATH",
        str(Path(__file__).with_name("assinantes.db")),
    )
).expanduser()
VIP_PRICE = 29.90
VIP_PRICE_TEXT = "R$29,90"
VIP_DURATION_DAYS = 30

TELEGRAM_TOKEN = (
    os.environ.get("TELEGRAM_TOKEN", "").strip()
    or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
)
SYNCPAY_CLIENT_ID = os.environ.get("SYNCPAY_CLIENT_ID", "").strip()
SYNCPAY_CLIENT_SECRET = os.environ.get("SYNCPAY_CLIENT_SECRET", "").strip()
SYNCPAY_API_BASE_URL = os.environ.get("SYNCPAY_API_BASE_URL", "https://api.syncpayments.com.br").strip().rstrip("/")
SYNCPAY_WEBHOOK_TOKEN = os.environ.get("SYNCPAY_WEBHOOK_TOKEN", "").strip()
SYNCPAY_REQUEST_TIMEOUT = float(os.environ.get("SYNCPAY_REQUEST_TIMEOUT", "30"))
GRUPO_VIP_ID_RAW = os.environ.get("GRUPO_VIP_ID", "").strip()
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip()

START_TEXT = """Olá! Sou o assistente do grupo de Surebet 👋

Use os botões abaixo para escolher o que você quer aprender."""

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

SYNCPAY_AUTH_LOCK = threading.Lock()
SYNCPAY_AUTH_TOKEN = ""
SYNCPAY_AUTH_EXPIRES_AT: datetime | None = None


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


def telegram_webhook_url() -> str:
    if not WEBHOOK_URL:
        return ""
    normalized = WEBHOOK_URL.rstrip("/")
    if normalized.endswith("/telegram-webhook"):
        return normalized
    if normalized.endswith("/webhook"):
        normalized = normalized[: -len("/webhook")]
    return f"{normalized}/telegram-webhook"


def telegram_delivery_mode() -> str:
    explicit_mode = os.environ.get("TELEGRAM_DELIVERY_MODE", "").strip().lower()
    if explicit_mode in {"polling", "webhook"}:
        return explicit_mode
    return "webhook" if telegram_webhook_url() else "polling"


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


def ensure_assinantes_columns(connection: sqlite3.Connection) -> None:
    existing_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(assinantes)").fetchall()
    }
    required_columns = {
        "cpf": "TEXT",
        "email": "TEXT",
        "telefone": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE assinantes ADD COLUMN {column_name} {column_type}"
            )


def init_database() -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
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
        ensure_assinantes_columns(connection)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pagamentos_processados (
                payment_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                processed_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS syncpay_cobrancas (
                identifier TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                pix_code TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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


def save_payment_profile(user_id: int, nome: str, cpf: str, email: str, telefone: str) -> None:
    with database_connection() as connection:
        connection.execute(
            """
            INSERT INTO assinantes (
                user_id, nome, status, data_pagamento, vencimento, cpf, email, telefone
            )
            VALUES (?, ?, 'pendente', NULL, NULL, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                nome = excluded.nome,
                cpf = excluded.cpf,
                email = excluded.email,
                telefone = excluded.telefone
            """,
            (user_id, nome, cpf, email, telefone),
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


def get_syncpay_charge(identifier: str) -> sqlite3.Row | None:
    with database_connection() as connection:
        return connection.execute(
            "SELECT * FROM syncpay_cobrancas WHERE identifier = ?",
            (identifier,),
        ).fetchone()


def save_syncpay_charge(identifier: str, user_id: int, status: str, pix_code: str) -> None:
    timestamp = datetime.now(APP_TIMEZONE).isoformat()
    with database_connection() as connection:
        connection.execute(
            """
            INSERT INTO syncpay_cobrancas (identifier, user_id, status, pix_code, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(identifier) DO UPDATE SET
                user_id = excluded.user_id,
                status = excluded.status,
                pix_code = excluded.pix_code,
                updated_at = excluded.updated_at
            """,
            (identifier, user_id, status, pix_code, timestamp, timestamp),
        )


def update_syncpay_charge_status(identifier: str, status: str, pix_code: str | None = None) -> None:
    timestamp = datetime.now(APP_TIMEZONE).isoformat()
    with database_connection() as connection:
        if pix_code is None:
            connection.execute(
                """
                UPDATE syncpay_cobrancas
                SET status = ?, updated_at = ?
                WHERE identifier = ?
                """,
                (status, timestamp, identifier),
            )
        else:
            connection.execute(
                """
                UPDATE syncpay_cobrancas
                SET status = ?, pix_code = ?, updated_at = ?
                WHERE identifier = ?
                """,
                (status, pix_code, timestamp, identifier),
            )


def syncpay_profile_complete(assinante: sqlite3.Row | None) -> bool:
    if assinante is None:
        return False
    return all(
        str(assinante[field] or "").strip()
        for field in ("cpf", "email", "telefone")
    )


def calculate_cpf_check_digit(digits: str) -> str:
    total = sum(int(digit) * multiplier for digit, multiplier in zip(digits, range(len(digits) + 1, 1, -1)))
    remainder = (total * 10) % 11
    return "0" if remainder == 10 else str(remainder)


def normalize_syncpay_phone(value: str) -> str:
    digits = "".join(character for character in value if character.isdigit())
    if len(digits) in {12, 13} and digits.startswith("55"):
        digits = digits[2:]
    return digits


def syncpay_phone_valid(value: str) -> bool:
    digits = normalize_syncpay_phone(value)
    return len(digits) in {10, 11}


def generate_syncpay_profile(user_id: int) -> tuple[str, str, str]:
    base_digits = f"{abs(user_id):09d}"[-9:]
    if len(set(base_digits)) == 1:
        base_digits = "123456789"

    first_check_digit = calculate_cpf_check_digit(base_digits)
    cpf = (
        base_digits
        + first_check_digit
        + calculate_cpf_check_digit(base_digits + first_check_digit)
    )
    email = f"telegram-{abs(user_id)}@example.com"
    telefone = f"11{abs(user_id) % (10**9):09d}"
    return cpf, email, telefone


def get_or_create_syncpay_profile(user_id: int, nome: str) -> tuple[str, str, str]:
    assinante = get_assinante(user_id)
    if syncpay_profile_complete(assinante):
        stored_cpf = str(assinante["cpf"] or "").strip()
        stored_email = str(assinante["email"] or "").strip()
        stored_phone = normalize_syncpay_phone(str(assinante["telefone"] or "").strip())
        if syncpay_phone_valid(stored_phone):
            if stored_phone != str(assinante["telefone"] or "").strip():
                save_payment_profile(user_id, nome, stored_cpf, stored_email, stored_phone)
            return stored_cpf, stored_email, stored_phone

    cpf, email, telefone = generate_syncpay_profile(user_id)
    save_payment_profile(user_id, nome, cpf, email, telefone)
    return cpf, email, telefone


def missing_subscription_config() -> list[str]:
    missing: list[str] = []
    if not SYNCPAY_CLIENT_ID:
        missing.append("SYNCPAY_CLIENT_ID")
    if not SYNCPAY_CLIENT_SECRET:
        missing.append("SYNCPAY_CLIENT_SECRET")
    if not SYNCPAY_API_BASE_URL:
        missing.append("SYNCPAY_API_BASE_URL")
    if not normalized_group_vip_id():
        missing.append("GRUPO_VIP_ID")
    if not notification_webhook_url():
        missing.append("WEBHOOK_URL")
    return missing


def parse_external_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def syncpay_api_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{SYNCPAY_API_BASE_URL}{path}"


def decode_json_response(response: requests.Response) -> dict[str, Any]:
    if not response.content:
        return {}
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Resposta inválida da SyncPay: {response.text}") from exc
    if isinstance(payload, dict):
        return payload
    raise RuntimeError(f"Formato inesperado da SyncPay: {payload}")


def get_syncpay_access_token(force_refresh: bool = False) -> str:
    global SYNCPAY_AUTH_TOKEN, SYNCPAY_AUTH_EXPIRES_AT

    if not SYNCPAY_CLIENT_ID or not SYNCPAY_CLIENT_SECRET:
        raise RuntimeError("Credenciais da SyncPay não configuradas.")

    with SYNCPAY_AUTH_LOCK:
        now = datetime.now(timezone.utc)
        if (
            not force_refresh
            and SYNCPAY_AUTH_TOKEN
            and SYNCPAY_AUTH_EXPIRES_AT is not None
            and now + timedelta(seconds=60) < SYNCPAY_AUTH_EXPIRES_AT
        ):
            return SYNCPAY_AUTH_TOKEN

        response = requests.post(
            syncpay_api_url("/api/partner/v1/auth-token"),
            json={
                "client_id": SYNCPAY_CLIENT_ID,
                "client_secret": SYNCPAY_CLIENT_SECRET,
            },
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=SYNCPAY_REQUEST_TIMEOUT,
        )
        payload = decode_json_response(response)
        if response.status_code >= 400:
            raise RuntimeError(f"Erro ao autenticar na SyncPay: {payload or response.text}")

        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError(f"SyncPay não retornou access_token: {payload}")

        expires_at = parse_external_datetime(str(payload.get("expires_at") or "").strip())
        if expires_at is None:
            expires_in = int(payload.get("expires_in") or 3600)
            expires_at = now + timedelta(seconds=expires_in)

        SYNCPAY_AUTH_TOKEN = access_token
        SYNCPAY_AUTH_EXPIRES_AT = expires_at
        return access_token


def syncpay_request(
    method: str,
    path: str,
    *,
    force_refresh: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.setdefault("Accept", "application/json")
    if "json" in kwargs:
        headers.setdefault("Content-Type", "application/json")
    headers["Authorization"] = f"Bearer {get_syncpay_access_token(force_refresh=force_refresh)}"

    response = requests.request(
        method=method,
        url=syncpay_api_url(path),
        headers=headers,
        timeout=SYNCPAY_REQUEST_TIMEOUT,
        **kwargs,
    )
    payload = decode_json_response(response)

    if response.status_code == 401 and not force_refresh:
        return syncpay_request(method, path, force_refresh=True, **kwargs)

    if response.status_code >= 400:
        raise RuntimeError(f"Erro na SyncPay ({response.status_code}): {payload or response.text}")

    return payload


def fetch_syncpay_transaction(identifier: str) -> dict[str, Any]:
    payload = syncpay_request("GET", f"/api/partner/v1/transaction/{identifier}")
    data = payload.get("data") or {}
    if not data:
        raise RuntimeError(f"Transação {identifier} não encontrada na SyncPay.")
    return data


def create_syncpay_charge(
    user_id: int,
    nome: str,
) -> str:
    webhook_url = notification_webhook_url()
    if not webhook_url:
        raise RuntimeError("WEBHOOK_URL não configurado.")

    cpf, email, telefone = get_or_create_syncpay_profile(user_id, nome)

    payload = {
        "amount": VIP_PRICE,
        "description": "Assinatura VIP Surebet - 30 dias",
        "webhook_url": webhook_url,
        "client": {
            "name": nome,
            "cpf": cpf,
            "email": email,
            "phone": telefone,
        },
    }

    response = syncpay_request("POST", "/api/partner/v1/cash-in", json=payload)
    pix_code = str(response.get("pix_code") or "").strip()
    identifier = str(response.get("identifier") or "").strip()

    if not pix_code or not identifier:
        raise RuntimeError(f"Não foi possível gerar o PIX na SyncPay: {response}")

    save_pending_assinante(user_id, nome)
    save_payment_profile(user_id, nome, cpf, email, telefone)
    save_syncpay_charge(identifier, user_id, "pending", pix_code)

    return (
        "💳 ASSINAR VIP\n\n"
        f"Valor: {VIP_PRICE_TEXT}\n"
        f"Identificador: {identifier}\n\n"
        "Use o código PIX copia e cola abaixo para concluir sua assinatura:\n\n"
        f"{pix_code}\n\n"
        "Assim que o pagamento for aprovado, você receberá automaticamente o link exclusivo do grupo VIP."
    )


def extract_syncpay_identifier(payload: dict[str, Any]) -> str:
    data = payload.get("data")
    if isinstance(data, dict):
        value = (
            data.get("id")
            or data.get("identifier")
            or data.get("reference_id")
        )
        if value:
            return str(value).strip()
    return (
        str(payload.get("identifier") or "").strip()
        or str(request.args.get("identifier") or "").strip()
        or str(request.args.get("id") or "").strip()
    )


def syncpay_payload_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return {}


def syncpay_webhook_authorized() -> bool:
    if not SYNCPAY_WEBHOOK_TOKEN:
        return True
    authorization = request.headers.get("Authorization", "").strip()
    expected = f"Bearer {SYNCPAY_WEBHOOK_TOKEN}"
    return secrets.compare_digest(authorization, expected)


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


def process_completed_payment(identifier: str, payload_data: dict[str, Any] | None = None) -> None:
    if payment_already_processed(identifier):
        logger.info("Pagamento %s já processado anteriormente.", identifier)
        return

    charge = get_syncpay_charge(identifier)
    if charge is None:
        logger.warning("Cobrança SyncPay %s recebida sem mapeamento local.", identifier)
        return

    data = payload_data or {}
    status = str(data.get("status") or "").strip().lower()
    pix_code = str(data.get("pix_code") or "").strip() or str(charge["pix_code"] or "").strip()

    if not status:
        transaction = fetch_syncpay_transaction(identifier)
        status = str(transaction.get("status") or "").strip().lower()
        pix_code = str(transaction.get("pix_code") or "").strip() or pix_code

    update_syncpay_charge_status(identifier, status or "pending", pix_code or None)

    if status != "completed":
        logger.info(
            "Cobrança SyncPay %s ainda não concluída. Status atual: %s",
            identifier,
            status or "desconhecido",
        )
        return

    user_id = int(charge["user_id"])
    assinante = get_assinante(user_id)
    client = data.get("client") if isinstance(data.get("client"), dict) else {}
    nome = (
        str((assinante["nome"] if assinante else "") or "").strip()
        or str(client.get("name") or "").strip()
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
    mark_payment_processed(identifier, user_id)
    logger.info("Pagamento SyncPay %s aprovado para user_id=%s.", identifier, user_id)


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
    await safe_answer_callback(query)
    try:
        await query.edit_message_text(text, reply_markup=back_to_menu_keyboard())
    except TelegramError:
        if query.message:
            await query.message.reply_text(text, reply_markup=back_to_menu_keyboard())


async def safe_answer_callback(query: Any, text: str | None = None) -> None:
    try:
        if text is None:
            await query.answer()
        else:
            await query.answer(text)
    except TelegramError as exc:
        logger.warning("Falha ao responder callback query: %s", exc)


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    message = update.effective_message
    if chat is None or message is None:
        return

    menu_message_id = context.user_data.get("menu_message_id")
    if menu_message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat.id,
                message_id=menu_message_id,
                text=START_TEXT,
                reply_markup=main_menu_keyboard(),
            )
            return
        except TelegramError:
            context.user_data.pop("menu_message_id", None)

    menu_message = await message.reply_text(START_TEXT, reply_markup=main_menu_keyboard())
    context.user_data["menu_message_id"] = menu_message.message_id


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_main_menu(update, context)


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
        await safe_answer_callback(update.callback_query)
        status_message = await message.reply_text("Gerando sua cobrança PIX...")
    else:
        status_message = await message.reply_text("Gerando sua cobrança PIX...")

    try:
        payment_text = await asyncio.to_thread(
            create_syncpay_charge,
            user.id,
            user.full_name,
        )
    except Exception as exc:
        logger.exception("Falha ao gerar cobrança PIX na SyncPay: %s", exc)
        await status_message.edit_text(
            "❌ Não consegui gerar sua cobrança PIX agora. Tente novamente em alguns instantes.",
            reply_markup=back_to_menu_keyboard(),
        )
        return

    await status_message.edit_text(
        payment_text,
        reply_markup=back_to_menu_keyboard(),
    )


async def assinar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await handle_subscription_request(update, context, via_callback=False)


async def group_service_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None:
        return

    if chat.type not in {"group", "supergroup"}:
        return

    try:
        await message.delete()
    except TelegramError as exc:
        logger.warning(
            "Não consegui apagar service message no chat %s (mensagem %s): %s",
            chat.id,
            message.message_id,
            exc,
        )


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data
    if data == CALLBACK_MENU:
        await safe_answer_callback(query)
        try:
            if query.message:
                context.user_data["menu_message_id"] = query.message.message_id
            await query.edit_message_text(START_TEXT, reply_markup=main_menu_keyboard())
        except TelegramError:
            if query.message:
                context.user_data.pop("menu_message_id", None)
            await show_main_menu(update, context)
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

    await safe_answer_callback(query, "Opção inválida.")


async def telegram_bot_main() -> None:
    global TELEGRAM_APP, TELEGRAM_LOOP

    if not TELEGRAM_TOKEN:
        logger.warning("TELEGRAM_TOKEN não configurado. Bot não será iniciado.")
        return

    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("assinar", assinar_command))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(
        MessageHandler(
            filters.StatusUpdate.NEW_CHAT_MEMBERS | filters.StatusUpdate.LEFT_CHAT_MEMBER,
            group_service_message_handler,
        )
    )

    TELEGRAM_APP = application
    TELEGRAM_LOOP = asyncio.get_running_loop()

    await application.initialize()
    await application.start()
    if telegram_delivery_mode() == "webhook":
        webhook_url = telegram_webhook_url()
        if not webhook_url:
            raise RuntimeError("WEBHOOK_URL não configurado para o webhook do Telegram.")
        await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
        )
        logger.info("Bot do Telegram iniciado com webhook em %s.", webhook_url)
    else:
        await application.bot.delete_webhook(drop_pending_updates=False)
        if application.updater is None:
            raise RuntimeError("Updater do Telegram não foi inicializado.")
        await application.updater.start_polling(drop_pending_updates=False)
        logger.info("Bot do Telegram iniciado com polling.")
    BOT_READY.set()

    await asyncio.Event().wait()


def telegram_thread_target() -> None:
    global TELEGRAM_APP, TELEGRAM_LOOP

    retry_delay_seconds = 10
    while True:
        try:
            asyncio.run(telegram_bot_main())
            return
        except Exception as exc:
            BOT_READY.clear()
            TELEGRAM_APP = None
            TELEGRAM_LOOP = None

            if exc.__class__.__name__ == "Conflict":
                logger.warning(
                    "Conflito no polling do Telegram durante a troca de instância. "
                    "Nova tentativa em %s segundos.",
                    retry_delay_seconds,
                )
                threading.Event().wait(retry_delay_seconds)
                continue

            logger.exception("Thread do Telegram finalizada com erro: %s", exc)
            return


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


@app.before_request
def bootstrap_services() -> None:
    ensure_services_started()


@app.get("/")
def index() -> tuple[dict[str, str], int]:
    return {"service": "surebet-telegram-bot", "status": "ok"}, 200


@app.get("/health")
def health() -> tuple[str, int]:
    return "OK", 200


@app.post("/webhook")
def syncpay_webhook() -> tuple[dict[str, Any], int]:
    if not syncpay_webhook_authorized():
        return {"status": "forbidden"}, 401

    payload = request.get_json(silent=True) or {}
    event_name = (
        str(request.headers.get("event") or payload.get("event") or "")
        .strip()
        .lower()
    )
    identifier = extract_syncpay_identifier(payload)

    if event_name and not event_name.startswith("cashin"):
        return {"status": "ignored", "reason": event_name}, 200

    if not identifier:
        return {"status": "ignored", "reason": "missing_identifier"}, 200

    try:
        process_completed_payment(identifier, syncpay_payload_data(payload))
    except Exception as exc:
        logger.exception("Falha ao processar webhook SyncPay %s: %s", identifier, exc)
        return {"status": "error"}, 500

    return {"status": "ok", "identifier": identifier}, 200


@app.post("/telegram-webhook")
def telegram_webhook() -> tuple[dict[str, Any], int]:
    if not BOT_READY.wait(timeout=25) or TELEGRAM_APP is None:
        return {"status": "error", "reason": "bot_not_initialized"}, 503

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return {"status": "ignored", "reason": "invalid_payload"}, 400

    try:
        update = Update.de_json(payload, TELEGRAM_APP.bot)
        run_telegram_coroutine(TELEGRAM_APP.process_update(update))
    except Exception as exc:
        logger.exception("Falha ao processar update do Telegram: %s", exc)
        return {"status": "error"}, 500

    return {"status": "ok"}, 200

if __name__ == "__main__":
    ensure_services_started()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "10000")),
        threaded=True,
        use_reloader=False,
    )
