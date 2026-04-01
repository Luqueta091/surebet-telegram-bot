from __future__ import annotations

import asyncio
import json
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
from flask import Flask, redirect, render_template_string, request, session, url_for
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
DEFAULT_CURRENCY_CODE = "BRL"
FUNNEL_VIDEO_PATH = Path(__file__).with_name("vids.mp4")
FUNNEL_CONFIG_PATH = Path(
    os.environ.get(
        "FUNNEL_CONFIG_PATH",
        str(Path(__file__).with_name("funnel_config.json")),
    )
).expanduser()
FUNNEL_EDITOR_PASSWORD = os.environ.get("FUNNEL_EDITOR_PASSWORD", "").strip()

DEFAULT_PLANS: dict[str, dict[str, Any]] = {
    "week_offer": {
        "label": "⭐ 1 Semana VIP ⭐",
        "price": 6.02,
        "price_text": "R$ 6.02",
        "duration_days": 7,
        "description": "Assinatura semanal VIP Surebet",
        "kind": "initial",
    },
    "lifetime_offer": {
        "label": "🔥 VIP Vitalício 🔥",
        "price": 14.99,
        "price_text": "R$ 14.99",
        "duration_days": None,
        "description": "Assinatura vitalícia VIP Surebet",
        "kind": "initial",
    },
    "lifetime_secret_offer": {
        "label": "💎 Vitalício + Materiais Extras",
        "price": 22.89,
        "price_text": "R$ 22.89",
        "duration_days": None,
        "description": "Assinatura vitalícia VIP Surebet + materiais extras",
        "kind": "initial",
    },
    "week_downsell": {
        "label": "⭐ 1 Semana VIP ⭐ por R$ 5.72 (5% OFF)",
        "price": 5.72,
        "price_text": "R$ 5.72",
        "duration_days": 7,
        "description": "Assinatura semanal VIP Surebet - downsell",
        "kind": "downsell",
    },
    "lifetime_downsell": {
        "label": "🔥 VIP Vitalício 🔥 por R$ 14.24 (5% OFF)",
        "price": 14.24,
        "price_text": "R$ 14.24",
        "duration_days": None,
        "description": "Assinatura vitalícia VIP Surebet - downsell",
        "kind": "downsell",
    },
    "lifetime_secret_downsell": {
        "label": "💎 Vitalício + Materiais Extras por R$ 21.75 (5% OFF)",
        "price": 21.75,
        "price_text": "R$ 21.75",
        "duration_days": None,
        "description": "Assinatura vitalícia VIP Surebet + materiais extras - downsell",
        "kind": "downsell",
    },
    "upsell_1_primary": {
        "label": "⚡ Upsell 1 Principal",
        "price": 29.90,
        "price_text": "R$ 29.90",
        "duration_days": None,
        "description": "Upsell 1 principal VIP Surebet",
        "kind": "upsell_1",
    },
    "upsell_1_secondary": {
        "label": "🎯 Upsell 1 Alternativo",
        "price": 19.90,
        "price_text": "R$ 19.90",
        "duration_days": 30,
        "description": "Upsell 1 alternativo VIP Surebet",
        "kind": "upsell_1",
    },
    "upsell_2_primary": {
        "label": "🚀 Upsell 2 Principal",
        "price": 39.90,
        "price_text": "R$ 39.90",
        "duration_days": None,
        "description": "Upsell 2 principal VIP Surebet",
        "kind": "upsell_2",
    },
    "upsell_2_secondary": {
        "label": "💼 Upsell 2 Alternativo",
        "price": 24.90,
        "price_text": "R$ 24.90",
        "duration_days": 30,
        "description": "Upsell 2 alternativo VIP Surebet",
        "kind": "upsell_2",
    },
}

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

DEFAULT_FUNNEL_TEXTS = {
    "vip_funnel_text": """⬆️ VEJA COMO É O VIP POR DENTRO
DIVIDIDO EM TÓPICOS PARA VOCÊ 🔴

No VIP você recebe:

✅ Entradas organizadas
✅ Mais agilidade para executar
✅ Acesso ao ambiente premium
✅ Conteúdo direto ao ponto
✅ Suporte mais próximo

🚀 Acesso imediato
⏱️ Condição promocional por tempo limitado

Escolha uma opção abaixo para continuar 👇""",
    "downsell_text": """20 segundos e essa condição pode sair do ar ✅

Liberamos uma condição melhor para sua entrada no VIP.

Se quiser aproveitar o menor valor disponível agora, escolha uma das opções abaixo 👇""",
    "upsell_1_text": """🔼 ESPAÇO RESERVADO PARA UPSELL 1

Preencha este bloco com a copy do seu primeiro upsell.

Quando quiser ativar, basta ajustar os textos e preços abaixo 👇""",
    "upsell_2_text": """🔼 ESPAÇO RESERVADO PARA UPSELL 2

Preencha este bloco com a copy do seu segundo upsell.

Quando quiser ativar, basta ajustar os textos e preços abaixo 👇""",
}

CALLBACK_SUREBET = "surebet_info"
CALLBACK_CALC = "surebet_calc"
CALLBACK_ENTRIES = "surebet_entries"
CALLBACK_BANKROLL = "surebet_bankroll"
CALLBACK_SUBSCRIBE = "vip_subscribe"
CALLBACK_MENU = "back_to_menu"
CALLBACK_DOWNSELL = "vip_downsell"
CALLBACK_PLAN_PREFIX = "vip_plan:"
CALLBACK_DOWNSELL_PLAN_PREFIX = "vip_downsell_plan:"
CALLBACK_UPSELL_1 = "vip_upsell_1"
CALLBACK_UPSELL_2 = "vip_upsell_2"
CALLBACK_UPSELL_1_PLAN_PREFIX = "vip_upsell_1_plan:"
CALLBACK_UPSELL_2_PLAN_PREFIX = "vip_upsell_2_plan:"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.environ.get("TELEGRAM_TOKEN", "surebet-editor-secret"))

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


def load_funnel_config() -> dict[str, Any]:
    if not FUNNEL_CONFIG_PATH.exists():
        return {}
    try:
        with FUNNEL_CONFIG_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, ValueError) as exc:
        logger.warning("Falha ao ler funnel_config.json: %s", exc)
        return {}
    return data if isinstance(data, dict) else {}


def save_funnel_config(config: dict[str, Any]) -> None:
    FUNNEL_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FUNNEL_CONFIG_PATH.open("w", encoding="utf-8") as file:
        json.dump(config, file, ensure_ascii=False, indent=2)


def current_plans() -> dict[str, dict[str, Any]]:
    config = load_funnel_config()
    configured_plans = config.get("plans")
    merged = {key: value.copy() for key, value in DEFAULT_PLANS.items()}
    if not isinstance(configured_plans, dict):
        return merged

    for plan_code, default_plan in merged.items():
        raw_override = configured_plans.get(plan_code)
        if not isinstance(raw_override, dict):
            continue
        override = raw_override.copy()
        if "price" in override:
            try:
                override["price"] = float(override["price"])
            except (TypeError, ValueError):
                override.pop("price", None)
        if "duration_days" in override and override["duration_days"] not in {"", None}:
            try:
                override["duration_days"] = int(override["duration_days"])
            except (TypeError, ValueError):
                override.pop("duration_days", None)
        elif override.get("duration_days") == "":
            override["duration_days"] = None
        merged[plan_code].update(override)
    return merged


def current_funnel_text(key: str) -> str:
    config = load_funnel_config()
    texts = config.get("texts")
    if isinstance(texts, dict):
        value = texts.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return DEFAULT_FUNNEL_TEXTS[key]


def build_funnel_editor_config() -> dict[str, Any]:
    return {
        "texts": {key: current_funnel_text(key) for key in DEFAULT_FUNNEL_TEXTS},
        "plans": current_plans(),
    }


def initial_offer_keyboard() -> InlineKeyboardMarkup:
    plans = current_plans()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(plans["week_offer"]["label"] + " por " + plans["week_offer"]["price_text"], callback_data=CALLBACK_PLAN_PREFIX + "week_offer")],
            [InlineKeyboardButton(plans["lifetime_offer"]["label"] + " por " + plans["lifetime_offer"]["price_text"], callback_data=CALLBACK_PLAN_PREFIX + "lifetime_offer")],
            [InlineKeyboardButton(plans["lifetime_secret_offer"]["label"] + " por " + plans["lifetime_secret_offer"]["price_text"], callback_data=CALLBACK_PLAN_PREFIX + "lifetime_secret_offer")],
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def downsell_keyboard() -> InlineKeyboardMarkup:
    plans = current_plans()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(plans["week_downsell"]["label"], callback_data=CALLBACK_DOWNSELL_PLAN_PREFIX + "week_downsell")],
            [InlineKeyboardButton(plans["lifetime_downsell"]["label"], callback_data=CALLBACK_DOWNSELL_PLAN_PREFIX + "lifetime_downsell")],
            [InlineKeyboardButton(plans["lifetime_secret_downsell"]["label"], callback_data=CALLBACK_DOWNSELL_PLAN_PREFIX + "lifetime_secret_downsell")],
            [InlineKeyboardButton("🔼 Abrir Upsell 1", callback_data=CALLBACK_UPSELL_1)],
            [InlineKeyboardButton("🔼 Abrir Upsell 2", callback_data=CALLBACK_UPSELL_2)],
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def upsell_1_keyboard() -> InlineKeyboardMarkup:
    plans = current_plans()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(plans["upsell_1_primary"]["label"] + " por " + plans["upsell_1_primary"]["price_text"], callback_data=CALLBACK_UPSELL_1_PLAN_PREFIX + "upsell_1_primary")],
            [InlineKeyboardButton(plans["upsell_1_secondary"]["label"] + " por " + plans["upsell_1_secondary"]["price_text"], callback_data=CALLBACK_UPSELL_1_PLAN_PREFIX + "upsell_1_secondary")],
            [InlineKeyboardButton("🔼 Ir para Upsell 2", callback_data=CALLBACK_UPSELL_2)],
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def upsell_2_keyboard() -> InlineKeyboardMarkup:
    plans = current_plans()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(plans["upsell_2_primary"]["label"] + " por " + plans["upsell_2_primary"]["price_text"], callback_data=CALLBACK_UPSELL_2_PLAN_PREFIX + "upsell_2_primary")],
            [InlineKeyboardButton(plans["upsell_2_secondary"]["label"] + " por " + plans["upsell_2_secondary"]["price_text"], callback_data=CALLBACK_UPSELL_2_PLAN_PREFIX + "upsell_2_secondary")],
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def get_plan(plan_code: str) -> dict[str, Any]:
    plan = current_plans().get(plan_code)
    if plan is None:
        raise KeyError(plan_code)
    return plan


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


def ensure_syncpay_charge_columns(connection: sqlite3.Connection) -> None:
    existing_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(syncpay_cobrancas)").fetchall()
    }
    required_columns = {
        "plan_code": "TEXT",
        "amount": "REAL",
    }
    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE syncpay_cobrancas ADD COLUMN {column_name} {column_type}"
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
                plan_code TEXT,
                amount REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        ensure_syncpay_charge_columns(connection)


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


def activate_assinante(user_id: int, nome: str, data_pagamento: str, vencimento: str | None) -> None:
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


def save_syncpay_charge(
    identifier: str,
    user_id: int,
    status: str,
    pix_code: str,
    plan_code: str,
    amount: float,
) -> None:
    timestamp = datetime.now(APP_TIMEZONE).isoformat()
    with database_connection() as connection:
        connection.execute(
            """
            INSERT INTO syncpay_cobrancas (identifier, user_id, status, pix_code, plan_code, amount, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identifier) DO UPDATE SET
                user_id = excluded.user_id,
                status = excluded.status,
                pix_code = excluded.pix_code,
                plan_code = excluded.plan_code,
                amount = excluded.amount,
                updated_at = excluded.updated_at
            """,
            (identifier, user_id, status, pix_code, plan_code, amount, timestamp, timestamp),
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
    plan_code: str,
) -> str:
    webhook_url = notification_webhook_url()
    if not webhook_url:
        raise RuntimeError("WEBHOOK_URL não configurado.")
    plan = get_plan(plan_code)

    cpf, email, telefone = get_or_create_syncpay_profile(user_id, nome)

    payload = {
        "amount": plan["price"],
        "description": plan["description"],
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
    save_syncpay_charge(identifier, user_id, "pending", pix_code, plan_code, float(plan["price"]))

    duration_days = plan.get("duration_days")
    duration_text = (
        f"{duration_days} dias"
        if isinstance(duration_days, int)
        else "vitalício"
    )

    return (
        "💳 ASSINAR VIP\n\n"
        f"Plano: {plan['label']}\n"
        f"Valor: {plan['price_text']}\n"
        f"Acesso: {duration_text}\n"
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
    plan_code = str(charge["plan_code"] or "").strip()
    try:
        plan = get_plan(plan_code)
    except KeyError:
        logger.warning(
            "Cobrança SyncPay %s aprovada sem plano válido (%s).",
            identifier,
            plan_code,
        )
        return
    assinante = get_assinante(user_id)
    client = data.get("client") if isinstance(data.get("client"), dict) else {}
    nome = (
        str((assinante["nome"] if assinante else "") or "").strip()
        or str(client.get("name") or "").strip()
        or f"Usuario {user_id}"
    )

    data_pagamento = current_date()
    duration_days = plan.get("duration_days")
    vencimento = (
        data_pagamento + timedelta(days=int(duration_days))
        if isinstance(duration_days, int)
        else None
    )
    invite_link = run_telegram_coroutine(create_unique_invite_link(user_id))
    access_text = (
        f"Seu acesso ao grupo VIP foi liberado até {vencimento.strftime('%d/%m/%Y')}."
        if vencimento is not None
        else "Seu acesso ao grupo VIP foi liberado em modo vitalício."
    )
    renewal_text = (
        "Quando quiser renovar, use /assinar."
        if vencimento is not None
        else "Você não precisa renovar."
    )
    welcome_text = (
        "✅ Pagamento aprovado!\n\n"
        f"Plano: {plan['label']}\n"
        f"{access_text}\n\n"
        f"Link exclusivo:\n{invite_link}\n\n"
        "Esse link aceita apenas 1 entrada.\n"
        f"{renewal_text}"
    )

    run_telegram_coroutine(send_private_message(user_id, welcome_text))
    activate_assinante(
        user_id,
        nome,
        data_pagamento.isoformat(),
        vencimento.isoformat() if vencimento is not None else None,
    )
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
                    "Para renovar, use /assinar.",
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


async def show_subscription_offer(
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
    if existing and existing["status"] == "ativo":
        stored_vencimento = str(existing["vencimento"] or "").strip()
        try:
            vencimento = datetime.fromisoformat(stored_vencimento).date() if stored_vencimento else None
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
        if not stored_vencimento:
            active_text = (
                "✅ Sua assinatura VIP já está ativa.\n\n"
                "Tipo de acesso: vitalício\n\n"
                "Você não precisa renovar."
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

    if via_callback:
        query = update.callback_query
        if query is None:
            return
        await safe_answer_callback(query)

    if FUNNEL_VIDEO_PATH.exists():
        with FUNNEL_VIDEO_PATH.open("rb") as video_file:
            await message.reply_video(
                video=video_file,
                caption=current_funnel_text("vip_funnel_text"),
                reply_markup=initial_offer_keyboard(),
            )
        return

    await message.reply_text(
        current_funnel_text("vip_funnel_text"),
        reply_markup=initial_offer_keyboard(),
    )


async def create_charge_for_plan(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    plan_code: str,
) -> None:
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return

    try:
        plan = get_plan(plan_code)
    except KeyError:
        if update.callback_query is not None:
            await safe_answer_callback(update.callback_query, "Plano inválido.")
        return

    if update.callback_query is not None:
        await safe_answer_callback(update.callback_query)
    status_message = await message.reply_text(
        f"Gerando sua cobrança PIX para {plan['label']}..."
    )

    try:
        payment_text = await asyncio.to_thread(
            create_syncpay_charge,
            user.id,
            user.full_name,
            plan_code,
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


async def show_offer_stage(update: Update, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    query = update.callback_query
    if query is None:
        return
    await safe_answer_callback(query)
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
    except TelegramError:
        if query.message:
            await query.message.reply_text(text, reply_markup=reply_markup)


async def assinar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_subscription_offer(update, context, via_callback=False)


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
        await show_subscription_offer(update, context, via_callback=True)
        return

    if data.startswith(CALLBACK_PLAN_PREFIX):
        await show_offer_stage(
            update,
            current_funnel_text("downsell_text"),
            downsell_keyboard(),
        )
        return

    if data.startswith(CALLBACK_DOWNSELL_PLAN_PREFIX):
        plan_code = data[len(CALLBACK_DOWNSELL_PLAN_PREFIX):]
        await create_charge_for_plan(update, context, plan_code)
        return

    if data == CALLBACK_UPSELL_1:
        await show_offer_stage(
            update,
            current_funnel_text("upsell_1_text"),
            upsell_1_keyboard(),
        )
        return

    if data == CALLBACK_UPSELL_2:
        await show_offer_stage(
            update,
            current_funnel_text("upsell_2_text"),
            upsell_2_keyboard(),
        )
        return

    if data.startswith(CALLBACK_UPSELL_1_PLAN_PREFIX):
        plan_code = data[len(CALLBACK_UPSELL_1_PLAN_PREFIX):]
        await create_charge_for_plan(update, context, plan_code)
        return

    if data.startswith(CALLBACK_UPSELL_2_PLAN_PREFIX):
        plan_code = data[len(CALLBACK_UPSELL_2_PLAN_PREFIX):]
        await create_charge_for_plan(update, context, plan_code)
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


def funnel_editor_authorized() -> bool:
    if not FUNNEL_EDITOR_PASSWORD:
        return True
    return bool(session.get("funnel_editor_authenticated"))


FUNNEL_EDITOR_TEMPLATE = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Editor de Funil</title>
  <style>
    :root { color-scheme: light; }
    body { font-family: Arial, sans-serif; margin: 0; background: #f4f6f8; color: #111; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
    .top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .grid { display: grid; grid-template-columns: 1.3fr 1fr; gap: 20px; }
    .card { background: #fff; border-radius: 14px; box-shadow: 0 4px 20px rgba(0,0,0,.06); padding: 20px; }
    h1, h2, h3 { margin: 0 0 14px; }
    h2 { font-size: 18px; margin-top: 24px; }
    h3 { font-size: 15px; margin-top: 18px; }
    label { display: block; font-size: 13px; font-weight: 700; margin: 12px 0 6px; }
    input, textarea { width: 100%; box-sizing: border-box; border: 1px solid #ccd3da; border-radius: 10px; padding: 10px 12px; font: inherit; }
    textarea { min-height: 130px; resize: vertical; }
    .plans { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .plan { border: 1px solid #e4e8ec; border-radius: 12px; padding: 14px; }
    .plan-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .actions { position: sticky; bottom: 0; background: #fff; padding-top: 14px; margin-top: 20px; display: flex; gap: 10px; }
    button, .button { border: 0; border-radius: 10px; padding: 12px 16px; font-weight: 700; cursor: pointer; text-decoration: none; display: inline-block; }
    .primary { background: #0d8b4f; color: #fff; }
    .secondary { background: #eef2f5; color: #111; }
    .notice { background: #e8fff1; color: #0d6a3c; padding: 12px 14px; border-radius: 10px; margin-bottom: 14px; }
    .preview-stage { border: 1px solid #e4e8ec; border-radius: 12px; padding: 14px; margin-bottom: 14px; background: #fafcfd; }
    .preview-text { white-space: pre-wrap; background: #fff; border: 1px solid #e4e8ec; border-radius: 10px; padding: 12px; min-height: 110px; }
    .preview-buttons { display: grid; gap: 8px; margin-top: 12px; }
    .preview-buttons span { background: #dff1e7; color: #1b5d3d; border-radius: 10px; padding: 10px 12px; font-size: 14px; }
    .muted { color: #67707a; font-size: 13px; }
    .login { max-width: 420px; margin: 60px auto; }
    @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } .plans { grid-template-columns: 1fr; } .plan-grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <div class="wrap">
    {% if login_required %}
      <div class="card login">
        <h1>Entrar no Editor</h1>
        <p class="muted">Configure a senha em <code>FUNNEL_EDITOR_PASSWORD</code>.</p>
        <form method="post" action="{{ url_for('funnel_editor_login') }}">
          <label>Senha</label>
          <input type="password" name="password" required>
          <div class="actions">
            <button class="primary" type="submit">Entrar</button>
          </div>
        </form>
      </div>
    {% else %}
      <div class="top">
        <div>
          <h1>Editor Visual do Funil</h1>
          <div class="muted">Edite textos, preços e botões do funil sem mexer no código.</div>
        </div>
        <div>
          {% if password_enabled %}
            <a class="button secondary" href="{{ url_for('funnel_editor_logout') }}">Sair</a>
          {% endif %}
        </div>
      </div>
      {% if saved %}
        <div class="notice">Funil salvo com sucesso.</div>
      {% endif %}
      <div class="grid">
        <form class="card" method="post" action="{{ url_for('funnel_editor_save') }}">
          <h2>Textos</h2>
          {% for text_key, text_value in texts.items() %}
            <label>{{ text_labels[text_key] }}</label>
            <textarea name="text__{{ text_key }}" data-preview-text="{{ text_key }}">{{ text_value }}</textarea>
          {% endfor %}

          <h2>Planos</h2>
          <div class="plans">
            {% for plan_code, plan in plans.items() %}
              <div class="plan">
                <h3>{{ plan_code }}</h3>
                <label>Label</label>
                <input name="plan__{{ plan_code }}__label" value="{{ plan['label'] }}" data-preview-plan="{{ plan_code }}__label">
                <div class="plan-grid">
                  <div>
                    <label>Preço</label>
                    <input name="plan__{{ plan_code }}__price" value="{{ plan['price'] }}">
                  </div>
                  <div>
                    <label>Preço exibido</label>
                    <input name="plan__{{ plan_code }}__price_text" value="{{ plan['price_text'] }}" data-preview-plan="{{ plan_code }}__price_text">
                  </div>
                  <div>
                    <label>Duração em dias</label>
                    <input name="plan__{{ plan_code }}__duration_days" value="{{ '' if plan['duration_days'] is none else plan['duration_days'] }}">
                  </div>
                  <div>
                    <label>Tipo</label>
                    <input name="plan__{{ plan_code }}__kind" value="{{ plan['kind'] }}">
                  </div>
                </div>
                <label>Descrição interna</label>
                <input name="plan__{{ plan_code }}__description" value="{{ plan['description'] }}">
              </div>
            {% endfor %}
          </div>
          <div class="actions">
            <button class="primary" type="submit">Salvar funil</button>
            <button class="secondary" type="submit" formaction="{{ url_for('funnel_editor_reset') }}" formmethod="post">Restaurar padrão</button>
          </div>
        </form>

        <div class="card">
          <h2>Preview</h2>
          <div class="preview-stage">
            <h3>Oferta inicial</h3>
            <div class="preview-text" id="preview_vip_funnel_text">{{ texts["vip_funnel_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_week_offer">{{ plans["week_offer"]["label"] }} por {{ plans["week_offer"]["price_text"] }}</span>
              <span id="preview_plan_lifetime_offer">{{ plans["lifetime_offer"]["label"] }} por {{ plans["lifetime_offer"]["price_text"] }}</span>
              <span id="preview_plan_lifetime_secret_offer">{{ plans["lifetime_secret_offer"]["label"] }} por {{ plans["lifetime_secret_offer"]["price_text"] }}</span>
            </div>
          </div>
          <div class="preview-stage">
            <h3>Downsell</h3>
            <div class="preview-text" id="preview_downsell_text">{{ texts["downsell_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_week_downsell">{{ plans["week_downsell"]["label"] }}</span>
              <span id="preview_plan_lifetime_downsell">{{ plans["lifetime_downsell"]["label"] }}</span>
              <span id="preview_plan_lifetime_secret_downsell">{{ plans["lifetime_secret_downsell"]["label"] }}</span>
            </div>
          </div>
          <div class="preview-stage">
            <h3>Upsell 1</h3>
            <div class="preview-text" id="preview_upsell_1_text">{{ texts["upsell_1_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_upsell_1_primary">{{ plans["upsell_1_primary"]["label"] }} por {{ plans["upsell_1_primary"]["price_text"] }}</span>
              <span id="preview_plan_upsell_1_secondary">{{ plans["upsell_1_secondary"]["label"] }} por {{ plans["upsell_1_secondary"]["price_text"] }}</span>
            </div>
          </div>
          <div class="preview-stage">
            <h3>Upsell 2</h3>
            <div class="preview-text" id="preview_upsell_2_text">{{ texts["upsell_2_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_upsell_2_primary">{{ plans["upsell_2_primary"]["label"] }} por {{ plans["upsell_2_primary"]["price_text"] }}</span>
              <span id="preview_plan_upsell_2_secondary">{{ plans["upsell_2_secondary"]["label"] }} por {{ plans["upsell_2_secondary"]["price_text"] }}</span>
            </div>
          </div>
        </div>
      </div>
    {% endif %}
  </div>
  <script>
    for (const el of document.querySelectorAll('[data-preview-text]')) {
      el.addEventListener('input', () => {
        const target = document.getElementById('preview_' + el.dataset.previewText);
        if (target) target.textContent = el.value;
      });
    }
    for (const el of document.querySelectorAll('[data-preview-plan]')) {
      el.addEventListener('input', () => {
        const [planCode, field] = el.dataset.previewPlan.split('__');
        const target = document.getElementById('preview_plan_' + planCode);
        if (!target) return;
        const label = document.querySelector(`[name="plan__${planCode}__label"]`)?.value || '';
        const priceText = document.querySelector(`[name="plan__${planCode}__price_text"]`)?.value || '';
        target.textContent = priceText ? `${label} por ${priceText}` : label;
      });
    }
  </script>
</body>
</html>
"""


@app.get("/funnel-editor")
def funnel_editor() -> str:
    if not funnel_editor_authorized():
        return render_template_string(
            FUNNEL_EDITOR_TEMPLATE,
            login_required=True,
            password_enabled=bool(FUNNEL_EDITOR_PASSWORD),
        )

    config = build_funnel_editor_config()
    return render_template_string(
        FUNNEL_EDITOR_TEMPLATE,
        login_required=False,
        password_enabled=bool(FUNNEL_EDITOR_PASSWORD),
        saved=request.args.get("saved") == "1",
        texts=config["texts"],
        plans=config["plans"],
        text_labels={
            "vip_funnel_text": "Texto da oferta inicial",
            "downsell_text": "Texto do downsell",
            "upsell_1_text": "Texto do upsell 1",
            "upsell_2_text": "Texto do upsell 2",
        },
    )


@app.post("/funnel-editor/login")
def funnel_editor_login() -> Any:
    if not FUNNEL_EDITOR_PASSWORD:
        return redirect(url_for("funnel_editor"))
    if secrets.compare_digest(request.form.get("password", ""), FUNNEL_EDITOR_PASSWORD):
        session["funnel_editor_authenticated"] = True
    return redirect(url_for("funnel_editor"))


@app.get("/funnel-editor/logout")
def funnel_editor_logout() -> Any:
    session.pop("funnel_editor_authenticated", None)
    return redirect(url_for("funnel_editor"))


@app.post("/funnel-editor/save")
def funnel_editor_save() -> Any:
    if not funnel_editor_authorized():
        return redirect(url_for("funnel_editor"))

    config = {
        "texts": {},
        "plans": {},
    }
    for text_key in DEFAULT_FUNNEL_TEXTS:
        config["texts"][text_key] = request.form.get(f"text__{text_key}", DEFAULT_FUNNEL_TEXTS[text_key])

    for plan_code, default_plan in DEFAULT_PLANS.items():
        saved_plan: dict[str, Any] = {}
        for field_name, default_value in default_plan.items():
            raw_value = request.form.get(f"plan__{plan_code}__{field_name}", default_value)
            if field_name == "price":
                try:
                    saved_plan[field_name] = float(raw_value)
                except (TypeError, ValueError):
                    saved_plan[field_name] = float(default_value)
            elif field_name == "duration_days":
                if raw_value in {"", None}:
                    saved_plan[field_name] = None
                else:
                    try:
                        saved_plan[field_name] = int(raw_value)
                    except (TypeError, ValueError):
                        saved_plan[field_name] = default_value
            else:
                saved_plan[field_name] = str(raw_value)
        config["plans"][plan_code] = saved_plan

    save_funnel_config(config)
    return redirect(url_for("funnel_editor", saved=1))


@app.post("/funnel-editor/reset")
def funnel_editor_reset() -> Any:
    if not funnel_editor_authorized():
        return redirect(url_for("funnel_editor"))
    if FUNNEL_CONFIG_PATH.exists():
        FUNNEL_CONFIG_PATH.unlink()
    return redirect(url_for("funnel_editor", saved=1))


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
