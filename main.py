from __future__ import annotations

import asyncio
import json
import logging
import os
import pprint
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
from telegram import CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup, Update
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
logger = logging.getLogger("telegram-funnel-bot")
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
FUNNEL_MEDIA_DIR = Path(__file__).with_name("funnel_media")
FUNNEL_CONFIG_PATH = Path(
    os.environ.get(
        "FUNNEL_CONFIG_PATH",
        str(Path(__file__).with_name("funnel_config.json")),
    )
).expanduser()
FUNNEL_EDITOR_PASSWORD = os.environ.get("FUNNEL_EDITOR_PASSWORD", "").strip()
INITIAL_FOLLOWUP_DELAY = timedelta(minutes=20)
REPEATED_DOWNSELL_DELAY = timedelta(minutes=30)
MAIN_FILE_PATH = Path(__file__).resolve()
FUNNEL_CONFIG_START_MARKER = "# === FUNNEL_CONFIG_START ==="
FUNNEL_CONFIG_END_MARKER = "# === FUNNEL_CONFIG_END ==="

DEFAULT_PLANS: dict[str, dict[str, Any]] = {
    "week_offer": {
        "label": "Oferta 1",
        "price": 6.02,
        "price_text": "R$ 6.02",
        "duration_days": 7,
        "description": "Oferta principal 1",
        "kind": "initial",
    },
    "lifetime_offer": {
        "label": "Oferta 2",
        "price": 14.99,
        "price_text": "R$ 14.99",
        "duration_days": None,
        "description": "Oferta principal 2",
        "kind": "initial",
    },
    "lifetime_secret_offer": {
        "label": "Oferta 3",
        "price": 22.89,
        "price_text": "R$ 22.89",
        "duration_days": None,
        "description": "Oferta principal 3",
        "kind": "initial",
    },
    "week_downsell": {
        "label": "Downsell 1",
        "price": 5.72,
        "price_text": "R$ 5.72",
        "duration_days": 7,
        "description": "Oferta downsell 1",
        "kind": "downsell",
    },
    "lifetime_downsell": {
        "label": "Downsell 2",
        "price": 14.24,
        "price_text": "R$ 14.24",
        "duration_days": None,
        "description": "Oferta downsell 2",
        "kind": "downsell",
    },
    "lifetime_secret_downsell": {
        "label": "Downsell 3",
        "price": 21.75,
        "price_text": "R$ 21.75",
        "duration_days": None,
        "description": "Oferta downsell 3",
        "kind": "downsell",
    },
    "upsell_1_primary": {
        "label": "⚡ Upsell 1 Principal",
        "price": 29.90,
        "price_text": "R$ 29.90",
        "duration_days": None,
        "description": "Oferta upsell 1 principal",
        "kind": "upsell_1",
    },
    "upsell_2_primary": {
        "label": "🚀 Upsell 2 Principal",
        "price": 39.90,
        "price_text": "R$ 39.90",
        "duration_days": None,
        "description": "Oferta upsell 2 principal",
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

DEFAULT_FUNNEL_TEXTS = {
    "vip_funnel_text": "Configure aqui o texto da oferta inicial no editor do funil.",
    "downsell_text": "Configure aqui o texto do downsell no editor do funil.",
    "upsell_1_text": "Configure aqui o texto do upsell 1 no editor do funil.",
    "upsell_2_text": "Configure aqui o texto do upsell 2 no editor do funil.",
}
# === FUNNEL_CONFIG_START ===
EMBEDDED_FUNNEL_CONFIG: dict[str, Any] = {'texts': {'vip_funnel_text': '⬆️ VEJA COMO É O VIP POR DENTRO \r\n'
                              'DIVIDIDO EM TÓPICOS PARA VOCÊ 🔴\r\n'
                              '\r\n'
                              '\u200b😍 '
                              'О\u200b\u200b\u200b\u200b\u200b\u200b\u200bn\u200b\u200b\u200b\u200b\u200b\u200b\u200bl\u200b\u200b\u200b\u200b\u200b\u200b\u200bу\u200b\u200b\u200b\u200b\u200b\u200b\u200bF\u200b\u200b\u200b\u200b\u200b\u200b\u200bа\u200b\u200b\u200b\u200b\u200b\u200b\u200bn\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b🔴 '
                              '\u200bV\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bd\u200b\u200b\u200b\u200b\u200b\u200b\u200bе\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200br\u200b\u200b\u200b\u200b\u200b\u200b\u200bа\u200b\u200b\u200b\u200b\u200b\u200b\u200br\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\r\n'
                              '😈\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200bР\u200b\u200b\u200b\u200b\u200b\u200b\u200br\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bv\u200b\u200b\u200b\u200b\u200b\u200b\u200bа\u200b\u200b\u200b\u200b\u200b\u200b\u200bс\u200b\u200b\u200b\u200b\u200b\u200b\u200bу\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b🌟 '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200bL\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bv\u200b\u200b\u200b\u200b\u200b\u200b\u200bе\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁺\u200b\u200b\u200b\u200b\u200b\u200b\u200b¹\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁸\u200b\u200b\u200b\u200b\u200b\u200b\u200b\r\n'
                              '🌈\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200bΝ\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200bv\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bn\u200b\u200b\u200b\u200b\u200b\u200b\u200bh\u200b\u200b\u200b\u200b\u200b\u200b\u200bа\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁺\u200b\u200b\u200b\u200b\u200b\u200b\u200b¹\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁸\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b️\u200b\u200b\u200b❤️ '
                              '\u200b\u200b\u200bС\u200b\u200b\u200b\u200b\u200b\u200b\u200bl\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200bе\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200bF\u200b\u200b\u200b\u200b\u200b\u200b\u200br\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bе\u200b\u200b\u200b\u200b\u200b\u200b\u200bn\u200b\u200b\u200b\u200b\u200b\u200b\u200bd\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b\r\n'
                              '👀\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200bI\u200b\u200b\u200b\u200b\u200b\u200b\u200bn\u200b\u200b\u200b\u200b\u200b\u200b\u200bс\u200b\u200b\u200b\u200b\u200b\u200b\u200b3\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200bt\u200b\u200b\u200b\u200b\u200b\u200b\u200b0\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b🥵\u200bЕ\u200b\u200b\u200b\u200b\u200b\u200b\u200bm\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200bР\u200b\u200b\u200b\u200b\u200b\u200b\u200bú\u200b\u200b\u200b\u200b\u200b\u200b\u200bb\u200b\u200b\u200b\u200b\u200b\u200b\u200bl\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bс\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\r\n'
                              '💕\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200bF\u200b\u200b\u200b\u200b\u200b\u200b\u200bе\u200b\u200b\u200b\u200b\u200b\u200b\u200bt\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bс\u200b\u200b\u200b\u200b\u200b\u200b\u200bh\u200b\u200b\u200b\u200b\u200b\u200b\u200bе\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b.\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b🌈 '
                              '\u200b\u200b\u200bА\u200b\u200b\u200b\u200b\u200b\u200b\u200bm\u200b\u200b\u200b\u200b\u200b\u200b\u200bа\u200b\u200b\u200b\u200b\u200b\u200b\u200bd\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200br\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\r\n'
                              '💋\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200bМ\u200b\u200b\u200b\u200b\u200b\u200b\u200bі\u200b\u200b\u200b\u200b\u200b\u200b\u200bl\u200b\u200b\u200b\u200b\u200b\u200b\u200bf\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b🔞\u200bPR\u200b\u200b01B1D1NH0s¹\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁸\u200b\u200b\u200b\u200b\u200b\u200b\u200b\r\n'
                              '\U0001fae3\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200bF\u200b4\u200bМ\u200b1\u200bl\u200b1\u200bа\u200b '
                              '\u200bЅ\u200b4\u200bс\u200b4\u200bn\u200bа\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\u200b\u200b\u200b\u200b👅 '
                              '\u200b\u200b\u200bО\u200b\u200b\u200b\u200b\u200b\u200b\u200bс\u200b\u200b\u200b\u200b\u200b\u200b\u200bu\u200b\u200b\u200b\u200b\u200b\u200b\u200bt\u200b\u200b\u200b\u200b\u200b\u200b\u200bо\u200b\u200b\u200b\u200b\u200b\u200b\u200bѕ\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁺\u200b\u200b\u200b\u200b\u200b\u200b\u200b¹\u200b\u200b\u200b⁸\u200b\r\n'
                              '😡\u200b \u200bF\u200bа\u200bv\u200bе\u200bl\u200badа\u200bѕ\u200b '
                              '\u200b \u200b \u200b 🔥\u200b '
                              '\u200b\u200b\u200bΚ\u200b\u200b\u200bА\u200b\u200b\u200bМ\u200b\u200b\u200b1\u200b\u200b\u200bL\u200b\u200b\u200bI\u200b\u200b\u200bΝ\u200b\u200b\u200bН\u200b\u200b\u200b4\u200b\r\n'
                              '🙈\u200b '
                              '\u200bЅ\u200b\u200b\u200bе\u200b\u200b\u200bх\u200b\u200b\u200bо\u200b '
                              '\u200bn\u200bа\u200b\u200b\u200b '
                              '\u200b\u200b\u200bf\u200b\u200b\u200bа\u200b\u200b\u200bс\u200b\u200b\u200bu\u200b\u200b\u200bl\u200b\u200b\u200bd\u200b\u200b\u200bа\u200b\u200b\u200bd\u200b\u200b\u200bе\u200b⁺\u200b\u200b\u200b\u200b\u200b\u200b\u200b¹\u200b\u200b\u200b\u200b\u200b\u200b\u200b⁸\u200b\r\n'
                              '🌈 '
                              '\u200bU\u200bn\u200b1\u200bv\u200bе\u200br\u200bѕ\u200b!\u200bt\u200b4\u200br\u200b1\u200bа\u200bѕ\u200b '
                              '\u200bV\u200b4\u200bz\u200bа\u200bd\u200b4\u200bѕ¹\u200b\u200b\u200b⁸\r\n'
                              '\r\n'
                              '🗂 \u200b+\u200b '
                              '\u200b\u200b2\u200b\u200b\u200b0\u200b\u200b\u200b7\u200b\u200b\u200b.\u200b\u200b\u200b6\u200b\u200b\u200b2\u200b\u200b\u200b9\u200b\u200b\u200b '
                              '\u200b\u200b\u200bm\u200b\u200b\u200bí\u200b\u200b\u200bd\u200b\u200b\u200bі\u200b\u200b\u200bа\u200b\u200b\u200bѕ\u200b\u200b\u200b '
                              '\u200b\u200b\u200bn\u200b\u200b\u200bо\u200b\u200b\u200b '
                              '\u200b\u200b\u200bn\u200b\u200b\u200bо\u200b\u200b\u200bѕ\u200b\u200b\u200bѕ\u200b\u200b\u200bо\u200b\u200b\u200b '
                              '\u200b\u200b\u200bV\u200b\u200b\u200bI\u200b\u200b\u200bР\u200b\u200b\u200b '
                              '\u200b\u200b\u200b\r\n'
                              '🔴 \u200b+\u200b '
                              '\u200b\u200b\u200b3\u200b\u200b\u200b1\u200b\u200b\u200b.\u200b\u200b\u200b7\u200b\u200b\u200b3\u200b\u200b\u200b5\u200b\u200b\u200b '
                              '\u200b\u200b\u200bm\u200b\u200b\u200bí\u200b\u200b\u200bd\u200b\u200b\u200bі\u200b\u200b\u200bа\u200b\u200b\u200bѕ\u200b\u200b\u200b '
                              '\u200b\u200b\u200bО\u200b\u200b\u200bС\u200b\u200b\u200bU\u200b\u200b\u200bL\u200b\u200b\u200bТ\u200b\u200b\u200bА\u200b\u200b\u200bЅ\r\n'
                              '😈 + \u200b6\u200b \u200bG\u200br\u200bu\u200bр\u200bо\u200bѕ\u200b '
                              '\u200bѕ\u200bе\u200bс\u200br\u200bе\u200bt\u200bо\u200bѕ\r\n'
                              '\r\n'
                              '\u200b⚠️ ѕ\u200b\u200b\u200bu\u200b\u200b\u200bа\u200b\u200b\u200b '
                              '\u200b\u200b\u200bg\u200b\u200b\u200bо\u200b\u200b\u200bz\u200b\u200b\u200bа\u200b\u200b\u200bd\u200b\u200b\u200bа\u200b\u200b\u200b '
                              '\u200b\u200b\u200bg\u200b\u200b\u200bа\u200b\u200b\u200br\u200b\u200b\u200bа\u200b\u200b\u200bn\u200b\u200b\u200bt\u200b\u200b\u200bі\u200b\u200b\u200bd\u200b\u200b\u200bа\u200b\u200b\u200b '
                              '\u200b\u200b\u200bо\u200b\u200b\u200bu\u200b\u200b\u200b '
                              '\u200b\u200b\u200bѕ\u200b\u200b\u200bе\u200b\u200b\u200bu\u200b\u200b\u200b '
                              '\u200b\u200b\u200bd\u200b\u200b\u200bі\u200b\u200b\u200bn\u200b\u200b\u200bh\u200b\u200b\u200bе\u200b\u200b\u200bі\u200b\u200b\u200br\u200b\u200b\u200bо\u200b\u200b\u200b '
                              '\u200b\u200b\u200bd\u200b\u200b\u200bе\u200b\u200b\u200b '
                              '\u200b\u200b\u200bv\u200b\u200b\u200bо\u200b\u200b\u200bl\u200b\u200b\u200bt\u200b\u200b\u200bа! '
                              '⏱️\r\n'
                              '\r\n'
                              '🚀 𝐀𝐜𝐞𝐬𝐬𝐨 𝐢𝐦𝐞𝐝𝐢𝐚𝐭𝐨!\r\n'
                              '⏱️ 𝗣𝗥𝗢𝗠𝗢𝗖̧𝗔̃𝗢 𝗘𝗡𝗖𝗘𝗥𝗥𝗔 𝗘𝗠 𝟲 𝗠𝗜𝗡𝗨𝗧𝗢𝗦!\r\n'
                              '💥(9 𝘃𝗮𝗴𝗮𝘀 𝗿𝗲𝘀𝘁𝗮𝗻𝘁𝗲𝘀)💥\r\n'
                              '\r\n'
                              '⚠️ Esta conversa pode sumir em alguns minutos! ⏱️\r\n'
                              '\r\n'
                              '                    ⬇️ ᴇɴᴛʀᴀ ᴀɢᴏʀᴀ ⬇️',
           'downsell_text': '20 ѕеgundоѕ Ехсluímоѕ ѕе nãо еntrаr vосê fіса dе fоrа dа МЕGА '
                            'РRОМОÇÃО ✅\r\n'
                            '\r\n'
                            '👑 𝐈𝐍𝐂3𝐒𝐓0 𝐅𝐀𝐌𝐈𝐋𝐈𝐀𝐑👑   , É о mаіоr саnаl dе IΝС3ЅТ0 🤒 dо tеlеgrаm✈️ '
                            'соm muіtаѕ mídіаѕ dе ѕехо еm fаmílіа 🔞 \r\n'
                            '\r\n'
                            '📂  𝐈𝐑𝐌𝐀\u200c𝐎 𝐂𝐎𝐌𝐄𝐍𝐃𝐎 𝐈𝐑𝐌𝐀\u200c 🔥\r\n'
                            '📂  𝐓𝐈𝐎 𝐂𝐎𝐌𝐄𝐍𝐃𝐎 𝐒𝐎𝐁𝐑𝐈𝐍𝐇𝐀\r\n'
                            '📂  𝐒𝐎𝐁𝐑𝐈𝐍𝐇𝐀𝐒 𝐏𝐔𝐓𝐈𝐍𝐇𝐀𝐒  🔥\r\n'
                            '📂  𝐏𝐑𝐈𝐌𝐀𝐒 𝐕𝐈𝐂𝐈𝐀𝐃𝐀𝐒 𝐄𝐌 𝐒𝐄𝐗𝐎\r\n'
                            '📂  𝐑𝐄𝐀𝐋 𝐅𝐀𝐌𝐈𝐋𝐘 𝐒𝐄𝐗 🔥🔥\r\n'
                            '\r\n'
                            'Gаrаntа ѕuа vаgа аquі: ↙️\r\n'
                            '\r\n'
                            '💎VIР ЕМ РRОМОÇÃО💎\r\n'
                            '\r\n'
                            '👇 Сlіquе аbаіхо раrа реgаr а рrоmоçãо 👇',
           'upsell_1_text': '🔞LIVES BANIDAS🔥   \r\n'
                            '😈Não perca ao acesso das Lives mais exclusivas do Brasil! 🇧🇷   \r\n'
                            '📁SEAPARADAS POR PASTAS, TUDO ORGANIZADO!\r\n'
                            '📁  💎CONTEÚDOS ATUALIZADOS DIARIAMENTE.. 2026    \r\n'
                            '🔥APENAS HOJE→ ACESSO VIP PERMANENTE por apenas  \r\n'
                            'R$4,99 no PIX 🔥  \r\n'
                            '👇🏻CLIQUE NO BOTÃO ABAIXO E GARANTA JÁ SEU VIP🔞',
           'upsell_2_text': '☎️Ligação Perdida    🔞 𝐒𝐔𝐀 𝐆0𝐙4𝐃4 𝐆𝐀𝐑𝐀𝐍𝐓𝐈𝐃𝐀 𝐎𝐔 𝐒𝐄𝐔 𝐃𝐈𝐍𝐇𝐄𝐈𝐑𝐎 𝐃𝐄 𝐕𝐎𝐋𝐓𝐀 '
                            '❤️\u200d🔥  \r\n'
                            '🔥 𝙎𝙚𝙥𝙖𝙧𝙖𝙙𝙤𝙨 𝙥𝙤𝙧 𝙘𝙖𝙩𝙚𝙜𝙤𝙧𝙞𝙖:   \r\n'
                            '📁𝗩𝗮𝘇𝗮𝗱¡𝗻𝗵𝗼𝘀 𝗱𝗲 𝗡𝗼𝘃¡𝗻𝗵𝗮𝘀 𝗱𝗼 𝗧𝗶𝗸𝗧𝗼𝗸 𝗲 𝗜𝗻𝘀𝘁𝗮𝗴𝗿𝗮𝗺  \r\n'
                            '🔞 𝐍𝐨𝐯𝐢𝐧𝐡𝐚𝐬⁺¹⁸ 𝐜𝐡𝐨𝐫𝐚𝐧𝐝𝐨 𝐧𝐚 𝐫𝐨𝐥𝐚 \r\n'
                            '🩸𝐍𝐨𝐯𝐢𝐧𝐡𝐚⁺¹⁸ 𝐩𝐞𝐫𝐝𝐞𝐧𝐝𝐨 𝐨 𝐜𝐚𝐛𝟒𝐜𝟎 \r\n'
                            '🔞 𝐈𝐧𝐜𝟑𝐬𝐭𝐨⁺¹⁸ 𝐒𝐞𝐜𝐫𝟑𝐭𝟎 𝐫𝐞𝐚𝐥 \r\n'
                            '👙 𝐏𝟒𝐢 𝐜𝐨𝐦𝟑𝐧𝐝𝐨 𝐟𝟏𝐥𝐡𝐚⁺¹⁸  \r\n'
                            '🔞 𝐎𝐜𝐮𝐥𝐭𝐨𝐬 𝐛𝐫𝐮𝐭𝐚𝐥 \r\n'
                            '🔞 𝐕𝐚𝐳𝟒𝐝𝟎𝐬 𝐒𝟑𝐜𝐫𝐞𝐭𝟎𝐬⁺¹⁸ \r\n'
                            '😈𝐕𝐢́𝐝𝐞𝐨𝐬 𝐛𝐚𝐧𝐢𝐝𝐨𝐬 𝐞𝐦 𝟕 𝐩𝐚𝐢́𝐬𝐞𝐬 \r\n'
                            '🎁 𝐂𝐨𝐦𝐩𝐫𝐞 𝐮𝐦 𝐩𝐥𝐚𝐧𝐨 𝐡𝐨𝐣𝐞 𝐞 𝐠𝐚𝐧𝐡𝐞 5 𝐠𝐫𝐮𝐩𝐨𝐬 𝐒𝐄𝐂𝐑𝟑𝐓𝟎𝐒 \r\n'
                            '🔐🔥  🚀 𝐀𝐜𝐞𝐬𝐬𝐨 𝐢𝐦𝐞𝐝𝐢𝐚𝐭𝐨 🔒 𝐂𝐨𝐦𝐩𝐫𝐚 𝟏𝟎𝟎% 𝐚𝐧ô𝐧𝐢𝐦𝐚 ♾️ 𝐕𝐢𝐭𝐚𝐥í𝐜𝐢𝐨 𝐫𝐞𝐚𝐥 💬 '
                            '𝐒𝐮𝐩𝐨𝐫𝐭𝐞 𝟐𝟒𝐡 𝐧𝐨 𝐓𝐞𝐥𝐞𝐠𝐫𝐚𝐦 🗓 𝐏𝐫𝐨𝐦𝐨çã𝐨 𝐞𝐧𝐜𝐞𝐫𝐫𝐚 𝐞𝐦 𝟓 𝐌𝐈𝐍𝐔𝐓𝐎𝐒!  ⌛️ 5 𝗩𝗔𝗚𝗔𝗦 '
                            '𝗖𝗢𝗠 𝗗𝗘𝗦𝗖𝗢𝗡𝗧𝗢 DE 4,65 𝗣𝗢𝗥 𝗧𝗘𝗠𝗣𝗢 𝗟𝗜𝗠𝗜𝗧𝗔𝗗𝗢 ⬇️'},
 'plans': {'week_offer': {'label': '⭐ 1 Semana 30% OFF ⭐',
                          'price': 6.02,
                          'price_text': 'R$ 6.02',
                          'duration_days': 7,
                          'description': 'Assinatura semanal VIP',
                          'kind': 'initial'},
           'lifetime_offer': {'label': '🔥 VIP Vitalício 🔥',
                              'price': 14.99,
                              'price_text': 'R$ 14.99',
                              'duration_days': None,
                              'description': 'Assinatura vitalícia VIP',
                              'kind': 'initial'},
           'lifetime_secret_offer': {'label': '🌸Vitalício + Pastas Secretas',
                                     'price': 22.89,
                                     'price_text': 'R$ 22.89',
                                     'duration_days': None,
                                     'description': 'Assinatura vitalícia VIP + materiais extras',
                                     'kind': 'initial'},
           'week_downsell': {'label': '⭐ 1 Semana 30% OFF ⭐ por R$ 5.72 ',
                             'price': 5.72,
                             'price_text': 'R$ 5.72',
                             'duration_days': 7,
                             'description': 'Assinatura semanal VIP - downsell',
                             'kind': 'downsell'},
           'lifetime_downsell': {'label': '🔥 VIP Vitalício 🔥 por R$ 14.24 (5% OFF)',
                                 'price': 14.24,
                                 'price_text': 'R$ 14.24',
                                 'duration_days': None,
                                 'description': 'Assinatura vitalícia VIP   - downsell',
                                 'kind': 'downsell'},
           'lifetime_secret_downsell': {'label': '🌸Vitalício + Pastas Secretas',
                                        'price': 21.75,
                                        'price_text': 'R$ 21.75',
                                        'duration_days': None,
                                        'description': 'Assinatura vitalícia VIP   + materiais '
                                                       'extras - downsell',
                                        'kind': 'downsell'},
           'upsell_1_primary': {'label': '',
                                'price': 4.99,
                                'price_text': '',
                                'duration_days': None,
                                'description': 'Upsell 1 principal VIP',
                                'kind': 'upsell_1'},
           'upsell_2_primary': {'label': '',
                                'price': 4.65,
                                'price_text': '',
                                'duration_days': None,
                                'description': 'Upsell 2 principal VIP  ',
                                'kind': 'upsell_2'}},
 'videos': {'vip_funnel_video': 'vip_funnel_video.mp4',
            'downsell_video': 'downsell_video.mp4',
            'upsell_1_video': 'upsell_1_video.mp4',
            'upsell_2_video': 'upsell_2_video.mp4'}}
# === FUNNEL_CONFIG_END ===

FUNNEL_VIDEO_KEYS = {
    "vip_funnel_video": "Vídeo da oferta inicial",
    "downsell_video": "Vídeo do downsell",
    "upsell_1_video": "Vídeo do upsell 1",
    "upsell_2_video": "Vídeo do upsell 2",
}

CALLBACK_SUBSCRIBE = "vip_subscribe"
CALLBACK_MENU = "back_to_menu"
CALLBACK_PLAN_PREFIX = "vip_plan:"
CALLBACK_DOWNSELL_PLAN_PREFIX = "vip_downsell_plan:"
CALLBACK_UPSELL_2 = "vip_upsell_2"
CALLBACK_CREATE_CHARGE_PREFIX = "vip_create_charge:"
CALLBACK_UPSELL_1_PLAN_PREFIX = "vip_upsell_1_plan:"
CALLBACK_UPSELL_2_PLAN_PREFIX = "vip_upsell_2_plan:"

app = Flask(__name__)
app.secret_key = (
    os.environ.get("FLASK_SECRET_KEY", "").strip()
    or os.environ.get("TELEGRAM_TOKEN", "").strip()
    or "funnel-editor-secret"
)

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


def current_datetime() -> datetime:
    return datetime.now(APP_TIMEZONE)


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)]]
    )


def pix_payment_keyboard(pix_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📋 Copiar código PIX",
                    copy_text=CopyTextButton(text=pix_code),
                )
            ],
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def load_funnel_config() -> dict[str, Any]:
    if isinstance(EMBEDDED_FUNNEL_CONFIG, dict) and EMBEDDED_FUNNEL_CONFIG:
        return EMBEDDED_FUNNEL_CONFIG
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
    global EMBEDDED_FUNNEL_CONFIG

    source = MAIN_FILE_PATH.read_text(encoding="utf-8")
    start_index = source.find(f"{FUNNEL_CONFIG_START_MARKER}\nEMBEDDED_FUNNEL_CONFIG")
    end_index = source.find(FUNNEL_CONFIG_END_MARKER, start_index if start_index != -1 else 0)
    if start_index == -1 or end_index == -1 or end_index <= start_index:
        raise RuntimeError("Marcadores de configuração do funil não encontrados no main.py.")

    rendered_config = pprint.pformat(config, sort_dicts=False, width=100)
    replacement = (
        f"{FUNNEL_CONFIG_START_MARKER}\n"
        f"EMBEDDED_FUNNEL_CONFIG: dict[str, Any] = {rendered_config}\n"
        f"{FUNNEL_CONFIG_END_MARKER}"
    )
    updated_source = (
        source[:start_index]
        + replacement
        + source[end_index + len(FUNNEL_CONFIG_END_MARKER):]
    )
    MAIN_FILE_PATH.write_text(updated_source, encoding="utf-8")
    EMBEDDED_FUNNEL_CONFIG = config


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


def current_funnel_videos() -> dict[str, str]:
    config = load_funnel_config()
    raw_videos = config.get("videos")
    if not isinstance(raw_videos, dict):
        return {}
    videos: dict[str, str] = {}
    for key in FUNNEL_VIDEO_KEYS:
        value = raw_videos.get(key)
        if isinstance(value, str) and value.strip():
            videos[key] = value.strip()
    return videos


def funnel_video_path(video_key: str) -> Path | None:
    configured = current_funnel_videos().get(video_key)
    if configured:
        candidate = FUNNEL_MEDIA_DIR / configured
        if candidate.exists():
            return candidate
    if video_key == "vip_funnel_video" and FUNNEL_VIDEO_PATH.exists():
        return FUNNEL_VIDEO_PATH
    return None


def build_funnel_editor_config() -> dict[str, Any]:
    return {
        "texts": {key: current_funnel_text(key) for key in DEFAULT_FUNNEL_TEXTS},
        "plans": current_plans(),
        "videos": current_funnel_videos(),
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
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def upsell_1_keyboard(base_plan_code: str) -> InlineKeyboardMarkup:
    plans = current_plans()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(plans["upsell_1_primary"]["label"] + " por " + plans["upsell_1_primary"]["price_text"], callback_data=CALLBACK_UPSELL_1_PLAN_PREFIX + "upsell_1_primary|" + base_plan_code)],
            [
                InlineKeyboardButton("✅ ADICIONAR✅", callback_data=CALLBACK_UPSELL_1_PLAN_PREFIX + "upsell_1_primary|" + base_plan_code),
                InlineKeyboardButton("❌ NÃO QUERO❌", callback_data=CALLBACK_CREATE_CHARGE_PREFIX + base_plan_code),
            ],
            [InlineKeyboardButton("🔙 Voltar ao menu", callback_data=CALLBACK_MENU)],
        ]
    )


def upsell_2_keyboard(base_plan_code: str) -> InlineKeyboardMarkup:
    plans = current_plans()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(plans["upsell_2_primary"]["label"] + " por " + plans["upsell_2_primary"]["price_text"], callback_data=CALLBACK_UPSELL_2_PLAN_PREFIX + "upsell_2_primary|" + base_plan_code)],
            [
                InlineKeyboardButton("✅ ADICIONAR✅", callback_data=CALLBACK_UPSELL_2_PLAN_PREFIX + "upsell_2_primary|" + base_plan_code),
                InlineKeyboardButton("❌ NÃO QUERO❌", callback_data=CALLBACK_MENU),
            ],
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
        "base_plan_code": "TEXT",
        "next_followup_at": "TEXT",
        "last_downsell_at": "TEXT",
        "upsell_2_sent_at": "TEXT",
        "reminder_count": "INTEGER DEFAULT 0",
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


def replace_pending_syncpay_charges(user_id: int, keep_identifier: str) -> None:
    with database_connection() as connection:
        connection.execute(
            """
            UPDATE syncpay_cobrancas
            SET status = 'replaced',
                next_followup_at = NULL,
                updated_at = ?
            WHERE user_id = ?
              AND identifier != ?
              AND status = 'pending'
            """,
            (current_datetime().isoformat(), user_id, keep_identifier),
        )


def get_due_syncpay_followups(reference_time: datetime) -> list[sqlite3.Row]:
    with database_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM syncpay_cobrancas
            WHERE status = 'pending'
              AND next_followup_at IS NOT NULL
              AND next_followup_at != ''
              AND next_followup_at <= ?
            ORDER BY next_followup_at ASC
            """,
            (reference_time.isoformat(),),
        ).fetchall()
    return list(rows)


def get_latest_pending_syncpay_charge_for_user(user_id: int) -> sqlite3.Row | None:
    with database_connection() as connection:
        return connection.execute(
            """
            SELECT *
            FROM syncpay_cobrancas
            WHERE user_id = ?
              AND status = 'pending'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()


def update_syncpay_followup_state(
    identifier: str,
    *,
    next_followup_at: datetime | None,
    last_downsell_at: datetime | None = None,
    upsell_2_sent_at: datetime | None = None,
    reminder_count: int | None = None,
) -> None:
    with database_connection() as connection:
        connection.execute(
            """
            UPDATE syncpay_cobrancas
            SET next_followup_at = ?,
                last_downsell_at = COALESCE(?, last_downsell_at),
                upsell_2_sent_at = COALESCE(?, upsell_2_sent_at),
                reminder_count = COALESCE(?, reminder_count),
                updated_at = ?
            WHERE identifier = ?
            """,
            (
                next_followup_at.isoformat() if next_followup_at is not None else None,
                last_downsell_at.isoformat() if last_downsell_at is not None else None,
                upsell_2_sent_at.isoformat() if upsell_2_sent_at is not None else None,
                reminder_count,
                current_datetime().isoformat(),
                identifier,
            ),
        )


def save_syncpay_charge(
    identifier: str,
    user_id: int,
    status: str,
    pix_code: str,
    plan_code: str,
    amount: float,
    *,
    base_plan_code: str | None = None,
) -> None:
    now = current_datetime()
    timestamp = now.isoformat()
    next_followup_at = (
        now + INITIAL_FOLLOWUP_DELAY
        if status == "pending"
        else None
    )
    with database_connection() as connection:
        connection.execute(
            """
            INSERT INTO syncpay_cobrancas (
                identifier,
                user_id,
                status,
                pix_code,
                plan_code,
                amount,
                base_plan_code,
                next_followup_at,
                last_downsell_at,
                upsell_2_sent_at,
                reminder_count,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, ?, ?)
            ON CONFLICT(identifier) DO UPDATE SET
                user_id = excluded.user_id,
                status = excluded.status,
                pix_code = excluded.pix_code,
                plan_code = excluded.plan_code,
                amount = excluded.amount,
                base_plan_code = excluded.base_plan_code,
                next_followup_at = excluded.next_followup_at,
                last_downsell_at = NULL,
                upsell_2_sent_at = NULL,
                reminder_count = 0,
                updated_at = excluded.updated_at
            """,
            (
                identifier,
                user_id,
                status,
                pix_code,
                plan_code,
                amount,
                base_plan_code,
                next_followup_at.isoformat() if next_followup_at is not None else None,
                timestamp,
                timestamp,
            ),
        )


def update_syncpay_charge_status(identifier: str, status: str, pix_code: str | None = None) -> None:
    timestamp = current_datetime().isoformat()
    with database_connection() as connection:
        clear_followup = status != "pending"
        if pix_code is None:
            connection.execute(
                """
                UPDATE syncpay_cobrancas
                SET status = ?,
                    next_followup_at = CASE WHEN ? THEN NULL ELSE next_followup_at END,
                    updated_at = ?
                WHERE identifier = ?
                """,
                (status, clear_followup, timestamp, identifier),
            )
        else:
            connection.execute(
                """
                UPDATE syncpay_cobrancas
                SET status = ?,
                    pix_code = ?,
                    next_followup_at = CASE WHEN ? THEN NULL ELSE next_followup_at END,
                    updated_at = ?
                WHERE identifier = ?
                """,
                (status, pix_code, clear_followup, timestamp, identifier),
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


def syncpay_first_value(source: dict[str, Any] | None, *keys: str) -> str:
    if not isinstance(source, dict):
        return ""
    for key in keys:
        value = source.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return ""


def extract_syncpay_pix_code(source: dict[str, Any] | None) -> str:
    return syncpay_first_value(
        source,
        "pix_code",
        "paymentCode",
        "paymentcode",
        "pixCode",
    )


def extract_syncpay_transaction_id(source: dict[str, Any] | None) -> str:
    return syncpay_first_value(
        source,
        "identifier",
        "idTransaction",
        "idtransaction",
        "id",
        "reference_id",
        "externalreference",
    )


def extract_syncpay_status(source: dict[str, Any] | None) -> str:
    raw_status = syncpay_first_value(
        source,
        "status",
        "status_transaction",
        "statusTransaction",
    ).lower()
    if raw_status in {"paid", "approved", "complete", "completed"}:
        return "completed"
    if raw_status in {"pending", "waiting_payment", "waiting", "created"}:
        return "pending"
    return raw_status


def parse_plan_and_base(raw_value: str) -> tuple[str, str | None]:
    plan_code, separator, base_plan_code = raw_value.partition("|")
    return plan_code, (base_plan_code or None) if separator else None


def create_syncpay_charge(
    user_id: int,
    nome: str,
    plan_code: str,
    *,
    base_plan_code: str | None = None,
) -> tuple[str, str]:
    webhook_url = notification_webhook_url()
    if not webhook_url:
        raise RuntimeError("WEBHOOK_URL não configurado.")
    plan = get_plan(plan_code)
    base_plan = get_plan(base_plan_code) if base_plan_code else None
    total_amount = float(plan["price"]) + (float(base_plan["price"]) if base_plan is not None else 0.0)
    display_label = (
        f"{base_plan['label']} + {plan['label']}"
        if base_plan is not None
        else plan["label"]
    )
    display_price_text = (
        f"R$ {total_amount:.2f}"
        if base_plan is not None
        else str(plan["price_text"])
    )

    cpf, email, telefone = get_or_create_syncpay_profile(user_id, nome)

    payload = {
        "amount": total_amount,
        "description": (
            f"{base_plan['description']} + {plan['description']}"
            if base_plan is not None
            else plan["description"]
        ),
        "webhook_url": webhook_url,
        "client": {
            "name": nome,
            "cpf": cpf,
            "email": email,
            "phone": telefone,
        },
    }

    response = syncpay_request("POST", "/api/partner/v1/cash-in", json=payload)
    response_data = response.get("data") if isinstance(response.get("data"), dict) else response
    pix_code = extract_syncpay_pix_code(response_data) or extract_syncpay_pix_code(response)
    identifier = extract_syncpay_transaction_id(response_data) or extract_syncpay_transaction_id(response)

    if not pix_code or not identifier:
        raise RuntimeError(f"Não foi possível gerar o PIX na SyncPay: {response}")

    save_pending_assinante(user_id, nome)
    save_payment_profile(user_id, nome, cpf, email, telefone)
    save_syncpay_charge(
        identifier,
        user_id,
        "pending",
        pix_code,
        plan_code,
        total_amount,
        base_plan_code=base_plan_code,
    )
    replace_pending_syncpay_charges(user_id, identifier)

    duration_days = plan.get("duration_days")
    duration_text = (
        f"{duration_days} dias"
        if isinstance(duration_days, int)
        else "vitalício"
    )

    payment_text = (
        "💳 ASSINAR VIP\n\n"
        f"Valor total: {display_price_text}\n\n"
        "Use o botão abaixo para copiar o código PIX.\n\n"
        "Assim que o pagamento for aprovado, você receberá automaticamente o link exclusivo do grupo VIP."
    )
    return payment_text, pix_code


def extract_syncpay_identifier(payload: dict[str, Any]) -> str:
    data = payload.get("data")
    if isinstance(data, dict):
        value = extract_syncpay_transaction_id(data)
        if value:
            return value
    return (
        extract_syncpay_transaction_id(payload)
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


async def send_funnel_stage_message(
    user_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup,
    *,
    video_key: str | None = None,
) -> None:
    if TELEGRAM_APP is None:
        raise RuntimeError("Bot do Telegram não inicializado.")
    video_path = funnel_video_path(video_key) if video_key else None
    if video_path is not None:
        with video_path.open("rb") as video_file:
            await TELEGRAM_APP.bot.send_video(
                chat_id=user_id,
                video=video_file,
            )
        await TELEGRAM_APP.bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=reply_markup,
        )
        return
    await TELEGRAM_APP.bot.send_message(
        chat_id=user_id,
        text=text,
        reply_markup=reply_markup,
    )


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
    status = extract_syncpay_status(data)
    pix_code = extract_syncpay_pix_code(data) or str(charge["pix_code"] or "").strip()

    if not status:
        transaction = fetch_syncpay_transaction(identifier)
        status = extract_syncpay_status(transaction)
        pix_code = extract_syncpay_pix_code(transaction) or pix_code

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
    base_plan_code = str(charge["base_plan_code"] or "").strip()
    access_plan_code = base_plan_code or plan_code
    try:
        plan = get_plan(plan_code)
        access_plan = get_plan(access_plan_code)
    except KeyError:
        logger.warning(
            "Cobrança SyncPay %s aprovada sem plano válido (%s).",
            identifier,
            access_plan_code,
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
    duration_days = access_plan.get("duration_days")
    vencimento = (
        data_pagamento + timedelta(days=int(duration_days))
        if isinstance(duration_days, int)
        else None
    )
    activate_assinante(
        user_id,
        nome,
        data_pagamento.isoformat(),
        vencimento.isoformat() if vencimento is not None else None,
    )
    mark_payment_processed(identifier, user_id)
    logger.info("Pagamento SyncPay %s aprovado para user_id=%s.", identifier, user_id)


def process_pending_followups() -> None:
    due_charges = get_due_syncpay_followups(current_datetime())
    if not due_charges:
        return

    for charge in due_charges:
        identifier = str(charge["identifier"])
        latest_charge = get_syncpay_charge(identifier)
        if latest_charge is None or str(latest_charge["status"] or "").strip().lower() != "pending":
            continue

        user_id = int(latest_charge["user_id"])
        now = current_datetime()
        reminder_count = int(latest_charge["reminder_count"] or 0)
        first_downsell = not str(latest_charge["last_downsell_at"] or "").strip()

        try:
            run_telegram_coroutine(
                send_funnel_stage_message(
                    user_id,
                    current_funnel_text("downsell_text"),
                    downsell_keyboard(),
                    video_key="downsell_video",
                )
            )
            upsell_2_sent_at = None
            if first_downsell:
                run_telegram_coroutine(
                    send_funnel_stage_message(
                        user_id,
                        current_funnel_text("upsell_2_text"),
                        upsell_2_keyboard(
                            str(latest_charge["base_plan_code"] or latest_charge["plan_code"] or "").strip()
                        ),
                        video_key="upsell_2_video",
                    )
                )
                upsell_2_sent_at = now
        except Exception as exc:
            logger.warning("Falha ao enviar follow-up da cobrança %s: %s", identifier, exc)
            continue

        update_syncpay_followup_state(
            identifier,
            next_followup_at=now + REPEATED_DOWNSELL_DELAY,
            last_downsell_at=now,
            upsell_2_sent_at=upsell_2_sent_at,
            reminder_count=reminder_count + 1,
        )


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


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await show_subscription_offer(update, context, via_callback=False)


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

    initial_video = funnel_video_path("vip_funnel_video")
    if initial_video is not None:
        with initial_video.open("rb") as video_file:
            await message.reply_video(video=video_file)
        await message.reply_text(
            current_funnel_text("vip_funnel_text"),
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
    *,
    base_plan_code: str | None = None,
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
        payment_text, pix_code = await asyncio.to_thread(
            create_syncpay_charge,
            user.id,
            user.full_name,
            plan_code,
            base_plan_code=base_plan_code,
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
        reply_markup=pix_payment_keyboard(pix_code),
    )


async def show_offer_stage(
    update: Update,
    text: str,
    reply_markup: InlineKeyboardMarkup,
    *,
    video_key: str | None = None,
) -> None:
    query = update.callback_query
    if query is None:
        return
    await safe_answer_callback(query)
    video_path = funnel_video_path(video_key) if video_key else None
    if video_path is not None and query.message:
        with video_path.open("rb") as video_file:
            await query.message.reply_video(video=video_file)
        await query.message.reply_text(text, reply_markup=reply_markup)
        return
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
        await show_subscription_offer(update, context, via_callback=True)
        return

    if data.startswith(CALLBACK_PLAN_PREFIX):
        base_plan_code = data[len(CALLBACK_PLAN_PREFIX):]
        await show_offer_stage(
            update,
            current_funnel_text("upsell_1_text"),
            upsell_1_keyboard(base_plan_code),
            video_key="upsell_1_video",
        )
        return

    if data.startswith(CALLBACK_CREATE_CHARGE_PREFIX):
        plan_code = data[len(CALLBACK_CREATE_CHARGE_PREFIX):]
        await create_charge_for_plan(update, context, plan_code, base_plan_code=plan_code)
        return

    if data.startswith(CALLBACK_DOWNSELL_PLAN_PREFIX):
        plan_code = data[len(CALLBACK_DOWNSELL_PLAN_PREFIX):]
        await create_charge_for_plan(update, context, plan_code, base_plan_code=plan_code)
        return

    if data == CALLBACK_UPSELL_2:
        current_base_plan = None
        if query.from_user:
            latest_pending = await asyncio.to_thread(
                get_latest_pending_syncpay_charge_for_user,
                query.from_user.id,
            )
            if latest_pending is not None and int(latest_pending["user_id"]) == query.from_user.id:
                current_base_plan = str(latest_pending["base_plan_code"] or latest_pending["plan_code"] or "").strip() or None
        await show_offer_stage(
            update,
            current_funnel_text("upsell_2_text"),
            upsell_2_keyboard(current_base_plan or "lifetime_offer"),
            video_key="upsell_2_video",
        )
        return

    if data.startswith(CALLBACK_UPSELL_1_PLAN_PREFIX):
        plan_code, base_plan_code = parse_plan_and_base(data[len(CALLBACK_UPSELL_1_PLAN_PREFIX):])
        await create_charge_for_plan(update, context, plan_code, base_plan_code=base_plan_code)
        return

    if data.startswith(CALLBACK_UPSELL_2_PLAN_PREFIX):
        plan_code, base_plan_code = parse_plan_and_base(data[len(CALLBACK_UPSELL_2_PLAN_PREFIX):])
        await create_charge_for_plan(update, context, plan_code, base_plan_code=base_plan_code)
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
    scheduler.add_job(
        process_pending_followups,
        trigger="interval",
        minutes=1,
        id="process_pending_followups",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    SCHEDULER = scheduler
    logger.info("APScheduler iniciado para expirações diárias e follow-ups de pagamento.")


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
    return {"service": "funnel-bot", "status": "ok"}, 200


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
        <form class="card" method="post" action="{{ url_for('funnel_editor_save') }}" enctype="multipart/form-data">
          <h2>Textos</h2>
          {% for text_key, text_value in texts.items() %}
            <label>{{ text_labels[text_key] }}</label>
            <textarea name="text__{{ text_key }}" data-preview-text="{{ text_key }}">{{ text_value }}</textarea>
          {% endfor %}

          <h2>Vídeos</h2>
          {% for video_key, video_label in video_labels.items() %}
            <div class="plan">
              <h3>{{ video_label }}</h3>
              <div class="muted">
                Atual: {{ videos.get(video_key, 'nenhum arquivo enviado') }}
              </div>
              <label>Enviar novo vídeo</label>
              <input type="file" name="video__{{ video_key }}" accept="video/*">
              <label><input type="checkbox" name="clear_video__{{ video_key }}" value="1"> Remover vídeo atual</label>
            </div>
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
            <div class="muted">Vídeo: {{ videos.get("vip_funnel_video", "padrão/nenhum") }}</div>
            <div class="preview-text" id="preview_vip_funnel_text">{{ texts["vip_funnel_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_week_offer">{{ plans["week_offer"]["label"] }} por {{ plans["week_offer"]["price_text"] }}</span>
              <span id="preview_plan_lifetime_offer">{{ plans["lifetime_offer"]["label"] }} por {{ plans["lifetime_offer"]["price_text"] }}</span>
              <span id="preview_plan_lifetime_secret_offer">{{ plans["lifetime_secret_offer"]["label"] }} por {{ plans["lifetime_secret_offer"]["price_text"] }}</span>
            </div>
          </div>
          <div class="preview-stage">
            <h3>Downsell</h3>
            <div class="muted">Vídeo: {{ videos.get("downsell_video", "nenhum") }}</div>
            <div class="preview-text" id="preview_downsell_text">{{ texts["downsell_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_week_downsell">{{ plans["week_downsell"]["label"] }}</span>
              <span id="preview_plan_lifetime_downsell">{{ plans["lifetime_downsell"]["label"] }}</span>
              <span id="preview_plan_lifetime_secret_downsell">{{ plans["lifetime_secret_downsell"]["label"] }}</span>
            </div>
          </div>
          <div class="preview-stage">
            <h3>Upsell 1</h3>
            <div class="muted">Vídeo: {{ videos.get("upsell_1_video", "nenhum") }}</div>
            <div class="preview-text" id="preview_upsell_1_text">{{ texts["upsell_1_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_upsell_1_primary">{{ plans["upsell_1_primary"]["label"] }} por {{ plans["upsell_1_primary"]["price_text"] }}</span>
              <span>✅ ADICIONAR✅</span>
              <span>❌ NÃO QUERO❌</span>
            </div>
          </div>
          <div class="preview-stage">
            <h3>Upsell 2</h3>
            <div class="muted">Vídeo: {{ videos.get("upsell_2_video", "nenhum") }}</div>
            <div class="preview-text" id="preview_upsell_2_text">{{ texts["upsell_2_text"] }}</div>
            <div class="preview-buttons">
              <span id="preview_plan_upsell_2_primary">{{ plans["upsell_2_primary"]["label"] }} por {{ plans["upsell_2_primary"]["price_text"] }}</span>
              <span>✅ ADICIONAR✅</span>
              <span>❌ NÃO QUERO❌</span>
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
        videos=config["videos"],
        text_labels={
            "vip_funnel_text": "Texto da oferta inicial",
            "downsell_text": "Texto do downsell",
            "upsell_1_text": "Texto do upsell 1",
            "upsell_2_text": "Texto do upsell 2",
        },
        video_labels=FUNNEL_VIDEO_KEYS,
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
        "videos": current_funnel_videos(),
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

    FUNNEL_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    for video_key in FUNNEL_VIDEO_KEYS:
        if request.form.get(f"clear_video__{video_key}") == "1":
            existing_name = config["videos"].pop(video_key, None)
            if existing_name:
                existing_path = FUNNEL_MEDIA_DIR / existing_name
                if existing_path.exists():
                    existing_path.unlink()

        uploaded = request.files.get(f"video__{video_key}")
        if not uploaded or not uploaded.filename:
            continue

        suffix = Path(uploaded.filename).suffix.lower() or ".mp4"
        filename = f"{video_key}{suffix}"
        target = FUNNEL_MEDIA_DIR / filename

        existing_name = config["videos"].get(video_key)
        if existing_name and existing_name != filename:
            existing_path = FUNNEL_MEDIA_DIR / existing_name
            if existing_path.exists():
                existing_path.unlink()

        uploaded.save(target)
        config["videos"][video_key] = filename

    save_funnel_config(config)
    return redirect(url_for("funnel_editor", saved=1))


@app.post("/funnel-editor/reset")
def funnel_editor_reset() -> Any:
    if not funnel_editor_authorized():
        return redirect(url_for("funnel_editor"))
    save_funnel_config({})
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
