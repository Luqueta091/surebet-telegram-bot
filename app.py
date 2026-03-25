import logging
import os
from typing import Any

import httpx
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("surebet-bot")

app = Flask(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "surebet-webhook").strip()
WEBHOOK_BASE_URL = (
    os.getenv("WEBHOOK_BASE_URL", "").strip()
    or os.getenv("RENDER_EXTERNAL_URL", "").strip()
)

API_BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""
WEBHOOK_PATH = f"/telegram/webhook/{WEBHOOK_SECRET}"
WEBHOOK_URL = f"{WEBHOOK_BASE_URL.rstrip('/')}{WEBHOOK_PATH}" if WEBHOOK_BASE_URL else ""

START_TEXT = """Olá! Sou o assistente do grupo de Surebet 👋

Escolha o que você quer aprender:

📘 O que é Surebet?
🧮 Como calcular?
⚡ Como usar as entradas?
💰 Qual banca usar?"""

CONTENT_BY_ACTION = {
    "what_is_surebet": """📘 O QUE É SUREBET?

Surebet significa "aposta segura". É quando você aposta em todos os resultados possíveis de um jogo — em casas diferentes — e garante lucro independente do que acontecer.

Como isso é possível?
As casas de apostas competem entre si e às vezes oferecem odds em desacordo. Quando isso acontece, a soma das probabilidades fica abaixo de 100% — e aí entra o seu lucro.

Exemplo simples:
• Casa A: Time 1 vence → odd 2.10
• Casa B: Time 2 vence → odd 2.10

Apostando R$100 em cada lado → você recebe R$210 em qualquer resultado → lucro de R$10 garantido ✅

Isso não é sorte. É matemática.""",
    "how_to_calculate": """🧮 COMO CALCULAR UMA SUREBET?

Fórmula:
(1 ÷ odd1) + (1 ÷ odd2) < 1 = SUREBET ✅

Exemplo prático:
• Casa A: odd 2.10 → 1 ÷ 2.10 = 0.476
• Casa B: odd 2.10 → 1 ÷ 2.10 = 0.476
• Soma: 0.476 + 0.476 = 0.952 ✅

Como distribuir a banca:
Stake A = (Banca ÷ odd A) ÷ soma total
Stake B = (Banca ÷ odd B) ÷ soma total

💡 Aqui no grupo as entradas já vêm com os valores calculados — você só segue!""",
    "how_to_use_entries": """⚡ COMO USAR AS ENTRADAS DO GRUPO?

Passo a passo:

1️⃣ Ative as notificações do grupo
2️⃣ Ao receber a entrada, abra as duas (ou mais) casas indicadas
3️⃣ Aposte o valor exato em cada casa
4️⃣ Lucro garantido — não importa quem vencer!

⚠️ ATENÇÃO: As odds mudam em minutos. Quanto mais rápido você apostar, mais seguro é o lucro. Nunca aposte em apenas um lado.""",
    "bankroll_management": """💰 GESTÃO DE BANCA

Recomendação para iniciantes:
Use entre 5% e 10% da sua banca por entrada.

Exemplo:
Banca total: R$500
Por entrada: R$25 a R$50

Casas recomendadas para ter cadastro:
• Bet365
• Betano
• Sportingbet
• Betfair

💡 Dica: tenha pelo menos 3 casas ativas. Quanto mais casas, mais oportunidades você encontra.""",
    "recommended_houses": """🏢 CASAS RECOMENDADAS

Para começar, tenha cadastro nestas casas:
• Bet365
• Betano
• Sportingbet
• Betfair

💡 Dica: mantenha pelo menos 3 casas ativas e saldo distribuído entre elas para entrar rápido nas oportunidades.""",
}

TEXT_ACTIONS = {
    "/start": "start",
    "/menu": "start",
    "📘 o que é surebet?": "what_is_surebet",
    "🧮 como calcular?": "how_to_calculate",
    "⚡ como usar as entradas?": "how_to_use_entries",
    "💰 qual banca usar?": "bankroll_management",
    "🏢 casas recomendadas": "recommended_houses",
}

_webhook_checked = False


def main_menu_keyboard() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "📘 O que é Surebet?", "callback_data": "what_is_surebet"}],
            [{"text": "🧮 Como calcular?", "callback_data": "how_to_calculate"}],
            [{"text": "⚡ Como usar as entradas?", "callback_data": "how_to_use_entries"}],
            [{"text": "💰 Qual banca usar?", "callback_data": "bankroll_management"}],
            [{"text": "🏢 Casas recomendadas", "callback_data": "recommended_houses"}],
        ]
    }


def section_keyboard() -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": "🏢 Casas recomendadas", "callback_data": "recommended_houses"}],
            [{"text": "🏠 Voltar ao menu", "callback_data": "start"}],
        ]
    }


def keyboard_for_action(action: str) -> dict[str, Any]:
    return main_menu_keyboard() if action == "start" else section_keyboard()


def resolve_text(action: str) -> str:
    if action == "start":
        return START_TEXT
    return CONTENT_BY_ACTION.get(
        action,
        "Não entendi sua seleção. Use os botões abaixo para continuar.",
    )


def require_bot_token() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("A variável TELEGRAM_BOT_TOKEN não foi configurada.")


def telegram_request(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    require_bot_token()
    with httpx.Client(timeout=15.0) as client:
        response = client.post(f"{API_BASE_URL}/{method}", json=payload)
        response.raise_for_status()
        data = response.json()

    if not data.get("ok"):
        raise RuntimeError(f"Telegram API retornou erro em {method}: {data}")
    return data


def send_message(chat_id: int, text: str, reply_markup: dict[str, Any]) -> None:
    telegram_request(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "reply_markup": reply_markup,
            "disable_web_page_preview": True,
        },
    )


def answer_callback_query(callback_query_id: str) -> None:
    telegram_request("answerCallbackQuery", {"callback_query_id": callback_query_id})


def handle_action(chat_id: int, action: str) -> None:
    send_message(chat_id, resolve_text(action), keyboard_for_action(action))


def ensure_webhook() -> None:
    global _webhook_checked

    if _webhook_checked or not BOT_TOKEN or not WEBHOOK_URL:
        return

    _webhook_checked = True
    try:
        result = telegram_request(
            "setWebhook",
            {
                "url": WEBHOOK_URL,
                "allowed_updates": ["message", "callback_query"],
                "drop_pending_updates": False,
            },
        )
        logger.info("Webhook configurado: %s", result.get("description"))
    except Exception as exc:  # pragma: no cover - log de inicialização
        logger.exception("Falha ao configurar webhook: %s", exc)


@app.get("/")
def index() -> tuple[dict[str, Any], int]:
    return (
        {
            "service": "surebet-telegram-bot",
            "status": "ok",
            "webhook_path": WEBHOOK_PATH,
            "webhook_ready": bool(BOT_TOKEN and WEBHOOK_URL),
        },
        200,
    )


@app.get("/health")
def health() -> tuple[dict[str, str], int]:
    return {"status": "healthy"}, 200


@app.post(WEBHOOK_PATH)
def telegram_webhook() -> tuple[dict[str, bool], int]:
    ensure_webhook()

    if not BOT_TOKEN:
        logger.error("Webhook recebido sem TELEGRAM_BOT_TOKEN configurado.")
        return {"ok": False}, 503

    update = request.get_json(silent=True) or {}
    logger.info("Update recebido: %s", update.keys())

    try:
        callback_query = update.get("callback_query")
        if callback_query:
            action = callback_query.get("data", "start")
            message = callback_query.get("message", {})
            chat = message.get("chat", {})
            chat_id = chat.get("id")
            callback_query_id = callback_query.get("id")

            if chat_id and callback_query_id:
                answer_callback_query(callback_query_id)
                handle_action(chat_id, action)
            return {"ok": True}, 200

        message = update.get("message", {})
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        text = (message.get("text") or "").strip()
        normalized_text = text.lower()

        if chat_id:
            action = TEXT_ACTIONS.get(normalized_text, "start")
            handle_action(chat_id, action)

        return {"ok": True}, 200
    except Exception as exc:  # pragma: no cover - erro de runtime
        logger.exception("Erro ao processar update: %s", exc)
        return {"ok": False}, 500


ensure_webhook()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
