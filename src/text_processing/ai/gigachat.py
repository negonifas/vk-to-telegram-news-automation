"""
Все функции для работы с GigaChat должны быть в этом файле.
Тут инициализируется GigaChat и функция для переписывания текста через него.
Возвращаем переписанный текст.
"""
import os
import asyncio
from dotenv import load_dotenv
from gigachat import GigaChat
from loguru import logger

# --- GigaChat Initialization ---
load_dotenv()
giga_chat_token = os.getenv("GIGA_CHAT_TOKEN")
if not giga_chat_token:
    raise ValueError("Не найден токен GIGA_CHAT_TOKEN в .env файле")

giga = GigaChat(
    credentials=giga_chat_token,
    verify_ssl_certs=False,
)

SYSTEM_PROMPT = """
Ты — AI-редактор, эксперт по рерайтингу новостных текстов. 
Твоя задача — переписать предоставленный текст, соблюдая следующие правила:
1.  **Сохрани основной смысл**: Все ключевые факты, имена, цифры и события должны остаться без изменений.
2.  **Сделай текст уникальным**: Используй другие формулировки, синонимы и структуру предложений.
3.  **Стиль для Telegram**: Текст должен быть написан в информационном, легко читаемом стиле, подходящем для новостного Telegram-канала.
4.  **Ничего лишнего**: В ответе должен быть ТОЛЬКО переписанный текст. Не добавляй от себя никаких комментариев, приветствий или заключений вроде 'Вот переписанный текст:'.
5.  **Сохраняй тон**: Если исходный текст нейтральный, переписанный тоже должен быть нейтральным.
"""

async def rewrite_text_giga(text_to_rewrite: str) -> str | None:
    """
    Отправляет текст в GigaChat для переписывания.
    Возвращает переписанный текст или None в случае ошибки.
    """
    payload = f"{SYSTEM_PROMPT}\n\nПерепиши следующий текст:\n\n{text_to_rewrite}"
    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: giga.chat(payload)
        )
        if response and response.choices:
            return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"❌ Ошибка при обращении к GigaChat: {e}")
    return None 