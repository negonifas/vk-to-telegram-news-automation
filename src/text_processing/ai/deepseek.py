"""
Все функции для работы с DeepSeek должны быть в этом файле.
Тут инициализируется DeepSeek и функция для переписывания текста через него.
Возвращаем переписанный текст.
"""
import os
from dotenv import load_dotenv
from openai import OpenAI
from loguru import logger

# --- DeepSeek Initialization ---
load_dotenv()
deepseek_token = os.getenv("DEEPSEEK_TOKEN")
if not deepseek_token:
    raise ValueError("Не найден токен DEEPSEEK_TOKEN в .env файле")

client = OpenAI(api_key=deepseek_token, base_url="https://api.deepseek.com")

SYSTEM_PROMPT = """
Ты — AI-редактор, эксперт по рерайтингу новостных текстов. 
Твоя задача — переписать предоставленный текст, соблюдая следующие правила:
1.  **Сохрани основной смысл**: Все ключевые факты, имена, цифры и события должны остаться без изменений.
2.  **Сделай текст уникальным**: Используй другие формулировки, синонимы и структуру предложений.
3.  **Стиль для Telegram**: Текст должен быть написан в информационном, легко читаемом стиле, подходящем для новостного Telegram-канала.
4.  **Ничего лишнего**: В ответе должен быть ТОЛЬКО переписанный текст. Не добавляй от себя никаких комментариев, приветствий или заключений вроде 'Вот переписанный текст:'.
5.  **Сохраняй тон**: Если исходный текст нейтральный, переписанный тоже должен быть нейтральным.
"""

def rewrite_text_deepseek(text_to_rewrite: str) -> str | None:
    """
    Отправляет текст в DeepSeek для переписывания.
    Возвращает переписанный текст или None в случае ошибки.
    """
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text_to_rewrite},
            ],
            temperature=1,
            max_tokens=1024,
            stream=False,
        )
        if response and response.choices:
            return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"❌ Ошибка при обращении к DeepSeek: {e}")
    return None

if __name__ == "__main__":
    # Пример теста
    test_text = "Компания 'Техно-Прорыв' объявила о выпуске нового смартфона 'Галактика-25', который оснащен инновационным голографическим дисплеем и батареей на 100 часов работы. Продажи стартуют 1 сентября по цене 999 долларов."
    print("📝 Исходный текст:\n", test_text)
    rewritten = rewrite_text_deepseek(test_text)
    if rewritten:
        print("\n✅ Переписанный текст (DeepSeek):\n", rewritten)
    else:
        print("\n❌ Не удалось переписать текст через DeepSeek.") 