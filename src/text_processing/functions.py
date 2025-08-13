import random
import asyncio
from loguru import logger

# Функция для логирования пауз
async def sleep_with_log(min_sec=6, max_sec=11):
    delay = random.uniform(min_sec, max_sec)
    logger.info(f"Ждём {delay:.2f} секунд...\n\n")
    await asyncio.sleep(delay)

