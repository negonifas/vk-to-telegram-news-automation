import asyncpg
from loguru import logger
import os
from dotenv import load_dotenv
import asyncio
import time


load_dotenv()

async def create_db_pool(max_retries: int = 5, delay: int = 3):
    """
    Соединений с базой данных с повтором при неудаче.

    :param max_retries: Максимальное количество попыток подключения
    :param delay: Задержка между попытками (в секундах)
    :return: asyncpg pool
    :raises Exception: если все попытки неудачны
    """
    attempt = 1

    while attempt <= max_retries:
        try:
            logger.info(f"🔌 Попытка #{attempt} подключения к базе данных...")
            pool = await asyncpg.create_pool(
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                database=os.getenv("DB_NAME"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT", "5432"),
                min_size=1,
                max_size=10,
                timeout=10  # Дополнительная защита — максимум 10 секунд на коннект
            )
            logger.success("✅ Database pool created successfully.")
            return pool

        except (asyncio.TimeoutError, OSError, ConnectionError) as conn_err:
            logger.warning(f"⚠️ Подключение не удалось (попытка #{attempt}): {conn_err}")
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка при подключении (попытка #{attempt}): {e}")

        if attempt < max_retries:
            await asyncio.sleep(delay)
            attempt += 1
        else:
            break

    logger.critical(f"❌ Все {max_retries} попытки подключения к базе данных исчерпаны.")
    raise RuntimeError("Failed to create database pool after multiple attempts.")


async def create_db_pool_diagnostic(
    max_retries: int = 5,
    delay: int = 3,
    enable_ssl: bool = True,
    show_connection_params: bool = True,
):
    """
    Диагностическая обёртка для создания пула соединений.
    Логирует подробности каждой попытки подключения.

    :param max_retries: сколько раз пробовать
    :param delay: пауза между попытками
    :param enable_ssl: использовать ли SSL-соединение
    :param show_connection_params: логировать ли параметры подключения (кроме пароля)
    :return: asyncpg.Pool
    """
    attempt = 1

    db_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "database": os.getenv("DB_NAME"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "min_size": 1,
        "max_size": 10,
        "timeout": 10,
        "ssl": "require" if enable_ssl else None,
    }

    if show_connection_params:
        safe_params = db_params.copy()
        safe_params.pop("password", None)
        logger.info(f"🛠 Подключение с параметрами: {safe_params}")

    while attempt <= max_retries:
        logger.info(f"🔌 Попытка #{attempt} установить пул соединений...")
        start = time.perf_counter()

        try:
            pool = await asyncpg.create_pool(**db_params)
            duration = time.perf_counter() - start
            logger.success(f"✅ Пул создан успешно за {duration:.2f} сек (попытка #{attempt})")
            return pool

        except (asyncio.TimeoutError, OSError, ConnectionError) as conn_err:
            duration = time.perf_counter() - start
            logger.warning(f"⚠️ Ошибка подключения #{attempt} (время: {duration:.2f} сек): {conn_err}")

        except Exception as e:
            duration = time.perf_counter() - start
            logger.error(f"❌ Неизвестная ошибка #{attempt} (время: {duration:.2f} сек): {e}")

        if attempt < max_retries:
            logger.info(f"⏳ Ждём {delay} секунд перед следующей попыткой...")
            await asyncio.sleep(delay)
            attempt += 1
        else:
            break

    logger.critical("❌ Все попытки подключения исчерпаны.")
    raise RuntimeError("Failed to create pool after multiple retries.")

# async def test_connection():
#     try:
#         pool = await asyncpg.create_pool(
#             user=os.getenv("DB_USER"),
#             password=os.getenv("DB_PASS"),
#             database=os.getenv("DB_NAME"),
#             host=os.getenv("DB_HOST"),
#             port=int(os.getenv("DB_PORT", 5432)),
#             min_size=1,
#             max_size=5
#         )
#         async with pool.acquire() as conn:
#             result = await conn.fetch("SELECT now()")
#             print("✅ Connected! Time on server:", result[0]["now"])
#         await pool.close()
#     except Exception as e:
#         print("❌ Connection failed:", e)
#
# if __name__ == "__main__":
#     asyncio.run(test_connection())