import json
import asyncio
import sys
import os
import time
import dotenv
from telethon import TelegramClient
from userbot.userbot_tg_functions import PostResult  # Класс для хранения результатов публикации
from telethon.tl.types import PeerChannel  # Для работы с каналами
from userbot.userbot_tg_functions import userbot_post_to_channel
from src.text_processing.pipeline import process_posts
from database.db import create_db_pool, create_db_pool_diagnostic
from src.config_channels import channel_list
from config import credentials
# from run import prepare_vk_post_for_tg
from src.text_processing.pipeline import get_vk_last_posts, prepare_vk_post_for_tg
from loguru import logger
from pprint import pprint
from src.text_processing.functions import sleep_with_log

dotenv.load_dotenv()

# =============================
# Тестовый запуск пайплайна с реальным парсингом VK
# =============================

def load_test_posts(filepath: str) -> list:
    """
    Загружает тестовые посты из указанного JSON-файла.
    Возвращает список постов (list of dict).
    Если файл не найден или повреждён — выводит ошибку и завершает программу.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            print(f"❌ Ошибка: Ожидался список постов в файле {filepath}, а получено: {type(data)}")
            sys.exit(1)
        print(f"✅ Загружено {len(data)} постов из файла '{filepath}'")
        return data
    except FileNotFoundError:
        print(f"❌ Файл не найден: {filepath}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Ошибка чтения JSON в файле {filepath}: {e}")
        sys.exit(1)

async def main(token: str):
    # Старт сессии
    start_time1 = time.monotonic()
    
    # Кол-во постов для каждой группы
    count = 3

    # 0. Создаем пул соединений с базой данных
    try:
        pool = await create_db_pool()
        logger.info("🔄 Создали пул соединений с базой данных")
    except RuntimeError as e:
        logger.critical(f"🚫 Не удалось установить соединение с БД: {e}")
        return
    
#     # Тестируем создание пула соединения с БД
#     pool = await create_db_pool_diagnostic(
#     max_retries=3,
#     delay=2,
#     enable_ssl=False,  # попробуй без SSL
#     show_connection_params=True
# )
#     logger.info("🔄 Создали пул соединений с базой данных")
#     while True:
#         user_input = input("Нажмите Enter, чтобы продолжить или !!! для выхода\n")
#         if user_input == "!!!":
#             print("Выход из программы...")
#             sys.exit(0)
#         elif user_input == "":
#             break


    # # 0.1 Очищаем таблицы перед тестом
    # async with pool.acquire() as conn:
    #     await conn.execute("TRUNCATE TABLE posts RESTART IDENTITY CASCADE;")
    #     await conn.execute("TRUNCATE TABLE skipped_posts RESTART IDENTITY CASCADE;")
    # logger.info("🗑️ Таблицы 'posts' и 'skipped_posts' очищены.")
    # while True:
    #     user_input = input("Нажмите Enter, чтобы продолжить или !!! для выхода\n")
    #     if user_input == "!!!":
    #         print("Выход из программы...")
    #         sys.exit(0)
    #     elif user_input == "":
    #         break  # только Enter продолжает выполнение
    
    # 1. Получаем свежие посты из VK через парсер
    print("🔄 Получаем свежие посты из VK...")
    
    posts, vk_stats = get_vk_last_posts(
        access_token=token,
        group_names=channel_list,
        count=count,
        save_path=None,
        delay_between_requests=0.3
    )
    
    # Выводим статистику парсинга VK
    logger.info(f"📊 VK Parsing Stats: {vk_stats['processed_groups']}/{vk_stats['total_groups']} groups, {vk_stats['total_posts']} posts, {vk_stats['reposts']} reposts")

    # pprint(posts)

    # # Принудительная пауза с подтверждением продолжения
    # while True:
    #     user_input = input("Нажмите Enter, чтобы продолжить или !!! для выхода\n")
    #     if user_input == "!!!":
    #         print("Выход из программы...")
    #         sys.exit(0)
    #     elif user_input == "":
    #         break
    
    # Создаем словарь для подсчета количества для каждой group_name
    group_counts = {}
    for item in posts:
        group_name = item['group_name']
        group_counts[group_name] = group_counts.get(group_name, 0) + 1
    # Выводим результат
    print(f"Просмотренно постов {count} из каждой группы.\n"
          f"Извлечено постов для каждой группы: {group_counts}\n"
          f"Всего извлечено постов: {len(posts)}")
    
    # while True:
    #     user_input = input("Нажмите Enter, чтобы продолжить или !!! для выхода\n")
    #     if user_input == "!!!":
    #         print("Выход из программы...")
    #         sys.exit(0)
    #     elif user_input == "":
    #         break  # только Enter продолжает выполнение


    # Подготавливаем посты к обработке (фильтрация, нормализация и т.д.)
    prepared_posts, filtered_out_count = prepare_vk_post_for_tg(posts, interleave_groups=True)
    
    # # Содержание пре-подготовленых постов
    # pprint(prepared_posts)

    print(f"✅ Подготовлено {len(prepared_posts)} постов для обработки. Отфильтровано: {filtered_out_count} Причина: Репост\n")

    # Принудительная пауза с подтверждением продолжения
    while True:
        user_input = input("Нажмите Enter, чтобы продолжить или !!! для выхода\n")
        if user_input == "!!!":
            print("Выход из программы...")
            sys.exit(0)
        elif user_input == "":
            break


    # 2. Запускаем пайплайн обработки
    stats, approved_posts = await process_posts(prepared_posts, pool)
    # logger.info(f"✅ Пайплайн обработки завершен. Статистика: {stats}")
    # 3. Выводим подробную статистику
    print("\n=== Итоговая статистика ===")
    for k, v in stats.items():
        print(f"{k}: {v}")

    # Проверяем, что посты были успешно записаны в базу
    if stats['inserted'] > 0:
        print(f"\n✅ В базу записано {stats['inserted']} постов")
        print(f"📤 Готовим к отправке в Telegram {len(approved_posts)} одобренных постов")
    else:
        print("\n⚠️ В базу ничего не записалось, отправка в Telegram отменена")

    # 4.1 Инициилизация TG userbot
    api_id = int(os.getenv("API_ID"))
    api_hash = os.getenv("API_HASH")
    session_name = os.getenv("SESSION_NAME")
    bot = TelegramClient(session_name, api_id, api_hash)  # для Telethon
    try:
        await bot.start()
        logger.info("👍 Telethon-клиент / userbot запущен")
    except Exception as e:
        logger.error(f"❌ Ошибка при старте Telethon-клиента: {e}")
        return
    
    # 4.2 Публикуем данные в ТГ
    start_time2 = time.monotonic()
    post_count = 0
    error_count = 0
    # channel_id = os.getenv("PUBLIC_TG_CHANNEL_ID")  # Публичный канал
    channel_id = os.getenv("PRIVATE_TG_CHANNEL_ID")  # Приватный канал

    # ~~~ Универсальный способ получить ID канала ~~~
    channel_entity = None
    # Разделяем на публичный и приватный каналы
    try:
        if channel_id and channel_id.isdigit():
            # Приватный канал — передан числовой ID
            channel_entity = await bot.get_entity(PeerChannel(int(channel_id)))
        else:
            # Публичный канал — передан username (строка с @)
            channel_entity = await bot.get_entity(channel_id)
    except Exception as e:
        logger.error(f"❌ Ошибка при получении канала: {e}")

    # ~~~ Отправляем посты ~~~
    try:
        if approved_posts: # Используем approved_posts
            for prepared_data in approved_posts: # Используем approved_posts
                try:
                    result = await userbot_post_to_channel(  # для Telethon
                        bot=bot,
                        channel_id=channel_entity,
                        prepared_data=prepared_data,
                    )
                    if not isinstance(result, PostResult):
                        logger.error("❌ Некорректный результат от userbot_post_to_channel")
                        error_count += 1
                        continue

                    if result.success:
                        post_count += 1
                        logger.success(f"🎉 Пост: {post_count} из группы '{result.group_name}' успешно опубликован!")
                        if result.post_url:
                            logger.info(f"🔗 {result.post_url}")
                        await sleep_with_log(1, 3)
                    else:
                        error_count += 1
                        logger.error(f"❌ Ошибка публикации поста из группы '{result.group_name}': {result.error}")
                        logger.error("\n" + "=" * 120 + "\n")

                except Exception as e:
                    logger.error(f"❌ Исключение при отправке поста: {e}")
                    error_count += 1
        else:
            logger.warning("Нет подготовленных постов для публикации")

        logger.success(f"✅ Итог: {post_count} постов успешно опубликовано, {error_count} ошибок.")

    except Exception as e:
        logger.error(f"❌ Ошибка при отправке постов: {e}")

    finally:
        await bot.disconnect()  # для telethon
    
    # 5. Точно закрываем соединения с базой данных
    await pool.close()
    logger.info("🔒 Соединения с базой данных закрыты.")

    end_time = time.monotonic()
    total_time = end_time - start_time1
    total_time2 = end_time - start_time2

    minutes, seconds = divmod(total_time, 60)
    minutes2, seconds2 = divmod(total_time2, 60)

    logger.info(f"📩 Время выполнения TG-функции build_post_link: {int(minutes2)} мин {int(seconds2)} сек")
    logger.info(f"⏱ Общее время выполнения функций main: {int(minutes)} мин {int(seconds)} сек")
    

if __name__ == "__main__":
    try:
        asyncio.run(main(os.getenv("VK_API_TOKEN")))
    except Exception as e:
        logger.exception(f"❌ Произошла критическая ошибка при запуске main(): {e}")
 

# 1) source .venv/bin/activate
# 2) Enter 'python -m src.run_temp' in terminal to run the script