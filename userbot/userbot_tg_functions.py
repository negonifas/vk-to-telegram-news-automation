import requests
import asyncio
import random
import time
import os
from io import BytesIO
from src.vk_video_downloader import get_vk_video_info, download_vk_video
from loguru import logger
from dataclasses import dataclass
from typing import Optional


# Класс для хранения результатов публикации
@dataclass
class PostResult:
    success: bool
    group_name: str
    post_url: Optional[str] = None
    error: Optional[str] = None


# 🔧 Вспомогательная функция: скачивает файл в память и оборачивает в BufferedInputFile
# def download_to_buffer(url: str, filename: str = "file") -> BufferedInputFile:
#     response = requests.get(url)
#     response.raise_for_status()
#     return BufferedInputFile(response.content, filename=filename)

# 🔧 Вспомогательная функция: скачивает файл и возвращает BytesIO (Telethon-friendly)
def download_to_buffer(url: str, filename: str = "file") -> BytesIO:
    logger.info(f"Скачиваем файл по URL: {url}")
    
    for attempt in range(3):
        current_attempt = attempt + 1
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            logger.info(f"Попытка {current_attempt}/3 | ✅ Файл загружен в память ({len(response.content)} байт)")
            break
        # Логируем ошибку по Timeout
        except requests.exceptions.Timeout as e:
            if attempt == 2:  # Последняя попытка
                logger.error(f"Таймаут при загрузке {url} после 3 попыток")
                raise
            logger.warning(f"Попытка {attempt + 1}/3 | Таймаут при загрузке {url}")
            continue
        # Логируем остальные ошибки соединения
        except requests.exceptions.RequestException as e:
            if attempt == 2:  # Последняя попытка
                logger.error(f"Ошибка при загрузке {url} после 3 попыток: {e}")
                raise
            logger.warning(f"Попытка {attempt + 1}/3: {e} | Ошибка при загрузке {url}")
            continue

    buffer = BytesIO(response.content)
    buffer.name = filename  # Telethon использует имя при некоторых типах отправки
    # logger.info(f"✅ Файл загружен в память ({len(response.content)} байт)")
    return buffer


# Функция для логирования пауз
async def sleep_with_log(min_sec=8, max_sec=11):
    delay = random.uniform(min_sec, max_sec)
    logger.info(f"⏳ Ждём {delay:.2f} секунд...")
    await asyncio.sleep(delay)


async def userbot_post_to_channel(bot, channel_id, prepared_data):
    text = prepared_data.get("text", "").strip()
    media_urls = prepared_data.get("media_urls", [])
    gif_urls = prepared_data.get("gif_urls", [])
    video_urls = prepared_data.get("video_urls", [])
    link_preview = prepared_data.get("link_preview")
    link_preview_photo_url = link_preview.get("photo_url") if isinstance(link_preview, dict) else None
    group_name = prepared_data.get("group_name", "Unknown Group")
    original_post_url = prepared_data.get("original_post_url", "There is no post URL")
    post_date = prepared_data.get("post_date", "There is no post date")

    # Если channel_id является объектом
    post_link_prefix = f"https://t.me/{channel_id.username}" if getattr(channel_id, "username", None) else None

    # Флаг для отправки информационного сообщения
    post_info_msg = True

    # Флаг для отправки сообщения об ошибке в целевой канал
    channel_error_msg = True

    # Функция для построения ссылки на пост
    def build_post_link(message_id):
        if post_link_prefix:
            # для публичных каналов
            return f"{post_link_prefix}/{message_id}"
        # для приватных каналов
        elif isinstance(channel_id, int):
            clean_id = abs(channel_id) % (10 ** 10)
            return f"https://t.me/c/{clean_id}/{message_id}"
        else:
            return None

    # Функция для отправки информационного сообщения в начале каждой публикации
    async def send_info_message(bot, entity, post_date, group_name, original_post_url, link_preview=False):
        info_text = (
            f"🕒 Когда: {post_date}\n"
            f"👥 Группа: {group_name}\n"
            f"🔗 Первоисточник: {original_post_url}\n"
            f"📖 Тип: {scenario_name}"
        )
        await bot.send_message(entity, info_text, link_preview=link_preview)

    # --- Сценарий 0: Проверка на пустой пост ---
    if not text and not media_urls and not gif_urls and not video_urls and not link_preview_photo_url:
        logger.warning("⚠️ Нет текста и медиа, пропускаем отправку")
        return PostResult(success=False, group_name=prepared_data.get("group_name", "Unknown Group"),
                          error="Нет текста и медиа для отправки")

    # --- Сценарий 1: Только текст ---
    if not media_urls and not gif_urls and not video_urls and not link_preview_photo_url:
        scenario_name = "Сценарий 1 — только текст"
        logger.info(scenario_name)
        logger.info(f"Группа: {group_name}")
        logger.info(f"Первоисточник: {original_post_url}")

        # Отправляем информационное сообщение
        if post_info_msg:
            try:
                await send_info_message(bot, channel_id, post_date, group_name, original_post_url)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить информационное сообщение: {e}")

        # Отправляем основное сообщение
        try:
            message = await bot.send_message(
                entity=channel_id,
                message=text,
                link_preview=True
            )
            logger.info("✅ Текстовое сообщение отправлено")
            # await sleep_with_log()
            # post_link = f"https://t.me/{channel_id.lstrip('@')}/{message.id}"
            post_link = build_post_link(message.id)

            return PostResult(success=True, group_name=group_name, post_url=post_link)

        except Exception as e:
            logger.error(f"Ошибка при отправке текста: {e}")

            # Если возникла ошибка, отправляем сообщение об ошибке в канал
            if channel_error_msg:  # Проверяем, что флаг channel_error_msg равен True
                await bot.send_message(
                    entity=channel_id,
                    message=f"⚠️ Ошибка при отправке текста: {e}"
                )
            return PostResult(success=False, group_name=group_name, error=str(e))

    # --- Сценарий 2: Одна картинка + текст ---
    elif len(media_urls) == 1 and not gif_urls:
        scenario_name = "Сценарий 2 — одна картинка + текст"
        img_url = media_urls[0]
        logger.info(f"{scenario_name}: {img_url}")
        logger.info(f"Группа: {group_name}")
        logger.info(f"Первоисточник: {original_post_url}")

        # Отправляем информационное сообщение
        if post_info_msg:
            try:
                await send_info_message(bot, channel_id, post_date, group_name, original_post_url)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить информационное сообщение: {e}")

        # Отправляем основное сообщение
        try:
            input_file = download_to_buffer(img_url, filename="vk_image.jpg")
            caption = text if len(text) <= 2040 else None
            message = await bot.send_file(
                entity=channel_id,
                file=input_file,
                caption=caption
            )
            # await sleep_with_log()

            if len(text) > 2040:
                await bot.send_message(entity=channel_id, message=text)
                logger.info("🖼 Картинка + Текст отправлены отдельными сообщениями")
            else:
                logger.info("🖼 Картинка + Текст отправлены одним сообщением")

            # post_link = f"https://t.me/{channel_id.lstrip('@')}/{message.id}"
            post_link = build_post_link(message.id)
            return PostResult(success=True, group_name=group_name, post_url=post_link)

        except Exception as e:
            logger.error(f"Ошибка при отправке картинки: {e}")

            # Если возникла ошибка, отправляем сообщение об ошибке в канал
            if channel_error_msg:  # Проверяем, что флаг channel_error_msg равен True
                await bot.send_message(
                    entity=channel_id,
                    message=f"⚠️ Ошибка при отправке картинки: {e}"
                )
            return PostResult(success=False, group_name=group_name,
                              error=str(e))  # ИЗМЕНЕНО: возвращаем dataclass с ошибкой


    # --- Сценарий 3: Одна GIF-анимация + текст ---
    elif len(gif_urls) == 1:
        gif_url = gif_urls[0]
        scenario_name = "Сценарий 3 — одна GIF + текст"
        logger.info(f"{scenario_name}: {gif_url}")
        logger.info(f"Группа: {group_name}")
        logger.info(f"Первоисточник: {original_post_url}")

        # Отправляем информационное сообщение
        if post_info_msg:
            try:
                await send_info_message(bot, channel_id, post_date, group_name, original_post_url)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить информационное сообщение: {e}")

        # Отправляем основное сообщение
        try:
            input_file = download_to_buffer(gif_url, filename="vk_animation.gif")
            caption = text if len(text) <= 2040 else None
            gif_msg = await bot.send_file(
                entity=channel_id,
                file=input_file,
                caption=caption
            )
            results = [gif_msg]
            # await sleep_with_log()

            if len(text) > 2040:
                text_msg = await bot.send_message(entity=channel_id, message=text)
                results.append(text_msg)
                # await sleep_with_log()
                logger.info("✅ GIF + Текст отправлены отдельными сообщениями")
            else:
                logger.info("✅ GIF + Текст отправлен одним сообщением")

            # post_link = f"https://t.me/{channel_id.lstrip('@')}/{gif_msg.id}"
            post_link = build_post_link(gif_msg.id)
            return PostResult(success=True, group_name=group_name, post_url=post_link)  # ИЗМЕНЕНО: возвращаем dataclass

        except Exception as e:
            logger.error(f"Ошибка при отправке GIF: {e}")

            # Если возникла ошибка, отправляем сообщение об ошибке в канал
            if channel_error_msg:  # Проверяем, что флаг channel_error_msg равен True
                await bot.send_message(
                    entity=channel_id,
                    message=f"⚠️ Ошибка при отправке GIF: {e}"
                )
            return PostResult(success=False, group_name=group_name,
                              error=str(e))  # ИЗМЕНЕНО: возвращаем dataclass с ошибкой


    # --- Сценарий 4: Несколько картинок + текст ---
    elif len(media_urls) > 1:
        scenario_name = "Сценарий 4 — несколько картинок + текст"
        logger.info(scenario_name)
        logger.info(f"Группа: {group_name}")
        logger.info(f"Первоисточник: {original_post_url}")
        media_files = []

        for i, url in enumerate(media_urls):
            try:
                input_file = download_to_buffer(url, filename=f"vk_image_{i}.jpg")
                media_files.append(input_file)
            except Exception as e:
                logger.error(f"Ошибка при загрузке {url}: {e}")

        if not media_files:
            logger.error("❌ Нет доступных картинок для отправки")
            return PostResult(success=False, group_name=group_name, error="Нет доступных картинок для отправки")

        # Отправляем информационное сообщение
        if post_info_msg:
            try:
                await send_info_message(bot, channel_id, post_date, group_name, original_post_url)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить информационное сообщение: {e}")

        # Отправляем основное сообщение
        try:
            caption = text if len(text) <= 2040 else None
            group_msg = await bot.send_file(
                entity=channel_id,
                file=media_files,
                caption=caption
            )
            # group_msg может быть списком сообщений или одним сообщением
            messages = group_msg if isinstance(group_msg, list) else [group_msg]
            # await sleep_with_log()

            if 2040 < len(text) <= 4050:
                text_msg = await bot.send_message(entity=channel_id, message=text)
                messages.append(text_msg)
                # await sleep_with_log()
                logger.info("✅ Медиагруппа + Текст отправлены отдельными сообщениями")
            else:
                logger.info("✅ Медиагруппа + Текст отправлены одним сообщением")

            # Формируем ссылку на первое сообщение медиагруппы
            first_msg = messages[0]
            # post_link = f"https://t.me/{channel_id.lstrip('@')}/{first_msg.id}"
            post_link = build_post_link(first_msg.id)

            return PostResult(success=True, group_name=group_name, post_url=post_link)

        except Exception as e:
            logger.error(f"Ошибка при отправке медиагруппы: {e}")

            # Если возникла ошибка, отправляем сообщение об ошибке в канал
            if channel_error_msg:  # Проверяем, что флаг channel_error_msg равен True
                await bot.send_message(
                    entity=channel_id,
                    message=f"⚠️ Ошибка при отправке медиагруппы: {e}"
                )
            return PostResult(success=False, group_name=group_name, error=str(e))


    # --- Сценарий 5: Превью-ссылка + текст ---
    elif link_preview_photo_url:
        scenario_name = "Сценарий 5 — превью + текст"
        logger.info(scenario_name)
        logger.info(f"Группа: {group_name}")
        logger.info(f"Первоисточник: {original_post_url}")

        # Отправляем информационное сообщение
        if post_info_msg:
            try:
                await send_info_message(bot, channel_id, post_date, group_name, original_post_url)
            except Exception as e:
                logger.warning(f"⚠️ Не удалось отправить информационное сообщение: {e}")

        # Отправляем основное сообщение
        try:
            input_file = download_to_buffer(link_preview_photo_url, filename="vk_link_preview.jpg")
            caption = text if len(text) <= 2040 else None
            msg = await bot.send_file(
                entity=channel_id,
                file=input_file,
                caption=caption
            )
            # await sleep_with_log()

            if len(text) > 2040:
                await bot.send_message(entity=channel_id, message=text)
                logger.info("🖼 Превью + Текст отправлены отдельными сообщениями")
            else:
                logger.info("🖼 Превью + Текст отправлены одним сообщением")

            # post_link = f"https://t.me/{channel_id.lstrip('@')}/{msg.id}"
            post_link = build_post_link(msg.id)
            return PostResult(success=True, group_name=group_name, post_url=post_link)

        except Exception as e:
            logger.error(f"Ошибка при отправке превью: {e}")

            # Если возникла ошибка, отправляем сообщение об ошибке в канал
            if channel_error_msg:  # Проверяем, что флаг channel_error_msg равен True
                await bot.send_message(
                    entity=channel_id,
                    message=f"⚠️ Ошибка при отправке превью: {e}"
                )
            return PostResult(success=False, group_name=group_name, error=str(e))


    # --- Сценарий 6: Видео + текст ---
    elif video_urls:
        scenario_name = "Сценарий 6 — видео + текст"
        logger.info(scenario_name)
        logger.info(f"Группа: {group_name}")
        logger.info(f"Первоисточник: {original_post_url}")
        any_success = False
        last_post_link = None
        error_messages = []

        for video_url in video_urls:
            video_info = get_vk_video_info(video_url)
            if not video_info:
                logger.error(f"❌ Нет инфы по видео: {video_url}")
                error_messages.append(f"Нет инфы по видео: {video_url}")
                continue

            # Эта проверка и так не работала, а теперь ваще не нужна походу. Но пока тестирую
            # filesize = video_info.get("filesize") or video_info.get("filesize_approx") or 0
            # if filesize > 0:
            #     filesize_mb = filesize / (1024 * 1024)
            #     if filesize_mb > 250:
            #         logger.warning(f"⚠️ Видео больше 250MB ({filesize_mb:.2f}MB), пропущено: {video_url}")
            #         error_messages.append(f"Видео больше 250MB ({filesize_mb:.2f}MB): {video_url}")
            #         continue

            duration = video_info.get("duration", 0)
            if duration > 1800:
                logger.warning(f"⏳ Видео слишком длинное {duration} сек: {video_url}, пропущено")
                error_messages.append(f"⏳ Видео слишком длинное ({duration} сек): {video_url}")
                print()  # Добавляем отступ что бы сообщение не склеивалось со следующей интерполяцией
                continue

            video_file_path = download_vk_video(video_url)
            if not video_file_path or not os.path.isfile(video_file_path):
                logger.error(f"❌ Видео-файл не найден: {video_file_path}")
                error_messages.append(f"Видео-файл не найден: {video_file_path}")
                continue

            file_size_mb = os.path.getsize(video_file_path) / (1024 * 1024)
            if file_size_mb > 250:
                logger.warning(f"⚠️ Видео больше 250MB ({file_size_mb:.2f}MB), пропущено")
                error_messages.append(f"Видео больше 250MB ({file_size_mb:.2f}MB): {video_url}")
                continue

            # Отправляем информационное сообщение
            if post_info_msg:
                try:
                    await send_info_message(bot, channel_id, post_date, group_name, original_post_url)
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось отправить информационное сообщение: {e}")

            # Отправляем основное сообщение
            logger.info(f"📹 Размер файла: {file_size_mb:.2f} MB")
            logger.info(f"⏱ Длительность видео: {duration} сек")
            
            for attempt in range(3):
                try:
                    caption = text if len(text) <= 2040 else None
                    logger.info(f"📹 Начинается отправка видео файла (попытка {attempt + 1}/3)...")
                    start_time = time.monotonic()
                    msg = await bot.send_file(
                        entity=channel_id,
                        file=video_file_path,
                        caption=caption
                    )
                    end_time = time.monotonic()
                    logger.info(f"👍 Отправка видео файла завершена за {end_time - start_time:.2f} секунд")
                    # await sleep_with_log()
                    any_success = True
                    # last_post_link = f"https://t.me/{channel_id.lstrip('@')}/{msg.id}"
                    last_post_link = build_post_link(msg.id)

                    if len(text) > 2040:
                        await bot.send_message(entity=channel_id, message=text)
                        logger.info("✅ Видео + Текст отправлены отдельными сообщениями")
                    else:
                        logger.info("✅ Видео + Текст отправлено одним сообщением")
                    
                    break  # Успешная отправка, выходим из цикла

                except ConnectionError as e:
                    logger.warning(f"⚠️ Сетевая ошибка при отправке видео (попытка {attempt + 1}/3): {e}")
                    if attempt < 2:  # Если это не последняя попытка
                        delay = 5 * (attempt + 1)  # Экспоненциальная задержка: 5, 10, 15 сек
                        logger.info(f"⏳ Ждём {delay} секунд перед повторной попыткой...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"❌ Не удалось отправить видео после 3 попыток: {e}")
                        error_messages.append(f"Сетевая ошибка при отправке видео {video_url}: {e}")
                        if channel_error_msg:
                            await bot.send_message(
                                entity=channel_id,
                                message=f"⚠️ Сетевая ошибка при отправке видео: {video_url}"
                            )

                except Exception as e:
                    logger.error(f"❌ Критическая ошибка при отправке видео: {type(e).__name__}: {e}")
                    logger.error(f"📁 Размер файла: {file_size_mb:.2f} MB, путь: {video_file_path}")
                    
                    # Если возникла ошибка, отправляем сообщение об ошибке в канал
                    if channel_error_msg:  # Проверяем, что флаг channel_error_msg равен True
                        await bot.send_message(
                            entity=channel_id,
                            message=f"⚠️ Ошибка при отправке видео + текст: {video_url}: {e}"
                        )
                    error_messages.append(f"Критическая ошибка при отправке видео {video_url}: {e}")
                    break  # При критической ошибке не повторяем попытки

        if any_success:
            return PostResult(success=True, group_name=group_name, post_url=last_post_link)
        else:
            error_text = "; ".join(error_messages) if error_messages else "Не удалось отправить ни одно видео"
            return PostResult(success=False, group_name=group_name, error=error_text)

    else:
        logger.error("❌ Неизвестный сценарий для prepared_data")
        return PostResult(success=False, group_name=group_name, error="Неизвестный сценарий для prepared_data")
