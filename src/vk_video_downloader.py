import os
import re
import time
from pprint import pprint
from typing import Optional
from yt_dlp import YoutubeDL
from loguru import logger
from moviepy import VideoFileClip


def download_vk_video(video_url, output_path='./videos/'):
    os.makedirs(output_path, exist_ok=True)

    ydl_opts = {
        'format': 'best',
        'quiet': True,
        # 'proxy': 'socks5h://[::1]:2080',
    }

    with YoutubeDL(ydl_opts) as ydl:
        try:
            info_dict = ydl.extract_info(video_url, download=True)
            original_path = ydl.prepare_filename(info_dict)

            # Получаем название и расширение
            title = info_dict.get('title', 'video').lower()
            title = re.sub(r'[\s\-]+', '_', title)            # заменяем пробелы и тире на _
            title = re.sub(r'[^\w_]', '', title)              # удаляем всё лишнее
            title = title[:100]                               # ограничиваем длину (по желанию)

            ext = info_dict.get('ext', 'mp4')
            new_filename = f"{title}.{ext}"
            new_filepath = os.path.join(output_path, new_filename)

            os.rename(original_path, new_filepath)

            full_path = os.path.abspath(new_filepath)
            logger.info(f"Видео сохранено в: {full_path}")
            return full_path

        except Exception as e:
            logger.error(f"Ошибка скачивания видео: {e}")
            return None


def get_vk_video_info(video_url, retries=2, delay=3):
    """
    Получает информацию о видео с VK через yt-dlp.
    Повторяет попытку при ошибке 'No video formats found'.

    :param video_url: Ссылка на VK видео
    :param retries: Кол-во повторных попыток (по умолчанию 2)
    :param delay: Задержка между попытками в секундах
    :return: Словарь с информацией о видео или None
    """

    ydl_opts = {
        'quiet': True,
        'skip_download': True,
        'format': 'best',  # берем все форматы видео и проверяем размер
        # 'proxy': 'socks5h://[::1]:2080',
    }

    for attempt in range(retries + 1):
        try:
            with YoutubeDL(ydl_opts) as ydl:
                video_info = ydl.extract_info(video_url, download=False)
                return video_info

        except Exception as e:
            error_msg = str(e)
            logger.error(f"[Попытка {attempt + 1}/{retries + 1}] yt-dlp error: {error_msg}")

            if "No video formats found" in error_msg and attempt < retries:
                logger.warning(f"⏳ Видео пока не готово. Повтор через {delay} сек...")
                time.sleep(delay)
            else:
                break

    # (Опционально) Сохраняем HTML для отладки
    # try:
    #     html = requests.get(video_url).text
    #     with open("vk_debug.html", "w", encoding="utf-8") as f:
    #         f.write(html)
    #     logger.info("💾 HTML страницы VK сохранён как vk_debug.html")
    # except Exception as html_e:
    #     logger.warning(f"Не удалось сохранить HTML: {html_e}")

    return None


if __name__ == "__main__":
    # video_link = "https://vk.com/video-191849492_456239682"
    video_link = "https://www.youtube.com/watch?v=LrI62HmR5vc"

    # ~~~ Получить информацию о видео ~~~
    video_info = get_vk_video_info(video_link)
    logger.info(f"Длительность видео '{video_info.get('title')}':\n"
                f" {video_info.get('duration')} сек.\n"
                )
    # pprint(video_info)

    # ~~~ Скачать видео ~~~
    download_vk_video(video_link)
