import re
import json
import pandas as pd
import requests
import os
from yt_dlp import YoutubeDL
from loguru import logger
from typing import Optional, Union, Dict, List
from config import credentials


def get_vk_access_token(app_id: str):

    """
    Функция для получения токена VK
    :param app_id: id VK приложения
    :return: authorization_URL
    """

    import webbrowser
    from urllib.parse import parse_qs, urlparse

    # Параметры вашего приложения VK
    app_id = app_id  # Замените на реальный ID
    redirect_uri = "https://oauth.vk.com/blank.html"
    # scope = "friends,photos,groups"  # Запрашиваемые права (через запятую)
    scope = "friends,photos,groups,video"

    # Формируем URL для авторизации
    auth_url = (
        f"https://oauth.vk.com/authorize?"
        f"client_id={app_id}&"
        f"display=page&"
        f"redirect_uri={redirect_uri}&"
        f"scope={scope}&"
        f"response_type=token&"
        f"v=5.131"
    )

    # Открываем браузер для авторизации
    print(f"Открываю браузер для авторизации: {auth_url}")
    webbrowser.open(auth_url)

    # Вручную скопируйте URL после редиректа (с access_token)
    result_url = input(
        "После авторизации вставьте URL из адресной строки браузера (начинается с 'https://oauth.vk.com/blank.html#'): "
    )

    # Парсим токен из URL
    parsed = urlparse(result_url)
    fragment = parsed.fragment
    params = parse_qs(fragment)

    access_token = params["access_token"][0]
    user_id = params["user_id"][0]

    print(f"\nВаш access_token: {access_token}")
    print(f"User ID: {user_id}")


def get_vk_user_info(access_token: str, user_id: str):
    import requests
    url = "https://api.vk.com/method/users.get"
    params = {
        "user_ids": user_id,
        "fields": "first_name,last_name,photo_200",
        "access_token": access_token,
        "v": "5.131"
    }
    response = requests.get(url, params=params)
    user_info = response.json()  # Сохраняем результат в переменную

    print("\nДанные пользователя:")  # Теперь это выполнится
    print(user_info)  # Печатаем результат

    return user_info  # Возвращаем данные


def get_all_group_members(
    access_token: str,
    group_id: str,
    save_path: str = None,  # Путь типа "/home/user/members.csv"
    delay: float = 0.4
):
    """
    Получает участников группы и сохраняет в CSV.

    :param access_token: Токен VK API
    :param group_id: ID группы (например, "durov")
    :param save_path: Путь к файлу .csv (если None — не сохраняет)
    :param delay: Задержка между запросами (секунды)
    :return: DataFrame с данными
    """
    import requests
    import pandas as pd
    import os
    import time

    url = 'https://api.vk.com/method/groups.getMembers'
    all_members = []
    offset = 0
    count = 1000

    try:
        while True:
            params = {
                'group_id': group_id,
                'access_token': access_token,
                'v': '5.131',
                'count': count,
                'offset': offset,
                'fields': 'first_name,last_name,sex,bdate,city,country,mobile_phone'
            }

            response = requests.get(url, params=params)
            data = response.json()

            if 'response' not in data:
                print("Ошибка:", data)
                break

            members = data['response']['items']
            if not members:
                break

            all_members.extend(members)
            offset += count
            print(f"Загружено: {len(all_members)} участников")
            time.sleep(delay)

        # Конвертируем в DataFrame
        df = pd.DataFrame(all_members)

        # Обработка вложенных полей (например, 'city' -> 'city.title')
        if 'city' in df.columns:
            df['city'] = df['city'].apply(lambda x: x.get('title') if isinstance(x, dict) else None)

        if 'country' in df.columns:
            df['country'] = df['country'].apply(lambda x: x.get('title') if isinstance(x, dict) else None)

        # Сохраняем в CSV
        if save_path:
            if not save_path.endswith('.csv'):
                save_path += '.csv'  # Добавляем расширение, если забыли
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            df.to_csv(save_path, index=False,
                      encoding='utf-8-sig')  # utf-8-sig для корректного отображения кириллицы в Excel
            print(f"Данные сохранены в: {save_path}")

        return df

    except Exception as e:
        print(f"Ошибка: {e}")
        return pd.DataFrame()



def get_last_posts(
    access_token: str,
    group_name: str,
    count: int = 2,
    save_path: str = None
) -> Optional[List[Dict]]:
    """
    Получает последние посты группы ВК и возвращает их в виде списка словарей.
    Сохраняет JSON на диск, если указан путь (save_path).
    """

    # Получаем ID группы
    group_info = requests.get(
        "https://api.vk.com/method/groups.getById",
        params={
            "group_id": group_name,
            "access_token": access_token,
            "v": "5.131"
        }
    ).json()

    if "error" in group_info:
        print(f"Ошибка: {group_info['error']['error_msg']}")
        return None

    group_id = -int(group_info["response"][0]["id"])

    # Получаем посты
    posts = requests.get(
        "https://api.vk.com/method/wall.get",
        params={
            "owner_id": group_id,
            "count": count,
            "access_token": access_token,
            "v": "5.131"
        }
    ).json()

    if "error" in posts:
        print(f"Ошибка: {posts['error']['error_msg']}")
        return None

    items = posts["response"]["items"]
    result = []

    for post in items:
        attachments = post.get("attachments", [])
        media_urls = []

        for attach in attachments:
            if attach["type"] == "photo":
                sizes = attach["photo"]["sizes"]
                base_size = next((s for s in sizes if s["type"] == "base"), None)
                media_urls.append(base_size["url"] if base_size else sizes[-1]["url"])

            elif attach["type"] == "video":
                video = attach["video"]
                owner_id = video["owner_id"]
                video_id = video["id"]

                # Запрашиваем информацию о видео
                video_info = requests.get(
                    "https://api.vk.com/method/video.get",
                    params={
                        "videos": f"{owner_id}_{video_id}",
                        "access_token": access_token,
                        "v": "5.131"
                    }
                ).json()

                if "error" in video_info:
                    print(f"Ошибка получения видео: {video_info['error']['error_msg']}")
                    continue

                video_data = video_info["response"]["items"][0]

                # Проверяем тип видео
                if video_data.get("type") == "short_video":
                    print("Пропущено: это short_video, нет прямой ссылки")
                    continue

                # Проверяем, есть ли прямая ссылка на .mp4
                files = video_data.get("files", {})
                direct_url = (
                    files.get("mp4_720") or
                    files.get("mp4_480") or
                    files.get("mp4_360") or
                    files.get("mp4_240")
                )

                if direct_url:
                    media_urls.append(direct_url)
                else:
                    # Опционально: можно сохранить обычную ссылку на видео
                    media_urls.append(f"https://vk.com/video{owner_id}_{video_id}")

        result.append({
            "date": pd.to_datetime(post["date"], unit="s").strftime("%Y-%m-%d %H:%M:%S"),
            "text": post.get("text", ""),
            "post_url": f"https://vk.com/wall{post['owner_id']}_{post['id']}",
            "media_urls": media_urls  # может быть пустым
        })

    # Сохраняем на диск (опционально)
    if save_path:
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        print(f"Данные сохранены в {save_path}")

    return result



def convert_vk_links_to_markdown(text: str) -> str:
    """
    Заменяет [club123|Текст] или [id123|Текст] на Markdown-ссылки [Текст](https://vk.com/club123)
    """
    pattern = re.compile(r"\[(club|id)(\d+)\|([^\]]+)\]")

    def replacer(match):
        type_, id_, label = match.groups()
        url = f"https://vk.com/{type_}{id_}"
        return f"[{label}]({url})"

    return pattern.sub(replacer, text)



def remove_vk_links_but_keep_text(text: str) -> str:
    """
    Удаляет любые ссылки в формате [что-то|Текст],
    оставляя только видимый текст (label), без ссылки.

    Пример:
    [club123|Моя группа] → Моя группа
    [https://vk.com/53ncpe?w=wall-188948864_1820|Новгородский центр] → Новгородский центр
    """

    # Ищем шаблоны вида [что-то|Текст] и заменяем на Текст (вторую группу)
    pattern = re.compile(r"\[[^\|\]]+\|([^\]]+)\]")

    return pattern.sub(lambda m: m.group(1), text)



def prepare_vk_post_for_tg(
    posts: List[Dict],
    post_index: Optional[int] = None
) -> Dict[str, Union[str, List[str]]]:
    """
    Подготавливает данные поста из ВК для публикации в Telegram.

    :param posts: Список постов из ВК (в виде словарей)
    :param post_index: Индекс нужного поста (опционально)
    :return: Словарь с подготовленными данными: {"text", "media_urls"}
             Если в посте есть видео — media_urls будет пустым.
    """

    # --- 1. Проверка входных данных ---
    if not isinstance(posts, list) or not posts:
        raise ValueError("Некорректный формат данных: ожидается непустой список постов")

    # --- 2. Выбор поста ---
    if post_index is None:
        post = posts[0]
    else:
        if post_index < 0 or post_index >= len(posts):
            raise IndexError("Индекс выходит за границы списка.")
        post = posts[post_index]

    # --- 3. Форматирование текста ---
    raw_text = post.get("text", "").strip()

    # Удаление лишних пробелов и пустых строк
    # lines = [line.strip() for line in raw_text.replace("\\n", "\n").splitlines()]
    # clean_text = "\n".join(line for line in lines if line)
    clean_text = raw_text.replace("\\n", "\n").strip()

    # # --- 3.1. Замена ссылок VK на Markdown-ссылки ---
    # clean_text = convert_vk_links_to_markdown(clean_text)
    # --- 3.1. Замена ссылок VK на текст без Markdown-ссылок ---
    clean_text = remove_vk_links_but_keep_text(clean_text)

    # --- 4. Проверка вложений и сбор изображений ---
    # media_urls = []
    attachments = post.get("attachments", [])

    # --- 4.1. Если есть хотя бы одно видео — не добавляем медиа вообще ---
    if any(att.get("type") == "video" for att in attachments):
        media_urls = []  # Видео есть — медиа не берём
    else:
        # --- 4.2. Обрабатываем только изображения (photo) ---
        media_urls = post.get("media_urls", [])

    # --- 5. Возврат результата ---
    return {
        "text": clean_text,
        "media_urls": media_urls  # Только изображения, если нет видео
    }


def get_vk_video_info(video_url) -> Optional[Dict]:
    """
    Функция для получения информации о видео
    :param video_url:
    :return словарь с информацией о видео:
    """
    ydl_opts = {
        'quiet': True,
        'skip_download': True,
    }
    with YoutubeDL(ydl_opts) as ydl:
        try:
            video_info = ydl.extract_info(video_url, download=False)
            return video_info
        except Exception as e:
            logger.error(e)
            return None


def download_vk_video(video_url, output_path='./videos/'):
    os.makedirs(output_path, exist_ok=True)

    ydl_opts = {
        'format': 'best',
        'quiet': True,
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
