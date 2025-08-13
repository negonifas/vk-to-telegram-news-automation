import asyncio
import hashlib
import torch
import os
import time
import requests
import re
import json
from typing import List, Dict, Any, Optional, Union, Tuple
from dotenv import load_dotenv
from src.text_processing.ai.gigachat import rewrite_text_giga
from src.text_processing.ai.deepseek import rewrite_text_deepseek
from loguru import logger
from database.db import create_db_pool
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
from src.vk_function import remove_vk_links_but_keep_text

load_dotenv()

# Загружаем модель один раз при импорте модуля
# Если не установлен sentence-transformers: pip install sentence-transformers
model = SentenceTransformer(os.getenv("LOCAL_BERT_VECTOR_MODEL_PATH"))  # Модель скачана на диск
# model = SentenceTransformer("all-mpnet-base-v2") # векторная размерность 768
# model = SentenceTransformer("paraphrase-mpnet-base-v2")  # не поддерживается моим процессором

# Порог косинусного сходства для семантических дублей
SEMANTIC_THRESHOLD = 0.95

# Флаг для отключения AI (для тестирования)
AI_DISABLED = False  # Поставь True для отключения AI

# --- AI Provider Switcher ---
def ai_provider():
    ai = os.getenv("AI_PROVIDER", "gigachat").lower()
    if ai == "deepseek":
        logger.info("AI-провайдер: DeepSeek")
        return rewrite_text_deepseek
    else:
        logger.info("AI-провайдер: GigaChat")
        return rewrite_text_giga

async def log_skipped_post(conn, new_post_url: str, reason: str,
                          hash_value: str = None, similar_post_url: str = None,
                          similarity: float = None, group_name: str = None,
                          raw_text: str = None):
    """
    Логирует пропущенный пост в таблицу skipped_posts.
    """
    # Получаем дату похожего поста из базы данных
    similar_post_date = None
    if similar_post_url:
        try:
            similar_post_result = await conn.fetchrow(
                "SELECT created_at FROM posts WHERE original_post_url = $1", 
                similar_post_url
            )
            if similar_post_result:
                similar_post_date = similar_post_result['created_at']
        except Exception as e:
            logger.warning(f"Не удалось получить дату похожего поста: {e}")
    
    await conn.execute(
        """
        INSERT INTO skipped_posts (
            new_post_url, reason, hash, similar_post_url, similarity, group_name, raw_text, similar_post_date
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (new_post_url, reason, hash) DO NOTHING
        """,
        new_post_url, reason, hash_value, similar_post_url, similarity, group_name, raw_text, similar_post_date
    )


async def filter_by_hash(posts: List[dict], conn) -> tuple[List[dict], int]:  # Новый синтаксис
    """
    Фильтрация постов по хэшу (точные дубли) с проверкой в базе данных.
    """
    unique_posts = []
    skipped = 0
    for post in posts:
        text = post.get("text", "").strip()
        if not text:
            skipped += 1
            continue
        hash_value = hashlib.sha256(text.encode("utf-8")).hexdigest()
        
        # Проверяем, есть ли такой хэш в базе данных
        result = await conn.fetchrow("SELECT hash, original_post_url FROM posts WHERE hash = $1", hash_value)
        if result:
            # Хэш уже есть в базе — пропускаем
            await log_skipped_post(
                conn,
                new_post_url=post.get("original_post_url"),
                reason="hash_duplicate",
                hash_value=hash_value,
                similar_post_url=result['original_post_url'],  # URL похожего поста
                group_name=post.get("group_name"),
                raw_text=text
            )
            skipped += 1
            continue
        
        # Добавляем хэш к посту для последующего сохранения
        post['hash'] = hash_value
        unique_posts.append(post)
    return unique_posts, skipped


async def filter_by_url(posts: List[dict], conn) -> tuple[List[dict], int]:
    """
    Фильтрация постов по оригинальному URL с проверкой в базе данных.
    Дополнительно проверяет дубли в рамках одной сессии.
    """
    unique_posts = []
    skipped = 0
    seen_urls = set()  # Для отслеживания URL в рамках одной сессии
    
    for post in posts:
        url = post.get("original_post_url")
        group_name = post.get("group_name", "")
        
        if not url:
            unique_posts.append(post)
            continue
        
        # Проверяем дубли в рамках одной сессии
        # url_key = f"{url}_{group_name}"  # БРЕД!
        if url in seen_urls:
            await log_skipped_post(
                conn,
                new_post_url=url,
                reason="session_duplicate",
                similar_post_url=url,
                group_name=group_name,
                raw_text=post.get("text", "")
            )
            skipped += 1
            continue
        
        # Проверяем, есть ли такой URL в базе данных
        result = await conn.fetchrow("SELECT original_post_url FROM posts WHERE original_post_url = $1", url)
        if result:
            # URL уже есть в базе — пропускаем
            await log_skipped_post(
                conn,
                new_post_url=url,
                reason="url_duplicate",
                similar_post_url=result['original_post_url'],  # URL похожего поста
                group_name=group_name,
                raw_text=post.get("text", "")
            )
            skipped += 1
            continue
        
        # Добавляем в отслеживаемые
        seen_urls.add(url)
        unique_posts.append(post)
    return unique_posts, skipped

def select_best_post_from_group(posts: List[dict]) -> dict:
    """
    Выбирает лучший пост из группы семантических дублей.
    
    Критерии выбора (в порядке приоритета):
    1. Наличие медиаконтента (картинки, видео, GIF)
    2. Количество текста (больше = лучше)
    3. Наличие ссылок и превью
    
    Args:
        posts: Список постов-дублей для выбора лучшего
        
    Returns:
        dict: Лучший пост из группы
    """
    if not posts:
        return None
    
    # Если только один пост - возвращаем его
    if len(posts) == 1:
        return posts[0]
    
    best_post = posts[0]
    best_score = 0
    
    for post in posts:
        score = 0
        
        # Критерий 1: Медиаконтент (высший приоритет)
        media_urls = post.get("media_urls", [])
        video_urls = post.get("video_urls", [])
        gif_urls = post.get("gif_urls", [])
        
        if media_urls or video_urls or gif_urls:
            score += 1000  # Высокий приоритет медиаконтенту
        
        # Критерий 2: Количество текста
        text = post.get("text", "")
        text_length = len(text.strip())
        score += text_length
        
        # Критерий 3: Наличие ссылок и превью
        link_preview = post.get("link_preview", {})
        if link_preview and link_preview.get("url"):
            score += 100
        
        # Критерий 4: Наличие превью картинки у ссылки
        if link_preview and link_preview.get("photo_url"):
            score += 50
        
        # Обновляем лучший пост, если текущий имеет больший скор
        if score > best_score:
            best_score = score
            best_post = post
    
    return best_post

async def filter_by_semantic(posts: List[dict], conn) -> tuple[List[dict], int]:
    """
    Семантическая фильтрация постов с проверкой в базе данных.
    
    ИЗМЕНЕНИЕ: Теперь обрабатывает группы семантических дублей в рамках одного запуска.
    Вместо простого пропуска второго поста выбирает лучший из группы дублей.
    """
    unique_posts = []
    skipped = 0
    
    # Получаем все векторы из базы данных для сравнения
    db_vectors = await conn.fetch("SELECT vector_raw, original_post_url FROM posts WHERE vector_raw IS NOT NULL")
    db_embeddings = []
    db_urls = []
    if db_vectors:
        for row in db_vectors:
            if row['vector_raw']:
                # Преобразуем строку вектора обратно в тензор
                vector_str = row['vector_raw']
                # Убираем скобки и разделяем по запятой
                # vector_list = [float(x) for x in vector_str.strip('[]').split(',')]  # Хрупкая конструкция
                vector_list = json.loads(vector_str)  # Более точно и универсально
                db_embeddings.append(torch.tensor(vector_list))
                db_urls.append(row['original_post_url'])
        
        # ОТЛАДКА: выводим количество векторов для сравнения
        logger.info(f"🔍 Загружено {len(db_embeddings)} векторов из базы для семантического сравнения")
        # logger.info(f"🔍 URL'ы в базе: {db_urls}")
    
    # НОВАЯ ЛОГИКА: Сначала обрабатываем все посты и находим группы дублей
    processed_posts = []  # Посты с векторами
    semantic_groups = []  # Группы семантических дублей
    
    # Шаг 1: Обрабатываем все посты и считаем векторы
    for post in posts:
        text = post.get("text", "").strip()
        if not text:
            skipped += 1
            continue
        
        emb = model.encode(text, convert_to_tensor=True, normalize_embeddings=True)
        post['vector_raw'] = emb.tolist()  # Сохраняем вектор для последующего использования
        processed_posts.append(post)
    
    # Шаг 2: Находим группы семантических дублей
    used_indices = set()
    
    for i, post in enumerate(processed_posts):
        if i in used_indices:
            continue
        
        # Создаём группу для текущего поста
        current_group = [post]
        used_indices.add(i)
        
        # Ищем похожие посты среди оставшихся
        for j in range(i + 1, len(processed_posts)):
            if j in used_indices:
                continue
            # === ЗАЩИТА: не сравниваем пост сам с собой ===
            if processed_posts[i].get("original_post_url") == processed_posts[j].get("original_post_url"):
                continue
            
            other_post = processed_posts[j]
            
            # Сравниваем векторы
            emb1 = torch.tensor(post['vector_raw'])
            emb2 = torch.tensor(other_post['vector_raw'])
            similarity = float(util.cos_sim(emb1.unsqueeze(0), emb2.unsqueeze(0))[0][0])
            
            if similarity > SEMANTIC_THRESHOLD:
                # Нашли дубль - добавляем в группу
                current_group.append(other_post)
                used_indices.add(j)
                
                logger.info(f"🔍 Найден семантический дубль в рамках запуска: {post.get('original_post_url')} <-> {other_post.get('original_post_url')} (сходство: {similarity:.4f})")
        
        # Если группа содержит больше одного поста - это группа дублей
        if len(current_group) > 1:
            semantic_groups.append(current_group)
            logger.info(f"🔍 Создана группа из {len(current_group)} семантических дублей")
        else:
            # Одиночный пост - добавляем в уникальные
            unique_posts.append(post)
    
    # Шаг 3: Обрабатываем группы дублей
    for group in semantic_groups:
        # Выбираем лучший пост из группы
        best_post = select_best_post_from_group(group)
        
        if best_post:
            unique_posts.append(best_post)
            skipped += len(group) - 1  # Пропускаем все, кроме лучшего
            
            logger.info(f"✅ Выбран лучший пост из группы: {best_post.get('original_post_url')}")
            logger.info(f"📊 Пропущено {len(group) - 1} дублей из группы")
    
    # Шаг 4: Проверяем с базой данных (старая логика для постов, которых нет в базе)
    final_unique_posts = []
    
    for post in unique_posts:
        # Сравниваем с векторами из базы данных
        is_duplicate_with_db = False
        
        if db_embeddings:
            emb = torch.tensor(post['vector_raw'])
            db_batch = torch.stack(db_embeddings)
            similarities = util.cos_sim(emb, db_batch)[0]
            max_sim = float(similarities.max())
            
            if max_sim > SEMANTIC_THRESHOLD:
                # Находим индекс максимального сходства
                max_idx = int(similarities.argmax())
                similar_url = db_urls[max_idx] if max_idx < len(db_urls) else None
                
                logger.info(f"🔍 Семантический дубль с базой: {post.get('original_post_url')} -> {similar_url} (сходство: {max_sim:.4f})")
                logger.info(f"🔍 Текст нового поста: '{post.get('text', '')[:100]}...'")
                
                # Получаем текст похожего поста для сравнения
                similar_post_result = await conn.fetchrow("SELECT raw_text FROM posts WHERE original_post_url = $1", similar_url)
                if similar_post_result:
                    similar_text = similar_post_result['raw_text']
                    logger.info(f"🔍 Текст похожего поста: '{similar_text[:100]}...'")
                
                # Логируем как семантический дубль
                await log_skipped_post(
                    conn,
                    new_post_url=post.get("original_post_url"),
                    reason="semantic_duplicate",
                    similarity=max_sim,
                    similar_post_url=similar_url,
                    group_name=post.get("group_name"),
                    raw_text=post.get("text", "")
                )
                skipped += 1
                is_duplicate_with_db = True
        
        if not is_duplicate_with_db:
            final_unique_posts.append(post)
    
    return final_unique_posts, skipped


async def filter_by_video_size(posts: List[dict], conn) -> tuple[List[dict], int]:
    """
    Фильтрация постов по размеру видео файлов.
    Пропускает посты с видео больше 250MB.
    """
    unique_posts = []
    skipped = 0
    
    for post in posts:
        video_urls = post.get("video_urls", [])
        
        if not video_urls:
            # Если нет видео, пропускаем проверку
            unique_posts.append(post)
            continue
        
        # Проверяем размер каждого видео
        oversized_videos = []
        for video_url in video_urls:
            try:
                # # Получаем информацию о видео
                # from src.vk_video_downloader import get_vk_video_info
                # video_info = get_vk_video_info(video_url)
                
                # if video_info:
                #     # Получаем размер файла в MB (пробуем разные поля)
                #     file_size = video_info.get("filesize") or video_info.get("filesize_approx") or 0
                #     file_size_mb = file_size / (1024 * 1024) if file_size else 0
                    
                #     if file_size_mb > 250:
                #         oversized_videos.append(f"{video_url} ({file_size_mb:.2f}MB)")
                #         logger.warning(f"⚠️ Видео больше 250MB ({file_size_mb:.2f}MB): {video_url}")
                #     elif file_size_mb == 0:
                #         # Если размер неизвестен, логируем для отладки
                #         logger.info(f"📹 Размер видео неизвестен: {video_url}")
                #         # Пропускаем проверку, если размер неизвестен
                #         continue
                #
                # Проверяем размер видео новым способом, через начало скачивания файла.
                from temp import get_vk_video_size
                size = get_vk_video_size(video_url)
                if size > 150:
                    oversized_videos.append(f"{video_url} ({size:.2f}MB)")
                    logger.warning(f"⚠️ Видео больше 150MB ({size:.2f}MB): {video_url}")
                elif size == 0:
                    # Если размер неизвестен, логируем для отладки
                    logger.info(f"📹 Размер видео неизвестен: {video_url}")
                    # Пропускаем проверку, если размер неизвестен
                    continue

                
            except Exception as e:
                logger.error(f"Ошибка при проверке размера видео {video_url}: {e}")
                # При ошибке считаем видео большим для безопасности
                oversized_videos.append(f"{video_url} (ошибка проверки)")
        
        if oversized_videos:
            # Если есть большие видео, пропускаем весь пост
            await log_skipped_post(
                conn,
                new_post_url=post.get("original_post_url"),
                reason="video_too_large",
                group_name=post.get("group_name"),
                raw_text=post.get("text", "")
            )
            skipped += 1
            continue
        
        unique_posts.append(post)
    
    return unique_posts, skipped

async def rewrite_posts_ai(posts: List[dict], rewrite_func) -> tuple[List[dict], int]:  # Новый синтаксис
    """
    Переписывает тексты постов через AI-провайдера (GigaChat/DeepSeek).
    
    Args:
        posts: Список постов с полем "text" (оригинальный текст)
        rewrite_func: Функция AI-переписывания (async или sync)
    
    Returns:
        tuple: (обновленные_посты, количество_переписанных)
        
    Логика:
        1. Если AI_DISABLED=True - копирует оригинал в rewritten_text
        2. Иначе отправляет текст в AI через rewrite_func
        3. Валидирует результат (не пустой, изменился)
        4. Сохраняет в post["rewritten_text"] и post["text"]
        5. При ошибке AI использует оригинал
        
    Каждый пост получает:
        - post["rewritten_text"] - переписанный текст (для БД)
        - post["text"] - текст для Telegram (переписанный или оригинал)
    """
    logger.info(f"📝 Будет переписано {len(posts)} постов через AI...")
    rewritten_count = 0
    updated_posts = []
    for post in posts:
        original_text = post.get("text", "").strip()
        
        if AI_DISABLED:
            # ЗАТЫЧКА: просто копируем оригинальный текст
            post["rewritten_text"] = original_text
            # Для Telegram используем оригинальный текст
            post["text"] = original_text
            rewritten_count += 1
            logger.info(f"🔧 ЗАТЫЧКА: текст скопирован для: {post.get('original_post_url')}")
        else:
            # Оригинальный код AI
            rewritten_text = None
            try:
                # Проверяем, является ли функция асинхронной
                if asyncio.iscoroutinefunction(rewrite_func):
                    rewritten_text = await rewrite_func(original_text)
                else:
                    # Для синхронных функций используем asyncio.to_thread
                    rewritten_text = await asyncio.to_thread(rewrite_func, original_text)
            except Exception as e:
                logger.error(f"Ошибка AI-переписывания: {e}")
            if rewritten_text and rewritten_text.strip() and rewritten_text.strip() != original_text:
                post["rewritten_text"] = rewritten_text.strip()
                # Для Telegram используем переписанный текст
                post["text"] = rewritten_text.strip()
                rewritten_count += 1
                logger.success(f"✅ Текст успешно переписан для: {post.get('original_post_url')}")
            else:
                post["rewritten_text"] = original_text
                # Для Telegram используем оригинальный текст
                post["text"] = original_text
                logger.warning(f"⚠️ Не удалось переписать текст для: {post.get('original_post_url')}. Используем оригинал.")
        
        updated_posts.append(post)
    return updated_posts, rewritten_count

async def save_to_db(posts: List[dict], pool) -> int:
    """
    Сохраняет обработанные посты в базу данных (таблица posts).
    Для каждого поста выполняет INSERT INTO posts ...
    Возвращает количество успешно сохранённых постов.
    """
    inserted = 0
    async with pool.acquire() as conn:
        for post in posts:
            try:
                text = post.get("text", "").strip()
                rewritten_text = post.get("rewritten_text", "").strip()
                hash_value = post.get('hash') or hashlib.sha256(text.encode("utf-8")).hexdigest()
                original_post_url = post.get("original_post_url")
                group_name = post.get("group_name")
                post_date_str = post.get("post_date")
                try:
                    post_date = datetime.strptime(post_date_str, "%Y-%m-%d %H:%M:%S") if post_date_str else None
                except Exception:
                    post_date = None
                media_urls = post.get("media_urls", [])
                gif_urls = post.get("gif_urls", [])
                video_urls = post.get("video_urls", [])
                link_preview = post.get("link_preview") or {}
                link_preview_url = link_preview.get("url")
                link_preview_photo_url = link_preview.get("photo_url")

                # Вектор оригинального текста
                vector_raw = post.get('vector_raw')
                vector_raw_str = None
                if vector_raw:
                    vector_raw_str = "[" + ",".join(map(str, vector_raw)) + "]"
                
                # Вектор переписанного текста
                vector_rewritten = None
                vector_rewritten_str = None
                if rewritten_text and rewritten_text != text:
                    try:
                        rewritten_emb = model.encode(rewritten_text, normalize_embeddings=True)
                        vector_rewritten = rewritten_emb.tolist()
                        vector_rewritten_str = "[" + ",".join(map(str, vector_rewritten)) + "]"
                    except Exception as e:
                        logger.error(f"Ошибка при векторизации переписанного текста: {e}")
                
                await conn.execute(
                    """
                    INSERT INTO posts (
                        hash, raw_text, rewritten_text, vector_raw, vector_rewritten,
                        original_post_url, group_name, post_date,
                        media_urls, gif_urls, video_urls, link_preview_url, link_preview_photo_url
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (hash) DO NOTHING
                    """,
                    hash_value, text, rewritten_text, vector_raw_str, vector_rewritten_str,
                    original_post_url, group_name, post_date,
                    media_urls, gif_urls, video_urls, link_preview_url, link_preview_photo_url
                )
                inserted += 1
            except Exception as e:
                logger.error(f"Ошибка при сохранении поста в базу: {e}")
    logger.info(f"✅ В базу сохранено {inserted} постов.")
    return inserted

async def process_posts(posts: List[dict], pool) -> tuple[Dict[str, Any], List[dict]]:
    """
    Основная функция пайплайна обработки постов.
    Теперь принимает список постов и логирует пропуски в базу.
    Возвращает кортеж: (статистика, список одобренных постов)
    """
    total = len(posts)
    async with pool.acquire() as conn:
        posts, skipped_by_url = await filter_by_url(posts, conn)
        posts, skipped_by_hash = await filter_by_hash(posts, conn)
        posts, skipped_by_semantic = await filter_by_semantic(posts, conn)
        posts, skipped_by_size = await filter_by_video_size(posts, conn)
    
    rewrite_func = ai_provider()
    posts, rewritten = await rewrite_posts_ai(posts, rewrite_func)

    # Сохраняем финальный список одобренных постов для возврата
    approved_posts = posts.copy()

    # передаём pool внутрь
    inserted = await save_to_db(posts, pool)

    skipped = skipped_by_hash + skipped_by_url + skipped_by_semantic + skipped_by_size
    stats = {
        "total": total,
        "inserted": inserted,
        "skipped": skipped,
        "skipped_by_hash": skipped_by_hash,
        "skipped_by_url": skipped_by_url,
        "skipped_by_semantic": skipped_by_semantic,
        "skipped_by_size": skipped_by_size,
        "rewritten": rewritten,
        "errors": 0
    } 
    
    return stats, approved_posts

def get_vk_last_posts(
    access_token: str,
    group_names: List[str],
    count: int = 5,
    save_path: Optional[str] = None,
    delay_between_requests: float = 0.3,
) -> Tuple[List[Dict], Dict[str, Any]]:
    """
    Получает последние посты из VK групп с улучшенной обработкой ошибок и настраиваемыми задержками.
    
    Args:
        access_token: VK API токен
        group_names: Список имен групп
        count: Количество постов для получения из каждой группы
        save_path: Путь для сохранения результатов (опционально)
        delay_between_requests: Задержка между запросами в секундах (по умолчанию 0.3)
    """
    

    
    def get_group_info(access_token: str, group_name: str, api_version: str) -> dict:
        """
        Получает информацию о группе/пользователе через utils.resolveScreenName.
        Работает с любыми форматами: screen_name, числовые ID, id123456789.
        """
        try:
            # Используем utils.resolveScreenName для получения информации
            response = requests.get(
                "https://api.vk.com/method/utils.resolveScreenName",
                params={
                    "access_token": access_token,
                    "v": api_version,
                    "screen_name": group_name
                },
                timeout=10
            ).json()
            
            if "error" in response:
                raise Exception(f"VK API Error: {response['error']['error_msg']}")
            
            result = response["response"]
            if not result:
                raise Exception("Group not found")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to resolve screen name for {group_name}: {str(e)}")
            raise
    
    api_url = "https://api.vk.com/method/"
    api_version = "5.199"
    all_posts = []
    stats = {
        "total_groups": len(group_names),
        "processed_groups": 0,
        "failed_groups": 0,
        "skipped_types": {},
        "total_posts": 0,
        "reposts": 0,
        "group_errors": {},  # Детальная информация об ошибках групп
    }

    for group in group_names:
        # Определяем тип объекта для более информативного логирования
        try:
            group_info = get_group_info(access_token, group, api_version)
            object_type = group_info["type"]
            
            if object_type == "group":
                logger.info(f"👥 Fetching group data: {group}")
            elif object_type == "page":
                logger.info(f"📄 Fetching page data: {group}")
            elif object_type == "user":
                logger.info(f"👤 Fetching user data: {group}")
            else:
                logger.info(f"🌐 Fetching unknown data: {group}")
        except:
            # Если не удалось определить тип, используем общий формат
            logger.info(f"🌐 Fetching data: {group}")

        try:
            # Получаем информацию о группе через utils.resolveScreenName
            # group_info уже получен выше для логирования
            
            # Задержка между запросами
            time.sleep(delay_between_requests)

            # Определяем тип объекта и получаем ID
            object_type = group_info["type"]  # "group" или "user"
            object_id = group_info["object_id"]
            
            if object_type == "group" or object_type == "page":
                group_id = -object_id  # Для групп и публичных страниц используем отрицательный ID
                group_name = group  # Используем оригинальное имя
            elif object_type == "user":
                group_id = object_id  # Для пользователей используем положительный ID
                group_name = group  # Используем оригинальное имя
            else:
                logger.warning(f"[Group Error] {group}: Unknown object type: {object_type}")
                stats["failed_groups"] += 1
                stats["group_errors"][group] = f"Unknown object type: {object_type}"
                continue

            # Запрос постов группы с таймаутом
            wall_response = requests.get(
                f"{api_url}wall.get",
                params={
                    "access_token": access_token,
                    "v": api_version,
                    "owner_id": group_id,
                    "count": count
                },
                timeout=10  # Таймаут 10 секунд
            ).json()
            
            # Задержка между запросами
            time.sleep(delay_between_requests)

            if "error" in wall_response:
                error_msg = wall_response['error']['error_msg']
                logger.warning(f"[Wall Error] {group}: {error_msg}")
                stats["failed_groups"] += 1
                stats["group_errors"][group] = error_msg
                continue

            stats["processed_groups"] += 1

            for post in wall_response["response"]["items"]:
                stats["total_posts"] += 1

                post_id = post["id"]
                post_url = f"https://vk.com/wall{group_id}_{post_id}"

                post_date = datetime.utcfromtimestamp(post["date"]).strftime('%Y-%m-%d %H:%M:%S')
                is_repost = "copy_history" in post
                if is_repost:
                    stats["reposts"] += 1

                media_urls = []
                video_urls = []
                skipped_types = []
                link_preview = None
                doc_urls = []
                gif_urls = []

                for attach in post.get("attachments", []):
                    att_type = attach.get("type")
                    if att_type == "photo":
                        sizes = attach["photo"]["sizes"]
                        max_photo = max(sizes, key=lambda s: s["width"] * s["height"])
                        media_urls.append(max_photo["url"])
                    elif att_type == "video":
                        video = attach.get("video", {})
                        owner_id = video.get("owner_id")
                        video_id = video.get("id")
                        if owner_id is not None and video_id is not None:
                            video_url = f"https://vk.com/video{owner_id}_{video_id}"
                            video_urls.append(video_url)
                    elif att_type == "link":
                        link = attach.get("link", {})
                        url = link.get("url")
                        photo_url = None
                        photo = link.get("photo")
                        if photo:
                            sizes = photo.get("sizes", [])
                            if sizes:
                                photo_url = sizes[-1].get("url")
                        link_preview = {
                            "url": url,
                            "photo_url": photo_url
                        }
                    elif att_type == "doc":
                        doc = attach.get("doc", {})
                        ext = doc.get("ext")
                        direct_url = doc.get("url")
                        title = doc.get("title")

                        if ext == "gif" and direct_url:
                            gif_urls.append(direct_url)
                        elif direct_url:
                            doc_urls.append({
                                "url": direct_url,
                                "title": title,
                                "ext": ext
                            })
                    else:
                        skipped_types.append(att_type)
                        stats["skipped_types"].setdefault(att_type, 0)
                        stats["skipped_types"][att_type] += 1

                if skipped_types:
                    logger.info(f"📦 Post {post_url} skipped types: {', '.join(skipped_types)}")

                all_posts.append({
                    "text": post.get("text", "") if not is_repost else "",
                    "media_urls": media_urls,
                    "gif_urls": gif_urls,
                    "video_urls": video_urls,
                    "post_url": post_url,
                    "date": post_date,
                    "group_name": group_name,
                    "is_repost": is_repost,
                    "skipped_types": skipped_types,
                    "link_preview": link_preview,
                    "doc_urls": doc_urls,
                })
                
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Timeout error for group {group}")
            stats["failed_groups"] += 1
            stats["group_errors"][group] = "Request timeout"
            continue
        except requests.exceptions.RequestException as e:
            logger.error(f"🌐 Network error for group {group}: {str(e)}")
            stats["failed_groups"] += 1
            stats["group_errors"][group] = f"Network error: {str(e)}"
            continue
        except Exception as e:
            logger.error(f"❌ Unexpected error for group {group}: {str(e)}")
            stats["failed_groups"] += 1
            stats["group_errors"][group] = f"Unexpected error: {str(e)}"
            continue

    logger.success(f"🎯 Обработано групп: {stats['processed_groups']}/{stats['total_groups']} | Постов: {stats['total_posts']} | Репостов: {stats['reposts']}")
    if stats["failed_groups"]:
        logger.warning(f"⚠️ Групп с ошибками: {stats['failed_groups']}")
        # Выводим детали ошибок
        for group, error in stats["group_errors"].items():
            logger.warning(f"   • {group}: {error}")
    if stats["skipped_types"]:
        logger.info(f"📌 Пропущенные типы вложений: {stats['skipped_types']}")

    return all_posts, stats


def prepare_vk_post_for_tg(
    posts: List[Dict],
    interleave_groups: bool = False
) -> Tuple[List[Dict[str, Union[str, List[str]]]], int]:
    if not isinstance(posts, list) or not posts:
        raise ValueError("Некорректный формат: ожидается непустой список постов")

    original_count = len(posts)
    
    # Оставляет только те посты, у которых поле 'is_repost' отсутствует или равно False
    posts = [post for post in posts if not post.get('is_repost', False)]
    reposts_skipped = original_count - len(posts)

    results = []

    grouped = {}
    for post in posts:
        group = post.get('group_name', 'default')
        grouped.setdefault(group, []).append(post)

    def prepare_post(post):
        text = post.get("text", "").replace("\\n", "\n").strip()
        text = remove_vk_links_but_keep_text(text)
        text = re.sub(r'https?://vk\.com[^\s]*', '', text)
        text = re.sub(r"\s*#\S+", "", text).strip()
        media_urls = post.get("media_urls", [])
        link_preview = post.get("link_preview", {})
        video_urls = post.get("video_urls", [])
        doc_urls = post.get("doc", {})
        gif_urls = post.get("gif_urls", [])
        group_name = post.get("group_name", "")
        original_post_url = post.get("post_url", "")
        post_date = post.get("date", "")

        # if not text and not media_urls and not link_preview:
        #     print(f"Пустой пост от {post.get('group_name')} с id {post.get('post_url', 'N/A')}")
        #     return None
        if not text and not media_urls and not link_preview and not video_urls:
            print(f"Пустой пост от {post.get('group_name')} с id {post.get('post_url', 'N/A')}")
            return None

        return {
            "text": text,
            "media_urls": media_urls,
            "video_urls": video_urls,
            "link_preview": link_preview,
            "gif_urls": gif_urls,
            "group_name": group_name,
            "original_post_url": original_post_url,
            "post_date": post_date,
        }

    # Обработка постов
    logger.info(f"🔍 Обрабатываем {len(grouped)} групп постов...")
    
    if interleave_groups:
        # Перемешиваем посты из разных групп
        max_len = max(len(group) for group in grouped.values())
        logger.info(f"🔄 Перемешиваем посты (максимум {max_len} постов из каждой группы)")
        
        for i in range(max_len):
            for group_name, group in grouped.items():
                if i < len(group):
                    post = group[i]
                    prepared = prepare_post(post)
                    if prepared:
                        logger.info(f"✅ Добавляем пост {i+1} из группы '{group_name}': {prepared.get('original_post_url')}")
                        results.append(prepared)
    else:
        # Обычная обработка по группам
        for group_name, group_posts in grouped.items():
            logger.info(f"📝 Обрабатываем группу '{group_name}' ({len(group_posts)} постов)")
            for post in group_posts:
                prepared = prepare_post(post)
                if prepared:
                    logger.info(f"✅ Добавляем пост из группы '{group_name}': {prepared.get('original_post_url')}")
                    results.append(prepared)

    return results, reposts_skipped
