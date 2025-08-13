# 🤖 VK-to-Telegram News Automation

![Python](https://img.shields.io/badge/python-3.9+-blue.svg)
![AI](https://img.shields.io/badge/AI-GigaChat%20%7C%20DeepSeek-green.svg)
![Database](https://img.shields.io/badge/Database-PostgreSQL-blue.svg)
![Status](https://img.shields.io/badge/Status-Production-brightgreen.svg)

Автоматизированная система для парсинга контента из VK сообществ, обработки через AI и публикации в Telegram каналы.
Проект включает интеллектуальную фильтрацию дубликатов, переписывание текстов через нейросети и полную автоматизацию медиа-контента.

## 🎯 Основные возможности

### 📥 **Парсинг VK**
- Автоматический сбор постов из VK групп
- Поддержка текста, изображений, видео, GIF
- Обработка ссылок и предпросмотров
- Настраиваемые интервалы запросов

### 🧠 **AI-обработка контента**
- **Переписывание текстов** через GigaChat/DeepSeek API
- **Семантический анализ** дубликатов (BERT embeddings)
- **Интеллектуальная фильтрация** повторяющегося контента
- Настраиваемые пороги схожести (cosine similarity)

### 🔍 **Система дедупликации**
- **Хэш-фильтрация** точных дубликатов
- **URL-фильтрация** уже обработанных материалов
- **Векторный поиск** семантически похожих постов
- Логирование всех пропущенных постов в БД

### 📤 **Публикация в Telegram**
- **Userbot** через Telethon
- **Bot API** через aiogram
- Автоматическая обработка медиа-групп
- Скачивание и конвертация изображений/видео

### 🗄️ **База данных**
- **PostgreSQL** с асинхронными операциями
- Хранение постов, медиа и векторных представлений
- Детальная аналитика и статистика
- Отслеживания пропущенных постов

## 🏗️ Архитектура системы

```
VK Groups Parsing → Content Processing → Telegram Publishing
       ↓                    ↓                     ↓
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ • Группы VK     │  │ • Фильтрация    │  │ • Userbot       │
│ • Медиа-файлы   │  │ • AI-рерайт     │  │ • Каналы TG     │
│ • Метаданные    │  │ • Векторизация  │  │ • Медиа-группы  │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │                    │                     │
         └────────────────────▼─────────────────────┘
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │                 │
                    │ • posts         │
                    │ • skipped_posts │
                    │ • векторы BERT  │
                    └─────────────────┘

Поток обработки:
1. VK API → Парсинг постов из групп
2. Фильтры → URL/Hash/Semantic дедупликация  
3. AI API → Переписывание текстов (GigaChat/DeepSeek)
4. PostgreSQL → Сохранение обработанных данных
5. Telegram API → Публикация через userbot/bot
```

## 🗄️ Структура базы данных

### **Таблица `posts` (основные посты)**
```sql
posts:
├── id (SERIAL PRIMARY KEY)           -- Уникальный ID поста
├── hash (VARCHAR UNIQUE)             -- SHA256 хэш оригинального текста
├── raw_text (TEXT)                   -- Исходный текст из VK
├── rewritten_text (TEXT)             -- Переписанный AI текст
├── vector_raw (TEXT)                 -- BERT вектор оригинала (JSON array)
├── vector_rewritten (TEXT)           -- BERT вектор рерайта (JSON array)
├── original_post_url (VARCHAR)       -- Ссылка на пост в VK
├── group_name (VARCHAR)              -- Название VK группы
├── post_date (TIMESTAMP)             -- Дата поста в VK
├── media_urls (TEXT[])               -- Массив URL изображений
├── gif_urls (TEXT[])                 -- Массив URL GIF
├── video_urls (TEXT[])               -- Массив URL видео
├── link_preview_url (VARCHAR)        -- URL предпросмотра ссылки
├── link_preview_photo_url (VARCHAR)  -- URL фото предпросмотра
└── created_at (TIMESTAMP DEFAULT NOW) -- Время обработки
```

### **Таблица `skipped_posts` (отклоненные посты)**
```sql
skipped_posts:
├── id (SERIAL PRIMARY KEY)           -- Уникальный ID записи
├── new_post_url (VARCHAR)            -- URL нового поста
├── reason (VARCHAR)                  -- Причина пропуска:
│   ├── "hash_duplicate"              --   • Точный дубль (хэш)
│   ├── "url_duplicate"               --   • Дубль URL
│   ├── "semantic_duplicate"          --   • Семантический дубль (AI)
│   └── "video_size_limit"            --   • Превышен размер видео
├── hash (VARCHAR)                    -- Хэш текста (если есть)
├── similar_post_url (VARCHAR)        -- URL похожего поста в БД
├── similarity (FLOAT)                -- Коэффициент схожести (0.0-1.0)
├── group_name (VARCHAR)              -- Название VK группы
├── raw_text (TEXT)                   -- Исходный текст поста
├── similar_post_date (TIMESTAMP)     -- Дата похожего поста
└── created_at (TIMESTAMP DEFAULT NOW) -- Время пропуска
```

**Индексы для производительности:**
- `posts(hash)` - быстрый поиск дублей
- `posts(original_post_url)` - поиск по URL
- `posts(group_name, created_at)` - аналитика по группам
- `skipped_posts(reason, created_at)` - статистика пропусков

## 🛠️ Технологический стек

### **Backend & Core**
- **Python 3.9+** - основной язык разработки
- **asyncio** - асинхронное программирование
- **PostgreSQL** - основная база данных
- **asyncpg** - асинхронный драйвер для PostgreSQL

### **AI & ML**
- **Sentence Transformers** - векторизация текстов
- **PyTorch** - ML фреймворк
- **GigaChat API** - переписывание текстов (Сбер)
- **DeepSeek API** - альтернативный AI провайдер

### **API Integration**
- **VK API** - парсинг постов из сообществ
- **Telethon** - Telegram userbot для приватных каналов
- **aiogram** - Telegram Bot API
- **requests** - HTTP клиент

### **Media Processing**
- **moviepy** - обработка видео
- **yt-dlp** - скачивание медиа контента
- **Pillow** - работа с изображениями

### **DevOps & Utils**
- **loguru** - логирование
- **python-dotenv** - управление переменными окружения
- **aiofiles** - асинхронная работа с файлами

## 🚀 Быстрый старт

### 1. Клонирование и установка

```bash
git clone https://github.com/yourusername/vk-telegram-bot
cd vk-telegram-bot
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### 2. Настройка окружения

```bash
cp .env.example .env
# Отредактируйте .env с вашими токенами
```

### 3. Настройка базы данных

```sql
-- Создайте PostgreSQL базу и выполните миграции
-- SQL схемы находятся в database/schema.sql
```

### 4. Запуск

```bash
python run.py
```

## ⚙️ Конфигурация

### **VK API**
```env
VK_API_TOKEN=your_vk_token
```

### **Telegram**
```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
SESSION_NAME=your_session_name
PRIVATE_TG_CHANNEL_ID=your_channel_id
```

### **AI Провайдеры**
```env
AI_PROVIDER=gigachat  # или deepseek
GIGA_CHAT_TOKEN=your_gigachat_token
DEEPSEEK_TOKEN=your_deepseek_token
```

### **ML Модели**
```env
LOCAL_BERT_VECTOR_MODEL_PATH=/path/to/sentence-transformer
```

## 📊 Основные компоненты

### **1. Pipeline обработки (`src/text_processing/pipeline.py`)**
- Главный модуль обработки постов
- Фильтрация и дедупликация
- AI-переписывание текстов
- Векторный поиск семантических дубликатов

### **2. VK интеграция (`src/vk_function.py`)**
- Парсинг постов из VK групп
- Обработка текст/медиа контента

### **3. Telegram публикация (`userbot/userbot_tg_functions.py`)**
- Userbot (Telethon) для приватных каналов
- Обработка медиа-групп
- Автоматическое форматирование

### **4. База данных (`database/db.py`)**
- Асинхронные операции с PostgreSQL
- Connection pooling
- Миграции и схемы

## 📈 Статистика и мониторинг

Система предоставляет детальную статистику:

- **Обработанные посты** - количество успешно обработанных материалов
- **Дедупликация** - процент отфильтрованных дубликатов  
- **AI обработка** - успешность переписывания текстов
- **Публикация** - статистика отправки в Telegram
- **Производительность** - время выполнения операций

## 👨‍💻 Автор

**Разработчик**: [Ваше имя]  
**Позиция**: Python Developer / AI Engineer  
**Технологии**: Python, AI/ML, PostgreSQL, Telegram API, VK API
