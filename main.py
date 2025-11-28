# -*- coding: utf-8 -*-
import os
import sys
import json
import logging
import tempfile
import re
import random
import asyncio
import shutil
import time
import aiohttp
import psutil
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')
REDIS_URL = os.environ.get('REDIS_URL')

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    sys.exit(1)

print("🔧 Универсальный Music Bot запускается...")

# Оптимизированные настройки
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 30))
DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', 90))
SEARCH_TIMEOUT = int(os.environ.get('SEARCH_TIMEOUT', 20))
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 10))

# Ускоренные настройки для SoundCloud
SOUNDCLOUD_OPTS = {
    'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best',
    'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 2,
    'fragment_retries': 2,
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'noplaylist': True,
    'max_filesize': MAX_FILE_SIZE_MB * 1024 * 1024,
    'ignoreerrors': True,
    'socket_timeout': 20,
    'extractaudio': True,
    'audioformat': 'best',
}

# Список для случайных треков
RANDOM_SEARCHES = [
    'lo fi beats', 'chillhop', 'deep house', 'synthwave', 'indie rock',
    'electronic music', 'jazz lounge', 'ambient', 'study music',
    'focus music', 'relaxing music', 'instrumental', 'acoustic',
    'piano covers', 'guitar music', 'vocal trance', 'dubstep',
    'tropical house', 'future bass', 'retro wave', 'city pop',
    'latin music', 'reggaeton', 'k-pop', 'j-pop', 'classical piano',
    'orchestral', 'film scores', 'video game music'
]

# Резервные пожелания (используются если ИИ недоступен)
FALLBACK_WISHES = [
    "Хорошего дня! 🌟", "Отличного настроения! 😊", "Пусть день будет прекрасным! ✨",
    "Удачи во всех начинаниях! 🍀", "Прекрасной музыки! 🎵", "Наслаждайтесь моментом! 🌈",
    "Пусть этот день принесет радость! 🌞", "Отличного прослушивания! 🎧", "Вдохновения и творчества! 🎨",
    "Прекрасного дня и хорошей музыки! 🎶"
]

# ==================== IMPORT TELEGRAM & YT-DLP ====================
try:
    from telegram import Update
    from telegram.ext import (
        Application, CommandHandler, MessageHandler, 
        filters, ContextTypes
    )
    from telegram.error import Conflict, TimedOut, NetworkError
    import yt_dlp
    print("✅ Все зависимости загружены")
except ImportError as exc:
    print(f"❌ Ошибка импорта: {exc}")
    os.system("pip install python-telegram-bot yt-dlp aiohttp redis psutil")
    try:
        from telegram import Update
        from telegram.ext import (
            Application, CommandHandler, MessageHandler,
            filters, ContextTypes
        )
        from telegram.error import Conflict, TimedOut, NetworkError
        import yt_dlp
        print("✅ Зависимости успешно установлены")
    except ImportError as exc2:
        print(f"❌ Ошибка импорта после установки: {exc2}")
        sys.exit(1)

# ==================== REDIS CLIENT ====================
try:
    import redis.asyncio as redis
    
    class RedisClient:
        def __init__(self):
            self.redis_url = REDIS_URL
            self.redis = None
            
        async def connect(self):
            if not self.redis_url:
                print("ℹ️ Redis URL не указан, кэширование отключено")
                return
                
            try:
                self.redis = redis.from_url(self.redis_url, decode_responses=True)
                await self.redis.ping()
                print("✅ Redis подключен")
            except Exception as e:
                print(f"❌ Ошибка подключения к Redis: {e}")
                self.redis = None
        
        async def cache_get(self, key: str):
            if not self.redis:
                return None
            try:
                data = await self.redis.get(key)
                return json.loads(data) if data else None
            except:
                return None
        
        async def cache_set(self, key: str, value, expire: int = 3600):
            if not self.redis:
                return
            try:
                await self.redis.setex(key, expire, json.dumps(value))
            except:
                pass
        
        async def increment_rate_limit(self, user_id: int, window: int = 60):
            if not self.redis:
                return 1
                
            key = f"rate_limit:{user_id}"
            try:
                async with self.redis.pipeline() as pipe:
                    pipe.incr(key)
                    pipe.expire(key, window)
                    results = await pipe.execute()
                    return results[0]
            except:
                return 1
    
    redis_client = RedisClient()
    
except ImportError:
    print("❌ Redis не установлен, кэширование отключено")
    
    class RedisClient:
        async def connect(self): pass
        async def cache_get(self, key): return None
        async def cache_set(self, key, value, expire=3600): pass
        async def increment_rate_limit(self, user_id, window=60): return 1
    
    redis_client = RedisClient()

# ==================== SIMPLE HEALTH SERVER ====================
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json as json_lib

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "timestamp": time.time(),
                "service": "music-bot"
            }
            self.wfile.write(json_lib.dumps(response).encode('utf-8'))
        elif self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                metrics = {
                    "memory_usage_mb": round(memory_info.rss / 1024 / 1024, 2),
                    "memory_percent": round(process.memory_percent(), 2),
                    "cpu_percent": round(process.cpu_percent(), 2),
                    "active_threads": process.num_threads(),
                }
                self.wfile.write(json_lib.dumps(metrics).encode('utf-8'))
            except Exception as e:
                self.wfile.write(json_lib.dumps({"error": str(e)}).encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write("Music Bot is running!\n\n/health - status\n/metrics - metrics".encode('utf-8'))
    
    def log_message(self, format, *args):
        # Отключаем логирование запросов для чистоты вывода
        return

class SimpleHealthServer:
    def __init__(self, port=8080):
        self.port = port
        self.server = None
    
    def start(self):
        def run():
            try:
                self.server = HTTPServer(('0.0.0.0', self.port), HealthHandler)
                print(f"✅ Health server запущен на порту {self.port}")
                self.server.serve_forever()
            except Exception as e:
                print(f"❌ Ошибка health server: {e}")
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()

# ==================== PROGRESS BAR ====================
class ProgressBar:
    def __init__(self, total_steps: int, width: int = 10):
        self.total_steps = total_steps
        self.width = width
        self.current_step = 0
        self.start_time = time.time()
    
    def get_bar(self, step: int = None):
        if step is not None:
            self.current_step = step
        
        progress = min(self.current_step / self.total_steps, 1.0)
        filled = int(self.width * progress)
        empty = self.width - filled
        
        elapsed = time.time() - self.start_time
        elapsed_str = f"{elapsed:.1f}с"
        
        return f"[{'█' * filled}{'░' * empty}] {int(progress * 100)}% ({elapsed_str})"
    
    def get_stage_text(self, stage: int, stage_name: str):
        stages = {
            1: "Анализ запроса...",
            2: "Поиск трека...", 
            3: "Скачивание...",
            4: "Отправка..."
        }
        
        stage_text = stages.get(stage, stage_name)
        return f"{self.get_bar(stage)}\n{stage_text}"

class ProgressManager:
    @staticmethod
    def search_progress():
        return ProgressBar(total_steps=4, width=8)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== AI INTEGRATION ====================
class AIIntegration:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.base_url = "https://api.deepseek.com/v1/chat/completions"
        self.enabled = bool(self.api_key)
        self.session = None
        
        if self.enabled:
            print("✅ Улучшения поиска активированы")
        else:
            print("ℹ️ Улучшения поиска не активированы, используются стандартные методы")
    
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self.session
    
    async def generate_wish(self, user_query: str, track_title: str, artist: str = None) -> str:
        if not self.enabled:
            return random.choice(FALLBACK_WISHES)
        
        prompt = self._build_wish_prompt(user_query, track_title, artist)
        
        try:
            session = await self.get_session()
            
            async with session.post(
                self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {
                            "role": "system", 
                            "content": "Ты - музыкальный помощник, который создает короткие, теплые и персонализированные пожелания для пользователей. Пожелания должны быть не более 1-2 предложений, дружелюбными и подходящими для любого пола."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 60,
                    "temperature": 0.8,
                    "stream": False
                }
            ) as response:
                
                if response.status == 200:
                    data = await response.json()
                    wish = data['choices'][0]['message']['content'].strip()
                    wish = re.sub(r'^["\']|["\']$', '', wish)
                    print(f"💫 Сгенерировано пожелание: {wish}")
                    return wish
                else:
                    error_text = await response.text()
                    print(f"❌ Ошибка API: {response.status} - {error_text}")
                    
        except asyncio.TimeoutError:
            print("⏰ Таймаут запроса")
        except Exception as e:
            print(f"❌ Ошибка при генерации пожелания: {e}")
        
        return random.choice(FALLBACK_WISHES)
    
    async def enhance_search_query(self, original_query: str) -> str:
        if not self.enabled:
            return original_query
        
        prompt = self._build_search_prompt(original_query)
        
        try:
            session = await self.get_session()
            
            async with session.post(
                self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {
                            "role": "system", 
                            "content": "Ты - музыкальный эксперт, который помогает улучшить поисковые запросы для SoundCloud. Ты должен анализировать запросы пользователей и преобразовывать их в более эффективные для поиска музыки."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 50,
                    "temperature": 0.7,
                    "stream": False
                }
            ) as response:
                
                if response.status == 200:
                    data = await response.json()
                    enhanced_query = data['choices'][0]['message']['content'].strip()
                    enhanced_query = re.sub(r'^["\']|["\']$', '', enhanced_query)
                    print(f"🎯 Улучшен запрос: '{original_query}' -> '{enhanced_query}'")
                    
                    if not enhanced_query or len(enhanced_query) > 100:
                        return original_query
                    
                    return enhanced_query
                else:
                    error_text = await response.text()
                    print(f"❌ Ошибка API при улучшении запроса: {response.status} - {error_text}")
                    
        except asyncio.TimeoutError:
            print("⏰ Таймаут улучшения запроса")
        except Exception as e:
            print(f"❌ Ошибка при улучшении запроса: {e}")
        
        return original_query
    
    def _build_wish_prompt(self, user_query: str, track_title: str, artist: str = None) -> str:
        base_prompt = f"""
Пользователь искал музыку по запросу: "{user_query}"
В результате найден трек: "{track_title}"
"""
        
        if artist and artist != 'Неизвестный исполнитель':
            base_prompt += f"Исполнитель: {artist}\n"
        
        base_prompt += """
Придумай короткое, теплое и персонализированное пожелание (1-2 предложения) на русском языке, связанное с музыкой, найденным треком или настроением пользователя.
Пожелание должно быть:
- Коротким (максимум 15-20 слов)
- Дружелюбным и поддерживающим
- Уместным для музыкального контекста
- С 1-2 релевантными эмодзи в конце
- Подходящим для любого пола

Примеры хороших пожеланий:
"Наслаждайтесь этим прекрасным треком! 🎵 Пусть музыка наполнит ваш день радостью! 🌟"
"Отличный выбор! Пусть этот звук станет саундтреком вашего прекрасного дня! 🎶"

Верни ТОЛЬКО готовое пожелание, без дополнительных комментариев.
"""
        return base_prompt.strip()
    
    def _build_search_prompt(self, original_query: str) -> str:
        prompt = f"""
Пользователь ищет музыку по запросу: "{original_query}"

Проанализируй этот запрос и улучши его для поиска на SoundCloud. Сделай запрос более точным и релевантным для поиска музыки.

Правила улучшения:
1. Если запрос общий (например, "музыка для учебы"), уточни жанр или настроение
2. Если есть опечатки - исправь их
3. Если можно добавить популярные ключевые слова для лучшего поиска - добавь
4. Сохрани оригинальный смысл запроса
5. Сделай запрос на русском языке
6. Верни ТОЛЬКО улучшенный запрос, без пояснений

Примеры:
"рок" -> "рок музыка 2024"
"музыка для тренировки" -> "энергичная музыка для тренировки фитнес"
"классика" -> "классическая музыка оркестр"
"релакс" -> "расслабляющая музыка для релаксации"

Улучшенный запрос:
"""
        return prompt.strip()
    
    async def close(self):
        if self.session:
            await self.session.close()

# ==================== RATE LIMITER ====================
class RateLimiter:
    def __init__(self):
        self.user_requests = defaultdict(list)
    
    def is_limited(self, user_id: int, limit: int = REQUESTS_PER_MINUTE, period: int = 60):
        now = datetime.now()
        user_requests = self.user_requests[user_id]
        user_requests = [req for req in user_requests if now - req < timedelta(seconds=period)]
        self.user_requests[user_id] = user_requests
        
        if len(user_requests) >= limit:
            return True
            
        user_requests.append(now)
        return False

# ==================== UNIVERSAL MUSIC BOT ====================
class UniversalMusicBot:
    def __init__(self):
        self.download_semaphore = asyncio.Semaphore(2)
        self.search_semaphore = asyncio.Semaphore(3)
        self.rate_limiter = RateLimiter()
        self.ai = AIIntegration()
        self.app = None
        self.health_server = SimpleHealthServer()
        logger.info('✅ Универсальный бот инициализирован')

    async def _cleanup_temp_dir(self, tmpdir):
        await asyncio.sleep(2)
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
            print(f"✅ Очищена временная директория: {tmpdir}")
        except Exception as e:
            print(f"⚠️ Не удалось очистить временную директорию: {e}")

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return 'Неизвестный трек'
        
        title = re.sub(r"[^\w\s\-\.\(\)\[\]]", '', title)
        
        tags = [
            'official video', 'official music video', 'lyric video', 'hd', '4k',
            '1080p', '720p', 'official audio', 'audio', 'video', 'clip', 'mv',
            'upload', 'uploaded', 'by', 'uploader', 'soundcloud'
        ]
        for tag in tags:
            title = re.sub(tag, '', title, flags=re.IGNORECASE)
        
        title = ' '.join(title.split()).strip()
        
        return title if title else 'Неизвестный трек'

    @staticmethod
    def format_duration(seconds) -> str:
        try:
            sec = int(float(seconds))
            minutes = sec // 60
            sec = sec % 60
            return f"{minutes:02d}:{sec:02d}"
        except Exception:
            return '00:00'

    @staticmethod
    def is_valid_url(url: str) -> bool:
        if not url:
            return False
        return bool(re.match(r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', url))

    def _create_application(self):
        self.app = Application.builder().token(BOT_TOKEN).build()

        # Обработчик ВСЕХ текстовых сообщений ВО ВСЕХ чатах
        self.app.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_all_messages
        ))

        # Команды
        self.app.add_handler(CommandHandler('start', self.start_command))
        self.app.add_handler(CommandHandler('find', self.handle_find_short))
        self.app.add_handler(CommandHandler('random', self.handle_random_short))
        self.app.add_handler(CommandHandler('stats', self.stats_command))
        self.app.add_handler(CommandHandler('help', self.help_command))

    async def handle_find_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = ' '.join(context.args)
        if not query:
            await update.message.reply_text(
                "❌ Укажи запрос для поиска\n💡 Пример: <code>/find coldplay</code>",
                parse_mode='HTML'
            )
            return
        await self.handle_find_command(update, context, f"найди {query}")

    async def handle_random_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.handle_random_command(update, context)

    # ==================== ОБРАБОТКА ВСЕХ СООБЩЕНИЙ ====================

    async def handle_all_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not update.message or not update.message.text:
                return
                
            message_text = update.message.text.strip().lower()
            chat_id = update.effective_chat.id
            user = update.effective_user
            
            print(f"🎯 Сообщение от {user.first_name}: {message_text}")

            # Rate limiting с Redis
            current_count = await redis_client.increment_rate_limit(user.id)
            if current_count >= REQUESTS_PER_MINUTE:
                await update.message.reply_text(
                    f"⏳ {user.mention_html()}, слишком много запросов!\n"
                    f"Подожди 1 минуту перед следующим запросом.",
                    parse_mode='HTML'
                )
                return

            # Реагируем ТОЛЬКО на команды "найди" и "рандом"
            if message_text.startswith('найди'):
                await self.handle_find_command(update, context, message_text)
            
            elif message_text.startswith('рандом'):
                await self.handle_random_command(update, context)
            
            # Игнорируем все остальные сообщения
            else:
                return
                
        except Exception as e:
            logger.exception(f'Ошибка обработки сообщения: {e}')

    async def handle_find_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        status_msg = None
        progress = ProgressManager.search_progress()
        
        try:
            user = update.effective_user
            chat_id = update.effective_chat.id
            original_message = update.message

            # Извлекаем запрос после "найди"
            original_query = self.extract_search_query(message_text)
            
            if not original_query:
                await original_message.reply_text(
                    f"❌ {user.mention_html()}, не указано что искать\n"
                    f"💡 Напиши: найди [название трека или исполнителя]",
                    parse_mode='HTML'
                )
                return

            # Определяем текст статуса в зависимости от типа чата
            if original_message.chat.type in ["group", "supergroup"]:
                status_text = f"🔍 {user.mention_html()} ищет: <code>{original_query}</code>"
            else:
                status_text = f"🔍 Ищу: <code>{original_query}</code>"

            # Отправляем статус с прогресс-баром
            status_msg = await original_message.reply_text(
                f"{status_text}\n{progress.get_stage_text(1, '🔍 Анализ запроса...')}",
                parse_mode='HTML'
            )

            # Этап 1: Улучшение запроса
            enhanced_query = original_query
            if self.ai.enabled:
                await status_msg.edit_text(
                    f"{status_text}\n{progress.get_stage_text(1, '🔍 Уточняем запрос...')}",
                    parse_mode='HTML'
                )
                try:
                    enhanced_query = await asyncio.wait_for(
                        self.ai.enhance_search_query(original_query),
                        timeout=3.0
                    )
                    if enhanced_query != original_query:
                        print(f"🎯 Используем уточненный запрос: {enhanced_query}")
                except asyncio.TimeoutError:
                    print("⏰ Таймаут уточнения запроса, используем оригинальный")
                except Exception as e:
                    print(f"❌ Ошибка уточнения запроса: {e}")
            else:
                await status_msg.edit_text(
                    f"{status_text}\n{progress.get_stage_text(1, '🔍 Анализируем запрос...')}",
                    parse_mode='HTML'
                )

            # Этап 2: Поиск с кэшированием
            await status_msg.edit_text(
                f"{status_text}\n{progress.get_stage_text(2, '🎵 Ищем лучший трек...')}",
                parse_mode='HTML'
            )
            
            # Проверяем кэш Redis
            cache_key = f"search:{enhanced_query.lower().strip()}"
            track = await redis_client.cache_get(cache_key)
            
            if not track:
                # Ищем трек если нет в кэше
                track = await self.find_track(enhanced_query)
                if track:
                    # Сохраняем в кэш на 1 час
                    await redis_client.cache_set(cache_key, track, 3600)
            
            if not track:
                await status_msg.edit_text(
                    f"❌ Не найдено по запросу: <code>{original_query}</code>\n"
                    f"💡 Попробуй другой запрос, {user.mention_html()}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Найден трек: {track['title']}")

            # Этап 3: Скачивание
            track_title = track.get('title', 'Неизвестный трек')
            stage_message = f'⏬ Скачиваем <b>{track_title}</b>...'
            await status_msg.edit_text(
                f"{status_text}\n{progress.get_stage_text(3, stage_message)}",
                parse_mode='HTML'
            )

            # Скачиваем трек
            file_path = await self.download_track(track.get('webpage_url'))
            if not file_path:
                print(f"❌ Не удалось скачать трек: {track['title']}")
                await status_msg.edit_text(
                    f"❌ Не удалось скачать трек\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Трек скачан: {file_path}")

            # Этап 4: Отправка + генерация пожелания
            await status_msg.edit_text(
                f"{status_text}\n{progress.get_stage_text(4, '📤 Отправляем в чат...')}",
                parse_mode='HTML'
            )

            # Параллельно генерируем пожелание и отправляем аудио
            wish_task = asyncio.create_task(
                self.ai.generate_wish(original_query, track['title'], track.get('artist'))
            )
            
            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    # Ждем завершения генерации пожелания (максимум 5 секунд)
                    try:
                        wish = await asyncio.wait_for(wish_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        wish = random.choice(FALLBACK_WISHES)
                        print("⏰ Таймаут генерации пожелания, используем стандартное")
                    
                    # Формируем подпись
                    caption = f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n⏱️ {self.format_duration(track.get('duration'))}\n\n{wish}"
                    
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=caption,
                        parse_mode='HTML'
                    )
                print(f"✅ Аудио отправлено в чат {chat_id}")
            except Exception as e:
                print(f"❌ Ошибка отправки аудио: {e}")
                if not wish_task.done():
                    wish_task.cancel()
                await status_msg.edit_text(
                    f"❌ Ошибка отправки трека\n"
                    f"💡 Попробуй еще раз",
                    parse_mode='HTML'
                )
                return

            # Удаляем временный файл
            try:
                os.remove(file_path)
                print(f"✅ Временный файл удален: {file_path}")
            except Exception as e:
                print(f"⚠️ Не удалось удалить временный файл: {e}")

            # Удаляем статус-сообщение (оставляем только аудио)
            try:
                await status_msg.delete()
                print("✅ Статус-сообщение удалено")
            except:
                # Если нельзя удалить, редактируем в финальный вид
                await status_msg.edit_text(
                    f"✅ Найдено: <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                    f"⏱️ {self.format_duration(track.get('duration'))}",
                    parse_mode='HTML'
                )

        except Exception as e:
            logger.exception(f'Ошибка при поиске: {e}')
            print(f"❌ Критическая ошибка в handle_find_command: {e}")
            if status_msg:
                await status_msg.edit_text(
                    f"❌ Ошибка при поиске\n"
                    f"💡 Попробуй еще раз, {user.mention_html()}",
                    parse_mode='HTML'
                )

    async def handle_random_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_msg = None
        progress = ProgressManager.search_progress()
        
        try:
            user = update.effective_user
            chat_id = update.effective_chat.id
            original_message = update.message

            # Определяем текст статуса в зависимости от типа чата
            if original_message.chat.type in ["group", "supergroup"]:
                status_text = f"🎲 {user.mention_html()} ищет случайный трек..."
            else:
                status_text = "🎲 Ищу случайный трек..."

            # Отправляем статус
            status_msg = await original_message.reply_text(
                f"{status_text}\n{progress.get_stage_text(1, '🎲 Выбираем жанр...')}",
                parse_mode='HTML'
            )

            # Этап 1: Поиск
            await status_msg.edit_text(f"{status_text}\n{progress.get_stage_text(2, '🎵 Ищем интересную музыку...')}", parse_mode='HTML')

            # Случайный запрос
            random_query = random.choice(RANDOM_SEARCHES)
            print(f"🎲 Случайный запрос: {random_query}")
            
            # Ищем трек
            track = await self.find_track(random_query)
            
            if not track:
                await status_msg.edit_text(
                    f"❌ Не удалось найти случайный трек\n"
                    f"💡 Попробуй еще раз, {user.mention_html()}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Найден случайный трек: {track['title']}")

            # Этап 2: Скачивание
            track_title = track.get('title', 'Неизвестный трек')
            stage_message = f'⏬ Скачиваем <b>{track_title}</b>...'
            await status_msg.edit_text(
                f"{status_text}\n{progress.get_stage_text(3, stage_message)}",
                parse_mode='HTML'
            )

            # Скачиваем трек
            file_path = await self.download_track(track.get('webpage_url'))
            if not file_path:
                print(f"❌ Не удалось скачать случайный трек: {track['title']}")
                await status_msg.edit_text(
                    f"❌ Не удалось скачать случайный трек\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Случайный трек скачан: {file_path}")

            # Этап 3: Отправка + генерация пожелания
            await status_msg.edit_text(
                f"{status_text}\n{progress.get_stage_text(4, '📤 Отправляем в чат...')}",
                parse_mode='HTML'
            )

            # Параллельно генерируем пожелание
            wish_task = asyncio.create_task(
                self.ai.generate_wish(random_query, track['title'], track.get('artist'))
            )

            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    # Ждем завершения генерации пожелания (максимум 5 секунд)
                    try:
                        wish = await asyncio.wait_for(wish_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        wish = random.choice(FALLBACK_WISHES)
                        print("⏰ Таймаут генерации пожелания, используем стандартное")
                    
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n⏱️ {self.format_duration(track.get('duration'))}\n\n{wish}",
                        parse_mode='HTML'
                    )
                print(f"✅ Случайное аудио отправлено в чат {chat_id}")
            except Exception as e:
                print(f"❌ Ошибка отправки случайного аудио: {e}")
                if not wish_task.done():
                    wish_task.cancel()
                await status_msg.edit_text(
                    f"❌ Ошибка отправки трека\n"
                    f"💡 Попробуй еще раз",
                    parse_mode='HTML'
                )
                return

            # Удаляем временный файл
            try:
                os.remove(file_path)
            except:
                pass

            # Удаляем статус-сообщение
            try:
                await status_msg.delete()
            except:
                await status_msg.edit_text(
                    f"✅ Случайный трек: <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                    f"⏱️ {self.format_duration(track.get('duration'))}",
                    parse_mode='HTML'
                )

        except Exception as e:
            logger.exception(f'Ошибка при поиске случайного трека: {e}')
            print(f"❌ Критическая ошибка в handle_random_command: {e}")
            if status_msg:
                await status_msg.edit_text(
                    f"❌ Ошибка при поиске\n"
                    f"💡 Попробуй еще раз, {user.mention_html()}",
                    parse_mode='HTML'
                )

    def extract_search_query(self, message_text: str) -> str:
        query = message_text.replace('найди', '').strip()
        stop_words = ['пожалуйста', 'мне', 'трек', 'песню', 'музыку', 'плз', 'plz']
        for word in stop_words:
            query = query.replace(word, '')
        return query.strip()

    # ==================== ПОИСК ТРЕКОВ ====================

    async def find_track(self, query: str):
        async with self.search_semaphore:
            ydl_opts = {
                'format': 'bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'ignoreerrors': True,
                'noplaylist': True,
                'socket_timeout': 15,
            }

            try:
                print(f"🔍 Начинаем поиск: {query}")
                
                def perform_search():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.extract_info(f"scsearch8:{query}", download=False)

                loop = asyncio.get_event_loop()
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, perform_search),
                    timeout=SEARCH_TIMEOUT
                )

                if not info:
                    print(f"❌ Поиск не дал результатов: {query}")
                    return None

                entries = info.get('entries', [])
                if not entries and info.get('_type') != 'playlist':
                    entries = [info]

                print(f"✅ Найдено {len(entries)} результатов")

                # Фильтрация и сортировка для лучшей релевантности
                filtered_entries = []
                for entry in entries:
                    if not entry:
                        continue

                    # Фильтруем по длительности (минимум 30 секунд, максимум 1 час)
                    duration = entry.get('duration') or 0
                    if duration < 30 or duration > 3600:
                        continue

                    title = self.clean_title(entry.get('title') or '')
                    if not title:
                        continue

                    # Исключаем результаты, где в названии есть uploader (но не исполнитель)
                    uploader = entry.get('uploader') or ''
                    if (uploader and 
                        uploader.lower() in title.lower() and 
                        not self._is_likely_artist(uploader, title)):
                        print(f"🚫 Пропускаем трек с uploader в названии: {title}")
                        continue

                    # Приоритет для "official" треков
                    priority = 0
                    title_lower = title.lower()
                    if 'official' in title_lower:
                        priority = 3
                    elif 'original' in title_lower:
                        priority = 2
                    elif 'cover' not in title_lower and 'remix' not in title_lower:
                        priority = 1

                    filtered_entries.append({
                        'entry': entry,
                        'priority': priority,
                        'duration': duration,
                        'title': title
                    })

                if not filtered_entries:
                    print("❌ Нет подходящих треков после фильтрации")
                    return None

                # Сортируем по приоритету и длительности
                filtered_entries.sort(key=lambda x: (-x['priority'], -x['duration']))

                # Берем лучший результат
                best_entry = filtered_entries[0]['entry']
                title = self.clean_title(best_entry.get('title') or '')
                webpage_url = best_entry.get('webpage_url') or best_entry.get('url') or ''
                duration = best_entry.get('duration') or 0
                artist = best_entry.get('uploader') or best_entry.get('uploader_id') or 'Неизвестно'

                print(f"🎵 Выбран лучший трек: {title} - {artist} ({duration} сек)")
                
                if not webpage_url:
                    print("❌ У трека нет webpage_url")
                    return None

                return {
                    'title': title,
                    'webpage_url': webpage_url,
                    'duration': duration,
                    'artist': artist
                }

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут поиска: {query}")
                print(f"❌ Таймаут поиска: {query}")
                return None
            except Exception as e:
                logger.warning(f'Ошибка поиска: {e}')
                print(f"❌ Ошибка поиска: {e}")
                return None

    def _is_likely_artist(self, uploader: str, title: str) -> bool:
        uploader_words = len(uploader.split())
        title_lower = title.lower()
        uploader_lower = uploader.lower()
        
        if title_lower.startswith(uploader_lower):
            return True
        
        if f"{uploader_lower} -" in title_lower or f"{uploader_lower} –" in title_lower:
            return True
            
        return uploader_words <= 3

    # ==================== СКАЧИВАНИЕ ====================

    async def download_track(self, url: str) -> str:
        if not self.is_valid_url(url):
            print(f"❌ Невалидный URL: {url}")
            return None

        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
            ydl_opts = SOUNDCLOUD_OPTS.copy()
            ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).100s.%(ext)s')

            print(f"⏬ Начинаем скачивание: {url}")

            def download_track():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        result = ydl.extract_info(url, download=True)
                        print(f"✅ yt-dlp завершил скачивание")
                        return result
                except Exception as e:
                    print(f"❌ Ошибка в yt-dlp: {e}")
                    return None

            info = await asyncio.wait_for(
                loop.run_in_executor(None, download_track),
                timeout=DOWNLOAD_TIMEOUT
            )

            if not info:
                print("❌ yt-dlp не вернул информацию")
                return None

            # Ищем Telegram-совместимые файлы
            telegram_audio_extensions = ['.mp3', '.m4a', '.ogg', '.wav', '.flac']
            
            for file in os.listdir(tmpdir):
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext in telegram_audio_extensions:
                    file_path = os.path.join(tmpdir, file)
                    
                    # Проверяем размер файла
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    print(f"📁 Найден файл: {file} ({file_size_mb:.2f} MB)")
                    
                    if file_size_mb >= MAX_FILE_SIZE_MB:
                        print(f"❌ Файл слишком большой: {file_size_mb} MB")
                        continue
                    
                    print(f"✅ Файл подходит: {file_path}")
                    return file_path

            print(f"❌ Не найдено подходящих файлов в {tmpdir}")
            return None

        except asyncio.TimeoutError:
            print(f"❌ Таймаут скачивания: {url}")
            return None
        except Exception as e:
            logger.exception(f'Ошибка скачивания: {e}')
            print(f"❌ Ошибка скачивания: {e}")
            return None
        finally:
            # Очищаем временную директорию
            asyncio.create_task(self._cleanup_temp_dir(tmpdir))

    # ==================== КОМАНДЫ ====================

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        features = []
        if self.ai.enabled:
            features.append("🎯 умный поиск")
            features.append("💝 персонализированные пожелания")
        else:
            features.append("⚡ быстрый поиск")
            features.append("✨ приятные пожелания")
        
        features_text = " + ".join(features)
        
        await update.message.reply_text(
            f"🎵 <b>Универсальный музыкальный бот</b>\n{features_text}\n\n"
            f"👋 Привет, {user.mention_html()}!\n\n"
            f"📢 <b>Доступные команды:</b>\n"
            f"• <code>найди [запрос]</code> - найти трек\n"
            f"• <code>/find [запрос]</code> - найти трек (команда)\n"
            f"• <code>рандом</code> - случайный трек\n"
            f"• <code>/random</code> - случайный трек (команда)\n"
            f"• <code>/stats</code> - статистика бота\n"
            f"• <code>/help</code> - помощь\n\n"
            f"🚀 <b>Начни поиск музыки!</b>",
            parse_mode='HTML'
        )

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        # Простая статистика
        stats_text = f"""
📊 <b>Статистика бота</b>

👤 Пользователь: {user.mention_html()}
🆔 ID: <code>{user.id}</code>

⚡ <b>Система:</b>
• Redis: {'✅' if REDIS_URL else '❌'}
• ИИ улучшения: {'✅' if self.ai.enabled else '❌'}
• Health checks: ✅

💡 <b>Использование:</b>
• Макс. размер файла: {MAX_FILE_SIZE_MB}MB
• Лимит запросов: {REQUESTS_PER_MINUTE}/мин
• Таймаут поиска: {SEARCH_TIMEOUT}сек

🎵 <b>Музыка:</b>
• Доступно жанров: {len(RANDOM_SEARCHES)}
• Форматы: MP3, M4A, OGG
• Источники: SoundCloud
"""
        await update.message.reply_text(stats_text, parse_mode='HTML')

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = f"""
🎵 <b>Помощь по командам</b>

<b>Поиск музыки:</b>
• <code>найди [запрос]</code> - поиск трека
• <code>/find [запрос]</code> - поиск (команда)
• <code>рандом</code> - случайный трек
• <code>/random</code> - случайный трек (команда)

<b>Информация:</b>
• <code>/stats</code> - статистика бота
• <code>/help</code> - эта справка
• <code>/start</code> - начать работу

<b>Примеры запросов:</b>
• <code>найди coldplay adventure</code>
• <code>найди lo fi beats</code>
• <code>найди классическая музыка</code>

💡 <b>Советы:</b>
• Используй конкретные запросы
• Один запрос = один трек
• Лимит: {REQUESTS_PER_MINUTE} запросов в минуту
"""
        await update.message.reply_text(help_text, parse_mode='HTML')

    # ==================== ЗАПУСК БОТА ====================

    def run(self):
        print('🚀 Запуск улучшенного Music Bot...')
        print('💡 Бот работает ВО ВСЕХ чатах (ЛС и группы)')
        print('🎯 Реагирует на: "найди", "/find", "рандом", "/random"')
        print('🛡️  Rate limiting: {} запросов/минуту'.format(REQUESTS_PER_MINUTE))
        print('⚡ Ускоренный поиск: 8 результатов + умная фильтрация')
        print('📊 Прогресс-бар: визуализация этапов поиска')
        
        if REDIS_URL:
            print('🔮 Redis кэширование: включено')
        else:
            print('🔮 Redis кэширование: отключено')
            
        if self.ai.enabled:
            print('🎯 Умный поиск: улучшение запросов + персонализированные пожелания')
        else:
            print('✨ Стандартный поиск: 10 вариантов пожеланий')

        # Запускаем health server
        self.health_server.start()

        self._create_application()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f'🔄 Попытка запуска {attempt + 1}/{max_retries}...')
                self.app.run_polling(
                    poll_interval=1,
                    timeout=30,
                    drop_pending_updates=True
                )
                break
                
            except Conflict as e:
                if attempt < max_retries - 1:
                    wait_time = 10 * (attempt + 1)
                    print(f'⚠️ Конфликт: {e}')
                    print(f'⏳ Ждем {wait_time} секунд перед повторной попыткой...')
                    time.sleep(wait_time)
                else:
                    print('❌ Не удалось запустить бота из-за конфликта. Убедитесь, что не запущено других инстансов бота.')
                    raise
                    
            except (TimedOut, NetworkError) as e:
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    print(f'⚠️ Сетевая ошибка: {e}')
                    print(f'⏳ Ждем {wait_time} секунд перед повторной попыткой...')
                    time.sleep(wait_time)
                else:
                    print('❌ Не удалось запустить бота из-за сетевых ошибок')
                    raise
                    
            except Exception as e:
                print(f'❌ Непредвиденная ошибка: {e}')
                raise

if __name__ == '__main__':
    bot = UniversalMusicBot()
    bot.run()
