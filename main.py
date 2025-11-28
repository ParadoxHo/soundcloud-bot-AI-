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
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    sys.exit(1)

print("🔧 Универсальный Music Bot запускается...")

# Оптимизированные настройки
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 50))
DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', 120))
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

# Резервные пожелания (используются если DeepSeek недоступен)
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
    os.system("pip install python-telegram-bot yt-dlp aiohttp")
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

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== DEEPSEEK INTEGRATION ====================
class DeepSeekIntegration:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.base_url = "https://api.deepseek.com/v1/chat/completions"
        self.enabled = bool(self.api_key)
        self.session = None
        
        if self.enabled:
            print("✅ DeepSeek интеграция активирована")
        else:
            print("ℹ️ DeepSeek API ключ не найден, используются стандартные пожелания")
    
    async def get_session(self):
        """Получает или создает aiohttp сессию"""
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self.session
    
    async def generate_wish(self, user_query: str, track_title: str, artist: str = None) -> str:
        """Генерирует персонализированное пожелание через DeepSeek"""
        if not self.enabled:
            return random.choice(FALLBACK_WISHES)
        
        prompt = self._build_prompt(user_query, track_title, artist)
        
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
                    
                    # Очистка ответа от кавычек и лишних символов
                    wish = re.sub(r'^["\']|["\']$', '', wish)
                    print(f"🤖 DeepSeek сгенерировал пожелание: {wish}")
                    return wish
                else:
                    error_text = await response.text()
                    print(f"❌ Ошибка DeepSeek API: {response.status} - {error_text}")
                    
        except asyncio.TimeoutError:
            print("⏰ Таймаут запроса к DeepSeek")
        except Exception as e:
            print(f"❌ Ошибка при обращении к DeepSeek: {e}")
        
        # Fallback на случайные пожелания при ошибке
        return random.choice(FALLBACK_WISHES)
    
    def _build_prompt(self, user_query: str, track_title: str, artist: str = None) -> str:
        """Создает промпт для DeepSeek на основе контекста"""
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
    
    async def close(self):
        """Закрывает сессию"""
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
        self.deepseek = DeepSeekIntegration()
        self.app = None
        logger.info('✅ Универсальный бот инициализирован')

    def __del__(self):
        """Деструктор для очистки ресурсов"""
        asyncio.create_task(self.deepseek.close())

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return 'Неизвестный трек'
        title = re.sub(r"[^\w\s\-\.\(\)\[\]]", '', title)
        tags = ['official video', 'official music video', 'lyric video', 'hd', '4k',
                '1080p', '720p', 'official audio', 'audio', 'video', 'clip', 'mv']
        for tag in tags:
            title = re.sub(tag, '', title, flags=re.IGNORECASE)
        return ' '.join(title.split()).strip()

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
        """Проверяет валидность URL"""
        if not url:
            return False
        return bool(re.match(r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', url))

    def _create_application(self):
        """Создает и настраивает приложение Telegram"""
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

    async def handle_find_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает команду /find"""
        query = ' '.join(context.args)
        if not query:
            await update.message.reply_text(
                "❌ Укажи запрос для поиска\n💡 Пример: <code>/find coldplay</code>",
                parse_mode='HTML'
            )
            return
        await self.handle_find_command(update, context, f"найди {query}")

    async def handle_random_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает команду /random"""
        await self.handle_random_command(update, context)

    # ==================== ОБРАБОТКА ВСЕХ СООБЩЕНИЙ ====================

    async def handle_all_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает ВСЕ сообщения из любых чатов"""
        try:
            if not update.message or not update.message.text:
                return
                
            message_text = update.message.text.strip().lower()
            chat_id = update.effective_chat.id
            user = update.effective_user
            
            print(f"🎯 Сообщение от {user.first_name}: {message_text}")

            # Rate limiting
            if self.rate_limiter.is_limited(user.id):
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
        """Обрабатывает поиск трека по запросу"""
        status_msg = None
        try:
            user = update.effective_user
            chat_id = update.effective_chat.id
            original_message = update.message
            
            # Извлекаем запрос после "найди"
            query = self.extract_search_query(message_text)
            
            if not query:
                await original_message.reply_text(
                    f"❌ {user.mention_html()}, не указано что искать\n"
                    f"💡 Напиши: найди [название трека или исполнителя]",
                    parse_mode='HTML'
                )
                return

            # Определяем текст статуса в зависимости от типа чата
            if original_message.chat.type in ["group", "supergroup"]:
                status_text = f"🔍 {user.mention_html()} ищет: <code>{query}</code>"
            else:
                status_text = f"🔍 Ищу: <code>{query}</code>"

            # Отправляем статус (единственное сообщение)
            status_msg = await original_message.reply_text(status_text, parse_mode='HTML')

            # Этап 1: Поиск
            await status_msg.edit_text(f"{status_text}\n⏳ Этап 1/3: Ищем лучший трек...", parse_mode='HTML')
            
            # Ищем трек
            track = await self.find_track(query)
            
            if not track:
                await status_msg.edit_text(
                    f"❌ Не найдено по запросу: <code>{query}</code>\n"
                    f"💡 Попробуй другой запрос, {user.mention_html()}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Найден трек: {track['title']}")

            # Этап 2: Скачивание
            await status_msg.edit_text(
                f"{status_text}\n⏳ Этап 2/3: Скачиваем <b>{track.get('title', 'Неизвестный трек')}</b>...",
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

            # Этап 3: Отправка + генерация пожелания
            await status_msg.edit_text(
                f"{status_text}\n⏳ Этап 3/3: Отправляем в чат...",
                parse_mode='HTML'
            )

            # Параллельно генерируем пожелание и отправляем аудио
            wish_task = asyncio.create_task(
                self.deepseek.generate_wish(query, track['title'], track.get('artist'))
            )
            
            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    # Ждем завершения генерации пожелания (максимум 5 секунд)
                    try:
                        wish = await asyncio.wait_for(wish_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        wish = random.choice(FALLBACK_WISHES)
                        print("⏰ Таймаут генерации пожелания, используем fallback")
                    
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n⏱️ {self.format_duration(track.get('duration'))}\n\n{wish}",
                        parse_mode='HTML'
                    )
                print(f"✅ Аудио отправлено в чат {chat_id}")
            except Exception as e:
                print(f"❌ Ошибка отправки аудио: {e}")
                # Отменяем задачу генерации пожелания если она еще выполняется
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
        """Обрабатывает запрос на случайный трек"""
        status_msg = None
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
            status_msg = await original_message.reply_text(status_text, parse_mode='HTML')

            # Этап 1: Поиск
            await status_msg.edit_text(f"{status_text}\n⏳ Этап 1/3: Ищем интересную музыку...", parse_mode='HTML')

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
            await status_msg.edit_text(
                f"{status_text}\n⏳ Этап 2/3: Скачиваем <b>{track.get('title', 'Неизвестный трек')}</b>...",
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
                f"{status_text}\n⏳ Этап 3/3: Отправляем в чат...",
                parse_mode='HTML'
            )

            # Параллельно генерируем пожелание
            wish_task = asyncio.create_task(
                self.deepseek.generate_wish(random_query, track['title'], track.get('artist'))
            )

            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    # Ждем завершения генерации пожелания (максимум 5 секунд)
                    try:
                        wish = await asyncio.wait_for(wish_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        wish = random.choice(FALLBACK_WISHES)
                        print("⏰ Таймаут генерации пожелания, используем fallback")
                    
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
        """Извлекает поисковый запрос из сообщения"""
        query = message_text.replace('найди', '').strip()
        stop_words = ['пожалуйста', 'мне', 'трек', 'песню', 'музыку', 'плз', 'plz']
        for word in stop_words:
            query = query.replace(word, '')
        return query.strip()

    # ==================== ПОИСК ТРЕКОВ ====================

    async def find_track(self, query: str):
        """Находит трек по запросу с улучшенной релевантностью"""
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

    # ==================== СКАЧИВАНИЕ ====================

    async def download_track(self, url: str) -> str:
        """Скачивает трек и возвращает путь к файлу"""
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
            async def cleanup():
                await asyncio.sleep(2)
                try:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                    print(f"✅ Очищена временная директория: {tmpdir}")
                except Exception as e:
                    print(f"⚠️ Не удалось очистить временную директорию: {e}")
            
            asyncio.create_task(cleanup())

    # ==================== КОМАНДЫ ====================

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user = update.effective_user
        
        ai_status = "🤖 с AI-пожеланиями" if self.deepseek.enabled else "⚡"
        
        await update.message.reply_text(
            f"🎵 <b>Универсальный музыкальный бот {ai_status}</b>\n\n"
            f"👋 Привет, {user.mention_html()}!\n\n"
            f"📢 <b>Доступные команды:</b>\n"
            f"• <code>найди [запрос]</code> - найти трек\n"
            f"• <code>/find [запрос]</code> - найти трек (команда)\n"
            f"• <code>рандом</code> - случайный трек\n"
            f"• <code>/random</code> - случайный трек (команда)\n\n"
            f"🚀 <b>Начни поиск музыки!</b>",
            parse_mode='HTML'
        )

    # ==================== ЗАПУСК БОТА ====================

    def run(self):
        print('🚀 Запуск улучшенного Music Bot...')
        print('💡 Бот работает ВО ВСЕХ чатах (ЛС и группы)')
        print('🎯 Реагирует на: "найди", "/find", "рандом", "/random"')
        print('🛡️  Rate limiting: {} запросов/минуту'.format(REQUESTS_PER_MINUTE))
        print('⚡ Ускоренный поиск: 8 результатов + быстрая фильтрация')
        print('⚡ Ускоренное скачивание: 2 одновременных загрузки')
        print('📊 Поэтапный статус: поиск → скачивание → отправка')
        
        if self.deepseek.enabled:
            print('🤖 DeepSeek AI: персонализированные пожелания')
        else:
            print('💝 Стандартные пожелания: 10 вариантов')

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
        finally:
            # Закрываем соединения при завершении
            asyncio.run(self.deepseek.close())

if __name__ == '__main__':
    bot = UniversalMusicBot()
    bot.run()
