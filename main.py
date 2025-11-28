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

print("🔧 Универсальный Music Bot с глубокой ИИ-интеграцией запускается...")

# Оптимизированные настройки
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 50))
DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', 90))
SEARCH_TIMEOUT = int(os.environ.get('SEARCH_TIMEOUT', 25))
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

# ==================== AI INTEGRATION ====================
class AdvancedAISearch:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.base_url = "https://api.deepseek.com/v1/chat/completions"
        self.enabled = bool(self.api_key)
        self.session = None
        
        if self.enabled:
            print("✅ Глубокая ИИ-интеграция в поиск активирована")
        else:
            print("❌ ИИ недоступен, используется стандартный поиск")
    
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self.session
    
    async def analyze_and_rank_tracks(self, user_query: str, tracks_data: list) -> list:
        """
        Глубокая ИИ-интеграция: анализирует и ранжирует треки по релевантности
        """
        if not self.enabled or len(tracks_data) == 0:
            return tracks_data
        
        prompt = self._build_ranking_prompt(user_query, tracks_data)
        
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
                            "content": """Ты - музыкальный эксперт, который глубоко анализирует и ранжирует треки по релевантности запросу пользователя.
                            Ты получаешь список треков и должен вернуть ТОЛЬКО JSON с рейтингом от 1 до 10 для каждого трека и кратким обоснованием.
                            Формат: {"rankings": [{"index": 0, "score": 8, "reason": "отлично подходит по настроению"}, ...]}"""
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 800,
                    "temperature": 0.3,
                    "stream": False
                }
            ) as response:
                
                if response.status == 200:
                    data = await response.json()
                    analysis_result = data['choices'][0]['message']['content'].strip()
                    return self._process_ai_ranking(tracks_data, analysis_result)
                else:
                    print(f"❌ Ошибка API анализа треков: {response.status}")
                    
        except asyncio.TimeoutError:
            print("⏰ Таймаут анализа треков")
        except Exception as e:
            print(f"❌ Ошибка при анализе треков: {e}")
        
        return tracks_data
    
    async def intelligent_search_expansion(self, original_query: str) -> dict:
        """
        Интеллектуальное расширение поискового запроса с учетом контекста
        """
        if not self.enabled:
            return {"primary": original_query, "fallbacks": []}
        
        prompt = self._build_search_expansion_prompt(original_query)
        
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
                            "content": """Ты - музыкальный аналитик. Проанализируй запрос и создай:
                            1. Основной улучшенный запрос
                            2. 2-3 альтернативных варианта для поиска
                            3. Рекомендуемые жанры/настроения
                            Верни ТОЛЬКО JSON: {"primary": "основной запрос", "fallbacks": ["вар1", "вар2"], "genres": ["жанр1", "жанр2"]}"""
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 400,
                    "temperature": 0.4,
                    "stream": False
                }
            ) as response:
                
                if response.status == 200:
                    data = await response.json()
                    expansion_result = data['choices'][0]['message']['content'].strip()
                    return self._parse_search_expansion(expansion_result, original_query)
                else:
                    print(f"❌ Ошибка API расширения поиска: {response.status}")
                    
        except Exception as e:
            print(f"❌ Ошибка при расширении поиска: {e}")
        
        return {"primary": original_query, "fallbacks": []}
    
    def _build_ranking_prompt(self, user_query: str, tracks_data: list) -> str:
        """Строит промпт для анализа и ранжирования треков"""
        tracks_info = []
        for i, track in enumerate(tracks_data):
            tracks_info.append(f"{i}. {track.get('title', 'N/A')} - {track.get('artist', 'N/A')} ({track.get('duration', 0)} сек)")
        
        prompt = f"""
Запрос пользователя: "{user_query}"

Проанализируй следующие треки и оцени их релевантность запросу по шкале от 1 до 10:

{chr(10).join(tracks_info)}

Критерии оценки:
1. Соответствие теме/настроению запроса
2. Качество и достоверность (официальные > каверы > ремиксы)
3. Соответствие ожидаемой длительности
4. Релевантность исполнителя
5. Отсутствие мусорных меток в названии

Верни ТОЛЬКО JSON в формате:
{{"rankings": [{{"index": 0, "score": 7, "reason": "краткое обоснование"}}, ...]}}
"""
        return prompt
    
    def _build_search_expansion_prompt(self, original_query: str) -> str:
        """Строит промпт для интеллектуального расширения поиска"""
        prompt = f"""
Запрос пользователя: "{original_query}"

Проанализируй этот музыкальный запрос и создай:
1. Основной улучшенный запрос для SoundCloud
2. 2-3 альтернативных варианта поиска
3. Рекомендуемые музыкальные жанры/настроения

Пример для "грустная музыка":
Основной: "меланхоличная инди музыка"
Альтернативы: ["эмоциональные баллады", "траурная электроника"]
Жанры: ["indie", "ambient", "acoustic"]

Верни ТОЛЬКО JSON.
"""
        return prompt
    
    def _process_ai_ranking(self, tracks_data: list, analysis_result: str) -> list:
        """Обрабатывает результат ИИ-ранжирования"""
        try:
            # Парсим JSON ответ
            ranking_data = json.loads(analysis_result)
            rankings = ranking_data.get("rankings", [])
            
            # Создаем словарь для быстрого доступа
            score_map = {r["index"]: r for r in rankings}
            
            # Добавляем scores к трекам
            for i, track in enumerate(tracks_data):
                if i in score_map:
                    track["ai_score"] = score_map[i]["score"]
                    track["ai_reason"] = score_map[i]["reason"]
                else:
                    track["ai_score"] = 0
            
            # Сортируем по AI score (по убыванию)
            tracks_data.sort(key=lambda x: x.get("ai_score", 0), reverse=True)
            
            print(f"🎯 ИИ проранжировал треки. Лучший score: {tracks_data[0].get('ai_score', 'N/A')}")
            return tracks_data
            
        except Exception as e:
            print(f"❌ Ошибка обработки ИИ-ранжирования: {e}")
            return tracks_data
    
    def _parse_search_expansion(self, expansion_result: str, original_query: str) -> dict:
        """Парсит результат расширения поиска"""
        try:
            return json.loads(expansion_result)
        except:
            return {"primary": original_query, "fallbacks": []}
    
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
        self.ai_search = AdvancedAISearch()
        self.app = None
        logger.info('✅ Универсальный бот с глубокой ИИ-интеграцией инициализирован')

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
        status_msg = None
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

            # Отправляем статус
            status_msg = await original_message.reply_text(status_text, parse_mode='HTML')

            # Этап 1: Интеллектуальное расширение поиска
            if self.ai_search.enabled:
                await status_msg.edit_text(f"{status_text}\n🎯 Этап 1/4: Анализирую запрос ИИ...", parse_mode='HTML')
                search_expansion = await self.ai_search.intelligent_search_expansion(original_query)
                enhanced_query = search_expansion["primary"]
                fallback_queries = search_expansion.get("fallbacks", [])
                
                if enhanced_query != original_query:
                    print(f"🎯 ИИ улучшил запрос: '{original_query}' -> '{enhanced_query}'")
            else:
                enhanced_query = original_query
                fallback_queries = []

            # Этап 2: Поиск треков
            await status_msg.edit_text(f"{status_text}\n⏳ Этап 2/4: Ищу треки...", parse_mode='HTML')
            
            # Основной поиск
            tracks = await self.find_multiple_tracks(enhanced_query, fallback_queries)
            
            if not tracks:
                await status_msg.edit_text(
                    f"❌ Не найдено по запросу: <code>{original_query}</code>\n"
                    f"💡 Попробуй другой запрос, {user.mention_html()}",
                    parse_mode='HTML'
                )
                return

            # Этап 3: Глубокий ИИ-анализ результатов
            if self.ai_search.enabled and len(tracks) > 1:
                await status_msg.edit_text(f"{status_text}\n🧠 Этап 3/4: Анализирую результаты ИИ...", parse_mode='HTML')
                tracks = await self.ai_search.analyze_and_rank_tracks(original_query, tracks)

            # Берем лучший трек
            best_track = tracks[0]
            print(f"✅ Выбран лучший трек: {best_track['title']} (AI score: {best_track.get('ai_score', 'N/A')})")

            # Этап 4: Скачивание
            await status_msg.edit_text(
                f"{status_text}\n⏳ Этап 4/4: Скачиваем <b>{best_track.get('title', 'Неизвестный трек')}</b>...",
                parse_mode='HTML'
            )

            # Скачиваем трек
            file_path = await self.download_track(best_track.get('webpage_url'))
            if not file_path:
                print(f"❌ Не удалось скачать трек: {best_track['title']}")
                await status_msg.edit_text(
                    f"❌ Не удалось скачать трек\n"
                    f"🎵 {best_track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Трек скачан: {file_path}")

            # Формируем информативное описание
            caption = self._build_track_caption(best_track, original_query)

            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_file,
                        title=(best_track.get('title') or 'Неизвестный трек')[:64],
                        performer=(best_track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=caption,
                        parse_mode='HTML'
                    )
                print(f"✅ Аудио отправлено в чат {chat_id}")
            except Exception as e:
                print(f"❌ Ошибка отправки аудио: {e}")
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

            # Удаляем статус-сообщение
            try:
                await status_msg.delete()
                print("✅ Статус-сообщение удалено")
            except:
                # Если нельзя удалить, редактируем в финальный вид
                await status_msg.edit_text(
                    f"✅ Найдено: <b>{best_track.get('title', 'Неизвестный трек')}</b>\n"
                    f"⏱️ {self.format_duration(best_track.get('duration'))}",
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
        """Обрабатывает запрос на случайный трек (без ИИ для скорости)"""
        status_msg = None
        try:
            user = update.effective_user
            chat_id = update.effective_chat.id
            original_message = update.message

            # Определяем текст статуса
            if original_message.chat.type in ["group", "supergroup"]:
                status_text = f"🎲 {user.mention_html()} ищет случайный трек..."
            else:
                status_text = "🎲 Ищу случайный трек..."

            # Отправляем статус
            status_msg = await original_message.reply_text(status_text, parse_mode='HTML')

            # Случайный запрос
            random_query = random.choice(RANDOM_SEARCHES)
            print(f"🎲 Случайный запрос: {random_query}")
            
            # Ищем трек (без ИИ для скорости)
            await status_msg.edit_text(f"{status_text}\n⏳ Ищу интересную музыку...", parse_mode='HTML')
            track = await self.find_track(random_query)
            
            if not track:
                await status_msg.edit_text(
                    f"❌ Не удалось найти случайный трек\n"
                    f"💡 Попробуй еще раз, {user.mention_html()}",
                    parse_mode='HTML'
                )
                return

            print(f"✅ Найден случайный трек: {track['title']}")

            # Скачиваем трек
            await status_msg.edit_text(
                f"{status_text}\n⏳ Скачиваем <b>{track.get('title', 'Неизвестный трек')}</b>...",
                parse_mode='HTML'
            )

            file_path = await self.download_track(track.get('webpage_url'))
            if not file_path:
                await status_msg.edit_text(
                    f"❌ Не удалось скачать случайный трек\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )
                return

            # Отправляем аудио
            try:
                with open(file_path, 'rb') as audio_file:
                    await context.bot.send_audio(
                        chat_id=chat_id,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n⏱️ {self.format_duration(track.get('duration'))}\n🎲 Случайная находка!",
                        parse_mode='HTML'
                    )
            except Exception as e:
                print(f"❌ Ошибка отправки случайного аудио: {e}")
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
            if status_msg:
                await status_msg.edit_text(
                    f"❌ Ошибка при поиске\n"
                    f"💡 Попробуй еще раз, {user.mention_html()}",
                    parse_mode='HTML'
                )

    def _build_track_caption(self, track: dict, original_query: str) -> str:
        """Создает информативное описание трека без пожеланий"""
        caption = f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
        caption += f"🎤 {track.get('artist', 'Неизвестный исполнитель')}\n"
        caption += f"⏱️ {self.format_duration(track.get('duration'))}\n"
        
        # Добавляем ИИ-инсайты если есть
        if track.get('ai_score'):
            caption += f"🎯 Рейтинг релевантности: {track['ai_score']}/10\n"
        
        if track.get('ai_reason'):
            caption += f"💡 {track['ai_reason']}\n"
        
        caption += f"🔍 По запросу: <i>{original_query}</i>"
        
        return caption

    def extract_search_query(self, message_text: str) -> str:
        query = message_text.replace('найди', '').strip()
        stop_words = ['пожалуйста', 'мне', 'трек', 'песню', 'музыку', 'плз', 'plz']
        for word in stop_words:
            query = query.replace(word, '')
        return query.strip()

    # ==================== УЛУЧШЕННЫЙ ПОИСК ТРЕКОВ ====================

    async def find_multiple_tracks(self, primary_query: str, fallback_queries: list = None) -> list:
        """Ищет треки по основному и запасным запросам, возвращает объединенный список"""
        all_tracks = []
        
        # Поиск по основному запросу
        primary_tracks = await self.find_track_batch(primary_query, limit=8)
        if primary_tracks:
            all_tracks.extend(primary_tracks)
            print(f"✅ Найдено {len(primary_tracks)} треков по основному запросу")
        
        # Поиск по запасным запросам если нужно больше результатов
        if fallback_queries and len(all_tracks) < 3:
            for fallback_query in fallback_queries[:2]:  # Максимум 2 запасных запроса
                fallback_tracks = await self.find_track_batch(fallback_query, limit=4)
                if fallback_tracks:
                    all_tracks.extend(fallback_tracks)
                    print(f"✅ Добавлено {len(fallback_tracks)} треков из запасного запроса: {fallback_query}")
        
        # Убираем дубликаты по URL
        seen_urls = set()
        unique_tracks = []
        for track in all_tracks:
            if track.get('webpage_url') not in seen_urls:
                seen_urls.add(track.get('webpage_url'))
                unique_tracks.append(track)
        
        print(f"🎵 Итого уникальных треков: {len(unique_tracks)}")
        return unique_tracks

    async def find_track_batch(self, query: str, limit: int = 5) -> list:
        """Ищет несколько треков по запросу"""
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
                        return ydl.extract_info(f"scsearch{limit}:{query}", download=False)

                loop = asyncio.get_event_loop()
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, perform_search),
                    timeout=SEARCH_TIMEOUT
                )

                if not info:
                    print(f"❌ Поиск не дал результатов: {query}")
                    return []

                entries = info.get('entries', [])
                if not entries and info.get('_type') != 'playlist':
                    entries = [info]

                print(f"✅ Найдено {len(entries)} результатов по запросу '{query}'")

                # Фильтрация и базовая сортировка
                filtered_entries = []
                for entry in entries:
                    if not entry:
                        continue

                    # Фильтруем по длительности
                    duration = entry.get('duration') or 0
                    if duration < 30 or duration > 3600:
                        continue

                    title = self.clean_title(entry.get('title') or '')
                    if not title:
                        continue

                    # Базовый приоритет
                    priority = 0
                    title_lower = title.lower()
                    if 'official' in title_lower:
                        priority = 3
                    elif 'original' in title_lower:
                        priority = 2
                    elif 'cover' not in title_lower and 'remix' not in title_lower:
                        priority = 1

                    filtered_entries.append({
                        'title': title,
                        'webpage_url': entry.get('webpage_url') or entry.get('url') or '',
                        'duration': duration,
                        'artist': entry.get('uploader') or entry.get('uploader_id') or 'Неизвестно',
                        'priority': priority
                    })

                # Сортируем по приоритету и длительности
                filtered_entries.sort(key=lambda x: (-x['priority'], -x['duration']))
                
                return filtered_entries[:limit]  # Возвращаем ограниченное количество

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут поиска: {query}")
                return []
            except Exception as e:
                logger.warning(f'Ошибка поиска: {e}')
                return []

    async def find_track(self, query: str):
        """Старая функция для обратной совместимости"""
        tracks = await self.find_track_batch(query, limit=1)
        return tracks[0] if tracks else None

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
        user = update.effective_user
        
        features = "🎯 глубокий ИИ-поиск + 🎵 точные результаты"
        
        await update.message.reply_text(
            f"🎵 <b>Универсальный музыкальный бот с ИИ-поиском</b>\n{features}\n\n"
            f"👋 Привет, {user.mention_html()}!\n\n"
            f"📢 <b>Доступные команды:</b>\n"
            f"• <code>найди [запрос]</code> - умный поиск с ИИ-анализом\n"
            f"• <code>/find [запрос]</code> - умный поиск (команда)\n"
            f"• <code>рандом</code> - случайный трек\n"
            f"• <code>/random</code> - случайный трек (команда)\n\n"
            f"🧠 <b>ИИ анализирует:</b>\n"
            f"• Релевантность запросу\n• Качество треков\n• Музыкальное настроение\n\n"
            f"🚀 <b>Начни умный поиск музыки!</b>",
            parse_mode='HTML'
        )

    # ==================== ЗАПУСК БОТА ====================

    def run(self):
        print('🚀 Запуск Music Bot с глубокой ИИ-интеграцией...')
        print('💡 Бот работает ВО ВСЕХ чатах (ЛС и группы)')
        print('🎯 Реагирует на: "найди", "/find", "рандом", "/random"')
        print('🧠 Глубокая ИИ-интеграция: анализ запросов + ранжирование треков')
        print('🔍 Умный поиск: multiple queries + ИИ-ранжирование')
        print('📊 Информативные результаты: рейтинги и обоснования')
        print('🚫 Без пожеланий: только факты о треках')

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
                    print('❌ Не удалось запустить бота из-за конфликта.')
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

    async def cleanup(self):
        await self.ai_search.close()

if __name__ == '__main__':
    bot = UniversalMusicBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("🛑 Бот остановлен")
    finally:
        asyncio.run(bot.cleanup())
