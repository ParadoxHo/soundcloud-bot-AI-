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

print("🔧 Music Bot с РЕАЛЬНО умным поиском запускается...")

# Оптимизированные настройки
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', 50))
DOWNLOAD_TIMEOUT = int(os.environ.get('DOWNLOAD_TIMEOUT', 90))
SEARCH_TIMEOUT = int(os.environ.get('SEARCH_TIMEOUT', 30))  # Увеличили для глубокого поиска
REQUESTS_PER_MINUTE = int(os.environ.get('REQUESTS_PER_MINUTE', 8))

# Список для случайных треков
RANDOM_SEARCHES = [
    'lo fi beats', 'chillhop', 'deep house', 'synthwave', 'indie rock',
    'electronic music', 'jazz lounge', 'ambient', 'study music',
    'focus music', 'relaxing music', 'instrumental', 'acoustic'
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

# ==================== REAL AI SEARCH ENGINE ====================
class RealAISearchEngine:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or DEEPSEEK_API_KEY
        self.base_url = "https://api.deepseek.com/v1/chat/completions"
        self.enabled = bool(self.api_key)
        self.session = None
        
        if self.enabled:
            print("✅ Реальный ИИ-поиск активирован")
        else:
            print("❌ ИИ недоступен, используется улучшенный стандартный поиск")
    
    async def get_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self.session
    
    async def smart_track_selection(self, user_query: str, search_results: list) -> dict:
        """
        РЕАЛЬНЫЙ умный выбор трека на основе глубокого анализа
        """
        if not self.enabled or len(search_results) == 0:
            return self._fallback_selection(search_results)
        
        # Шаг 1: Анализ музыкальных предпочтений пользователя
        music_profile = await self._analyze_music_preferences(user_query)
        
        # Шаг 2: Глубокий анализ каждого трека
        analyzed_tracks = []
        for track in search_results[:12]:  # Анализируем больше треков
            analysis = await self._analyze_single_track(track, user_query, music_profile)
            if analysis:
                analyzed_tracks.append(analysis)
        
        # Шаг 3: Выбор лучшего трека
        best_track = self._select_best_track(analyzed_tracks, music_profile)
        
        return best_track
    
    async def _analyze_music_preferences(self, user_query: str) -> dict:
        """Анализирует музыкальные предпочтения из запроса"""
        prompt = f"""
        Пользователь ищет музыку по запросу: "{user_query}"
        
        Проанализируй этот запрос и определи:
        1. Музыкальный жанр/направление
        2. Настроение (энергичное/расслабляющее/грустное/веселое)
        3. Цель прослушивания (работа/отдых/тренировка/учёба)
        4. Ожидаемые характеристики трека (темп, наличие вокала, инструменты)
        
        Верни ТОЛЬКО JSON:
        {{
            "genre": "основной жанр",
            "mood": "настроение", 
            "purpose": "цель",
            "expected_tempo": "быстрый/медленный/умеренный",
            "vocals": "с вокалом/инструментальный",
            "priority_factors": ["фактор1", "фактор2"]
        }}
        """
        
        try:
            session = await self.get_session()
            async with session.post(
                self.base_url,
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 500,
                    "temperature": 0.3
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    profile_text = data['choices'][0]['message']['content'].strip()
                    return json.loads(profile_text)
        except Exception as e:
            print(f"❌ Ошибка анализа предпочтений: {e}")
        
        # Fallback профиль
        return {
            "genre": "разное",
            "mood": "нейтральное", 
            "purpose": "прослушивание",
            "expected_tempo": "умеренный",
            "vocals": "любые",
            "priority_factors": ["качество", "релевантность"]
        }
    
    async def _analyze_single_track(self, track: dict, user_query: str, music_profile: dict) -> dict:
        """Глубокий анализ одного трека"""
        prompt = f"""
        Запрос пользователя: "{user_query}"
        Музыкальный профиль: {json.dumps(music_profile, ensure_ascii=False)}
        
        Анализируемый трек:
        - Название: {track.get('title', 'N/A')}
        - Исполнитель: {track.get('artist', 'N/A')}
        - Длительность: {track.get('duration', 0)} сек
        
        Проанализируй этот трек по критериям:
        1. Релевантность запросу "{user_query}" (0-10)
        2. Соответствие жанру "{music_profile.get('genre')}" (0-10)
        3. Соответствие настроению "{music_profile.get('mood')}" (0-10)
        4. Качество (официальный/оригинальный/кавер) (0-10)
        5. Общее впечатление (0-10)
        
        Верни ТОЛЬКО JSON:
        {{
            "track_data": {json.dumps(track, ensure_asciii=False)},
            "scores": {{
                "relevance": 0-10,
                "genre_match": 0-10,
                "mood_match": 0-10, 
                "quality": 0-10,
                "overall": 0-10
            }},
            "final_score": 0-100,
            "reason": "краткое обоснование"
        }}
        """
        
        try:
            session = await self.get_session()
            async with session.post(
                self.base_url,
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 600,
                    "temperature": 0.4
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    analysis_text = data['choices'][0]['message']['content'].strip()
                    analysis = json.loads(analysis_text)
                    
                    # Добавляем базовые метрики качества
                    analysis["quality_metrics"] = self._calculate_quality_metrics(track)
                    
                    return analysis
        except Exception as e:
            print(f"❌ Ошибка анализа трека: {e}")
        
        return None
    
    def _calculate_quality_metrics(self, track: dict) -> dict:
        """Вычисляет автоматические метрики качества"""
        title = track.get('title', '').lower()
        score = 0
        
        # Качество по названию
        if 'official' in title:
            score += 3
        elif 'original' in title:
            score += 2
        elif 'cover' not in title and 'remix' not in title:
            score += 1
        
        # Качество по длительности
        duration = track.get('duration', 0)
        if 120 <= duration <= 600:  # 2-10 минут - идеально
            score += 2
        elif 60 <= duration <= 1200:  # 1-20 минут - приемлемо
            score += 1
        
        # Качество по артисту (известный vs случайный)
        artist = track.get('artist', '').lower()
        if len(artist) > 3 and artist not in ['unknown', 'неизвестно', 'soundcloud']:
            score += 1
        
        return {"auto_quality_score": score, "max_auto_quality": 6}
    
    def _select_best_track(self, analyzed_tracks: list, music_profile: dict) -> dict:
        """Выбирает лучший трек на основе анализа"""
        if not analyzed_tracks:
            return None
        
        # Взвешенная оценка с приоритетами из профиля
        for track in analyzed_tracks:
            scores = track.get("scores", {})
            final_score = scores.get("relevance", 0) * 0.3
            final_score += scores.get("genre_match", 0) * 0.25
            final_score += scores.get("mood_match", 0) * 0.2
            final_score += scores.get("quality", 0) * 0.15
            final_score += scores.get("overall", 0) * 0.1
            
            # Добавляем автоматические метрики
            auto_quality = track.get("quality_metrics", {}).get("auto_quality_score", 0)
            final_score += (auto_quality / 6) * 10  # Нормализуем до 10 баллов
            
            track["calculated_score"] = final_score
        
        # Сортируем по итоговому score
        analyzed_tracks.sort(key=lambda x: x.get("calculated_score", 0), reverse=True)
        
        best_track = analyzed_tracks[0]
        best_track["track_data"]["ai_analysis"] = {
            "final_score": round(best_track["calculated_score"], 1),
            "reason": best_track.get("reason", "Лучшее соответствие запросу")
        }
        
        print(f"🎯 ИИ выбрал трек с score: {best_track['calculated_score']:.1f}")
        return best_track["track_data"]
    
    def _fallback_selection(self, search_results: list) -> dict:
        """Умный fallback выбор без ИИ"""
        if not search_results:
            return None
        
        scored_tracks = []
        for track in search_results:
            score = 0
            
            # Приоритет официальных треков
            title = track.get('title', '').lower()
            if 'official' in title:
                score += 30
            elif 'original' in title:
                score += 20
            elif 'cover' not in title and 'remix' not in title:
                score += 10
            
            # Приоритет по длительности
            duration = track.get('duration', 0)
            if 120 <= duration <= 600:
                score += 20
            elif 60 <= duration <= 1200:
                score += 10
            
            # Приоритет известных артистов
            artist = track.get('artist', '')
            if artist and len(artist) > 3 and artist.lower() not in ['unknown', 'неизвестно']:
                score += 10
            
            scored_tracks.append((score, track))
        
        scored_tracks.sort(key=lambda x: x[0], reverse=True)
        best_track = scored_tracks[0][1] if scored_tracks else search_results[0]
        best_track["fallback_analysis"] = {"method": "quality_heuristic", "score": scored_tracks[0][0]}
        
        return best_track
    
    async def close(self):
        if self.session:
            await self.session.close()

# ==================== ADVANCED MUSIC BOT ====================
class AdvancedMusicBot:
    def __init__(self):
        self.download_semaphore = asyncio.Semaphore(2)
        self.search_semaphore = asyncio.Semaphore(2)  # Уменьшили для глубины поиска
        self.rate_limiter = RateLimiter()
        self.ai_engine = RealAISearchEngine()
        self.app = None
        logger.info('✅ Продвинутый музыкальный бот инициализирован')

    # ... (остальные методы clean_title, format_duration, is_valid_url остаются похожими)

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return 'Неизвестный трек'
        title = re.sub(r"[^\w\s\-\.\(\)\[\]]", '', title)
        # Более агрессивная очистка
        junk_patterns = [
            'official video', 'official music video', 'lyric video', 'hd', '4k',
            '1080p', '720p', 'official audio', 'audio', 'video', 'clip', 'mv',
            'upload', 'uploaded', 'by', 'uploader', 'soundcloud', 'free download',
            'mp3', 'm4a', '2024', '2023', '2022'
        ]
        for pattern in junk_patterns:
            title = re.sub(pattern, '', title, flags=re.IGNORECASE)
        return ' '.join(title.split()).strip()

    async def deep_search(self, query: str) -> list:
        """Глубокий поиск с множественными стратегиями"""
        strategies = [
            self._search_soundcloud_basic,
            self._search_soundcloud_extended, 
            self._search_alternative_queries
        ]
        
        all_results = []
        seen_urls = set()
        
        for strategy in strategies:
            try:
                results = await strategy(query)
                for track in results:
                    if track.get('webpage_url') not in seen_urls:
                        seen_urls.add(track.get('webpage_url'))
                        all_results.append(track)
                
                if len(all_results) >= 15:  # Достаточно результатов
                    break
                    
            except Exception as e:
                print(f"⚠️ Ошибка в стратегии поиска: {e}")
                continue
        
        print(f"🔍 Всего найдено уникальных треков: {len(all_results)}")
        return all_results

    async def _search_soundcloud_basic(self, query: str, limit: int = 8) -> list:
        """Базовый поиск в SoundCloud"""
        return await self._search_soundcloud(f"scsearch{limit}:{query}")

    async def _search_soundcloud_extended(self, query: str, limit: int = 12) -> list:
        """Расширенный поиск с разными модификаторами"""
        searches = [
            f"scsearch{limit}:{query}",
            f"scsearch{limit//2}:{query} 2024",
            f"scsearch{limit//2}:{query} official"
        ]
        
        all_results = []
        for search_query in searches:
            results = await self._search_soundcloud(search_query)
            all_results.extend(results)
        
        return all_results

    async def _search_alternative_queries(self, original_query: str) -> list:
        """Поиск по альтернативным формулировкам"""
        alternatives = self._generate_alternative_queries(original_query)
        all_results = []
        
        for alt_query in alternatives[:3]:  # Максимум 3 альтернативных запроса
            results = await self._search_soundcloud(f"scsearch4:{alt_query}")
            all_results.extend(results)
        
        return all_results

    def _generate_alternative_queries(self, query: str) -> list:
        """Генерирует альтернативные поисковые запросы"""
        alternatives = []
        
        # Добавляем жанровые модификаторы
        genre_modifiers = ['', ' music', ' song', ' track', ' beat', ' mix']
        for modifier in genre_modifiers:
            alternatives.append(query + modifier)
        
        # Добавляем языковые варианты для международных жанров
        if any(word in query.lower() for word in ['rock', 'pop', 'jazz', 'house']):
            alternatives.append(query + ' русский')
            alternatives.append(query + ' russian')
        
        return alternatives

    async def _search_soundcloud(self, search_query: str) -> list:
        """Базовый метод поиска в SoundCloud"""
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
            def perform_search():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(search_query, download=False)

            loop = asyncio.get_event_loop()
            info = await asyncio.wait_for(
                loop.run_in_executor(None, perform_search),
                timeout=SEARCH_TIMEOUT
            )

            if not info:
                return []

            entries = info.get('entries', [])
            if not entries and info.get('_type') != 'playlist':
                entries = [info]

            processed_tracks = []
            for entry in entries:
                if not entry:
                    continue

                # Жесткая фильтрация
                duration = entry.get('duration') or 0
                if duration < 45 or duration > 1800:  # 45 сек - 30 мин
                    continue

                title = self.clean_title(entry.get('title') or '')
                if not title or len(title) < 3:
                    continue

                # Фильтрация мусорных треков
                if self._is_low_quality_track(title, entry.get('uploader', '')):
                    continue

                processed_tracks.append({
                    'title': title,
                    'webpage_url': entry.get('webpage_url') or entry.get('url') or '',
                    'duration': duration,
                    'artist': entry.get('uploader') or 'Неизвестный исполнитель',
                    'original_title': entry.get('title', '')
                })

            return processed_tracks

        except Exception as e:
            print(f"❌ Ошибка поиска: {e}")
            return []

    def _is_low_quality_track(self, title: str, uploader: str) -> bool:
        """Определяет низкокачественные треки"""
        title_lower = title.lower()
        uploader_lower = uploader.lower()
        
        # Признаки низкого качества
        low_quality_indicators = [
            'full album', 'playlist', 'mix', 'compilation', 'podcast',
            'live at', 'concert', 'session', 'preview', 'snippet',
            'cover by', 'remix by', 'lyrics', 'karaoke'
        ]
        
        for indicator in low_quality_indicators:
            if indicator in title_lower:
                return True
        
        # Uploader в названии (часто признак репостов)
        if uploader_lower and uploader_lower in title_lower:
            uploader_words = len(uploader.split())
            if uploader_words <= 2:  # Короткие имена uploader'ов часто спам
                return True
        
        return False

    # ... (обработчики сообщений и команд остаются похожими, но с использованием deep_search)

    async def handle_find_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        status_msg = None
        try:
            user = update.effective_user
            original_message = update.message
            
            query = self.extract_search_query(message_text)
            if not query:
                await original_message.reply_text("❌ Укажи что искать")
                return

            # Статус
            status_msg = await original_message.reply_text(f"🔍 Ищу: <code>{query}</code>", parse_mode='HTML')

            # Этап 1: Глубокий поиск
            await status_msg.edit_text(f"🔍 Ищу: <code>{query}</code>\n🎯 Этап 1/3: Глубокий поиск...", parse_mode='HTML')
            search_results = await self.deep_search(query)
            
            if not search_results:
                await status_msg.edit_text(f"❌ Не найдено по запросу: <code>{query}</code>", parse_mode='HTML')
                return

            # Этап 2: Умный выбор трека
            await status_msg.edit_text(f"🔍 Ищу: <code>{query}</code>\n🧠 Этап 2/3: Анализ {len(search_results)} треков...", parse_mode='HTML')
            best_track = await self.ai_engine.smart_track_selection(query, search_results)
            
            if not best_track:
                best_track = search_results[0]  # Fallback

            # Этап 3: Скачивание
            await status_msg.edit_text(f"🔍 Ищу: <code>{query}</code>\n⏬ Этап 3/3: Скачивание...", parse_mode='HTML')
            
            file_path = await self.download_track(best_track.get('webpage_url'))
            if not file_path:
                await status_msg.edit_text("❌ Ошибка скачивания")
                return

            # Отправка результата
            caption = self._create_result_caption(best_track, query)
            
            with open(file_path, 'rb') as audio_file:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=audio_file,
                    title=best_track.get('title', 'Трек')[:64],
                    performer=best_track.get('artist', 'Исполнитель')[:64],
                    caption=caption,
                    parse_mode='HTML'
                )

            # Очистка
            try:
                os.remove(file_path)
                await status_msg.delete()
            except:
                pass

        except Exception as e:
            logger.exception(f'Ошибка поиска: {e}')
            if status_msg:
                await status_msg.edit_text("❌ Ошибка поиска")

    def _create_result_caption(self, track: dict, query: str) -> str:
        """Создает информативное описание результата"""
        caption = f"🎵 <b>{track.get('title', 'Трек')}</b>\n"
        caption += f"🎤 {track.get('artist', 'Исполнитель')}\n"
        caption += f"⏱️ {self.format_duration(track.get('duration'))}\n"
        
        # Добавляем анализ если есть
        if track.get('ai_analysis'):
            analysis = track['ai_analysis']
            caption += f"🎯 Score: {analysis.get('final_score', 'N/A')}/100\n"
            caption += f"💡 {analysis.get('reason', '')}\n"
        elif track.get('fallback_analysis'):
            caption += f"⚡ Выбран по качеству (score: {track['fallback_analysis']['score']})\n"
        
        caption += f"🔍 Запрос: <i>{query}</i>"
        
        return caption

    # ... (остальные методы download_track, extract_search_query, start_command и т.д.)

    def extract_search_query(self, message_text: str) -> str:
        query = message_text.replace('найди', '').strip()
        stop_words = ['пожалуйста', 'мне', 'трек', 'песню', 'музыку', 'плз', 'plz', 'найти']
        for word in stop_words:
            query = query.replace(word, '')
        return query.strip()

    async def download_track(self, url: str) -> str:
        """Скачивание трека (аналогично предыдущей версии)"""
        if not self.is_valid_url(url):
            return None

        ydl_opts = {
            'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio/best',
            'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
            'quiet': True,
            'no_warnings': True,
            'retries': 2,
            'max_filesize': MAX_FILE_SIZE_MB * 1024 * 1024,
            'ignoreerrors': True,
        }

        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
            ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).100s.%(ext)s')

            def download_track():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(url, download=True)

            info = await asyncio.wait_for(
                loop.run_in_executor(None, download_track),
                timeout=DOWNLOAD_TIMEOUT
            )

            if not info:
                return None

            # Поиск файла
            for file in os.listdir(tmpdir):
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext in ['.mp3', '.m4a', '.ogg', '.wav']:
                    file_path = os.path.join(tmpdir, file)
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    
                    if file_size_mb < MAX_FILE_SIZE_MB:
                        return file_path

            return None

        except Exception as e:
            print(f"❌ Ошибка скачивания: {e}")
            return None
        finally:
            async def cleanup():
                await asyncio.sleep(2)
                try:
                    shutil.rmtree(tmpdir, ignore_errors=True)
                except:
                    pass
            asyncio.create_task(cleanup())

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
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_all_messages))
        self.app.add_handler(CommandHandler('start', self.start_command))
        self.app.add_handler(CommandHandler('find', self.handle_find_short))
        self.app.add_handler(CommandHandler('random', self.handle_random_short))

    async def handle_all_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            if not update.message or not update.message.text:
                return
                
            message_text = update.message.text.strip().lower()
            user = update.effective_user
            
            if self.rate_limiter.is_limited(user.id):
                await update.message.reply_text("⏳ Слишком много запросов")
                return

            if message_text.startswith('найди'):
                await self.handle_find_command(update, context, message_text)
            elif message_text.startswith('рандом'):
                await self.handle_random_command(update, context)
                
        except Exception as e:
            logger.exception(f'Ошибка обработки сообщения: {e}')

    async def handle_find_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = ' '.join(context.args)
        if not query:
            await update.message.reply_text("❌ Укажи запрос для поиска")
            return
        await self.handle_find_command(update, context, f"найди {query}")

    async def handle_random_short(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.handle_random_command(update, context)

    async def handle_random_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Случайный трек (упрощенная версия)"""
        try:
            random_query = random.choice(RANDOM_SEARCHES)
            search_results = await self.deep_search(random_query)
            
            if search_results:
                track = random.choice(search_results[:5])  # Случайный из лучших
                file_path = await self.download_track(track.get('webpage_url'))
                
                if file_path:
                    with open(file_path, 'rb') as audio_file:
                        await context.bot.send_audio(
                            chat_id=update.effective_chat.id,
                            audio=audio_file,
                            title=track.get('title', 'Трек')[:64],
                            performer=track.get('artist', 'Исполнитель')[:64],
                            caption=f"🎵 <b>{track.get('title', 'Трек')}</b>\n🎤 {track.get('artist', 'Исполнитель')}\n🎲 Случайная находка!",
                            parse_mode='HTML'
                        )
                    try:
                        os.remove(file_path)
                    except:
                        pass
        except Exception as e:
            print(f"❌ Ошибка случайного трека: {e}")

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        await update.message.reply_text(
            f"🎵 <b>Умный музыкальный бот</b>\n\n"
            f"👋 Привет, {user.mention_html()}!\n\n"
            f"🧠 <b>Умный поиск с ИИ-анализом:</b>\n"
            f"• Анализ музыкальных предпочтений\n"
            f"• Глубокая оценка релевантности\n" 
            f"• Фильтрация низкокачественного контента\n\n"
            f"📢 <b>Команды:</b>\n"
            f"• <code>найди [запрос]</code> - умный поиск\n"
            f"• <code>/find [запрос]</code> - умный поиск\n"
            f"• <code>рандом</code> - случайный трек\n\n"
            f"🚀 <b>Найди свою идеальную музыку!</b>",
            parse_mode='HTML'
        )

    def run(self):
        print('🚀 Запуск бота с РЕАЛЬНЫМ умным поиском...')
        print('🎯 Глубокая ИИ-интеграция в поиске')
        print('🔍 Множественные стратегии поиска')
        print('🧠 Анализ музыкальных предпочтений')
        print('⚡ Умная фильтрация качества')
        
        self._create_application()
        
        try:
            self.app.run_polling(drop_pending_updates=True)
        except Exception as e:
            print(f'❌ Ошибка запуска: {e}')

    async def cleanup(self):
        await self.ai_engine.close()

if __name__ == '__main__':
    bot = AdvancedMusicBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        print("🛑 Бот остановлен")
    finally:
        asyncio.run(bot.cleanup())
