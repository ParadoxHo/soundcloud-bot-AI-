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
from datetime import datetime, timedelta
from pathlib import Path
import concurrent.futures

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_IDS = os.environ.get('ADMIN_IDS', '').split(',')

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    print("📝 Добавьте переменную BOT_TOKEN в настройках Railway")
    sys.exit(1)

ADMIN_IDS = [id.strip() for id in ADMIN_IDS if id.strip()]

if not ADMIN_IDS:
    print("⚠️  Предупреждение: ADMIN_IDS не установлен. Админ-команды отключены.")
else:
    print(f"✅ Админы настроены: {ADMIN_IDS}")

RESULTS_PER_PAGE = 10
DATA_FILE = Path('user_data.json')
CHARTS_FILE = Path('charts_cache.json')
MAX_FILE_SIZE_MB = 45

# Увеличиваем параллелизм для скорости
MAX_CONCURRENT_DOWNLOADS = 5
DOWNLOAD_TIMEOUT = 300
SEARCH_TIMEOUT = 18  # Увеличили таймаут поиска

# Ускоренные таймауты
DYNAMIC_TIMEOUTS = {
    'short_track': 45,
    'medium_track': 90,  
    'long_track': 180,
    'very_long_track': 360,
    'search': 15
}

# Кэш для поисковых запросов (в памяти)
SEARCH_CACHE = {}
SEARCH_CACHE_TTL = 600  # 10 минут

# Предзагрузка популярных запросов
POPULAR_QUERIES_CACHE = {}
POPULAR_CACHE_TTL = 3600  # 1 час

# Ускоренные настройки для скачивания
FAST_DOWNLOAD_OPTS = {
    'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best',  # Приоритет m4a
    'outtmpl': os.path.join(tempfile.gettempdir(), 'music_bot_%(id)s_%(title).100s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 2,  # Увеличили для стабильности
    'fragment_retries': 2,
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'nooverwrites': False,
    'noplaylist': True,
    'ignoreerrors': True,
    'ignore_no_formats_error': True,
    'socket_timeout': 12,  # Увеличили таймаут
    'extractaudio': True,
    'audioformat': 'mp3',
    'audioquality': '0',
    'concurrent_fragment_downloads': 3,
}

# Оптимизированные настройки для файлов до 45MB
LARGE_FILE_OPTS = {
    'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best',  # Приоритет m4a
    'outtmpl': os.path.join(tempfile.gettempdir(), 'large_music_bot_%(id)s_%(title).100s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 3,
    'fragment_retries': 3,
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'nooverwrites': False,
    'noplaylist': True,
    'ignoreerrors': True,
    'ignore_no_formats_error': True,
    'socket_timeout': 15,
    'buffersize': 524288,
    'http_chunk_size': 5242880,
    'extractaudio': True,
    'audioformat': 'mp3',
    'audioquality': '0',
    'concurrent_fragment_downloads': 2,
}

FAST_INFO_OPTS = {
    'quiet': True,
    'no_warnings': True,
    'simulate': True,
    'format': 'bestaudio/best',
    'skip_download': True,
    'noplaylist': True,
    'extract_flat': True,
    'socket_timeout': 12,  # Увеличили таймаут
    'ignoreerrors': True,
}

DURATION_FILTERS = {
    'no_filter': 'Без фильтра',
    'up_to_5min': 'До 5 минут',
    'up_to_10min': 'До 10 минут', 
    'up_to_20min': 'До 20 минут',
}

# Обновленная структура плейлистов (10 пунктов - убрали электронную музыку)
SMART_PLAYLISTS = {
    'morning': {
        'name': '🌅 Утренний заряд',
        'queries': ['morning music', 'wake up music', 'positive morning', 'upbeat acoustic', 'fresh start'],
        'description': 'Позитивная музыка для хорошего начала дня'
    },
    'romance': {
        'name': '💖 Романтика',
        'queries': ['romantic music', 'love songs', 'slow dance', 'intimate music', 'couple music'],
        'description': 'Романтическая музыка для особенных моментов'
    },
    'nostalgia': {
        'name': '📻 Ностальгия',
        'queries': ['80s hits', '90s music', 'retro classics', 'oldies but goldies', 'vintage hits'],
        'description': 'Классические хиты для путешествия в прошлое'
    },
    'work_focus': {
        'name': '💼 Концентрация',
        'queries': ['lo fi study', 'focus music', 'ambient study', 'coding music', 'deep work'],
        'description': 'Музыка для концентрации и продуктивности'
    },
    'workout': {
        'name': '💪 Тренировка',
        'queries': ['workout music', 'gym motivation', 'edm workout', 'hip hop workout', 'energy music'],
        'description': 'Энергичная музыка для тренировок'
    },
    'relax': {
        'name': '😌 Релакс',
        'queries': ['chillhop', 'ambient relax', 'piano relax', 'meditation music', 'calm music'],
        'description': 'Спокойная музыка для расслабления'
    },
    'party': {
        'name': '🎉 Вечеринка', 
        'queries': ['party hits', 'dance music', 'club mix', 'top hits', 'festival music'],
        'description': 'Танцевальная музыка для вечеринок'
    },
    'road_trip': {
        'name': '🚗 Путешествие',
        'queries': ['road trip', 'driving music', 'travel mix', 'adventure music', 'scenic drive'],
        'description': 'Музыка для путешествий и поездок'
    },
    'sleep': {
        'name': '🌙 Сон',
        'queries': ['sleep music', 'deep sleep', 'calming sleep', 'piano sleep', 'ambient sleep'],
        'description': 'Расслабляющая музыка для здорового сна'
    },
    'rainy_day': {
        'name': '🌧️ Дождливый день',
        'queries': ['rainy day music', 'cozy jazz', 'rain sounds lofi', 'indie rainy day', 'chill rainy'],
        'description': 'Уютная музыка для дождливых дней'
    }
}

# Расширенный список случайных поисков (80+ жанров)
RANDOM_SEARCHES = [
    # Электронная музыка
    'lo fi beats', 'chillhop', 'deep house', 'synthwave', 'dubstep',
    'tropical house', 'future bass', 'retro wave', 'progressive house',
    'techno music', 'trance music', 'drum and bass', 'hardstyle', 
    'eurodance', 'disco house', 'tech house', 'minimal techno',
    'acid house', 'breakbeat', 'big room', 'electro swing',
    'glitch hop', 'moombahton', 'melodic dubstep', 'future house',
    
    # Рок и альтернатива
    'indie rock', 'alternative rock', 'indie pop', 'post rock', 
    'math rock', 'shoegaze', 'punk rock', 'emo revival',
    'garage rock', 'psychedelic rock', 'folk rock', 'blues rock',
    'hard rock', 'progressive rock', 'classic rock',
    
    # Хип-хоп и урбан
    'hip hop instrumental', 'rap beats', 'old school hip hop',
    'boom bap', 'trap music', 'drill music', 'r&b music',
    'neo soul', 'afrobeats', 'reggae', 'dancehall', 'grime',
    'uk drill',
    
    # Джаз и лаунж
    'jazz lounge', 'smooth jazz', 'lounge music', 'bossanova',
    
    # Фоновая и инструментальная
    'ambient music', 'study music', 'focus music', 'relaxing music',
    'instrumental music', 'acoustic music', 'piano covers',
    'guitar music', 'orchestral music', 'film scores',
    'video game music', 'classical piano', 'meditation music',
    
    # Мировые жанры
    'latin music', 'reggeaton', 'k-pop', 'j-pop', 'city pop',
    'salsa', 'flamenco', 'tango', 'bollywood', 'arabic music',
    'turkish pop', 'french pop', 'german techno', 'italian disco',
    'reggaeton', 'bachata', 'merengue', 'soca',
    
    # Современные тренды
    'hyperpop', 'vaporwave', 'witch house', 'seapunk',
    'bedroom pop', 'phonk', 'wave music', 'color bass',
    
    # Сезонные и тематические
    'summer hits', 'winter music', 'christmas music',
    'workout mix', 'gaming music', 'coding music',
    
    # По настроению
    'happy music', 'sad songs', 'epic music', 'motivational music'
]

POPULAR_SEARCHES = [
    'the weeknd', 'taylor swift', 'bad bunny', 'ariana grande', 'drake',
    'billie eilish', 'ed sheeran', 'dualipa', 'post malone', 'kanye west', 
    'coldplay', 'maroon 5', 'bruno mars', 'adele', 'justin bieber',
    'kendrick lamar', 'travis scott', 'doja cat', 'olivia rodrigo', 'harry styles'
]

# ==================== IMPORT TELEGRAM & YT-DLP ====================
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
    from telegram.ext import (
        Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, 
        ContextTypes
    )
    import yt_dlp
    print("✅ Все зависимости загружены")
except ImportError as exc:
    print(f"❌ Ошибка импорта: {exc}")
    print("📦 Устанавливаем зависимости...")
    os.system("pip install python-telegram-bot yt-dlp")
    try:
        from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
        from telegram.ext import (
            Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, 
            ContextTypes
        )
        import yt_dlp
        print("✅ Зависимости успешно установлены")
    except ImportError as exc2:
        print(f"❌ Ошибка импорта после установки: {exc2}")
        sys.exit(1)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ==================== USER DATA STORAGE ====================
user_data = {}
charts_cache = {}

def load_data():
    global user_data, charts_cache
    if DATA_FILE.exists():
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                user_data = json.load(f)
        except Exception as e:
            logger.warning(f"Не удалось загрузить {DATA_FILE}: {e}")
            user_data = {}
    else:
        user_data = {}

    if CHARTS_FILE.exists():
        try:
            with open(CHARTS_FILE, 'r', encoding='utf-8') as f:
                charts_cache = json.load(f)
        except Exception as e:
            logger.warning(f"Не удалось загрузить {CHARTS_FILE}: {e}")
            charts_cache = {}
    else:
        charts_cache = {}

def save_data():
    try:
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(user_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения данных: {e}")

def save_charts_cache():
    try:
        with open(CHARTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(charts_cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения кэша чартов: {e}")

load_data()

# ==================== АДМИН-ФУНКЦИИ ====================

def is_admin(user_id: str) -> bool:
    return str(user_id) in ADMIN_IDS

async def require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if not is_admin(user_id):
        await update.message.reply_text("❌ Команда не найдена")
        return False
    return True

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return

    user_count = len([k for k in user_data.keys() if not k.startswith('_')])
    total_downloads = sum(stats.get('downloads', 0) for stats in user_data.get('_user_stats', {}).values())
    total_searches = sum(stats.get('searches', 0) for stats in user_data.get('_user_stats', {}).values())

    text = f"""📊 <b>Админ статистика</b>

👥 Пользователей: {user_count}
📥 Всего скачиваний: {total_downloads}
🔍 Всего поисков: {total_searches}
💾 Размер user_data: {len(str(user_data))} символов
📈 Кэш чартов: {len(charts_cache.get('data', {}))} запросов
🔧 Админов: {len(ADMIN_IDS)}"""

    await update.message.reply_text(text, parse_mode='HTML')

async def admin_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return

    cleared_users = 0
    current_time = datetime.now()

    for user_id in list(user_data.keys()):
        if user_id.startswith('_') or user_id in ADMIN_IDS:
            continue

        user_stats = user_data.get('_user_stats', {}).get(user_id, {})
        last_search = user_stats.get('last_search')

        if last_search:
            try:
                last_active = datetime.strptime(last_search, '%d.%m.%Y %H:%M')
                if (current_time - last_active).days > 30:
                    del user_data[user_id]
                    if user_id in user_data.get('_user_stats', {}):
                        del user_data['_user_stats'][user_id]
                    cleared_users += 1
            except ValueError:
                del user_data[user_id]
                cleared_users += 1
        else:
            del user_data[user_id]
            cleared_users += 1

    save_data()

    await update.message.reply_text(
        f"✅ Очистка завершена!\n"
        f"🗑 Удалено неактивных пользователей: {cleared_users}\n"
        f"👥 Осталось пользователей: {len([k for k in user_data.keys() if not k.startswith('_')])}"
    )

async def admin_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return

    try:
        user_data_size = os.path.getsize('user_data.json') if os.path.exists('user_data.json') else 0
        charts_cache_size = os.path.getsize('charts_cache.json') if os.path.exists('charts_cache.json') else 0

        text = f"""📁 <b>Информация о файлах</b>

user_data.json: {user_data_size / 1024:.1f} KB
charts_cache.json: {charts_cache_size / 1024:.1f} KB
Всего пользователей: {len(user_data)}"""

        await update.message.reply_text(text, parse_mode='HTML')

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")

async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return

    text = """🔧 <b>Админ команды</b>

/admin_stats - 📊 Статистика бота
/admin_cleanup - 🗑 Очистка неактивных пользователей  
/admin_files - 📁 Информация о файлах
/admin_help - ❓ Эта справка"""

    await update.message.reply_text(text, parse_mode='HTML')

def setup_admin_commands(app):
    if ADMIN_IDS:
        app.add_handler(CommandHandler('admin_stats', admin_stats))
        app.add_handler(CommandHandler('admin_cleanup', admin_cleanup))
        app.add_handler(CommandHandler('admin_files', admin_files))
        app.add_handler(CommandHandler('admin_help', admin_help))
        print("✅ Админ-команды зарегистрированы")
    else:
        print("⚠️  Админ-команды отключены (ADMIN_IDS не настроен)")

# ==================== УЛУЧШЕННАЯ СИСТЕМА КЭШИРОВАНИЯ ====================

class SearchCache:
    def __init__(self):
        self.cache = {}
        self.max_size = 100
        
    def get(self, query: str):
        if query in self.cache:
            data, timestamp = self.cache[query]
            if datetime.now().timestamp() - timestamp < SEARCH_CACHE_TTL:
                return data
            else:
                del self.cache[query]
        return None
        
    def set(self, query: str, data):
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]
        self.cache[query] = (data, datetime.now().timestamp())

# ==================== ЧЕРНЫЙ СПИСОК ТРЕКОВ ====================

class TrackBlacklist:
    def __init__(self):
        self.blacklist = set()
        self.max_size = 1000
        
    def add(self, url: str):
        if len(self.blacklist) >= self.max_size:
            self.blacklist.pop()
        self.blacklist.add(url)
        
    def contains(self, url: str) -> bool:
        return url in self.blacklist

# ==================== ОСНОВНОЙ КЛАСС БОТА ====================

class StableMusicBot:
    def __init__(self):
        self.user_stats = user_data.get('_user_stats', {})
        self.track_info_cache = {}
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.search_semaphore = asyncio.Semaphore(5)
        self.search_cache = SearchCache()
        self.track_blacklist = TrackBlacklist()
        
        logger.info('✅ Бот инициализирован')

    async def preload_popular_queries(self):
        """Фоновая предзагрузка популярных запросов"""
        await asyncio.sleep(10)  # Ждем запуск бота
        logger.info("🔄 Начинаю предзагрузку популярных запросов...")
        
        for query in POPULAR_SEARCHES[:10] + RANDOM_SEARCHES[:20]:
            try:
                results = await self.search_soundcloud(query)
                if results:
                    POPULAR_QUERIES_CACHE[query] = {
                        'results': results[:5],
                        'timestamp': datetime.now().timestamp()
                    }
                await asyncio.sleep(1)  # Задержка между запросами
            except Exception as e:
                logger.warning(f"Ошибка предзагрузки {query}: {e}")
        
        logger.info(f"✅ Предзагружено {len(POPULAR_QUERIES_CACHE)} популярных запросов")

    def ensure_user(self, user_id: str):
        if str(user_id) not in user_data:
            user_data[str(user_id)] = {
                'filters': {'duration': 'no_filter', 'music_only': False},
                'search_results': [],
                'search_query': '',
                'current_page': 0,
                'total_pages': 0,
                'favorites': [],
                'search_history': [],
                'download_history': [],
                'download_queue': [],
                'random_track_result': [],
                'achievements': {},
                'preferences': {
                    'favorite_genres': [],
                    'disliked_genres': []
                }
            }
        if '_user_stats' not in user_data:
            user_data['_user_stats'] = {}
        if str(user_id) not in user_data['_user_stats']:
            user_data['_user_stats'][str(user_id)] = {
                'searches': 0,
                'downloads': 0,
                'first_seen': datetime.now().strftime('%d.%m.%Y %H:%M'),
                'last_search': None,
            }

    @staticmethod
    def clean_title(title: str) -> str:
        if not title:
            return 'Неизвестный трек'
        try:
            title = title.encode('utf-8').decode('utf-8')
        except:
            pass
        title = re.sub(r".*?|.*?", '', title)
        tags = ['official video', 'official music video', 'lyric video', 'hd', '4k',
                '1080p', '720p', 'official audio', 'audio']
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

    def validate_track_fast(self, track_info: dict) -> bool:
        title = (track_info.get('title') or '').lower()
        url = (track_info.get('webpage_url') or track_info.get('url') or '').lower()
        duration = track_info.get('duration', 0)
        
        if not title or not url:
            return False
            
        if duration <= 10:
            return False
            
        # Фильтрация треков, которые могут быть слишком большими
        if duration > 2700:  # 45 минут - потенциально очень большой файл
            return False
            
        problematic_keywords = {
            'unavailable', 'deleted', 'private', 'preview', 'sample',
            'clip', 'excerpt', 'snippet', 'teaser', 'demo', 'bootleg',
            'live@', 'concert', 'performance', 'recorded', 'rip'
        }
        
        if any(keyword in title for keyword in problematic_keywords):
            return False
            
        url_blacklist = ['/unavailable', '/deleted', 'private', 'preview']
        if any(pattern in url for pattern in url_blacklist):
            return False
            
        return True

    def apply_user_filters(self, tracks: list, user_id: str) -> list:
        if not tracks:
            return []
            
        user_entry = user_data.get(str(user_id), {})
        if not user_entry:
            return tracks
            
        filters = user_entry.get('filters', {})
        duration_filter = filters.get('duration', 'no_filter')
        
        filtered_tracks = []
        
        for track in tracks:
            duration = track.get('duration', 0)
            if duration_filter != 'no_filter':
                if duration_filter == 'up_to_5min' and duration > 300:
                    continue
                elif duration_filter == 'up_to_10min' and duration > 600:
                    continue
                elif duration_filter == 'up_to_20min' and duration > 1200:
                    continue
            
            filtered_tracks.append(track)
        
        return filtered_tracks

    async def check_file_size_before_download(self, url: str, track: dict) -> tuple:
        try:
            with yt_dlp.YoutubeDL(FAST_INFO_OPTS) as ydl:
                info = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: ydl.extract_info(url, download=False)
                )

                file_size = 0
                if info and 'filesize' in info and info['filesize']:
                    file_size = info['filesize'] / (1024 * 1024)
                elif info and 'filesize_approx' in info and info['filesize_approx']:
                    file_size = info['filesize_approx'] / (1024 * 1024)

                # Жесткое ограничение для бесплатного Railway
                can_download = file_size <= MAX_FILE_SIZE_MB if file_size > 0 else True

                return file_size, can_download

        except Exception as e:
            logger.warning(f"Не удалось получить размер файла: {e}")
            return 0, True

    def _get_dynamic_timeout(self, track: dict) -> int:
        duration = track.get('duration', 0)
        if duration < 180:
            return DYNAMIC_TIMEOUTS['short_track']
        elif duration < 600:
            return DYNAMIC_TIMEOUTS['medium_track']
        elif duration < 1800:
            return DYNAMIC_TIMEOUTS['long_track']
        else:
            return DYNAMIC_TIMEOUTS['very_long_track']

    async def _handle_large_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, file_size: float):
        title = track.get('title', 'Неизвестный трек')
        artist = track.get('artist', 'Неизвестный исполнитель')
        
        text = f"📦 <b>Крупный файл</b>\n\n"
        text += f"🎵 <b>{title}</b>\n"
        text += f"🎤 {artist}\n"
        text += f"💾 Размер: {file_size:.1f} MB\n\n"
        text += f"⏬ Начинаем скачивание..."

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                text, 
                parse_mode='HTML'
            )
        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                parse_mode='HTML'
            )

    async def _find_audio_file(self, tmpdir: str) -> str:
        try:
            if not os.path.exists(tmpdir):
                return None
                
            files = os.listdir(tmpdir)
            if not files:
                return None
                
            for file in files:
                filepath = os.path.join(tmpdir, file)
                
                if os.path.getsize(filepath) < 10 * 1024:
                    continue
                    
                ext = os.path.splitext(file)[1].lower()
                if ext in ['.mp3', '.m4a', '.ogg', '.wav', '.flac', '.aac']:
                    return file
                    
            if files:
                largest_file = max(files, key=lambda f: os.path.getsize(os.path.join(tmpdir, f)))
                if os.path.getsize(os.path.join(tmpdir, largest_file)) > 10 * 1024:
                    return largest_file
                    
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска файлов: {e}")
            return None

    async def _send_audio_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                             fpath: str, track: dict, actual_size_mb: float) -> bool:
        try:
            # Проверка окончательного размера файла
            if actual_size_mb > MAX_FILE_SIZE_MB:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ <b>Файл слишком большой</b>\n\n"
                         f"🎵 {track.get('title', 'Неизвестный трек')}\n"
                         f"💾 Размер: {actual_size_mb:.1f} MB\n\n"
                         f"📏 Максимальный размер: {MAX_FILE_SIZE_MB} MB\n"
                         f"🔧 Попробуйте найти другую версию трека",
                    parse_mode='HTML'
                )
                return False
            
            with open(fpath, 'rb') as f:
                await context.bot.send_audio(
                    chat_id=update.effective_chat.id,
                    audio=f,
                    title=(track.get('title') or 'Неизвестный трек')[:64],
                    performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                    caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n🎤 {track.get('artist', 'Неизвестный исполнитель')}\n⏱️ {self.format_duration(track.get('duration'))}\n💾 {actual_size_mb:.1f} MB",
                    parse_mode='HTML',
                )
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки файла: {e}")
            return False

    async def _cleanup_temp_dir(self, tmpdir: str):
        max_retries = 2
        for attempt in range(max_retries):
            try:
                if os.path.exists(tmpdir):
                    shutil.rmtree(tmpdir, ignore_errors=True)
                    logger.info(f"✅ Временные файлы очищены (попытка {attempt + 1})")
                    break
                else:
                    break
            except Exception as e:
                logger.warning(f"Не удалось очистить временную директорию (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)

    async def _pre_check_track(self, url: str, track: dict) -> bool:
        try:
            # Предварительная проверка размера
            file_size_mb, can_download = await self.check_file_size_before_download(url, track)
            if not can_download:
                logger.info(f"🚫 Файл слишком большой: {file_size_mb:.1f} MB")
                return False
                
            with yt_dlp.YoutubeDL({
                'quiet': True,
                'no_warnings': True,
                'simulate': True,
                'skip_download': True,
                'socket_timeout': 10,
            }) as ydl:
                info = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: ydl.extract_info(url, download=False)
                )
                
                if not info:
                    return False
                    
                formats = info.get('formats', [])
                if not formats:
                    return False
                    
                audio_formats = [f for f in formats if f.get('vcodec') == 'none']
                if not audio_formats:
                    return False
                    
                return True
                
        except Exception as e:
            logger.warning(f"Трек не прошел предварительную проверку: {e}")
            return False

    async def download_and_send_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, status_message=None) -> bool:
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        if self.track_blacklist.contains(url):
            logger.info(f"🚫 Трек в черном списке: {track.get('title')}")
            if status_message:
                await status_message.edit_text(f"🚫 Этот трек временно недоступен\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
            return False

        # Предварительная проверка размера
        file_size_mb, can_download = await self.check_file_size_before_download(url, track)
        if not can_download:
            logger.info(f"🚫 Файл слишком большой для скачивания: {file_size_mb:.1f} MB")
            if status_message:
                await status_message.edit_text(
                    f"❌ Файл слишком большой ({file_size_mb:.1f} MB)\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')[:30]}\n\n"
                    f"📏 Максимальный размер: {MAX_FILE_SIZE_MB} MB\n"
                    f"🔧 Попробуйте найти другую версию"
                )
            return False

        if not await self._pre_check_track(url, track):
            logger.info(f"🚫 Пропускаем проблемный трек: {track.get('title')}")
            if status_message:
                await status_message.edit_text(f"🚫 Этот трек временно недоступен\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
            return False

        try:
            # Обязательное уведомление о начале скачивания
            if not status_message:
                if hasattr(update, 'callback_query') and update.callback_query:
                    status_message = await update.callback_query.message.reply_text(f"⬇️ Скачиваем...\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
                else:
                    status_message = await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"⬇️ Скачиваем...\n🎵 {track.get('title', 'Неизвестный трек')[:30]}"
                    )
            else:
                await status_message.edit_text(f"⬇️ Скачиваем...\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")

            if file_size_mb > 25:
                return await self.download_large_track(update, context, track, status_message)
            else:
                return await self.download_fast_track(update, context, track, status_message)
                
        except asyncio.TimeoutError:
            logger.error(f"Таймаут скачивания трека: {track.get('title', 'Unknown')}")
            if status_message:
                await status_message.edit_text(f"❌ Таймаут скачивания\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
            return False
        except Exception as e:
            logger.exception(f'Критическая ошибка скачивания трека: {e}')
            if status_message:
                await status_message.edit_text(f"❌ Ошибка скачивания\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
            return False

    async def download_fast_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, status_message=None) -> bool:
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
            ydl_opts = FAST_DOWNLOAD_OPTS.copy()
            ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).80s.%(ext)s')

            def download_track():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        result = ydl.extract_info(url, download=True)
                        files = os.listdir(tmpdir)
                        return result if files else None
                except Exception as e:
                    logger.error(f"Ошибка скачивания {url}: {e}")
                    return None

            # Быстрый ретрай при таймауте
            try:
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, download_track),
                    timeout=90
                )
            except asyncio.TimeoutError:
                logger.info(f"🔄 Быстрый ретрай для: {track.get('title')}")
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, download_track),
                    timeout=60
                )

            files = os.listdir(tmpdir)
            if not files:
                logger.error(f"❌ Файлы не были скачаны для: {track.get('title')}")
                return False

            audio_file = None
            for file in files:
                filepath = os.path.join(tmpdir, file)
                if os.path.getsize(filepath) > 10 * 1024:
                    audio_file = file
                    break
            
            if not audio_file:
                logger.error(f"❌ Все файлы слишком маленькие: {files}")
                return False

            fpath = os.path.join(tmpdir, audio_file)
            actual_size_mb = os.path.getsize(fpath) / (1024 * 1024)

            # Финальная проверка размера
            if actual_size_mb > MAX_FILE_SIZE_MB:
                if status_message:
                    await status_message.edit_text(
                        f"❌ Файл слишком большой ({actual_size_mb:.1f} MB)\n"
                        f"🎵 {track.get('title', 'Неизвестный трек')[:30]}\n\n"
                        f"📏 Максимальный размер: {MAX_FILE_SIZE_MB} MB"
                    )
                return False

            if status_message:
                await status_message.edit_text(f"📤 Отправляем...\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")

            success = await self._send_audio_file(update, context, fpath, track, actual_size_mb)
            
            if success:
                if status_message:
                    await status_message.edit_text(f"✅ Готово!\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
                return True
            
            return False

        except asyncio.TimeoutError:
            logger.error(f"Таймаут при скачивании: {track.get('title', 'Unknown')}")
            return await self.download_large_track(update, context, track, status_message)
        except Exception as e:
            logger.exception(f'Ошибка быстрого скачивания: {e}')
            self.track_blacklist.add(url)
            return False
        finally:
            await self._cleanup_temp_dir(tmpdir)

    async def download_large_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, status_message=None) -> bool:
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
            ydl_opts = LARGE_FILE_OPTS.copy()
            ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).80s.%(ext)s')

            def download_track():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        result = ydl.extract_info(url, download=True)
                        files = os.listdir(tmpdir)
                        return result if files else None
                except Exception as e:
                    logger.error(f"Ошибка скачивания {url}: {e}")
                    return None

            info = await asyncio.wait_for(
                loop.run_in_executor(None, download_track),
                timeout=240
            )

            files = os.listdir(tmpdir)
            if not files:
                logger.error(f"❌ Файлы не были скачаны для: {track.get('title')}")
                return False

            audio_file = None
            for file in files:
                filepath = os.path.join(tmpdir, file)
                if os.path.getsize(filepath) > 10 * 1024:
                    audio_file = file
                    break
            
            if not audio_file:
                logger.error(f"❌ Все файлы слишком маленькие: {files}")
                return False

            fpath = os.path.join(tmpdir, audio_file)
            actual_size_mb = os.path.getsize(fpath) / (1024 * 1024)

            # Финальная проверка размера
            if actual_size_mb > MAX_FILE_SIZE_MB:
                if status_message:
                    await status_message.edit_text(
                        f"❌ Файл слишком большой ({actual_size_mb:.1f} MB)\n"
                        f"🎵 {track.get('title', 'Неизвестный трек')[:30]}\n\n"
                        f"📏 Максимальный размер: {MAX_FILE_SIZE_MB} MB"
                    )
                return False

            if status_message:
                await status_message.edit_text(f"📤 Отправляем...\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")

            success = await self._send_audio_file(update, context, fpath, track, actual_size_mb)
            
            if success:
                if status_message:
                    await status_message.edit_text(f"✅ Готово!\n🎵 {track.get('title', 'Неизвестный трек')[:30]}")
                return True
            
            return False

        except asyncio.TimeoutError:
            logger.error(f"Таймаут при скачивании большого файла: {track.get('title', 'Unknown')}")
            return False
        except Exception as e:
            logger.exception(f'Ошибка скачивания большого файла: {e}')
            self.track_blacklist.add(url)
            return False
        finally:
            await self._cleanup_temp_dir(tmpdir)

    # ==================== УСКОРЕННЫЙ ПОИСК НА SOUNDCLOUD ====================

    async def search_soundcloud(self, query: str, album_only: bool = False, user_id: str = None):
        # Проверяем кэш популярных запросов
        if query in POPULAR_QUERIES_CACHE:
            cache_data = POPULAR_QUERIES_CACHE[query]
            if datetime.now().timestamp() - cache_data['timestamp'] < POPULAR_CACHE_TTL:
                logger.info(f"✅ Используем предзагруженный кэш для: '{query}'")
                results = cache_data['results']
                if user_id:
                    results = self.apply_user_filters(results, user_id)
                return results

        # Проверяем обычный кэш
        cache_key = f"{query}_{user_id}"
        cached_results = self.search_cache.get(cache_key)
        if cached_results:
            logger.info(f"✅ Используем кэш для: '{query}'")
            return cached_results

        async with self.search_semaphore:
            ydl_opts = {
                'format': 'bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'ignoreerrors': True,
                'noplaylist': True,
                'socket_timeout': 12,  # Увеличили таймаут
            }

            results = []
            try:
                def perform_search():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.extract_info(f"scsearch30:{query}", download=False)  # Вернули 30 результатов

                loop = asyncio.get_event_loop()
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, perform_search),
                    timeout=SEARCH_TIMEOUT
                )

                if not info:
                    return results

                entries = info.get('entries', [])
                if not entries and info.get('_type') != 'playlist':
                    entries = [info]

                for entry in entries:
                    if not entry:
                        continue

                    if not self.validate_track_fast(entry):
                        continue

                    title = self.clean_title(entry.get('title') or '')
                    webpage_url = entry.get('webpage_url') or entry.get('url') or ''
                    duration = entry.get('duration') or 0
                    artist = entry.get('uploader') or entry.get('uploader_id') or 'Неизвестно'
                    thumbnail = entry.get('thumbnail')

                    if not title:
                        continue

                    results.append({
                        'title': title,
                        'webpage_url': webpage_url,
                        'duration': duration,
                        'artist': artist,
                        'source': 'track',
                        'thumbnail': thumbnail
                    })

                if user_id:
                    results = self.apply_user_filters(results, user_id)

                # Сохраняем в кэш
                self.search_cache.set(cache_key, results)

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут поиска для запроса: {query}")
                return []
            except Exception as e:
                logger.warning(f'Ошибка поиска SoundCloud: {e}')
                return []

            logger.info(f"✅ SoundCloud: {len(results)} отфильтрованных результатов для: '{query}'")
            return results

    # ==================== ОСНОВНЫЕ КОМАНДЫ ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        await self.show_main_menu(update, context)
        save_data()

    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text('🎵 Введите название песни или исполнителя:')

    async def charts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_charts(update, context)

    async def mood_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_mood_playlists(update, context)

    async def recommendations_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.show_recommendations(update, context)

    async def random_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        random_search = random.choice(RANDOM_SEARCHES)

        if update.callback_query:
            try:
                status_msg = await update.callback_query.message.reply_text(
                    f"🔍 <b>Ищу случайный трек</b>\n\n📝 Запрос: <code>{random_search}</code>",
                    parse_mode='HTML'
                )
            except:
                return
        else:
            status_msg = await update.message.reply_text(
                f"🔍 <b>Ищу случайный трек</b>\n\n📝 Запрос: <code>{random_search}</code>",
                parse_mode='HTML'
            )

        try:
            results = await self.search_soundcloud(random_search, user_id=str(user.id))
            if not results:
                await status_msg.edit_text(
                    "❌ <b>Не удалось найти случайный трек</b>\n\n"
                    "Попробуйте еще раз или выберите другой способ поиска",
                    parse_mode='HTML'
                )
                return

            random_track = random.choice(results)
            
            await status_msg.edit_text(
                f"✅ <b>Случайный трек найден!</b>\n\n"
                f"🎵 Трек: <b>{random_track.get('title', 'Неизвестный трек')}</b>\n"
                f"🎤 Исполнитель: {random_track.get('artist', 'Неизвестный исполнитель')}\n"
                f"⏱️ Длительность: {self.format_duration(random_track.get('duration'))}\n\n"
                f"⏬ <b>Начинаю скачивание...</b>",
                parse_mode='HTML'
            )

            success = await self.download_and_send_track(update, context, random_track, status_msg)

            if success:
                stats = user_data.get('_user_stats', {}).get(str(user.id), {})
                stats['downloads'] = stats.get('downloads', 0) + 1
                stats['searches'] = stats.get('searches', 0) + 1
                save_data()

                user_entry = user_data[str(user.id)]
                download_history = user_entry.get('download_history', [])
                download_history.append(random_track)
                user_entry['download_history'] = download_history[-50:]
                save_data()

                keyboard = [
                    [InlineKeyboardButton('🎲 Еще случайный трек', callback_data='random_track')],
                    [InlineKeyboardButton('🎯 Рекомендации', callback_data='show_recommendations')],
                    [InlineKeyboardButton('🔍 Новый поиск', callback_data='start_search')],
                ]

                await status_msg.edit_text(
                    "✅ <b>Случайный трек успешно скачан!</b>\n\n"
                    "Что хотите сделать дальше?",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML'
                )

        except Exception as e:
            logger.exception(f'Ошибка при поиске случайного трека: {e}')
            
            keyboard = [
                [InlineKeyboardButton('🎲 Попробовать снова', callback_data='random_track')],
                [InlineKeyboardButton('🔍 Новый поиск', callback_data='start_search')],
            ]

            await status_msg.edit_text(
                "❌ <b>Произошла ошибка при поиске случайного трека</b>\n\n"
                "Попробуйте еще раз или выберите другой способ поиска",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        filters = user_data[str(user.id)]['filters']
        current_duration = DURATION_FILTERS.get(filters.get('duration', 'no_filter'), 'Без фильтра')
        music_only = "✅ ВКЛ" if filters.get('music_only') else "❌ ВЫКЛ"

        text = f"""⚙️ <b>Настройки фильтров</b>

⏱️ <b>Фильтр по длительности:</b> {current_duration}
🎵 <b>Только музыка:</b> {music_only}

Выберите настройку для изменения:"""

        keyboard = [
            [InlineKeyboardButton('⏱️ Фильтр по длительности', callback_data='duration_menu')],
            [InlineKeyboardButton(f'🎵 Только музыка: {music_only}', callback_data='toggle_music')],
            [InlineKeyboardButton('🔙 Назад в меню', callback_data='back_to_main')],
        ]

        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text.strip()
        user = update.effective_user
        self.ensure_user(user.id)
        
        if len(text) < 2:
            await update.message.reply_text('❌ Введите хотя бы 2 символа')
            return

        stats = user_data['_user_stats'][str(user.id)]
        stats['searches'] += 1
        stats['last_search'] = datetime.now().strftime('%d.%m.%Y %H:%M')

        user_entry = user_data[str(user.id)]
        history = user_entry.get('search_history', [])
        history = [text] + [h for h in history if h != text][:9]
        user_entry['search_history'] = history

        try:
            results = await self.search_soundcloud(text, user_id=str(user.id))
            if not results:
                await update.message.reply_text('❌ По вашему запросу ничего не найдено.')
                return

            user_entry['search_results'] = results
            user_entry['search_query'] = text
            user_entry['current_page'] = 0
            user_entry['total_pages'] = (len(results) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            save_data()

            await self.show_results_page(update, context, user.id, 0)
        except Exception as e:
            logger.exception('Ошибка при поиске')
            await update.message.reply_text('❌ Ошибка при поиске.')

    async def show_results_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, page: int):
        user_entry = user_data.get(str(user_id), {})
        results = user_entry.get('search_results', [])
        total_pages = user_entry.get('total_pages', 0)
        query = user_entry.get('search_query', '')

        if page < 0 or page >= max(1, total_pages):
            page = 0

        start = page * RESULTS_PER_PAGE
        end = min(start + RESULTS_PER_PAGE, len(results))

        text = f"🔍 <b>Результаты по запросу:</b> <code>{query}</code>\n"
        text += f"📄 Страница {page + 1} из {max(1, total_pages)}\n"
        text += f"🎵 Найдено: {len(results)} результатов\n\n"

        keyboard = []
        for idx in range(start, end):
            track = results[idx]
            title = track.get('title', 'Неизвестный трек')
            artist = track.get('artist', 'Неизвестный исполнитель')
            duration = self.format_duration(track.get('duration'))

            short_title = title if len(title) <= 30 else title[:27] + '...'
            short_artist = artist if len(artist) <= 18 else artist[:15] + '...'

            button_text = f"🎵 {idx + 1}. {short_title} • {short_artist} • {duration}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'download:{idx}:{page}')])

        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'page:{page-1}'))
        if total_pages > 1:
            nav_buttons.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='current_page'))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton('Вперед ➡️', callback_data=f'page:{page+1}'))
        if nav_buttons:
            keyboard.append(nav_buttons)

        keyboard.extend([
            [InlineKeyboardButton('🔍 Новый поиск', callback_data='new_search')],
            [InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track')],
            [InlineKeyboardButton('⚙️ Настройки', callback_data='settings')],
        ])

        try:
            if update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'Ошибка отображения страницы результатов: {e}')

        user_data[str(user_id)]['current_page'] = page
        save_data()

    async def download_by_index(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int, return_page: int = 0):
        query = update.callback_query
        user = update.effective_user

        user_entry = user_data.get(str(user.id), {})
        results = user_entry.get('search_results', [])
        if index < 0 or index >= len(results):
            await query.edit_message_text('❌ Трек не найден')
            return

        # Создаем статус-сообщение если его нет
        try:
            status_msg = await query.message.reply_text(f"⬇️ Скачиваем...\n🎵 {results[index].get('title', 'Неизвестный трек')[:30]}")
        except:
            status_msg = None

        track = results[index]
        success = await self.download_and_send_track(update, context, track, status_msg)
        
        if success:
            stats = user_data.get('_user_stats', {}).get(str(user.id), {})
            stats['downloads'] = stats.get('downloads', 0) + 1
            save_data()

            user_entry = user_data[str(user.id)]
            download_history = user_entry.get('download_history', [])
            download_history.append(track)
            user_entry['download_history'] = download_history[-50:]
            save_data()

            await self.show_results_page(update, context, user.id, return_page)

    # ==================== CALLBACK ОБРАБОТЧИКИ ====================

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = (query.data or '')
        user = update.effective_user
        self.ensure_user(user.id)

        try:
            await query.answer()
        except Exception as e:
            if "too old" in str(e) or "timeout" in str(e) or "invalid" in str(e):
                logger.warning(f"Игнорирован старый callback: {e}")
                return
            else:
                logger.warning(f"Ошибка при answer callback: {e}")

        try:
            if data == 'start_search' or data == 'new_search':
                await query.edit_message_text('🎵 Введите название песни или исполнителя:')
                return

            if data == 'random_track':
                await self.random_track(update, context)
                return

            if data == 'show_recommendations' or data == 'refresh_recommendations':
                await self.show_recommendations(update, context)
                return

            if data == 'show_charts' or data == 'refresh_charts':
                await self.show_charts(update, context)
                return

            if data == 'mood_playlists':
                await self.show_mood_playlists(update, context)
                return

            if data == 'settings':
                await self.show_settings(update, context)
                return

            if data == 'duration_menu':
                await self.show_duration_menu(update, context)
                return

            if data == 'back_to_main':
                await self.show_main_menu(update, context)
                return

            if data == 'toggle_music':
                await self.toggle_music_filter(update, context)
                return

            if data.startswith('playlist:'):
                playlist_id = data.split(':', 1)[1]
                await self.generate_playlist(update, context, playlist_id)
                return

            if data.startswith('charts_page:'):
                page = int(data.split(':', 1)[1])
                await self.show_charts_page(update, context, page)
                return

            if data.startswith('playlist_page:'):
                page = int(data.split(':', 1)[1])
                await self.show_playlist_page(update, context, page)
                return

            if data.startswith('rec_page:'):
                page = int(data.split(':', 1)[1])
                await self.show_recommendations_page(update, context, page)
                return

            if data.startswith('rec_download:'):
                idx = int(data.split(':', 1)[1])
                await self.download_from_recommendations(update, context, idx)
                return

            if data.startswith('chart_download:'):
                idx = int(data.split(':', 1)[1])
                await self.download_from_charts(update, context, idx)
                return

            if data.startswith('playlist_download:'):
                idx = int(data.split(':', 1)[1])
                await self.download_from_playlist(update, context, idx)
                return

            if data.startswith('set_duration:'):
                key = data.split(':', 1)[1]
                await self.set_duration_filter(update, context, key)
                return

            if data.startswith('page:'):
                page = int(data.split(':', 1)[1])
                await self.show_results_page(update, context, user.id, page)
                return

            if data.startswith('download:'):
                parts = data.split(':')
                if len(parts) >= 3:
                    idx = int(parts[1])
                    return_page = int(parts[2])
                    await self.download_by_index(update, context, idx, return_page)
                return

            await query.edit_message_text('❌ Неизвестная команда')

        except Exception as e:
            logger.exception('Ошибка обработки callback')
            try:
                await query.message.reply_text('❌ Произошла ошибка')
            except:
                pass

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        text = f"🏠 <b>Главное меню</b>\n\n"
        text += f"👋 Привет, {user.first_name}!\n\n"
        text += f"🎵 <b>Выберите действие:</b>"

        keyboard = [
            [
                InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track'),
                InlineKeyboardButton('🔍 Поиск музыки', callback_data='start_search')
            ],
            [
                InlineKeyboardButton('📊 Топ чарты', callback_data='show_charts'),
                InlineKeyboardButton('🎭 Настроение', callback_data='mood_playlists')
            ],
            [
                InlineKeyboardButton('🎯 Рекомендации', callback_data='show_recommendations'),
                InlineKeyboardButton('⚙️ Настройки', callback_data='settings')
            ]
        ]

        if update.callback_query:
            await update.callback_query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

    # ==================== ФИЛЬТРЫ ====================

    async def show_duration_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        current_filter = user_data[str(user.id)]['filters'].get('duration', 'no_filter')

        text = "⏱️ <b>Выберите фильтр по длительности:</b>"

        keyboard = []
        for key, value in DURATION_FILTERS.items():
            prefix = "✅ " if key == current_filter else "🔘 "
            keyboard.append([InlineKeyboardButton(f"{prefix}{value}", callback_data=f'set_duration:{key}')])

        keyboard.append([InlineKeyboardButton('🔙 Назад к настройкам', callback_data='settings')])

        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def set_duration_filter(self, update: Update, context: ContextTypes.DEFAULT_TYPE, key: str):
        user = update.effective_user
        self.ensure_user(user.id)

        user_data[str(user.id)]['filters']['duration'] = key
        save_data()

        filter_name = DURATION_FILTERS.get(key, 'Без фильтра')
        await update.callback_query.answer(f'Фильтр установлен: {filter_name}')
        await self.show_settings(update, context)

    async def toggle_music_filter(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        current = user_data[str(user.id)]['filters'].get('music_only', False)
        user_data[str(user.id)]['filters']['music_only'] = not current
        save_data()

        status = "ВКЛЮЧЕН" if not current else "ВЫКЛЮЧЕН"
        await update.callback_query.answer(f'Фильтр "Только музыка" {status}')
        await self.show_settings(update, context)

    # ==================== РЕКОМЕНДАЦИИ ====================

    async def get_recommendations(self, user_id: str, limit: int = 30) -> list:
        user_entry = user_data.get(str(user_id), {})
        download_history = user_entry.get('download_history', [])
        search_history = user_entry.get('search_history', [])

        if not download_history and not search_history:
            return await self.get_popular_recommendations(limit, user_id)

        user_genres = self.analyze_user_preferences_fast(user_id)

        recommendations = []

        for track in download_history[-10:]:
            if track not in recommendations:
                recommendations.append(track)

        popular = await self.get_popular_recommendations(limit // 2, user_id)
        recommendations.extend(popular)

        unique_recommendations = []
        seen_titles = set()
        for track in recommendations:
            if track.get('title') and track['title'] not in seen_titles:
                seen_titles.add(track['title'])
                unique_recommendations.append(track)

        filtered_recommendations = self.apply_user_filters(unique_recommendations, user_id)
        
        random.shuffle(filtered_recommendations)
        return filtered_recommendations[:limit]

    def analyze_user_preferences_fast(self, user_id: str) -> list:
        user_entry = user_data.get(str(user_id), {})
        download_history = user_entry.get('download_history', [])

        if not download_history:
            return []

        recent_titles = [track.get('title', '').lower() for track in download_history[-5:]]

        genres = []
        for title in recent_titles:
            if any(word in title for word in ['lofi', 'chill', 'study']):
                genres.append('lofi')
            elif any(word in title for word in ['focus', 'work', 'coding']):
                genres.append('focus')
            elif any(word in title for word in ['rock', 'metal']):
                genres.append('rock')
            elif any(word in title for word in ['jazz', 'blues']):
                genres.append('jazz')

        return list(set(genres))[:3]

    async def get_popular_recommendations(self, limit: int = 15, user_id: str = None):
        popular_tracks = []

        for query in POPULAR_SEARCHES[:3]:
            try:
                results = await self.search_soundcloud(query, user_id=user_id)
                if results:
                    popular_tracks.extend(results[:6])
            except Exception as e:
                logger.warning(f"Ошибка поиска популярных треков: {e}")

        random.shuffle(popular_tracks)
        return popular_tracks[:limit]

    async def show_recommendations(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        try:
            if update.callback_query:
                status_msg = await update.callback_query.message.reply_text("🎯 Загружаю рекомендации...")
            else:
                status_msg = await update.message.reply_text("🎯 Загружаю рекомендации...")
        except:
            return

        try:
            recommendations = await self.get_recommendations(user.id, 25)

            if not recommendations:
                await status_msg.edit_text(
                    "📝 Пока не могу предложить персонализированные рекомендации.\n\n"
                    "Скачайте несколько треков, чтобы я узнал ваши предпочтения!",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track')],
                        [InlineKeyboardButton('🔍 Начать поиск', callback_data='start_search')],
                        [InlineKeyboardButton('📊 Топ чарты', callback_data='show_charts')],
                    ])
                )
                return

            user_data[str(user.id)]['current_recommendations'] = recommendations
            user_data[str(user.id)]['recommendations_page'] = 0
            user_data[str(user.id)]['recommendations_total_pages'] = (len(recommendations) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            save_data()

            await self.show_recommendations_page(update, context, 0, status_msg)

        except Exception as e:
            logger.exception(f'Ошибка показа рекомендаций: {e}')
            await status_msg.edit_text(
                '❌ Ошибка загрузки рекомендаций',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🔄 Попробовать снова', callback_data='show_recommendations')],
                    [InlineKeyboardButton('🏠 В меню', callback_data='back_to_main')],
                ])
            )

    async def show_recommendations_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int, status_msg=None):
        user = update.effective_user
        self.ensure_user(user.id)

        recommendations = user_data[str(user.id)].get('current_recommendations', [])
        total_pages = user_data[str(user.id)].get('recommendations_total_pages', 0)

        if page < 0 or page >= max(1, total_pages):
            page = 0

        start = page * RESULTS_PER_PAGE
        end = min(start + RESULTS_PER_PAGE, len(recommendations))

        text = f"🎯 <b>Ваши рекомендации</b>\n"
        text += f"📄 Страница {page + 1} из {max(1, total_pages)}\n"
        text += f"🎵 Найдено: {len(recommendations)} треков\n\n"

        history_count = len(user_data[str(user.id)].get('download_history', []))
        if history_count > 0:
            text += f"📊 На основе {history_count} скачанных треков\n\n"

        keyboard = []
        for idx in range(start, end):
            track = recommendations[idx]

            title = track.get('title', 'Неизвестный трек')
            artist = track.get('artist', 'Неизвестный исполнитель')
            duration = self.format_duration(track.get('duration'))

            short_title = title if len(title) <= 30 else title[:27] + '...'
            short_artist = artist if len(artist) <= 18 else artist[:15] + '...'

            button_text = f"🎵 {idx + 1}. {short_title} • {short_artist} • {duration}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'rec_download:{idx}')])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'rec_page:{page-1}'))
        if total_pages > 1:
            nav.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='rec_current_page'))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton('Вперед ➡️', callback_data=f'rec_page:{page+1}'))
        if nav:
            keyboard.append(nav)

        keyboard.extend([
            [InlineKeyboardButton('🔄 Обновить', callback_data='refresh_recommendations')],
            [
                InlineKeyboardButton('🎲 Случайный', callback_data='random_track'),
                InlineKeyboardButton('📊 Чарты', callback_data='show_charts')
            ],
            [
                InlineKeyboardButton('🔍 Поиск', callback_data='start_search'),
                InlineKeyboardButton('🏠 Меню', callback_data='back_to_main')
            ],
        ])

        try:
            if status_msg:
                await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'Ошибка отображения страницы рекомендаций: {e}')

        user_data[str(user.id)]['recommendations_page'] = page
        save_data()

    async def download_from_recommendations(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
        user = update.effective_user
        recommendations = user_data[str(user.id)].get('current_recommendations', [])

        if index < 0 or index >= len(recommendations):
            await update.callback_query.edit_message_text('❌ Трек не найден')
            return

        # Создаем статус-сообщение
        try:
            status_msg = await update.callback_query.message.reply_text(
                f"⬇️ Скачиваем...\n🎵 {recommendations[index].get('title', 'Неизвестный трек')[:30]}"
            )
        except:
            status_msg = None

        track = recommendations[index]
        await self.process_track_download_with_return(update, context, track, 'recommendations', 0, status_msg)

    # ==================== ЧАРТЫ ====================

    async def update_charts_cache(self, user_id: str = None):
        now = datetime.now()
        last_update = charts_cache.get('last_update')

        if last_update:
            last_update_date = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
            if now - last_update_date < timedelta(hours=6):
                return

        logger.info("🔄 Обновление кэша чартов...")

        charts_data = {}
        for query in POPULAR_SEARCHES[:4]:
            try:
                results = await self.search_soundcloud(query, user_id=user_id)
                if results:
                    charts_data[query] = results[:8]
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.warning(f"Ошибка обновления чарта для {query}: {e}")

        charts_cache['data'] = charts_data
        charts_cache['last_update'] = now.strftime('%Y-%m-%d %H:%M:%S')
        save_charts_cache()
        logger.info("✅ Кэш чартов обновлен")

    async def show_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        try:
            if update.callback_query:
                status_msg = await update.callback_query.message.reply_text("📊 Загружаю популярные треки...")
            else:
                status_msg = await update.message.reply_text("📊 Загружаю популярные треки...")
        except:
            return

        try:
            await self.update_charts_cache(user_id=str(user.id))

            charts_data = charts_cache.get('data', {})

            if not charts_data:
                await status_msg.edit_text("❌ Чарты временно недоступны. Попробуйте позже.")
                return

            all_tracks = []
            for query, tracks in charts_data.items():
                all_tracks.extend(tracks)

            random.shuffle(all_tracks)
            top_tracks = all_tracks[:25]

            user_data[str(user.id)]['current_charts'] = top_tracks
            user_data[str(user.id)]['charts_page'] = 0
            user_data[str(user.id)]['charts_total_pages'] = (len(top_tracks) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            save_data()

            await self.show_charts_page(update, context, 0, status_msg)

        except Exception as e:
            logger.exception(f'Ошибка показа чартов: {e}')
            await status_msg.edit_text('❌ Ошибка загрузки чартов. Попробуйте позже.')

    async def show_charts_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int, status_msg=None):
        user = update.effective_user
        self.ensure_user(user.id)

        charts = user_data[str(user.id)].get('current_charts', [])
        total_pages = user_data[str(user.id)].get('charts_total_pages', 0)

        if page < 0 or page >= max(1, total_pages):
            page = 0

        start = page * RESULTS_PER_PAGE
        end = min(start + RESULTS_PER_PAGE, len(charts))

        text = f"📊 <b>Топ чарты</b>\n"
        text += f"📄 Страница {page + 1} из {max(1, total_pages)}\n"
        text += f"🎵 Найдено: {len(charts)} треков\n\n"

        keyboard = []
        for idx in range(start, end):
            track = charts[idx]

            title = track.get('title', 'Неизвестный трек')
            artist = track.get('artist', 'Неизвестный исполнитель')
            duration = self.format_duration(track.get('duration'))

            short_title = title if len(title) <= 30 else title[:27] + '...'
            short_artist = artist if len(artist) <= 18 else artist[:15] + '...'

            button_text = f"🎵 {idx + 1}. {short_title} • {short_artist} • {duration}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'chart_download:{idx}')])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'charts_page:{page-1}'))
        if total_pages > 1:
            nav.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='charts_current_page'))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton('Вперед ➡️', callback_data=f'charts_page:{page+1}'))
        if nav:
            keyboard.append(nav)

        keyboard.extend([
            [InlineKeyboardButton('🔄 Обновить чарты', callback_data='refresh_charts')],
            [InlineKeyboardButton('🎯 Рекомендации', callback_data='show_recommendations')],
            [InlineKeyboardButton('🔍 Новый поиск', callback_data='new_search')],
            [InlineKeyboardButton('🔙 В главное меню', callback_data='back_to_main')],
        ])

        try:
            if status_msg:
                await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'Ошибка отображения страницы чартов: {e}')

        user_data[str(user.id)]['charts_page'] = page
        save_data()

    async def download_from_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
        user = update.effective_user
        charts = user_data[str(user.id)].get('current_charts', [])
        current_page = user_data[str(user.id)].get('charts_page', 0)

        if index < 0 or index >= len(charts):
            await update.callback_query.edit_message_text('❌ Трек не найден')
            return

        # Создаем статус-сообщение
        try:
            status_msg = await update.callback_query.message.reply_text(
                f"⬇️ Скачиваем...\n🎵 {charts[index].get('title', 'Неизвестный трек')[:30]}"
            )
        except:
            status_msg = None

        track = charts[index]
        await self.process_track_download_with_return(update, context, track, 'charts', current_page, status_msg)

    # ==================== НАСТРОЕНИЕ (ПЛЕЙЛИСТЫ) ====================

    async def show_mood_playlists(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = "🎭 <b>Музыка по настроению</b>\n\n"
        text += "Готовые подборки для любого настроения:\n\n"

        keyboard = []
        for playlist_id, playlist in SMART_PLAYLISTS.items():
            button_text = f"{playlist['name']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'playlist:{playlist_id}')])

        keyboard.extend([
            [InlineKeyboardButton('🔍 Новый поиск', callback_data='start_search')],
            [InlineKeyboardButton('🔙 В главное меню', callback_data='back_to_main')],
        ])

        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def generate_playlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE, playlist_id: str):
        user = update.effective_user
        self.ensure_user(user.id)

        playlist = SMART_PLAYLISTS.get(playlist_id)
        if not playlist:
            if update.callback_query:
                await update.callback_query.message.reply_text("❌ Плейлист не найден")
            else:
                await update.message.reply_text("❌ Плейлист не найден")
            return

        try:
            if update.callback_query:
                status_msg = await update.callback_query.message.reply_text(f"🎵 Создаю плейлист: {playlist['name']}...")
            else:
                status_msg = await update.message.reply_text(f"🎵 Создаю плейлист: {playlist['name']}...")
        except:
            return

        try:
            all_tracks = []
            for query in playlist['queries'][:2]:
                try:
                    results = await self.search_soundcloud(query, user_id=str(user.id))
                    if results:
                        all_tracks.extend(results[:8])
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.warning(f"Ошибка поиска для плейлиста {query}: {e}")

            if not all_tracks:
                await status_msg.edit_text("❌ Не удалось найти треки для плейлиста. Попробуйте позже.")
                return

            random.shuffle(all_tracks)
            playlist_tracks = all_tracks[:25]

            user_data[str(user.id)]['current_playlist'] = {
                'tracks': playlist_tracks,
                'name': playlist['name'],
                'description': playlist['description']
            }
            user_data[str(user.id)]['playlist_page'] = 0
            user_data[str(user.id)]['playlist_total_pages'] = (len(playlist_tracks) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            save_data()

            await self.show_playlist_page(update, context, 0, status_msg)

        except Exception as e:
            logger.exception(f'Ошибка создания плейлиста: {e}')
            await status_msg.edit_text('❌ Ошибка создания плейлиста. Попробуйте позже.')

    async def show_playlist_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int, status_msg=None):
        user = update.effective_user
        self.ensure_user(user.id)

        playlist_data = user_data[str(user.id)].get('current_playlist', {})
        tracks = playlist_data.get('tracks', [])
        playlist_name = playlist_data.get('name', 'Плейлист')
        playlist_description = playlist_data.get('description', '')

        total_pages = user_data[str(user.id)].get('playlist_total_pages', 0)

        if page < 0 or page >= max(1, total_pages):
            page = 0

        start = page * RESULTS_PER_PAGE
        end = min(start + RESULTS_PER_PAGE, len(tracks))

        text = f"🎭 <b>{playlist_name}</b>\n"
        text += f"📄 Страница {page + 1} из {max(1, total_pages)}\n"
        text += f"🎵 Найдено: {len(tracks)} треков\n"
        text += f"💡 {playlist_description}\n\n"

        keyboard = []
        for idx in range(start, end):
            track = tracks[idx]

            title = track.get('title', 'Неизвестный трек')
            artist = track.get('artist', 'Неизвестный исполнитель')
            duration = self.format_duration(track.get('duration'))

            short_title = title if len(title) <= 30 else title[:27] + '...'
            short_artist = artist if len(artist) <= 18 else artist[:15] + '...'

            button_text = f"🎵 {idx + 1}. {short_title} • {short_artist} • {duration}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'playlist_download:{idx}')])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'playlist_page:{page-1}'))
        if total_pages > 1:
            nav.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='playlist_current_page'))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton('Вперед ➡️', callback_data=f'playlist_page:{page+1}'))
        if nav:
            keyboard.append(nav)

        keyboard.extend([
            [InlineKeyboardButton('🔄 Другое настроение', callback_data='mood_playlists')],
            [InlineKeyboardButton('🔍 Новый поиск', callback_data='new_search')],
            [InlineKeyboardButton('🔙 В главное меню', callback_data='back_to_main')],
        ])

        try:
            if status_msg:
                await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'Ошибка отображения страницы плейлиста: {e}')

        user_data[str(user.id)]['playlist_page'] = page
        save_data()

    async def download_from_playlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
        user = update.effective_user
        playlist = user_data[str(user.id)].get('current_playlist', {})
        tracks = playlist.get('tracks', [])
        current_page = user_data[str(user.id)].get('playlist_page', 0)

        if index < 0 or index >= len(tracks):
            await update.callback_query.edit_message_text('❌ Трек не найден')
            return

        # Создаем статус-сообщение
        try:
            status_msg = await update.callback_query.message.reply_text(
                f"⬇️ Скачиваем...\n🎵 {tracks[index].get('title', 'Неизвестный трек')[:30]}"
            )
        except:
            status_msg = None

        track = tracks[index]
        await self.process_track_download_with_return(update, context, track, 'playlist', current_page, status_msg)

    async def process_track_download_with_return(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, source: str, return_page: int = 0, status_message=None):
        query = update.callback_query
        user = update.effective_user

        success = await self.download_and_send_track(update, context, track, status_message)

        if success:
            stats = user_data.get('_user_stats', {}).get(str(user.id), {})
            stats['downloads'] = stats.get('downloads', 0) + 1
            save_data()

            user_entry = user_data[str(user.id)]
            download_history = user_entry.get('download_history', [])
            download_history.append(track)
            user_entry['download_history'] = download_history[-50:]
            save_data()

            if source == 'recommendations':
                await self.show_recommendations_page(update, context, 0)
            elif source == 'charts':
                await self.show_charts_page(update, context, return_page)
            elif source == 'playlist':
                await self.show_playlist_page(update, context, return_page)

    def run(self):
        print('🚀 Запуск ускоренного Music Bot для Railway...')

        app = Application.builder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler('start', self.start))
        app.add_handler(CommandHandler('search', self.search_command))
        app.add_handler(CommandHandler('charts', self.charts_command))
        app.add_handler(CommandHandler('random', self.random_track))
        app.add_handler(CommandHandler('mood', self.mood_command))
        app.add_handler(CommandHandler('recommendations', self.recommendations_command))
        app.add_handler(CommandHandler('settings', self.show_settings))

        setup_admin_commands(app)

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        app.add_handler(CallbackQueryHandler(self.handle_callback))

        async def set_commands(application):
            commands = [
                ('start', '🚀 Запустить бота'),
                ('search', '🔍 Поиск музыки'),
                ('charts', '📊 Топ чарты'),
                ('random', '🎲 Случайный трек'),
                ('mood', '🎭 Настроение'),
                ('recommendations', '🎯 Рекомендации'),
                ('settings', '⚙️ Настройки'),
            ]

            await application.bot.set_my_commands(commands)
            print('✅ Улучшенное меню с командами настроено!')

        app.post_init = set_commands

        print('✅ Ускоренный бот запущен! Оптимизированы поиск и скачивание.')
        app.run_polling()

if __name__ == '__main__':
    bot = StableMusicBot()
    bot.run()
