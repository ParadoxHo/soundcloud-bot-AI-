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
MAX_FILE_SIZE_MB = 50

MAX_CONCURRENT_DOWNLOADS = 3
DOWNLOAD_TIMEOUT = 180
SEARCH_TIMEOUT = 30

DYNAMIC_TIMEOUTS = {
    'short_track': 30,
    'medium_track': 60, 
    'long_track': 120,
    'search': 25
}

SIMPLE_DOWNLOAD_OPTS = {
    'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio[ext=ogg]/bestaudio[ext=wav]/bestaudio[ext=flac]/bestaudio/best',
    'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 3,
    'fragment_retries': 3,
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'nooverwrites': True,
    'noplaylist': True,
    'max_filesize': 45000000,
    'ignoreerrors': True,
    'ignore_no_formats_error': True,
    'socket_timeout': 30,
}

FAST_INFO_OPTS = {
    'quiet': True,
    'no_warnings': True,
    'simulate': True,
    'format': 'bestaudio/best',
    'skip_download': True,
    'noplaylist': True,
    'extract_flat': True,
    'socket_timeout': 15,
    'ignoreerrors': True,
}

DURATION_FILTERS = {
    'no_filter': 'Без фильтра',
    'up_to_5min': 'До 5 минут',
    'up_to_10min': 'До 10 минут', 
    'up_to_20min': 'До 20 минут',
}

SMART_PLAYLISTS = {
    'work_focus': {
        'name': '💼 Фокус и работа',
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
    }
}

RANDOM_SEARCHES = [
    'lo fi beats', 'chillhop', 'deep house', 'synthwave', 'indie rock',
    'electronic music', 'jazz lounge', 'ambient', 'study music',
    'focus music', 'relaxing music', 'instrumental', 'acoustic',
    'piano covers', 'guitar music', 'vocal trance', 'dubstep',
    'tropical house', 'future bass', 'retro wave', 'city pop',
    'latin music', 'reggeaton', 'k-pop', 'j-pop', 'classical piano',
    'orchestral', 'film scores', 'video game music', 'retro gaming',
    'chill beats', 'lounge music', 'smooth jazz', 'progressive house',
    'techno music', 'trance music', 'hip hop instrumental', 'rap beats'
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

# ==================== УЛУЧШЕННАЯ СИСТЕМА УВЕДОМЛЕНИЙ ====================

class NotificationManager:
    @staticmethod
    async def send_progress(update, context, stage: str, track=None, **kwargs):
        stages = {
            'searching': "🔍 Ищем треки...",
            'downloading': "⬇️ Скачиваем аудио...", 
            'processing': "🔄 Обрабатываем файл...",
            'sending': "📤 Отправляем в Telegram...",
            'success': "✅ Готово!",
            'error': "❌ Ошибка"
        }
        
        message = stages.get(stage, "⏳ Работаем...")
        if track and stage != 'searching':
            title = track.get('title', 'Неизвестный трек')[:30]
            message = f"{message}\n🎵 {title}"
            
        try:
            if hasattr(update, 'callback_query') and update.callback_query:
                await update.callback_query.edit_message_text(message)
            else:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id, 
                    text=message
                )
        except Exception as e:
            logger.warning(f"Ошибка уведомления: {e}")

# ==================== ОСНОВНОЙ КЛАСС БОТА ====================

class StableMusicBot:
    def __init__(self):
        self.user_stats = user_data.get('_user_stats', {})
        self.track_info_cache = {}
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.search_semaphore = asyncio.Semaphore(3)
        self.notifications = NotificationManager()
        logger.info('✅ Бот инициализирован')

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

                duration = track.get('duration', 0)
                if duration > 1800:
                    can_download = file_size < (MAX_FILE_SIZE_MB * 0.7)
                else:
                    can_download = file_size < MAX_FILE_SIZE_MB

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
        else:
            return DYNAMIC_TIMEOUTS['long_track']

    async def _handle_large_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, file_size: float):
        title = track.get('title', 'Неизвестный трек')
        artist = track.get('artist', 'Неизвестный исполнитель')
        
        text = f"📦 <b>Файл слишком большой</b>\n\n"
        text += f"🎵 <b>{title}</b>\n"
        text += f"🎤 {artist}\n"
        text += f"💾 Размер: {file_size:.1f} MB\n\n"
        text += f"⚠️ <b>Превышен лимит {MAX_FILE_SIZE_MB} MB</b>\n\n"
        text += f"🎧 Вы можете:\n• Прослушать онлайн\n• Найти более короткую версию"

        keyboard = [
            [InlineKeyboardButton('🎧 Слушать онлайн', url=track.get('webpage_url', ''))],
            [InlineKeyboardButton('🔍 Найти другую версию', callback_data=f'search_alt:{title}')],
            [InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track')],
        ]

        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                text, 
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

    async def _find_compatible_audio_file(self, tmpdir: str) -> str:
        telegram_audio_extensions = ['.mp3', '.m4a', '.ogg', '.wav', '.flac']
        
        for file in os.listdir(tmpdir):
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext in telegram_audio_extensions:
                logger.info(f"✅ Найден совместимый файл: {file}")
                return file
        
        logger.error(f"❌ Совместимые файлы не найдены в: {os.listdir(tmpdir)}")
        return None

    async def _send_audio_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                             fpath: str, track: dict, actual_size_mb: float) -> bool:
        try:
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
        max_retries = 3
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
                    await asyncio.sleep(1)

    async def download_and_send_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict) -> bool:
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        file_size_mb, can_download = await self.check_file_size_before_download(url, track)
        
        if not can_download:
            await self._handle_large_file(update, context, track, file_size_mb)
            return False

        async with self.download_semaphore:
            try:
                await self.notifications.send_progress(update, context, 'downloading', track)
                
                timeout = self._get_dynamic_timeout(track)
                
                return await asyncio.wait_for(
                    self.simple_download(update, context, track),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Таймаут скачивания трека: {track.get('title', 'Unknown')}")
                await self.notifications.send_progress(update, context, 'error', track)
                return False
            except Exception as e:
                logger.exception(f'Ошибка скачивания трека: {e}')
                await self.notifications.send_progress(update, context, 'error', track)
                return False

    async def simple_download(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict) -> bool:
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
            await self.notifications.send_progress(update, context, 'processing', track)
            
            ydl_opts = SIMPLE_DOWNLOAD_OPTS.copy()
            ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).100s.%(ext)s')

            def download_track():
                try:
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.extract_info(url, download=True)
                except Exception as e:
                    logger.error(f"Ошибка скачивания: {e}")
                    return None

            info = await asyncio.wait_for(
                loop.run_in_executor(None, download_track),
                timeout=DOWNLOAD_TIMEOUT - 30
            )

            if not info:
                return False

            audio_file = await self._find_compatible_audio_file(tmpdir)
            if not audio_file:
                return False

            fpath = os.path.join(tmpdir, audio_file)
            actual_size_mb = os.path.getsize(fpath) / (1024 * 1024)
            
            if actual_size_mb >= MAX_FILE_SIZE_MB:
                await self._handle_large_file(update, context, track, actual_size_mb)
                return False

            await self.notifications.send_progress(update, context, 'sending', track)

            success = await self._send_audio_file(update, context, fpath, track, actual_size_mb)
            
            if success:
                await self.notifications.send_progress(update, context, 'success', track)
                return True
            return False

        except asyncio.TimeoutError:
            logger.error(f"Таймаут при скачивании: {track.get('title', 'Unknown')}")
            return False
        except Exception as e:
            logger.exception(f'Ошибка скачивания: {e}')
            return False
        finally:
            await self._cleanup_temp_dir(tmpdir)

    # ==================== ОСНОВНЫЕ КОМАНДЫ ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        await self.show_main_menu(update, context)
        save_data()

    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /search"""
        await update.message.reply_text('🎵 Введите название песни или исполнителя:')

    async def random_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск и скачивание случайного трека"""
        user = update.effective_user
        self.ensure_user(user.id)

        random_search = random.choice(RANDOM_SEARCHES)

        if update.callback_query:
            try:
                status_msg = await update.callback_query.message.reply_text(
                    f"🔍 <b>Ищу случайный трек</b>\n\n📝 Запрос: <code>{random_search}</code>\n⏱️ Ожидайте ~10-20 секунд",
                    parse_mode='HTML'
                )
            except:
                return
        else:
            status_msg = await update.message.reply_text(
                f"🔍 <b>Ищу случайный трек</b>\n\n📝 Запрос: <code>{random_search}</code>\n⏱️ Ожидайте ~10-20 секунд",
                parse_mode='HTML'
            )

        try:
            results = await self.search_soundcloud(random_search, album_only=False)
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

            success = await self.download_and_send_track(update, context, random_track)

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
        """Показывает настройки"""
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
        """Обработчик текстовых сообщений"""
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
            await self.notifications.send_progress(update, context, 'searching')
        except:
            return

        try:
            results = await self.search_soundcloud(text)
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
        """Показывает страницу результатов поиска"""
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
        """Скачивание трека по индексу"""
        query = update.callback_query
        user = update.effective_user

        user_entry = user_data.get(str(user.id), {})
        results = user_entry.get('search_results', [])
        if index < 0 or index >= len(results):
            await query.edit_message_text('❌ Трек не найден')
            return

        track = results[index]
        success = await self.download_and_send_track(update, context, track)
        
        if success:
            stats = user_data.get('_user_stats', {}).get(str(user.id), {})
            stats['downloads'] = stats.get('downloads', 0) + 1
            save_data()

            user_entry = user_data[str(user.id)]
            download_history = user_entry.get('download_history', [])
            download_history.append(track)
            user_entry['download_history'] = download_history[-50:]
            save_data()

            # Возвращаемся к той же странице результатов
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

            if data == 'show_recommendations':
                await self.show_recommendations(update, context)
                return

            if data == 'show_charts':
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

            if data == 'cancel_operation':
                await self.handle_cancel(update, context)
                return

            await query.edit_message_text('❌ Неизвестная команда')

        except Exception as e:
            logger.exception('Ошибка обработки callback')
            try:
                await query.message.reply_text('❌ Произошла ошибка')
            except:
                pass

    async def handle_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка отмены операций"""
        query = update.callback_query
        await query.answer("Операция отменена")
        await self.show_main_menu(update, context)

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает главное меню"""
        user = update.effective_user
        
        text = f"🏠 <b>Главное меню</b>\n\n"
        text += f"👋 Привет, {user.first_name}!\n\n"
        text += f"🎵 <b>Выберите действие:</b>"

        keyboard = [
            [
                InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track'),
                InlineKeyboardButton('🔍 Поиск', callback_data='start_search')
            ],
            [
                InlineKeyboardButton('🎯 Рекомендации', callback_data='show_recommendations'),
                InlineKeyboardButton('📊 Топ чарты', callback_data='show_charts')
            ],
            [
                InlineKeyboardButton('🎭 Настроение', callback_data='mood_playlists'),
                InlineKeyboardButton('⚙️ Настройки', callback_data='settings')
            ],
            [InlineKeyboardButton('❌ Отменить', callback_data='cancel_operation')]
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

    # ==================== ДОПОЛНИТЕЛЬНЫЕ МЕТОДЫ ====================

    async def search_soundcloud(self, query: str, album_only: bool = False):
        """Поиск на SoundCloud"""
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

            results = []
            try:
                def perform_search():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.extract_info(f"scsearch30:{query}", download=False)

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

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут поиска для запроса: {query}")
                return []
            except Exception as e:
                logger.warning(f'Ошибка поиска SoundCloud: {e}')
                return []

            logger.info(f"✅ SoundCloud: {len(results)} результатов для: '{query}'")
            return results

    async def show_recommendations(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает рекомендации"""
        await update.callback_query.edit_message_text('🎯 Рекомендации временно недоступны.')

    async def show_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает чарты"""
        await update.callback_query.edit_message_text('📊 Чарты временно недоступны.')

    async def show_mood_playlists(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает плейлисты по настроению"""
        await update.callback_query.edit_message_text('🎭 Плейлисты по настроению временно недоступны.')

    async def show_duration_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает меню фильтров по длительности"""
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
        """Устанавливает фильтр по длительности"""
        user = update.effective_user
        self.ensure_user(user.id)

        user_data[str(user.id)]['filters']['duration'] = key
        save_data()

        filter_name = DURATION_FILTERS.get(key, 'Без фильтра')
        await update.callback_query.answer(f'Фильтр установлен: {filter_name}')
        await self.show_settings(update, context)

    async def toggle_music_filter(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Переключает фильтр 'Только музыка'"""
        user = update.effective_user
        self.ensure_user(user.id)

        current = user_data[str(user.id)]['filters'].get('music_only', False)
        user_data[str(user.id)]['filters']['music_only'] = not current
        save_data()

        status = "ВКЛЮЧЕН" if not current else "ВЫКЛЮЧЕН"
        await update.callback_query.answer(f'Фильтр "Только музыка" {status}')
        await self.show_settings(update, context)

    def run(self):
        print('🚀 Запуск улучшенного Music Bot...')

        app = Application.builder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler('start', self.start))
        app.add_handler(CommandHandler('search', self.search_command))
        app.add_handler(CommandHandler('random', self.random_track))
        app.add_handler(CommandHandler('settings', self.show_settings))

        setup_admin_commands(app)

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        app.add_handler(CallbackQueryHandler(self.handle_callback))
        
        app.add_handler(CallbackQueryHandler(self.handle_cancel, pattern='^cancel_operation$'))

        async def set_commands(application):
            commands = [
                ('start', '🚀 Запустить бота'),
                ('search', '🔍 Начать поиск'),
                ('random', '🎲 Случайный трек'),
                ('settings', '⚙️ Настройки фильтров'),
            ]

            await application.bot.set_my_commands(commands)
            print('✅ Улучшенное меню с командами настроено!')

        app.post_init = set_commands

        print('✅ Улучшенный бот запущен и готов к работе!')
        app.run_polling()

if __name__ == '__main__':
    bot = StableMusicBot()
    bot.run()
