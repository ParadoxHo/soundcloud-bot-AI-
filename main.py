# -*- coding: utf-8 -*-
import os
import sys
import json
import logging
import tempfile
import re
import random
import asyncio
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

# Очищаем и форматируем ADMIN_IDS
ADMIN_IDS = [id.strip() for id in ADMIN_IDS if id.strip()]

if not ADMIN_IDS:
    print("⚠️  Предупреждение: ADMIN_IDS не установлен. Админ-команды отключены.")
else:
    print(f"✅ Админы настроены: {ADMIN_IDS}")

RESULTS_PER_PAGE = 8
DATA_FILE = Path('user_data.json')
CHARTS_FILE = Path('charts_cache.json')
MAX_FILE_SIZE_MB = 50

# ОГРАНИЧЕНИЯ ДЛЯ СТАБИЛЬНОСТИ
MAX_CONCURRENT_DOWNLOADS = 1
DOWNLOAD_TIMEOUT = 180
SEARCH_TIMEOUT = 30

# ПРОСТЫЕ НАСТРОЙКИ СКАЧИВАНИЯ БЕЗ КОНВЕРТАЦИИ
SIMPLE_DOWNLOAD_OPTS = {
    'format': 'bestaudio[filesize<45M]/bestaudio/best',
    'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    
    # БЕЗ КОНВЕРТАЦИИ - скачиваем как есть
    'retries': 2,
    'fragment_retries': 2,
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

# БЫСТРЫЕ НАСТРОЙКИ ДЛЯ ПОЛУЧЕНИЯ ИНФОРМАЦИИ
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

# Умные плейлисты (шаблоны)
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

# Список для случайных треков
RANDOM_SEARCHES = [
    'lo fi beats', 'chillhop', 'deep house', 'synthwave', 'indie rock',
    'electronic music', 'jazz lounge', 'ambient', 'study music',
    'focus music', 'relaxing music', 'instrumental', 'acoustic',
    'piano covers', 'guitar music', 'vocal trance', 'dubstep',
    'tropical house', 'future bass', 'retro wave', 'city pop',
    'latin music', 'reggaeton', 'k-pop', 'j-pop', 'classical piano',
    'orchestral', 'film scores', 'video game music', 'retro gaming',
    'chill beats', 'lounge music', 'smooth jazz', 'progressive house',
    'techno music', 'trance music', 'hip hop instrumental', 'rap beats'
]

# Популярные запросы для чартов (кэш)
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
    """Проверяет, является ли пользователь админом"""
    return str(user_id) in ADMIN_IDS

async def require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Декоратор для проверки прав админа"""
    user_id = str(update.effective_user.id)
    if not is_admin(user_id):
        await update.message.reply_text("❌ Команда не найдена")
        return False
    return True

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика только для админа"""
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
    """Очистка кэша только для админа"""
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
    """Информация о файлах только для админа"""
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
    """Помощь по админ-командам"""
    if not await require_admin(update, context):
        return

    text = """🔧 <b>Админ команды</b>

/admin_stats - 📊 Статистика бота
/admin_cleanup - 🗑 Очистка неактивных пользователей  
/admin_files - 📁 Информация о файлах
/admin_help - ❓ Эта справка"""

    await update.message.reply_text(text, parse_mode='HTML')

def setup_admin_commands(app):
    """Регистрация админ-команд"""
    if ADMIN_IDS:
        app.add_handler(CommandHandler('admin_stats', admin_stats))
        app.add_handler(CommandHandler('admin_cleanup', admin_cleanup))
        app.add_handler(CommandHandler('admin_files', admin_files))
        app.add_handler(CommandHandler('admin_help', admin_help))
        print("✅ Админ-команды зарегистрированы")
    else:
        print("⚠️  Админ-команды отключены (ADMIN_IDS не настроен)")

# ==================== MAIN BOT CLASS ====================
class StableMusicBot:
    def __init__(self):
        self.user_stats = user_data.get('_user_stats', {})
        self.track_info_cache = {}
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.search_semaphore = asyncio.Semaphore(3)
        logger.info('✅ Бот инициализирован')

    def ensure_user(self, user_id: str):
        if str(user_id) not in user_data:
            user_data[str(user_id)] = {
                'filters': {'duration': 'no_filter', 'music_only': False, 'album_only': False},
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

    # ==================== ПРОСТОЙ МЕТОД СКАЧИВАНИЯ ====================

    async def download_and_send_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict) -> bool:
        """Упрощенный метод скачивания БЕЗ конвертации"""
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        async with self.download_semaphore:
            try:
                return await asyncio.wait_for(
                    self.simple_download(update, context, track),
                    timeout=DOWNLOAD_TIMEOUT
                )
            except asyncio.TimeoutError:
                logger.error(f"Таймаут скачивания трека: {track.get('title', 'Unknown')}")
                return False
            except Exception as e:
                logger.exception(f'Ошибка скачивания трека: {e}')
                return False

    async def simple_download(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict) -> bool:
        """ПРОСТОЕ скачивание в исходном формате"""
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        loop = asyncio.get_event_loop()
        
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                ydl_opts = SIMPLE_DOWNLOAD_OPTS.copy()
                ydl_opts['outtmpl'] = os.path.join(tmpdir, '%(title).100s.%(ext)s')

                def download_track():
                    try:
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            return ydl.extract_info(url, download=True)
                    except Exception as e:
                        logger.error(f"Ошибка в download_track: {e}")
                        return None

                info = await asyncio.wait_for(
                    loop.run_in_executor(None, download_track),
                    timeout=DOWNLOAD_TIMEOUT - 30
                )

                if not info:
                    logger.error("❌ Не удалось скачать трек")
                    return False

                # Ищем любой аудио файл
                audio_extensions = ['.mp3', '.m4a', '.webm', '.ogg', '.wav', '.mp4', '.flac', '.aac']
                audio_files = []
                
                for file in os.listdir(tmpdir):
                    file_ext = os.path.splitext(file)[1].lower()
                    if file_ext in audio_extensions:
                        audio_files.append(file)

                if not audio_files:
                    logger.error("❌ Аудио файл не найден после скачивания")
                    return False
                
                # Используем первый найденный файл
                audio_file = audio_files[0]
                fpath = os.path.join(tmpdir, audio_file)
                
                # Проверяем размер файла
                actual_size_mb = os.path.getsize(fpath) / (1024 * 1024)
                
                if actual_size_mb > MAX_FILE_SIZE_MB:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f'❌ Файл слишком большой для отправки в Telegram ({actual_size_mb:.1f} MB)'
                    )
                    return False

                # Отправляем файл как аудио
                with open(fpath, 'rb') as f:
                    await context.bot.send_audio(
                        chat_id=update.effective_chat.id,
                        audio=f,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n🎤 {track.get('artist', 'Неизвестный исполнитель')}\n⏱️ {self.format_duration(track.get('duration'))}\n💾 {actual_size_mb:.1f} MB\n📦 <i>Исходный формат</i>",
                        parse_mode='HTML',
                    )
                
                logger.info(f"✅ Трек отправлен: {audio_file} ({actual_size_mb:.1f} MB)")
                return True

        except asyncio.TimeoutError:
            logger.error(f"Таймаут при скачивании: {track.get('title', 'Unknown')}")
            return False
        except Exception as e:
            logger.exception(f'Ошибка скачивания: {e}')
            return False

    # ==================== ОСТАЛЬНЫЕ МЕТОДЫ (без изменений) ====================

    async def get_file_size(self, url: str) -> float:
        """Получение размера файла"""
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

                return file_size

        except Exception as e:
            logger.warning(f"Не удалось получить размер файла: {e}")
            return 0

    async def send_streaming_option(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, file_size_mb: float) -> bool:
        try:
            title = track.get('title', 'Неизвестный трек')
            artist = track.get('artist', 'Неизвестный исполнитель')
            duration = self.format_duration(track.get('duration'))

            text = f"🎵 <b>{title}</b>\n🎤 {artist}\n⏱️ {duration}\n💾 {file_size_mb:.1f} MB\n\n"
            text += f"⚠️ <b>Файл слишком большой для скачивания в Telegram</b>\n"
            text += f"🎧 <i>Вы можете прослушать его онлайн</i>"

            keyboard = [
                [InlineKeyboardButton('🎧 Слушать онлайн', url=track.get('webpage_url', ''))],
                [InlineKeyboardButton('🔍 Новый поиск', callback_data='new_search')],
            ]

            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки опции стриминга: {e}")
            return False

    # ==================== РЕКОМЕНДАЦИИ ====================

    async def get_recommendations(self, user_id: str, limit: int = 6) -> list:
        """Получает рекомендации на основе истории пользователя"""
        user_entry = user_data.get(str(user_id), {})
        download_history = user_entry.get('download_history', [])
        search_history = user_entry.get('search_history', [])

        if not download_history and not search_history:
            return await self.get_popular_recommendations(limit)

        user_genres = self.analyze_user_preferences_fast(user_id)

        recommendations = []

        for track in download_history[-5:]:
            if track not in recommendations:
                recommendations.append(track)

        popular = await self.get_popular_recommendations(limit // 2)
        recommendations.extend(popular)

        unique_recommendations = []
        seen_titles = set()
        for track in recommendations:
            if track.get('title') and track['title'] not in seen_titles:
                seen_titles.add(track['title'])
                unique_recommendations.append(track)

        random.shuffle(unique_recommendations)
        return unique_recommendations[:limit]

    def analyze_user_preferences_fast(self, user_id: str) -> list:
        """Быстрый анализ предпочтений пользователя"""
        user_entry = user_data.get(str(user_id), {})
        download_history = user_entry.get('download_history', [])

        if not download_history:
            return []

        recent_titles = [track.get('title', '').lower() for track in download_history[-3:]]

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

        return list(set(genres))[:2]

    async def get_popular_recommendations(self, limit: int = 3) -> list:
        """Быстрые популярные рекомендации"""
        popular_tracks = []

        for query in POPULAR_SEARCHES[:2]:
            try:
                results = await self.search_soundcloud(query, album_only=False)
                if results:
                    popular_tracks.extend(results[:2])
            except Exception as e:
                logger.warning(f"Ошибка поиска популярных треков: {e}")

        random.shuffle(popular_tracks)
        return popular_tracks[:limit]

    async def show_recommendations(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает рекомендации пользователю"""
        user = update.effective_user
        self.ensure_user(user.id)

        try:
            status_msg = await update.callback_query.message.reply_text("🎯 Загружаю ваши рекомендации...")
        except:
            return

        try:
            recommendations = await self.get_recommendations(user.id, 6)

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

            text = "🎯 <b>Ваши рекомендации</b>\n\n"
            text += f"Найдено треков: {len(recommendations)}\n"

            history_count = len(user_data[str(user.id)].get('download_history', []))
            if history_count > 0:
                text += f"📊 На основе {history_count} скачанных треков\n"

            keyboard = []

            for idx, track in enumerate(recommendations):
                title = track.get('title', 'Неизвестный трек')
                artist = track.get('artist', 'Неизвестный исполнитель')
                duration = self.format_duration(track.get('duration'))

                short_title = title if len(title) <= 25 else title[:22] + '...'
                short_artist = artist if len(artist) <= 15 else artist[:12] + '...'

                button_text = f"🎵 {short_title} • {short_artist} • {duration}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f'rec_download:{idx}')])

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

            await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

            user_data[str(user.id)]['current_recommendations'] = recommendations
            save_data()

        except Exception as e:
            logger.exception(f'Ошибка показа рекомендаций: {e}')
            await status_msg.edit_text(
                '❌ Ошибка загрузки рекомендаций',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🔄 Попробовать снова', callback_data='show_recommendations')],
                    [InlineKeyboardButton('🏠 В меню', callback_data='back_to_main')],
                ])
            )

    # ==================== ЧАРТЫ ====================

    async def update_charts_cache(self):
        """Обновляет кэш чартов"""
        now = datetime.now()
        last_update = charts_cache.get('last_update')

        if last_update:
            last_update_date = datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
            if now - last_update_date < timedelta(hours=24):
                return

        logger.info("🔄 Обновление кэша чартов...")

        charts_data = {}
        for query in POPULAR_SEARCHES[:3]:
            try:
                results = await self.search_soundcloud(query, album_only=False)
                if results:
                    charts_data[query] = results[:6]
                await asyncio.sleep(1)
            except Exception as e:
                logger.warning(f"Ошибка обновления чарта для {query}: {e}")

        charts_cache['data'] = charts_data
        charts_cache['last_update'] = now.strftime('%Y-%m-%d %H:%M:%S')
        save_charts_cache()
        logger.info("✅ Кэш чартов обновлен")

    async def show_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает топ чарты"""
        user = update.effective_user
        self.ensure_user(user.id)

        try:
            status_msg = await update.callback_query.message.reply_text("📊 Загружаю популярные треки...")
        except:
            return

        try:
            await self.update_charts_cache()

            charts_data = charts_cache.get('data', {})

            if not charts_data:
                await status_msg.edit_text("❌ Чарты временно недоступны. Попробуйте позже.")
                return

            all_tracks = []
            for query, tracks in charts_data.items():
                all_tracks.extend(tracks)

            random.shuffle(all_tracks)
            top_tracks = all_tracks[:20]

            user_data[str(user.id)]['current_charts'] = top_tracks
            user_data[str(user.id)]['charts_page'] = 0
            user_data[str(user.id)]['charts_total_pages'] = (len(top_tracks) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            save_data()

            await self.show_charts_page(update, context, 0, status_msg)

        except Exception as e:
            logger.exception(f'Ошибка показа чартов: {e}')
            await status_msg.edit_text('❌ Ошибка загрузки чартов. Попробуйте позже.')

    async def show_charts_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, page: int, status_msg=None):
        """Показывает страницу чартов"""
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

    # ==================== УМНЫЕ ПЛЕЙЛИСТЫ ====================

    async def show_smart_playlists(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает меню умных плейлистов"""
        text = "🎯 <b>Умные плейлисты</b>\n\n"
        text += "Готовые подборки для любого настроения:\n\n"

        keyboard = []
        for playlist_id, playlist in SMART_PLAYLISTS.items():
            button_text = f"{playlist['name']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f'playlist:{playlist_id}')])

        keyboard.extend([
            [InlineKeyboardButton('🎯 Рекомендации', callback_data='show_recommendations')],
            [InlineKeyboardButton('📊 Топ чарты', callback_data='show_charts')],
            [InlineKeyboardButton('🔙 В главное меню', callback_data='back_to_main')],
        ])

        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def generate_playlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE, playlist_id: str):
        """Генерирует плейлист по шаблону"""
        user = update.effective_user
        self.ensure_user(user.id)

        playlist = SMART_PLAYLISTS.get(playlist_id)
        if not playlist:
            await update.callback_query.message.reply_text("❌ Плейлист не найден")
            return

        try:
            status_msg = await update.callback_query.message.reply_text(f"🎵 Создаю плейлист: {playlist['name']}...")
        except:
            return

        try:
            all_tracks = []
            for query in playlist['queries'][:2]:
                try:
                    results = await self.search_soundcloud(query, album_only=False)
                    if results:
                        all_tracks.extend(results[:6])
                    await asyncio.sleep(1)
                except Exception as e:
                    logger.warning(f"Ошибка поиска для плейлиста {query}: {e}")

            if not all_tracks:
                await status_msg.edit_text("❌ Не удалось найти треки для плейлиста. Попробуйте позже.")
                return

            random.shuffle(all_tracks)
            playlist_tracks = all_tracks[:20]

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
        """Показывает страницу плейлиста"""
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

        text = f"🎯 <b>{playlist_name}</b>\n"
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
            [InlineKeyboardButton('🔄 Другой плейлист', callback_data='smart_playlists')],
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
            logger.warning(f'Ошибка отображения страницы плейлиста: {e}')

        user_data[str(user.id)]['playlist_page'] = page
        save_data()

    # ==================== ОСНОВНЫЕ КОМАНДЫ ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        welcome = f"🎵 <b>SoundCloud Music Bot</b>\nПривет, {user.first_name}!\n\n" \
                  f"⚡ <i>Быстрый доступ к миллионам треков</i>"

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
                InlineKeyboardButton('🎶 Плейлисты', callback_data='smart_playlists'),
                InlineKeyboardButton('⚙️ Настройки', callback_data='settings')
            ]
        ]

        await update.message.reply_text(
            welcome,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        save_data()

    async def search_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /search"""
        await update.message.reply_text('🎵 Введите название песни или исполнителя:')

    async def random_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Поиск и автоматическое скачивание случайного трека"""
        user = update.effective_user
        self.ensure_user(user.id)

        random_search = random.choice(RANDOM_SEARCHES)

        if update.callback_query:
            try:
                status_msg = await update.callback_query.message.reply_text(f"🎲 Ищу случайный трек: <b>{random_search}</b>", parse_mode='HTML')
                chat_id = update.effective_chat.id
            except:
                return
        else:
            status_msg = await update.message.reply_text(f"🎲 Ищу случайный трек: <b>{random_search}</b>", parse_mode='HTML')
            chat_id = update.effective_chat.id

        try:
            results = await self.search_soundcloud(random_search, album_only=False)
            if not results:
                await status_msg.edit_text('❌ Не удалось найти случайный трек. Попробуйте еще раз.')
                return

            filtered = self._apply_filters(results, user.id)
            if not filtered:
                await status_msg.edit_text('❌ Не удалось найти случайный трек с текущими фильтрами.')
                return

            random_track = random.choice(filtered)
            await status_msg.edit_text(f"⏬ Скачиваю: <b>{random_track.get('title', 'Неизвестный трек')}</b>", parse_mode='HTML')

            success = await self.download_and_send_track(update, context, random_track)

            if success:
                stats = user_data.get('_user_stats', {}).get(str(user.id), {})
                stats['downloads'] = stats.get('downloads', 0) + 1
                stats['searches'] = stats.get('searches', 0) + 1
                save_data()

                keyboard = [
                    [InlineKeyboardButton('🎲 Еще случайный трек', callback_data='random_track')],
                    [InlineKeyboardButton('🎯 Рекомендации', callback_data='show_recommendations')],
                    [InlineKeyboardButton('🔍 Новый поиск', callback_data='start_search')],
                ]

                await context.bot.send_message(
                    chat_id=chat_id,
                    text="✅ Случайный трек успешно обработан! Что дальше?",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await status_msg.edit_text('❌ Не удалось обработать случайный трек. Попробуйте еще раз.')

        except Exception as e:
            logger.exception(f'Ошибка при поиске случайного трека: {e}')
            await status_msg.edit_text('❌ Ошибка при поиске случайного трека. Попробуйте позже.')

    # ==================== ОБРАБОТЧИКИ CALLBACK ====================

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

            if data == 'smart_playlists':
                await self.show_smart_playlists(update, context)
                return

            if data == 'settings':
                await self.show_settings(update, context)
                return

            if data == 'back_to_main':
                await self.show_main_menu(update, context)
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

            if data == 'toggle_music':
                await self.toggle_music_filter(update, context)
                return

            if data == 'toggle_album':
                await self.toggle_album_filter(update, context)
                return

            if data == 'current_page' or data == 'charts_current_page' or data == 'playlist_current_page':
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

    # ==================== МЕТОДЫ СКАЧИВАНИЯ ====================

    async def download_from_recommendations(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
        """Скачивание трека из рекомендаций"""
        user = update.effective_user
        recommendations = user_data[str(user.id)].get('current_recommendations', [])

        if index < 0 or index >= len(recommendations):
            await update.callback_query.edit_message_text('❌ Трек не найден')
            return

        track = recommendations[index]
        await self.process_track_download(update, context, track, 'recommendations')

    async def download_from_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
        """Скачивание трека из чартов"""
        user = update.effective_user
        charts = user_data[str(user.id)].get('current_charts', [])

        if index < 0 or index >= len(charts):
            await update.callback_query.edit_message_text('❌ Трек не найден')
            return

        track = charts[index]
        await self.process_track_download(update, context, track, 'charts')

    async def download_from_playlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int):
        """Скачивание трека из плейлиста"""
        user = update.effective_user
        playlist = user_data[str(user.id)].get('current_playlist', {})
        tracks = playlist.get('tracks', [])

        if index < 0 or index >= len(tracks):
            await update.callback_query.edit_message_text('❌ Трек не найден')
            return

        track = tracks[index]
        await self.process_track_download(update, context, track, 'playlist')

    async def process_track_download(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, source: str):
        """Обрабатывает скачивание трека"""
        query = update.callback_query
        user = update.effective_user

        title = track.get('title', 'Неизвестный трек')
        await query.edit_message_text(f'⏬ Скачиваю: {title}')

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

            quick_actions = [
                [InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track')],
                [InlineKeyboardButton('🎯 Еще рекомендации', callback_data='show_recommendations')],
                [InlineKeyboardButton('🔍 Новый поиск', callback_data='start_search')],
            ]

            await query.message.reply_text(
                f"✅ Трек скачан! Что дальше?",
                reply_markup=InlineKeyboardMarkup(quick_actions)
            )
        else:
            await query.edit_message_text('❌ Не удалось обработать трек. Попробуйте другой.')

    # ==================== ПОИСК И ФИЛЬТРЫ ====================

    async def search_soundcloud(self, query: str, album_only: bool = False):
        """Асинхронный поиск с ограничениями"""
        async with self.search_semaphore:
            ydl_opts = {
                'format': 'bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'ignoreerrors': True,
                'noplaylist': not album_only,
                'socket_timeout': 15,
            }

            results = []
            try:
                def perform_search():
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        return ydl.extract_info(f"scsearch10:{query}", download=False)

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

                    entry_type = entry.get('_type', '')
                    is_playlist = (entry_type == 'playlist' or 
                                 '/playlist/' in webpage_url or 
                                 '/sets/' in webpage_url)

                    if album_only:
                        if is_playlist:
                            track_count = len(entry.get('entries', []))
                            if track_count > 1:
                                results.append({
                                    'title': title,
                                    'webpage_url': webpage_url,
                                    'duration': duration,
                                    'artist': artist,
                                    'source': 'album',
                                    'track_count': track_count,
                                    'thumbnail': thumbnail,
                                    'entries': entry.get('entries', [])
                                })
                        elif duration > 600:
                            results.append({
                                'title': f"{title} (Длинная версия)",
                                'webpage_url': webpage_url,
                                'duration': duration,
                                'artist': artist,
                                'source': 'track',
                                'thumbnail': thumbnail
                            })
                    else:
                        if is_playlist:
                            track_count = len(entry.get('entries', []))
                            if track_count > 1:
                                results.append({
                                    'title': title,
                                    'webpage_url': webpage_url,
                                    'duration': duration,
                                    'artist': artist,
                                    'source': 'album',
                                    'track_count': track_count,
                                    'thumbnail': thumbnail,
                                    'entries': entry.get('entries', [])
                                })
                        else:
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

            logger.info(f"✅ SoundCloud: {len(results)} результатов для: '{query}' (album_only: {album_only})")
            return results

    def _apply_filters(self, results: list, user_id: int):
        filters = user_data.get(str(user_id), {}).get('filters', {'duration': 'no_filter', 'music_only': False})
        max_dur = {
            'up_to_5min': 300,
            'up_to_10min': 600,
            'up_to_20min': 1200,
        }.get(filters.get('duration', 'no_filter'), float('inf'))

        filtered = []
        for r in results:
            dur = r.get('duration') or 0

            if r.get('source') == 'track' and filters.get('duration') != 'no_filter' and dur > max_dur:
                continue

            if filters.get('music_only') and r.get('source') == 'track':
                title_l = r.get('title', '').lower()
                non_music = ['podcast', 'interview', 'lecture', 'speech', 'documentary', 'concert']
                if any(k in title_l for k in non_music):
                    continue
                if dur and dur > 3600:
                    continue

            filtered.append(r)

        return filtered

    async def show_results_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, page: int, status_msg=None):
        user_entry = user_data.get(str(user_id), {})
        results = user_entry.get('search_results', [])
        total_pages = user_entry.get('total_pages', 0)
        query = user_entry.get('search_query', '')
        filters = user_data.get(str(user_id), {}).get('filters', {})

        if page < 0 or page >= max(1, total_pages):
            page = 0

        start = page * RESULTS_PER_PAGE
        end = min(start + RESULTS_PER_PAGE, len(results))

        text = f"🔍 <b>Результаты по запросу:</b> <code>{query}</code>\n"
        text += f"📄 Страница {page + 1} из {max(1, total_pages)}\n"
        text += f"🎵 Найдено: {len(results)} результатов\n\n"

        keyboard = []
        for idx in range(start, end):
            r = results[idx]

            if r.get('source') == 'album':
                track_count = r.get('track_count', 0)
                title = r.get('title', 'Неизвестный альбом')
                short = title if len(title) <= 35 else title[:32] + '...'
                button_text = f"💿 {idx + 1}. {short} ({track_count} треков)"
            else:
                title = r.get('title', 'Неизвестный трек')
                artist = r.get('artist', 'Неизвестный исполнитель')
                duration = self.format_duration(r.get('duration'))

                short_title = title if len(title) <= 30 else title[:27] + '...'
                short_artist = artist if len(artist) <= 18 else artist[:15] + '...'

                button_text = f"🎵 {idx + 1}. {short_title} • {short_artist} • {duration}"

            cb = f"download:{idx}:{page}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=cb)])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'page:{page-1}'))
        if total_pages > 1:
            nav.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='current_page'))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton('Вперед ➡️', callback_data=f'page:{page+1}'))
        if nav:
            keyboard.append(nav)

        keyboard.extend([
            [InlineKeyboardButton('🔍 Новый поиск', callback_data='new_search')],
            [InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track')],
            [InlineKeyboardButton('⚙️ Настройки', callback_data='settings')],
        ])

        try:
            if status_msg:
                await status_msg.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
            else:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            logger.warning(f'Ошибка отображения страницы результатов: {e}')

        user_data[str(user_id)]['current_page'] = page
        save_data()

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = (update.message.text or '').strip()
        if not text or text.startswith('/'):
            return
        await self.search_music(update, context, text)

    async def search_music(self, update: Update, context: ContextTypes.DEFAULT_TYPE, query_text: str = None):
        user = update.effective_user
        self.ensure_user(user.id)

        if query_text is None:
            query_text = (update.message.text or '').strip()

        if len(query_text) < 2:
            await update.message.reply_text('❌ Введите хотя бы 2 символа')
            return

        stats = user_data['_user_stats'][str(user.id)]
        stats['searches'] += 1
        stats['last_search'] = datetime.now().strftime('%d.%m.%Y %H:%M')

        user_entry = user_data[str(user.id)]
        history = user_entry.get('search_history', [])
        history = [query_text] + [h for h in history if h != query_text][:9]
        user_entry['search_history'] = history

        filters = user_data[str(user.id)]['filters']
        album_only = filters.get('album_only', False)

        status_text = f"🔍 Ищу на SoundCloud: <b>{query_text}</b>"
        if album_only:
            status_text += " (только альбомы)"

        if update.message:
            status_msg = await update.message.reply_text(status_text, parse_mode='HTML')
        elif update.callback_query:
            status_msg = await update.callback_query.message.reply_text(status_text, parse_mode='HTML')
        else:
            status_msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=status_text,
                parse_mode='HTML'
            )

        try:
            results = await self.search_soundcloud(query_text, album_only=album_only)
            if not results:
                await status_msg.edit_text('❌ Ничего не найдено. Попробуйте другой запрос.')
                return

            filtered = self._apply_filters(results, user.id)
            if not filtered:
                await status_msg.edit_text('❌ Ничего не найдено с текущими фильтрами')
                return

            user_entry['search_results'] = filtered
            user_entry['search_query'] = query_text
            user_entry['current_page'] = 0
            user_entry['total_pages'] = (len(filtered) + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
            save_data()

            await self.show_results_page(update, context, user.id, 0, status_msg)
        except Exception as e:
            logger.exception('Ошибка при поиске')
            await status_msg.edit_text('❌ Ошибка при поиске. Попробуйте позже.')

    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user

        welcome = f"🎵 <b>SoundCloud Music Bot</b>\nПривет, {user.first_name}!\n\n" \
                  f"⚡ <i>Быстрый доступ к миллионам треков</i>"

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
                InlineKeyboardButton('🎶 Плейлисты', callback_data='smart_playlists'),
                InlineKeyboardButton('⚙️ Настройки', callback_data='settings')
            ]
        ]

        if update.callback_query:
            await update.callback_query.edit_message_text(
                welcome,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                welcome,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )

    # ==================== НАСТРОЙКИ ====================

    async def show_settings(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        filters = user_data[str(user.id)]['filters']
        current_duration = DURATION_FILTERS.get(filters.get('duration', 'no_filter'), 'Без фильтра')
        music_only = "✅ ВКЛ" if filters.get('music_only') else "❌ ВЫКЛ"
        album_only = "✅ ВКЛ" if filters.get('album_only') else "❌ ВЫКЛ"

        text = f"""⚙️ <b>Настройки фильтров</b>

⏱️ <b>Фильтр по длительности:</b> {current_duration}
🎵 <b>Только музыка:</b> {music_only}
💿 <b>Только альбомы:</b> {album_only}
   <i>(плейлисты и треки длиннее 10 минут)</i>

Выберите настройку для изменения:"""

        keyboard = [
            [InlineKeyboardButton('⏱️ Фильтр по длительности', callback_data='duration_menu')],
            [InlineKeyboardButton(f'🎵 Только музыка: {music_only}', callback_data='toggle_music')],
            [InlineKeyboardButton(f'💿 Только альбомы: {album_only}', callback_data='toggle_album')],
            [InlineKeyboardButton('🔙 Назад в меню', callback_data='back_to_main')],
        ]

        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        else:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

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

    async def toggle_album_filter(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        self.ensure_user(user.id)

        current = user_data[str(user.id)]['filters'].get('album_only', False)
        user_data[str(user.id)]['filters']['album_only'] = not current
        save_data()

        status = "ВКЛЮЧЕН" if not current else "ВЫКЛЮЧЕН"
        await update.callback_query.answer(f'Фильтр "Только альбомы" {status}')
        await self.show_settings(update, context)

    # ==================== СКАЧИВАНИЕ ПО ИНДЕКСУ ====================

    async def download_by_index(self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int, return_page: int = 0):
        query = update.callback_query
        user = update.effective_user

        user_entry = user_data.get(str(user.id), {})
        results = user_entry.get('search_results', [])
        if index < 0 or index >= len(results):
            await query.edit_message_text('❌ Трек не найден')
            return

        track = results[index]

        if track.get('source') == 'album':
            await self.download_album(update, context, track, return_page)
        else:
            await self.download_track(update, context, track, return_page)

    async def download_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, return_page: int = 0):
        query = update.callback_query
        user = update.effective_user

        title = track.get('title', 'Неизвестный трек')
        await query.edit_message_text(f'⏬ Скачиваю: {title}')

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

            if return_page is not None:
                await self.show_results_page(update, context, user.id, return_page)
        else:
            await query.edit_message_text('❌ Не удалось обработать трек. Попробуйте другой.')

    async def download_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE, album: dict, return_page: int = 0):
        query = update.callback_query
        user = update.effective_user

        tracks = album.get('entries', [])
        album_title = album.get('title', 'Неизвестный альбом')

        if not tracks:
            await query.edit_message_text('❌ В альбоме не найдено треков')
            return

        await query.edit_message_text(f'💿 Начинаю обработку альбома: {album_title}\nТреков: {len(tracks)}')

        successful_downloads = 0
        for idx, track in enumerate(tracks):
            if not track:
                continue

            track_title = track.get('title', f'Трек {idx + 1}')
            status_msg = await query.message.reply_text(f'💿 [{idx + 1}/{len(tracks)}] Скачиваю: {track_title}')

            success = await self.download_and_send_track(update, context, track)
            if success:
                successful_downloads += 1
                await status_msg.edit_text(f'✅ [{idx + 1}/{len(tracks)}] Обработано: {track_title}')
            else:
                await status_msg.edit_text(f'❌ [{idx + 1}/{len(tracks)}] Ошибка: {track_title}')

        stats = user_data.get('_user_stats', {}).get(str(user.id), {})
        stats['downloads'] = stats.get('downloads', 0) + successful_downloads
        save_data()

        await query.message.reply_text(f'💿 Альбом обработан!\nУспешно: {successful_downloads}/{len(tracks)} треков')
        await self.show_results_page(update, context, user.id, return_page)

    def run(self):
        print('🚀 Запуск SoundCloud Music Bot...')

        app = Application.builder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler('start', self.start))
        app.add_handler(CommandHandler('search', self.search_command))
        app.add_handler(CommandHandler('random', self.random_track))
        app.add_handler(CommandHandler('settings', self.show_settings))

        setup_admin_commands(app)

        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        app.add_handler(CallbackQueryHandler(self.handle_callback))

        async def set_commands(application):
            commands = [
                ('start', '🚀 Запустить бота'),
                ('search', '🔍 Начать поиск'),
                ('random', '🎲 Случайный трек'),
                ('settings', '⚙️ Настройки фильтров'),
            ]

            await application.bot.set_my_commands(commands)
            print('✅ Меню с командами настроено!')

        app.post_init = set_commands

        print('✅ Бот запущен и готов к работе!')
        app.run_polling()

if __name__ == '__main__':
    bot = StableMusicBot()
    bot.run()
