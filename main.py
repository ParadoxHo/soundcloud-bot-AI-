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
CHANNEL_ID = os.environ.get('CHANNEL_ID')  # ID канала для работы
ADMIN_IDS = os.environ.get('ADMIN_IDS', '').split(',')

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    print("📝 Добавьте переменную BOT_TOKEN в настройках Railway")
    sys.exit(1)

if not CHANNEL_ID:
    print("❌ Ошибка: CHANNEL_ID не установлен")
    print("📝 Добавьте переменную CHANNEL_ID в настройках Railway")
    sys.exit(1)

# Очищаем и форматируем ADMIN_IDS
ADMIN_IDS = [id.strip() for id in ADMIN_IDS if id.strip()]

if not ADMIN_IDS:
    print("⚠️  Предупреждение: ADMIN_IDS не установлен. Админ-команды отключены.")
else:
    print(f"✅ Админы настроены: {ADMIN_IDS}")

print(f"✅ Канал настроен: {CHANNEL_ID}")

RESULTS_PER_PAGE = 8
DATA_FILE = Path('user_data.json')
CHARTS_FILE = Path('charts_cache.json')
MAX_FILE_SIZE_MB = 50  # Максимальный размер для скачивания

# ОГРАНИЧЕНИЯ ДЛЯ СТАБИЛЬНОСТИ
MAX_CONCURRENT_DOWNLOADS = 1
DOWNLOAD_TIMEOUT = 180
SEARCH_TIMEOUT = 30

# ПРОСТЫЕ НАСТРОЙКИ СКАЧИВАНИЯ БЕЗ КОНВЕРТАЦИИ (ТОЛЬКО TELEGRAM-СОВМЕСТИМЫЕ ФОРМАТЫ)
SIMPLE_DOWNLOAD_OPTS = {
    'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio[ext=ogg]/bestaudio[ext=wav]/bestaudio[ext=flac]/bestaudio/best',
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
    'max_filesize': 45000000,  # ~45MB
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

# ==================== CHANNEL BOT CLASS ====================
class ChannelMusicBot:
    def __init__(self):
        self.download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        self.search_semaphore = asyncio.Semaphore(3)
        logger.info('✅ Бот для канала инициализирован')

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

    # ==================== ОСНОВНЫЕ КОМАНДЫ ДЛЯ КАНАЛА ====================

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start - показывает главное меню"""
        user = update.effective_user
        
        text = f"🎵 <b>Музыкальный бот для канала</b>\n\n"
        text += f"👋 Привет, {user.first_name}!\n\n"
        text += f"<b>Доступные команды:</b>\n"
        text += f"/music <запрос> - 🔍 Найти и скачать трек\n"
        text += f"/random - 🎲 Случайный трек\n"
        text += f"/playlists - 🎭 Готовые плейлисты\n"
        text += f"/charts - 📊 Популярные треки\n\n"
        text += f"💡 <b>Пример:</b> /music coldplay adventure of a lifetime"

        keyboard = [
            [
                InlineKeyboardButton('🎲 Случайный трек', callback_data='random_track'),
                InlineKeyboardButton('🔍 Поиск', callback_data='start_search')
            ],
            [
                InlineKeyboardButton('📊 Топ чарты', callback_data='show_charts'),
                InlineKeyboardButton('🎭 Плейлисты', callback_data='show_playlists')
            ]
        ]

        if hasattr(update, 'callback_query') and update.callback_query:
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

    async def music_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /music - поиск и скачивание трека"""
        if not context.args:
            await update.message.reply_text(
                "❌ <b>Укажите запрос для поиска</b>\n\n"
                "💡 <b>Пример:</b> <code>/music the weeknd blinding lights</code>",
                parse_mode='HTML'
            )
            return

        query = ' '.join(context.args)
        await self.search_and_download_music(update, context, query)

    async def random_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /random - случайный трек"""
        random_search = random.choice(RANDOM_SEARCHES)
        await self.search_and_download_music(update, context, random_search, is_random=True)

    async def playlists_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /playlists - показывает плейлисты"""
        await self.show_playlists_menu(update, context)

    async def charts_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /charts - показывает популярные треки"""
        await self.show_charts(update, context)

    # ==================== ПОИСК И СКАЧИВАНИЕ ====================

    async def search_and_download_music(self, update: Update, context: ContextTypes.DEFAULT_TYPE, query: str, is_random: bool = False):
        """Основная функция поиска и скачивания музыки в канал"""
        try:
            # Отправляем сообщение о начале поиска
            if is_random:
                status_text = f"🎲 <b>Ищу случайный трек...</b>\n\n📝 Запрос: <code>{query}</code>"
            else:
                status_text = f"🔍 <b>Ищу музыку...</b>\n\n📝 Запрос: <code>{query}</code>"
            
            status_msg = await context.bot.send_message(
                chat_id=CHANNEL_ID,
                text=status_text,
                parse_mode='HTML'
            )

            # Выполняем поиск
            results = await self.search_soundcloud(query)
            
            if not results:
                await status_msg.edit_text(
                    f"❌ <b>По запросу ничего не найдено</b>\n\n"
                    f"📝 Запрос: <code>{query}</code>\n\n"
                    f"💡 Попробуйте другой запрос",
                    parse_mode='HTML'
                )
                return

            # Берем первый (самый релевантный) результат
            track = results[0]
            
            # Обновляем статус - найден трек
            await status_msg.edit_text(
                f"✅ <b>Трек найден!</b>\n\n"
                f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                f"🎤 {track.get('artist', 'Неизвестный исполнитель')}\n"
                f"⏱️ {self.format_duration(track.get('duration'))}\n\n"
                f"⏬ <b>Начинаю скачивание...</b>",
                parse_mode='HTML'
            )

            # Скачиваем и отправляем трек
            success = await self.download_and_send_to_channel(context, track, status_msg)
            
            if success:
                # Редактируем статус на успешное завершение
                await status_msg.edit_text(
                    f"✅ <b>Трек успешно добавлен в канал!</b>\n\n"
                    f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                    f"🎤 {track.get('artist', 'Неизвестный исполнитель')}\n"
                    f"⏱️ {self.format_duration(track.get('duration'))}\n\n"
                    f"🔍 Запрос: <code>{query}</code>",
                    parse_mode='HTML'
                )
            else:
                # Если скачивание не удалось
                await status_msg.edit_text(
                    f"❌ <b>Не удалось скачать трек</b>\n\n"
                    f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                    f"💡 Попробуйте другой трек или запрос",
                    parse_mode='HTML'
                )

        except Exception as e:
            logger.exception(f'Ошибка при поиске и скачивании: {e}')
            try:
                await context.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=f"❌ <b>Произошла ошибка при обработке запроса</b>\n\n📝 Запрос: <code>{query}</code>",
                    parse_mode='HTML'
                )
            except:
                pass

    async def download_and_send_to_channel(self, context: ContextTypes.DEFAULT_TYPE, track: dict, status_msg=None) -> bool:
        """Скачивает трек и отправляет его в канал"""
        url = track.get('webpage_url') or track.get('url')
        if not url:
            return False

        async with self.download_semaphore:
            try:
                # Обновляем статус - скачивание
                if status_msg:
                    await status_msg.edit_text(
                        f"⏬ <b>Скачиваю трек...</b>\n\n"
                        f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                        f"📊 Статус: ⬇️ Загрузка аудио",
                        parse_mode='HTML'
                    )

                # Скачиваем трек
                file_path = await self.download_track(url)
                if not file_path:
                    return False

                # Отправляем в канал
                with open(file_path, 'rb') as audio_file:
                    await context.bot.send_audio(
                        chat_id=CHANNEL_ID,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n🎤 {track.get('artist', 'Неизвестный исполнитель')}\n⏱️ {self.format_duration(track.get('duration'))}",
                        parse_mode='HTML',
                    )

                # Очищаем временный файл
                try:
                    os.remove(file_path)
                except:
                    pass

                return True

            except Exception as e:
                logger.exception(f'Ошибка скачивания и отправки: {e}')
                return False

    async def download_track(self, url: str) -> str:
        """Скачивает трек и возвращает путь к файлу"""
        loop = asyncio.get_event_loop()
        tmpdir = tempfile.mkdtemp()
        
        try:
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
                return None

            # Ищем Telegram-совместимые файлы
            telegram_audio_extensions = ['.mp3', '.m4a', '.ogg', '.wav', '.flac']
            
            for file in os.listdir(tmpdir):
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext in telegram_audio_extensions:
                    file_path = os.path.join(tmpdir, file)
                    
                    # Проверяем размер файла
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    if file_size_mb >= MAX_FILE_SIZE_MB:
                        continue
                    
                    return file_path

            return None

        except asyncio.TimeoutError:
            logger.error(f"Таймаут при скачивании: {url}")
            return None
        except Exception as e:
            logger.exception(f'Ошибка скачивания: {e}')
            return None
        finally:
            # Очистка временных файлов будет после отправки
            pass

    # ==================== ПОИСК ====================

    async def search_soundcloud(self, query: str):
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

                    if not title:
                        continue

                    results.append({
                        'title': title,
                        'webpage_url': webpage_url,
                        'duration': duration,
                        'artist': artist,
                        'source': 'track'
                    })

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут поиска для запроса: {query}")
                return []
            except Exception as e:
                logger.warning(f'Ошибка поиска SoundCloud: {e}')
                return []

            logger.info(f"✅ SoundCloud: {len(results)} результатов для: '{query}'")
            return results

    # ==================== ПЛЕЙЛИСТЫ ====================

    async def show_playlists_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает меню плейлистов"""
        text = "🎭 <b>Готовые плейлисты</b>\n\nВыберите настроение:"

        keyboard = []
        for playlist_id, playlist in SMART_PLAYLISTS.items():
            button_text = f"{playlist['name']}"
            callback_data = f'playlist:{playlist_id}'
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

        keyboard.append([InlineKeyboardButton('🔙 Назад', callback_data='back_to_main')])

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

    async def handle_playlist_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE, playlist_id: str):
        """Обрабатывает выбор плейлиста"""
        playlist = SMART_PLAYLISTS.get(playlist_id)
        if not playlist:
            await update.callback_query.answer("❌ Плейлист не найден")
            return

        # Берем первый запрос из плейлиста для поиска
        query = playlist['queries'][0] if playlist['queries'] else playlist['name']
        
        await update.callback_query.answer(f"🎵 Ищем: {playlist['name']}")
        await self.search_and_download_music(update, context, query)

    # ==================== ЧАРТЫ ====================

    async def show_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает популярные треки"""
        # Берем случайный популярный запрос
        popular_query = random.choice(POPULAR_SEARCHES)
        
        if update.callback_query:
            await update.callback_query.answer(f"📊 Загружаем популярные треки...")
        
        await self.search_and_download_music(update, context, popular_query)

    # ==================== CALLBACK ОБРАБОТЧИКИ ====================

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        data = (query.data or '')
        
        try:
            await query.answer()
        except Exception as e:
            logger.warning(f"Ошибка при answer callback: {e}")

        try:
            if data == 'start_search':
                await query.edit_message_text(
                    "🔍 <b>Введите команду для поиска:</b>\n\n"
                    "💡 <b>Пример:</b> <code>/music coldplay adventure of a lifetime</code>",
                    parse_mode='HTML'
                )
                return

            if data == 'random_track':
                await self.random_command(update, context)
                return

            if data == 'show_charts':
                await self.show_charts(update, context)
                return

            if data == 'show_playlists':
                await self.show_playlists_menu(update, context)
                return

            if data == 'back_to_main':
                await self.start(update, context)
                return

            if data.startswith('playlist:'):
                playlist_id = data.split(':', 1)[1]
                await self.handle_playlist_selection(update, context, playlist_id)
                return

            await query.edit_message_text('❌ Неизвестная команда')

        except Exception as e:
            logger.exception('Ошибка обработки callback')
            try:
                await query.message.reply_text('❌ Произошла ошибка')
            except:
                pass

    # ==================== ЗАПУСК БОТА ====================

    def run(self):
        print('🚀 Запуск Music Bot для канала...')

        app = Application.builder().token(BOT_TOKEN).build()

        # Основные команды
        app.add_handler(CommandHandler('start', self.start))
        app.add_handler(CommandHandler('music', self.music_command))
        app.add_handler(CommandHandler('random', self.random_command))
        app.add_handler(CommandHandler('playlists', self.playlists_command))
        app.add_handler(CommandHandler('charts', self.charts_command))

        # Callback обработчики
        app.add_handler(CallbackQueryHandler(self.handle_callback))

        # Установка команд меню
        async def set_commands(application):
            commands = [
                ('start', '🚀 Запустить бота'),
                ('music', '🔍 Поиск музыки'),
                ('random', '🎲 Случайный трек'),
                ('playlists', '🎭 Готовые плейлисты'),
                ('charts', '📊 Популярные треки'),
            ]
            await application.bot.set_my_commands(commands)
            print('✅ Меню с командами настроено!')

        app.post_init = set_commands

        print('✅ Бот для канала запущен и готов к работе!')
        print(f'📢 Бот будет работать в канале: {CHANNEL_ID}')
        app.run_polling()

if __name__ == '__main__':
    bot = ChannelMusicBot()
    bot.run()
