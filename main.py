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
from datetime import datetime
from pathlib import Path

# ==================== CONFIG ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHANNEL_ID = os.environ.get('CHANNEL_ID')  # ID канала для работы

if not BOT_TOKEN:
    print("❌ Ошибка: BOT_TOKEN не установлен")
    sys.exit(1)

if not CHANNEL_ID:
    print("❌ Ошибка: CHANNEL_ID не установлен")
    sys.exit(1)

# Преобразуем CHANNEL_ID в int
try:
    CHANNEL_ID = int(CHANNEL_ID)
except ValueError:
    print("❌ Ошибка: CHANNEL_ID должен быть числом")
    sys.exit(1)

print(f"✅ Канал настроен: {CHANNEL_ID}")

MAX_FILE_SIZE_MB = 50
DOWNLOAD_TIMEOUT = 180
SEARCH_TIMEOUT = 30

# Настройки скачивания
SIMPLE_DOWNLOAD_OPTS = {
    'format': 'bestaudio[ext=mp3]/bestaudio[ext=m4a]/bestaudio[ext=ogg]/bestaudio/best',
    'outtmpl': os.path.join(tempfile.gettempdir(), '%(id)s.%(ext)s'),
    'quiet': True,
    'no_warnings': True,
    'retries': 2,
    'fragment_retries': 2,
    'skip_unavailable_fragments': True,
    'noprogress': True,
    'nopart': True,
    'noplaylist': True,
    'max_filesize': 45000000,
    'ignoreerrors': True,
    'socket_timeout': 30,
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
        Application, CommandHandler, MessageHandler, CallbackQueryHandler, 
        filters, ContextTypes
    )
    import yt_dlp
    print("✅ Все зависимости загружены")
except ImportError as exc:
    print(f"❌ Ошибка импорта: {exc}")
    os.system("pip install python-telegram-bot yt-dlp")
    try:
        from telegram import Update
        from telegram.ext import (
            Application, CommandHandler, MessageHandler, CallbackQueryHandler,
            filters, ContextTypes
        )
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

# ==================== CHANNEL MUSIC BOT ====================
class ChannelMusicBot:
    def __init__(self):
        self.download_semaphore = asyncio.Semaphore(1)
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

    # ==================== ОБРАБОТКА СООБЩЕНИЙ В КАНАЛЕ ====================

    async def handle_channel_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает все сообщения в канале"""
        try:
            # Проверяем, что есть сообщение
            if not update.message or not update.message.text:
                return
                
            message_text = update.message.text.strip().lower()
            chat_id = update.effective_chat.id
            
            print(f"📨 Получено сообщение в чате {chat_id}: {message_text}")
            
            if not message_text:
                return

            # Игнорируем команды
            if message_text.startswith('/'):
                print(f"🔸 Игнорируем команду: {message_text}")
                return

            # Если сообщение содержит "найди" - ищем трек
            if 'найди' in message_text:
                print(f"🔍 Обрабатываем поиск: {message_text}")
                await self.handle_find_command(update, context, message_text)
            
            # Если сообщение содержит "рандом" - случайный трек
            elif 'рандом' in message_text:
                print(f"🎲 Обрабатываем рандом: {message_text}")
                await self.handle_random_command(update, context)
            
            # Любое другое сообщение - тоже случайный трек
            else:
                print(f"🎲 Обрабатываем как рандом: {message_text}")
                await self.handle_random_command(update, context)
                
        except Exception as e:
            logger.exception(f'Ошибка обработки сообщения: {e}')
            print(f"❌ Ошибка: {e}")

    async def handle_find_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_text: str):
        """Обрабатывает поиск трека по запросу"""
        try:
            # Извлекаем запрос после "найди"
            query = self.extract_search_query(message_text)
            
            print(f"🔍 Извлечен запрос: '{query}' из '{message_text}'")
            
            if not query:
                await update.message.reply_text(
                    "❌ Не указано что искать\n"
                    "💡 Напиши: найди [название трека или исполнителя]"
                )
                return

            # Отправляем статус
            status_msg = await update.message.reply_text(
                f"🔍 <b>Ищу:</b> <code>{query}</code>\n"
                f"⏳ Ожидайте 10-20 секунд...",
                parse_mode='HTML'
            )

            # Ищем трек
            track = await self.find_track(query)
            
            if not track:
                await status_msg.edit_text(
                    f"❌ <b>Не найдено по запросу:</b> <code>{query}</code>\n"
                    f"💡 Попробуй другой запрос",
                    parse_mode='HTML'
                )
                return

            # Скачиваем и отправляем трек
            success = await self.download_and_send_track(context, track, status_msg)
            
            if success:
                # Редактируем статус на результат
                await status_msg.edit_text(
                    f"✅ <b>Найден трек по запросу:</b> <code>{query}</code>\n"
                    f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                    f"🎤 {track.get('artist', 'Неизвестный исполнитель')}",
                    parse_mode='HTML'
                )
            else:
                await status_msg.edit_text(
                    f"❌ <b>Не удалось скачать трек</b>\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )

        except Exception as e:
            logger.exception(f'Ошибка при поиске: {e}')
            print(f"❌ Ошибка поиска: {e}")
            try:
                await update.message.reply_text("❌ Ошибка при поиске трека")
            except:
                pass

    async def handle_random_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обрабатывает запрос на случайный трек"""
        try:
            # Отправляем статус
            status_msg = await update.message.reply_text(
                "🎲 <b>Ищу случайный трек...</b>\n"
                "⏳ Ожидайте 10-20 секунд...",
                parse_mode='HTML'
            )

            # Случайный запрос
            random_query = random.choice(RANDOM_SEARCHES)
            print(f"🎲 Случайный запрос: {random_query}")
            
            # Ищем трек
            track = await self.find_track(random_query)
            
            if not track:
                await status_msg.edit_text(
                    "❌ <b>Не удалось найти случайный трек</b>\n"
                    "💡 Попробуй еще раз",
                    parse_mode='HTML'
                )
                return

            # Скачиваем и отправляем трек
            success = await self.download_and_send_track(context, track, status_msg)
            
            if success:
                # Редактируем статус на результат
                await status_msg.edit_text(
                    f"✅ <b>Случайный трек:</b>\n"
                    f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                    f"🎤 {track.get('artist', 'Неизвестный исполнитель')}",
                    parse_mode='HTML'
                )
            else:
                await status_msg.edit_text(
                    f"❌ <b>Не удалось скачать случайный трек</b>\n"
                    f"🎵 {track.get('title', 'Неизвестный трек')}",
                    parse_mode='HTML'
                )

        except Exception as e:
            logger.exception(f'Ошибка при поиске случайного трека: {e}')
            print(f"❌ Ошибка случайного трека: {e}")
            try:
                await update.message.reply_text("❌ Ошибка при поиске случайного трека")
            except:
                pass

    def extract_search_query(self, message_text: str) -> str:
        """Извлекает поисковый запрос из сообщения"""
        # Удаляем слово "найди" и лишние пробелы
        query = message_text.replace('найди', '').strip()
        
        # Удаляем лишние слова
        stop_words = ['пожалуйста', 'мне', 'трек', 'песню', 'музыку', 'плз', 'plz']
        for word in stop_words:
            query = query.replace(word, '')
        
        return query.strip()

    # ==================== ПОИСК ТРЕКОВ ====================

    async def find_track(self, query: str):
        """Находит трек по запросу"""
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
                        return ydl.extract_info(f"scsearch5:{query}", download=False)

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

                for entry in entries:
                    if not entry:
                        continue

                    title = self.clean_title(entry.get('title') or '')
                    webpage_url = entry.get('webpage_url') or entry.get('url') or ''
                    duration = entry.get('duration') or 0
                    artist = entry.get('uploader') or entry.get('uploader_id') or 'Неизвестно'

                    if not title:
                        continue

                    print(f"🎵 Найден трек: {title} - {artist}")
                    return {
                        'title': title,
                        'webpage_url': webpage_url,
                        'duration': duration,
                        'artist': artist
                    }

            except asyncio.TimeoutError:
                logger.warning(f"Таймаут поиска: {query}")
                print(f"⏰ Таймаут поиска: {query}")
                return None
            except Exception as e:
                logger.warning(f'Ошибка поиска: {e}')
                print(f"❌ Ошибка поиска: {e}")
                return None

        return None

    # ==================== СКАЧИВАНИЕ И ОТПРАВКА ====================

    async def download_and_send_track(self, context: ContextTypes.DEFAULT_TYPE, track: dict, status_msg=None) -> bool:
        """Скачивает и отправляет трек в канал"""
        url = track.get('webpage_url')
        if not url:
            return False

        async with self.download_semaphore:
            try:
                # Обновляем статус - скачивание
                if status_msg:
                    await status_msg.edit_text(
                        f"⏬ <b>Скачиваю трек...</b>\n"
                        f"🎵 {track.get('title', 'Неизвестный трек')}",
                        parse_mode='HTML'
                    )

                print(f"⏬ Начинаем скачивание: {track.get('title')}")
                
                # Скачиваем трек
                file_path = await self.download_track(url)
                if not file_path:
                    print(f"❌ Не удалось скачать: {track.get('title')}")
                    return False

                print(f"✅ Трек скачан: {file_path}")

                # Отправляем в канал
                with open(file_path, 'rb') as audio_file:
                    await context.bot.send_audio(
                        chat_id=CHANNEL_ID,
                        audio=audio_file,
                        title=(track.get('title') or 'Неизвестный трек')[:64],
                        performer=(track.get('artist') or 'Неизвестный исполнитель')[:64],
                        caption=f"🎵 <b>{track.get('title', 'Неизвестный трек')}</b>\n"
                               f"🎤 {track.get('artist', 'Неизвестный исполнитель')}\n"
                               f"⏱️ {self.format_duration(track.get('duration'))}",
                        parse_mode='HTML',
                    )

                print(f"✅ Трек отправлен в канал")

                # Очищаем временный файл
                try:
                    os.remove(file_path)
                    print(f"✅ Временный файл удален")
                except Exception as e:
                    print(f"⚠️ Не удалось удалить временный файл: {e}")

                return True

            except Exception as e:
                logger.exception(f'Ошибка скачивания: {e}')
                print(f"❌ Ошибка скачивания: {e}")
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
                    logger.error(f"Ошибка скачивания: {e}")
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
            logger.error(f"Таймаут скачивания")
            return None
        except Exception as e:
            logger.exception(f'Ошибка скачивания: {e}')
            return None
        finally:
            # Очистка временной директории (кроме скачанного файла)
            pass

    # ==================== КОМАНДЫ ДЛЯ ЛИЧНЫХ СООБЩЕНИЙ ====================

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start для личных сообщений"""
        await update.message.reply_text(
            "🎵 <b>Музыкальный бот для канала</b>\n\n"
            "📢 <b>Как использовать:</b>\n"
            "• Напиши в канал: <code>найди [трек]</code> - найти трек\n"
            "• Напиши в канал: <code>рандом</code> - случайный трек\n"
            "• Любое сообщение в канале - случайный трек\n\n"
            "💡 <b>Примеры:</b>\n"
            "<code>найди coldplay adventure</code>\n"
            "<code>рандом</code>\n"
            "<code>привет</code> (отправит случайный трек)",
            parse_mode='HTML'
        )

    # ==================== ЗАПУСК БОТА ====================

    def run(self):
        print('🚀 Запуск Music Bot для канала...')
        print(f'📢 Бот будет отслеживать сообщения в канале: {CHANNEL_ID}')
        print(f'🔧 Тип CHANNEL_ID: {type(CHANNEL_ID)}')

        app = Application.builder().token(BOT_TOKEN).build()

        # Обработчик сообщений в КАНАЛЕ - ВАЖНО: без фильтра по командам
        app.add_handler(MessageHandler(
            filters.Chat(chat_id=CHANNEL_ID) & filters.TEXT,
            self.handle_channel_message
        ))

        # Команды для ЛИЧНЫХ сообщений
        app.add_handler(CommandHandler('start', self.start_command))

        print('✅ Бот запущен!')
        print('💡 Теперь можно писать сообщения в канал:')
        print('   • "найди coldplay" - поиск трека')
        print('   • "рандом" - случайный трек') 
        print('   • Любое сообщение - случайный трек')
        print('📋 Логи будут выводиться в консоль Railway')
        
        app.run_polling()

if __name__ == '__main__':
    bot = ChannelMusicBot()
    bot.run()
