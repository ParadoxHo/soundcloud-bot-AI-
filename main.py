import os
import io
import logging
import asyncio
import yt_dlp
import requests
import psycopg2
from psycopg2.extras import Json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_IDS = [int(x.strip()) for x in os.environ.get('ADMIN_IDS', '').split(',') if x.strip()]
DATABASE_URL = os.environ.get('DATABASE_URL')
PORT = int(os.environ.get('PORT', 8080))

class FreeUploadManager:
    """Менеджер для загрузки файлов на бесплатные хостинги"""
    
    def __init__(self):
        self.services = [
            self._upload_fileio,      # 2GB, 14 дней
            self._upload_transfersh,  # 10GB, 14 дней
        ]
    
    async def upload_file(self, file_data: bytes, filename: str) -> str:
        """Пробуем загрузить файл на все доступные сервисы"""
        for service in self.services:
            try:
                url = await service(file_data, filename)
                if url:
                    logger.info(f"Файл успешно загружен через {service.__name__}")
                    return url
            except Exception as e:
                logger.warning(f"Ошибка загрузки через {service.__name__}: {e}")
                continue
        raise Exception("Не удалось загрузить файл ни на один сервис")
    
    async def _upload_fileio(self, file_data: bytes, filename: str) -> str:
        """Загрузка на file.io (2GB, 14 дней)"""
        response = requests.post(
            'https://file.io',
            files={'file': (filename, file_data)},
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data['link']
        return None
    
    async def _upload_transfersh(self, file_data: bytes, filename: str) -> str:
        """Загрузка на transfer.sh (10GB, 14 дней)"""
        response = requests.put(
            f'https://transfer.sh/{filename}',
            data=file_data,
            timeout=30,
            headers={'Content-Type': 'application/octet-stream'}
        )
        if response.status_code == 200:
            return response.text.strip()
        return None

class DatabaseManager:
    """Управление базой данных PostgreSQL"""
    
    def __init__(self):
        self.conn = None
        self._memory_cache = {}
        self._cache_ttl = 300  # 5 минут кэш
        self._initialized = False
        
    def get_connection(self):
        """Получает соединение с БД с обработкой ошибок"""
        if self.conn is None or self.conn.closed:
            try:
                if DATABASE_URL:
                    self.conn = psycopg2.connect(DATABASE_URL, sslmode='require')
                    logger.info("✅ Успешное подключение к PostgreSQL")
                else:
                    logger.warning("❌ DATABASE_URL не установлен")
                    return None
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}")
                return None
        return self.conn
    
    def init_db(self):
        """Инициализирует базу данных с обработкой ошибок"""
        try:
            conn = self.get_connection()
            if conn is None:
                logger.warning("❌ Пропускаем инициализацию БД - нет подключения")
                return False
                
            cur = conn.cursor()
            
            # Таблица пользовательских данных
            cur.execute('''
                CREATE TABLE IF NOT EXISTS user_data (
                    user_id TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            # Таблица кэша чартов
            cur.execute('''
                CREATE TABLE IF NOT EXISTS charts_cache (
                    cache_key TEXT PRIMARY KEY,
                    data JSONB NOT NULL,
                    last_update TIMESTAMP DEFAULT NOW()
                )
            ''')
            
            conn.commit()
            self._initialized = True
            logger.info("✅ База данных инициализирована")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            return False
    
    def get_user_data(self, user_id: str) -> dict:
        """Получает данные пользователя с fallback на память"""
        # Если БД не работает, используем только память
        if not self._initialized:
            cache_key = f"user_{user_id}"
            if cache_key in self._memory_cache:
                return self._memory_cache[cache_key][0].copy()
            return {}
        
        # Проверяем кэш
        cache_key = f"user_{user_id}"
        if cache_key in self._memory_cache:
            cached_data, timestamp = self._memory_cache[cache_key]
            if (datetime.now() - timestamp).total_seconds() < self._cache_ttl:
                return cached_data.copy()
        
        try:
            conn = self.get_connection()
            if conn is None:
                return {}
                
            cur = conn.cursor()
            cur.execute('SELECT data FROM user_data WHERE user_id = %s', (user_id,))
            result = cur.fetchone()
            
            user_data = result[0] if result else {}
            
            # Сохраняем в кэш
            self._memory_cache[cache_key] = (user_data.copy(), datetime.now())
            
            return user_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных пользователя: {e}")
            return {}
    
    def save_user_data(self, user_id: str, data: dict):
        """Сохраняет данные пользователя с fallback на память"""
        # Всегда сохраняем в кэш
        cache_key = f"user_{user_id}"
        self._memory_cache[cache_key] = (data.copy(), datetime.now())
        
        # Пытаемся сохранить в БД, если она доступна
        if not self._initialized:
            return
            
        try:
            conn = self.get_connection()
            if conn is None:
                return
                
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO user_data (user_id, data) 
                VALUES (%s, %s)
                ON CONFLICT (user_id) 
                DO UPDATE SET data = %s, updated_at = NOW()
            ''', (user_id, Json(data), Json(data)))
            conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения данных пользователя: {e}")
    
    def get_charts_cache(self, key: str) -> dict:
        """Получает кэш чартов"""
        if not self._initialized:
            return {}
            
        try:
            conn = self.get_connection()
            if conn is None:
                return {}
                
            cur = conn.cursor()
            cur.execute('SELECT data FROM charts_cache WHERE cache_key = %s', (key,))
            result = cur.fetchone()
            return result[0] if result else {}
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения кэша чартов: {e}")
            return {}
    
    def save_charts_cache(self, key: str, data: dict):
        """Сохраняет кэш чартов"""
        if not self._initialized:
            return
            
        try:
            conn = self.get_connection()
            if conn is None:
                return
                
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO charts_cache (cache_key, data) 
                VALUES (%s, %s)
                ON CONFLICT (cache_key) 
                DO UPDATE SET data = %s, last_update = NOW()
            ''', (key, Json(data), Json(data)))
            conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения кэша чартов: {e}")

class MusicBot:
    def __init__(self):
        self.db = DatabaseManager()
        self.uploader = FreeUploadManager()
        self.application = None
        
        # Инициализация базы данных (не блокирующая)
        try:
            self.db.init_db()
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
            logger.info("🔄 Бот продолжит работу с in-memory хранилищем")
        
    async def ensure_user(self, user_id: str):
        """Создает пользователя если не существует"""
        user_data = self.db.get_user_data(user_id)
        if not user_data:
            default_data = {
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
                'preferences': {'favorite_genres': [], 'disliked_genres': []},
                'stats': {
                    'searches': 0,
                    'downloads': 0,
                    'first_seen': datetime.now().strftime('%d.%m.%Y %H:%M'),
                    'last_search': None,
                    'last_download': None
                }
            }
            self.db.save_user_data(user_id, default_data)
            return default_data
        return user_data
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        user_id = str(update.effective_user.id)
        await self.ensure_user(user_id)
        
        welcome_text = (
            "🎵 Добро пожаловать в Music Bot!\n\n"
            "Я помогу тебе найти и скачать музыку с SoundCloud.\n\n"
            "🔍 <b>Основные команды:</b>\n"
            "/search - поиск музыки\n"
            "/favorites - избранные треки\n" 
            "/history - история загрузок\n"
            "/stats - твоя статистика\n"
            "/help - помощь\n\n"
            "Просто отправь мне название трека или исполнителя!"
        )
        
        await update.message.reply_text(welcome_text, parse_mode='HTML')
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /help"""
        help_text = (
            "🎵 <b>Music Bot - Помощь</b>\n\n"
            "🔍 <b>Поиск музыки:</b>\n"
            "• Просто отправь название трека или исполнителя\n"
            "• Используй /search для расширенного поиска\n"
            "• Ищем только на SoundCloud\n\n"
            "💾 <b>Скачивание:</b>\n"
            "• Найди трек через поиск\n"
            "• Нажми кнопку 'Скачать'\n"
            "• Получи файл в высоком качестве\n\n"
            "⭐ <b>Избранное:</b>\n"
            "• Сохраняй треки в избранное кнопкой '⭐'\n"
            "• Смотри список: /favorites\n\n"
            "📊 <b>Статистика:</b>\n"
            "• /stats - твоя статистика\n"
            "• /history - история загрузок\n"
        )
        await update.message.reply_text(help_text, parse_mode='HTML')
    
    async def handle_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик поисковых запросов"""
        user_id = str(update.effective_user.id)
        query = update.message.text.strip()
        
        if not query:
            await update.message.reply_text("Пожалуйста, введите поисковый запрос")
            return
        
        # Показываем быстрый статус
        search_msg = await update.message.reply_text("🔍 Ищу на SoundCloud...")
        
        try:
            # Получаем данные пользователя
            user_data = await self.ensure_user(user_id)
            
            # Выполняем поиск
            search_results = await self._perform_soundcloud_search(query)
            
            if not search_results:
                await search_msg.edit_text("❌ По вашему запросу ничего не найдено на SoundCloud")
                return
            
            # Сохраняем результаты
            user_data['search_results'] = search_results
            user_data['search_query'] = query
            user_data['current_page'] = 0
            user_data['total_pages'] = (len(search_results) + 4) // 5
            user_data['stats']['searches'] += 1
            user_data['stats']['last_search'] = datetime.now().isoformat()
            
            # Добавляем в историю поиска
            if query not in user_data['search_history']:
                user_data['search_history'].insert(0, query)
                user_data['search_history'] = user_data['search_history'][:50]
            
            self.db.save_user_data(user_id, user_data)
            
            # Показываем результаты
            await self._show_search_results(update, user_data, search_msg)
            
        except Exception as e:
            logger.error(f"Ошибка поиска на SoundCloud: {e}")
            await search_msg.edit_text("❌ Произошла ошибка при поиске. Попробуйте другой запрос.")
    
    async def _perform_soundcloud_search(self, query: str) -> list:
        """Выполняет поиск на SoundCloud"""
        ydl_opts = {
            'format': 'bestaudio/best',
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"scsearch10:{query}", download=False)
                
                if 'entries' in info:
                    results = []
                    for entry in info['entries']:
                        if entry:
                            results.append({
                                'id': entry.get('id', entry.get('url', '')),
                                'title': entry.get('title', 'Без названия'),
                                'uploader': entry.get('uploader', 'Неизвестный артист'),
                                'duration': entry.get('duration', 0),
                                'webpage_url': entry.get('url', entry.get('webpage_url', '')),
                                'thumbnail': entry.get('thumbnail', ''),
                            })
                    return results
        except Exception as e:
            logger.error(f"Ошибка SoundCloud поиска: {e}")
        
        return []
    
    async def _show_search_results(self, update: Update, user_data: dict, search_msg=None):
        """Показывает страницу с результатами поиска"""
        user_id = str(update.effective_user.id)
        current_page = user_data['current_page']
        results = user_data['search_results']
        total_pages = user_data['total_pages']
        
        start_idx = current_page * 5
        end_idx = min(start_idx + 5, len(results))
        page_results = results[start_idx:end_idx]
        
        text = f"🔍 Результаты поиска: <b>{user_data['search_query']}</b>\n"
        text += f"📄 Страница {current_page + 1} из {total_pages}\n"
        text += f"🎧 <i>Поиск по SoundCloud</i>\n\n"
        
        keyboard = []
        
        for i, result in enumerate(page_results, start=1):
            idx = start_idx + i
            duration = self._format_duration(result.get('duration', 0))
            
            text += f"{idx}. <b>{result['title']}</b>\n"
            text += f"   👤 {result['uploader']} | ⏱ {duration}\n\n"
            
            keyboard.append([
                InlineKeyboardButton(
                    f"🎵 {i}. Скачать", 
                    callback_data=f"download:{result['id']}"
                ),
                InlineKeyboardButton(
                    f"⭐", 
                    callback_data=f"favorite:{result['id']}"
                )
            ])
        
        # Кнопки навигации
        nav_buttons = []
        if current_page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="search_prev"))
        
        if current_page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data="search_next"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔍 Новый поиск", callback_data="new_search")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if search_msg:
            await search_msg.edit_text(text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
    
    def _format_duration(self, seconds: int) -> str:
        """Форматирует длительность в читаемый вид"""
        if not seconds:
            return "?:??"
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик callback запросов"""
        query = update.callback_query
        await query.answer()
        
        user_id = str(update.effective_user.id)
        user_data = self.db.get_user_data(user_id)
        callback_data = query.data
        
        try:
            if callback_data.startswith('download:'):
                track_id = callback_data.split(':')[1]
                await self._download_track(update, context, track_id)
                
            elif callback_data.startswith('favorite:'):
                track_id = callback_data.split(':')[1]
                await self._toggle_favorite(update, track_id)
                
            elif callback_data == 'search_prev':
                user_data['current_page'] -= 1
                self.db.save_user_data(user_id, user_data)
                await self._show_search_results(update, user_data)
                
            elif callback_data == 'search_next':
                user_data['current_page'] += 1
                self.db.save_user_data(user_id, user_data)
                await self._show_search_results(update, user_data)
                
            elif callback_data == 'new_search':
                await query.edit_message_text("🔍 Введите поисковый запрос для SoundCloud:")
                
        except Exception as e:
            logger.error(f"Ошибка обработки callback: {e}")
            await query.edit_message_text("❌ Произошла ошибка. Попробуйте еще раз.")
    
    async def _download_track(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track_id: str):
        """Скачивает и отправляет трек с SoundCloud"""
        user_id = str(update.effective_user.id)
        user_data = self.db.get_user_data(user_id)
        
        # Находим трек в результатах поиска
        track = None
        for result in user_data['search_results']:
            if result['id'] == track_id:
                track = result
                break
        
        if not track:
            await update.callback_query.edit_message_text("❌ Трек не найден")
            return
        
        # Показываем статус скачивания
        status_msg = await update.callback_query.message.reply_text("⬇️ Подготовка трека...")
        
        try:
            await status_msg.edit_text("⬇️ Скачивание...")
            await self._download_file(update, context, track, status_msg)
            
            # Обновляем статистику
            user_data['stats']['downloads'] += 1
            user_data['stats']['last_download'] = datetime.now().isoformat()
            
            # Добавляем в историю загрузок
            download_record = {
                'title': track['title'],
                'artist': track['uploader'],
                'url': track['webpage_url'],
                'downloaded_at': datetime.now().isoformat(),
                'source': 'SoundCloud'
            }
            user_data['download_history'].insert(0, download_record)
            user_data['download_history'] = user_data['download_history'][:100]
            
            self.db.save_user_data(user_id, user_data)
            
        except Exception as e:
            logger.error(f"Ошибка скачивания с SoundCloud: {e}")
            await status_msg.edit_text("❌ Ошибка при скачивании. Попробуйте другой трек.")
    
    async def _download_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE, track: dict, status_msg):
        """Универсальное скачивание файла"""
        try:
            ydl_opts = {
                'format': 'bestaudio[ext=m4a]/bestaudio',
                'outtmpl': '-',
                'quiet': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                audio_data = ydl.extract_info(track['webpage_url'], download=True)
                
                # Пробуем отправить напрямую
                try:
                    audio_buffer = io.BytesIO(audio_data)
                    await status_msg.edit_text("⬇️ Отправка...")
                    await context.bot.send_audio(
                        chat_id=update.effective_chat.id,
                        audio=audio_buffer,
                        title=track['title'][:64],
                        performer=track.get('uploader', 'Unknown')[:64],
                        filename=f"{track['title'][:50]}.m4a"
                    )
                    await status_msg.delete()
                    
                except Exception as e:
                    # Если прямой отправкой не получилось, используем облако
                    logger.info("Прямая отправка не удалась, используем облако...")
                    await status_msg.edit_text("⬇️ Используем облако...")
                    
                    filename = f"{track['title'][:50]}.m4a"
                    file_url = await self.uploader.upload_file(audio_data, filename)
                    
                    await context.bot.send_audio(
                        chat_id=update.effective_chat.id,
                        audio=file_url,
                        title=track['title'][:64],
                        performer=track.get('uploader', 'Unknown')[:64]
                    )
                    await status_msg.delete()
                    
        except Exception as e:
            await status_msg.edit_text("❌ Ошибка при скачивании файла")
            raise e
    
    async def _toggle_favorite(self, update: Update, track_id: str):
        """Добавляет/убирает трек из избранного"""
        user_id = str(update.effective_user.id)
        user_data = self.db.get_user_data(user_id)
        
        # Находим трек
        track = None
        for result in user_data['search_results']:
            if result['id'] == track_id:
                track = result
                break
        
        if not track:
            await update.callback_query.edit_message_text("❌ Трек не найден")
            return
        
        # Проверяем, есть ли уже в избранном
        favorite_ids = [fav['id'] for fav in user_data['favorites']]
        
        if track_id in favorite_ids:
            # Удаляем из избранного
            user_data['favorites'] = [fav for fav in user_data['favorites'] if fav['id'] != track_id]
            message = "❌ Удалено из избранного"
        else:
            # Добавляем в избранное
            user_data['favorites'].append(track)
            message = "⭐ Добавлено в избранное"
        
        self.db.save_user_data(user_id, user_data)
        await update.callback_query.edit_message_text(message)
    
    async def show_favorites(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает избранные треки"""
        user_id = str(update.effective_user.id)
        user_data = await self.ensure_user(user_id)
        
        favorites = user_data.get('favorites', [])
        
        if not favorites:
            await update.message.reply_text("⭐ У вас пока нет избранных треков")
            return
        
        text = "⭐ <b>Ваши избранные треки с SoundCloud:</b>\n\n"
        keyboard = []
        
        for i, track in enumerate(favorites, 1):
            duration = self._format_duration(track.get('duration', 0))
            text += f"{i}. <b>{track['title']}</b>\n"
            text += f"   👤 {track['uploader']} | ⏱ {duration}\n\n"
            
            keyboard.append([
                InlineKeyboardButton(f"🎵 {i}. Скачать", callback_data=f"download:{track['id']}"),
                InlineKeyboardButton("❌ Удалить", callback_data=f"remove_favorite:{track['id']}")
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
    
    async def show_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает историю загрузок"""
        user_id = str(update.effective_user.id)
        user_data = await self.ensure_user(user_id)
        
        history = user_data.get('download_history', [])
        
        if not history:
            await update.message.reply_text("📥 У вас пока нет истории загрузок")
            return
        
        text = "📥 <b>История загрузок с SoundCloud:</b>\n\n"
        
        for i, record in enumerate(history[:10], 1):
            downloaded_at = datetime.fromisoformat(record['downloaded_at']).strftime('%d.%m.%Y %H:%M')
            text += f"{i}. <b>{record['title']}</b>\n"
            text += f"   👤 {record['artist']}\n"
            text += f"   📅 {downloaded_at}\n\n"
        
        if len(history) > 10:
            text += f"<i>... и еще {len(history) - 10} загрузок</i>"
        
        await update.message.reply_text(text, parse_mode='HTML')
    
    async def show_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает статистику пользователя"""
        user_id = str(update.effective_user.id)
        user_data = await self.ensure_user(user_id)
        
        stats = user_data.get('stats', {})
        
        text = "📊 <b>Ваша статистика:</b>\n\n"
        text += f"🔍 Поисков: <b>{stats.get('searches', 0)}</b>\n"
        text += f"💾 Загрузок: <b>{stats.get('downloads', 0)}</b>\n"
        text += f"⭐ Избранных треков: <b>{len(user_data.get('favorites', []))}</b>\n"
        text += f"🎧 <i>Все треки с SoundCloud</i>\n"
        
        if stats.get('first_seen'):
            text += f"🎯 С нами с: <b>{stats['first_seen']}</b>\n"
        
        await update.message.reply_text(text, parse_mode='HTML')
    
    def run_bot(self):
        """Запускает бота (синхронная версия для Railway)"""
        try:
            # Создаем и запускаем event loop вручную
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Запускаем бота
            loop.run_until_complete(self._run_async())
        except Exception as e:
            logger.error(f"❌ Критическая ошибка: {e}")
    
    async def _run_async(self):
        """Асинхронная версия запуска бота"""
        self.application = Application.builder().token(BOT_TOKEN).build()
        
        # Добавляем обработчики
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("favorites", self.show_favorites))
        self.application.add_handler(CommandHandler("history", self.show_history))
        self.application.add_handler(CommandHandler("stats", self.show_stats))
        
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_search))
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
        
        # Проверяем наличие переменных окружения
        if not BOT_TOKEN:
            logger.error("❌ BOT_TOKEN не установлен!")
            return
        
        logger.info("🚀 Запуск бота...")
        
        # Запуск в зависимости от окружения
        if os.environ.get('RAILWAY_ENVIRONMENT'):
            # Webhook для Railway
            public_domain = os.environ.get('RAILWAY_PUBLIC_DOMAIN')
            if not public_domain:
                logger.error("❌ RAILWAY_PUBLIC_DOMAIN не установлен!")
                return
            
            webhook_url = f"https://{public_domain}"
            
            await self.application.run_webhook(
                listen="0.0.0.0",
                port=PORT,
                url_path=BOT_TOKEN,
                webhook_url=f"{webhook_url}/{BOT_TOKEN}",
                drop_pending_updates=True
            )
        else:
            # Polling для локальной разработки
            await self.application.run_polling(drop_pending_updates=True)

# Запуск бота
if __name__ == '__main__':
    bot = MusicBot()
    bot.run_bot()
