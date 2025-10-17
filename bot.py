# bot.py

import os
import logging
import threading
import time
from datetime import datetime, time as dt_time, timedelta
import json
import pytz
import requests
from telegram import Update, ParseMode, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackContext, Job, ConversationHandler, MessageHandler, Filters, CallbackQueryHandler
from telegram.error import Conflict, BadRequest
import html
from http.server import BaseHTTPRequestHandler, HTTPServer

# ✅ ИМПОРТ GOOGLE SHEETS ИНТЕГРАЦИИ
try:
    from sheets_integration import SheetsManager
    sheets_manager = SheetsManager()
    SHEETS_AVAILABLE = True
    logger_temp = logging.getLogger(__name__)
    logger_temp.info("✅ Google Sheets integration loaded successfully")
except Exception as e:
    sheets_manager = None
    SHEETS_AVAILABLE = False
    logger_temp = logging.getLogger(__name__)
    logger_temp.warning(f"📵 Google Sheets integration not available: {e}")

# Константа для московского времени
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Время запуска бота
BOT_START_TIME = None

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')

    def do_HEAD(self):
        # Respond to health check HEAD requests
        self.send_response(200)
        self.end_headers()

def start_health_server():
    port = int(os.environ.get('PORT', 5000))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    server.serve_forever()

# --- Глобальный файл напоминаний ---
REMINDERS_FILE = "reminders.json"
POLLS_FILE = "polls.json"

logging.basicConfig(
    format="%(asctime)s — %(levelname)s — %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_moscow_time():
    """Получить текущее московское время"""
    return datetime.now(MOSCOW_TZ)

def moscow_time_to_utc(moscow_dt):
    """Конвертировать московское время в UTC"""
    if isinstance(moscow_dt, str):
        # Если строка, парсим ее как московское время
        naive_dt = datetime.strptime(moscow_dt, "%Y-%m-%d %H:%M")
        moscow_dt = MOSCOW_TZ.localize(naive_dt)
    elif moscow_dt.tzinfo is None:
        # Если naive datetime, считаем его московским
        moscow_dt = MOSCOW_TZ.localize(moscow_dt)
    
    return moscow_dt.astimezone(pytz.UTC)

def utc_to_moscow_time(utc_dt):
    """Конвертировать UTC время в московское"""
    if utc_dt.tzinfo is None:
        utc_dt = pytz.UTC.localize(utc_dt)
    return utc_dt.astimezone(MOSCOW_TZ)

def format_moscow_time(dt):
    """Форматировать время для отображения пользователю"""
    if isinstance(dt, str):
        return dt
    moscow_dt = utc_to_moscow_time(dt) if dt.tzinfo else MOSCOW_TZ.localize(dt)
    return moscow_dt.strftime("%Y-%m-%d %H:%M MSK")

def error_handler(update: Update, context: CallbackContext):
    """
    Handle errors by logging them without crashing the bot.
    Enhanced conflict handling with automatic restart mechanism.
    """
    if isinstance(context.error, Conflict):
        logger.error("🚨 CRITICAL: Bot conflict detected - multiple instances running!")
        logger.error("   This blocks ALL scheduled tasks (reminders, polls, etc.)")
        logger.error("   Conflict details: {}".format(str(context.error)))
        logger.error("   Attempting aggressive conflict resolution...")
        
        # Попытка агрессивного разрешения конфликта
        try:
            # Останавливаем текущий updater
            if hasattr(context, 'dispatcher') and hasattr(context.dispatcher, 'updater'):
                updater = context.dispatcher.updater
                logger.warning("🔄 Stopping current updater to resolve conflict...")
                updater.stop()
                
                # Пауза для завершения предыдущего экземпляра
                time.sleep(5)
                
                # Принудительно удаляем webhook и перезапускаем polling
                try:
                    updater.bot.delete_webhook(drop_pending_updates=True)
                    logger.info("✅ Webhook deleted during conflict resolution")
                except Exception as e:
                    logger.warning(f"⚠️ Could not delete webhook: {e}")
                
                # Перезапуск с увеличенным таймаутом
                time.sleep(3)
                updater.start_polling(drop_pending_updates=True, timeout=15, read_latency=10)
                logger.info("🚀 Bot restarted after conflict resolution")
                
        except Exception as restart_error:
            logger.error(f"❌ Failed to restart bot after conflict: {restart_error}")
            logger.error("   Manual intervention may be required on Render platform")
        
        return
    elif isinstance(context.error, BadRequest):
        logger.warning(f"⚠️ Bad request: {context.error}")
        return
    
    logger.error("❌ Uncaught exception:", exc_info=context.error)

def subscribe_chat(chat_id, chat_name="Unknown", chat_type="private", members_count=None):
    try:
        with open("subscribed_chats.json", "r") as f:
            data = f.read().strip()
            chats = json.loads(data) if data else []
    except (FileNotFoundError, json.JSONDecodeError):
        chats = []

    # Проверяем, является ли чат новым
    is_new_chat = chat_id not in chats
    
    if is_new_chat:
        chats.append(chat_id)
        save_chats(chats)
        logger.info(f"🆕 New chat subscribed: {chat_id} ({chat_name})")
        
        # ✅ МГНОВЕННАЯ ЗАПИСЬ В GOOGLE SHEETS
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                # Обновляем статистику чата
                sheets_manager.update_chat_stats(chat_id, chat_name, chat_type, members_count)
                
                # Логируем действие подписки
                moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                sheets_manager.log_operation(
                    timestamp=moscow_time,
                    action="CHAT_SUBSCRIBE",
                    user_id="SYSTEM",
                    username="AutoSubscribe",
                    chat_id=chat_id,
                    details=f"New chat subscribed: {chat_name} ({chat_type}), Members: {members_count or 'N/A'}",
                    reminder_id=""
                )
                
                # Обновляем список подписанных чатов в Google Sheets
                sheets_manager.sync_subscribed_chats_to_sheets(chats)
                
                logger.info(f"📊 Successfully synced new chat {chat_id} to Google Sheets")
                
            except Exception as e:
                logger.error(f"❌ Error syncing new chat to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - chat {chat_id} subscription not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        else:
            logger.warning("📵 Google Sheets not available for new chat sync")
    else:
        # Если чат уже существует, обновляем его информацию
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                sheets_manager.update_chat_stats(chat_id, chat_name, chat_type, members_count)
                logger.info(f"📊 Updated existing chat {chat_id} info in Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error updating chat info in Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - chat {chat_id} info not updated")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")

def save_chats(chats):
    with open("subscribed_chats.json", "w") as f:
        json.dump(chats, f)

# Функция ping для предотвращения засыпания на Render
def ping_self(context: CallbackContext):
    """
    Пингует сам себя чтобы не засыпать на Render free tier
    """
    try:
        base_url = os.environ.get('BASE_URL', 'https://telegram-repeat-bot.onrender.com')
        response = requests.get(base_url, timeout=5)
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        logger.info(f"Self-ping successful at {moscow_time}: {response.status_code}")
    except Exception as e:
        logger.warning(f"Self-ping failed: {e}")

def safe_html_escape(text):
    """
    Безопасно экранирует HTML, сохраняя корректные теги
    """
    if not text:
        return ""
    
    # Список разрешенных HTML тегов
    allowed_tags = ['<b>', '</b>', '<i>', '</i>', '<u>', '</u>', '<s>', '</s>', '<code>', '</code>', '<pre>', '</pre>']
    
    # Простая проверка на корректность HTML
    try:
        # Проверяем, что в тексте нет пустых атрибутов в тегах <a>
        if '<a ' in text and 'href=""' in text:
            # Удаляем пустые ссылки
            text = text.replace('<a href="">', '').replace('</a>', '')
        
        # Проверяем корректность других тегов
        if '<' in text and '>' in text:
            # Если есть HTML теги, возвращаем как есть
            return text
        else:
            # Если нет HTML тегов, экранируем
            return html.escape(text)
    except:
        # В случае ошибки возвращаем экранированный текст
        return html.escape(text)

# --- /start и /test команды ---
def start(update: Update, context: CallbackContext):
    """
    Обработчик команды /start.
    """
    try:
        chat_id = update.effective_chat.id
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        logger.info(f"Received /start from chat {chat_id} at {moscow_time}")
        
        # Получаем информацию о чате
        chat = update.effective_chat
        chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
        chat_type = chat.type
        members_count = None
        try:
            if chat_type in ["group", "supergroup"]:
                members_count = context.bot.get_chat_members_count(chat_id)
        except:
            pass
        
        subscribe_chat(chat_id, chat_name, chat_type, members_count)
        context.bot.send_message(chat_id=chat_id,
                                 text="✅ <b>Бот активирован в этом чате</b>\n⏰ <i>Время работы: московское (MSK)</i>",
                                 parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        try:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                   text="✅ Бот активирован в этом чате\n⏰ Время работы: московское (MSK)")
        except:
            pass

def test(update: Update, context: CallbackContext):
    """
    Обработчик команды /test для проверки работы бота.
    """
    try:
        chat_id = update.effective_chat.id
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        logger.info(f"Received /test from chat {chat_id} at {moscow_time}")
        
        # Получаем информацию о чате
        chat = update.effective_chat
        chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
        chat_type = chat.type
        members_count = None
        try:
            if chat_type in ["group", "supergroup"]:
                members_count = context.bot.get_chat_members_count(chat_id)
        except:
            pass
        
        subscribe_chat(chat_id, chat_name, chat_type, members_count)
        current_time = get_moscow_time().strftime("%Y-%m-%d %H:%M MSK")
        context.bot.send_message(chat_id=chat_id,
                                 text=f"✅ <b>Бот работает корректно!</b>\n⏰ <i>Текущее время: {current_time}</i>",
                                 parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error in test command: {e}")
        try:
            current_time = get_moscow_time().strftime("%Y-%m-%d %H:%M MSK")
            context.bot.send_message(chat_id=update.effective_chat.id,
                                   text=f"✅ Бот работает корректно!\n⏰ Текущее время: {current_time}")
        except:
            pass

# --- Константы для ConversationHandler состояний ---
REMINDER_DATE, REMINDER_TEXT = range(2)
DAILY_TIME, DAILY_TEXT = range(2)
WEEKLY_DAY, WEEKLY_TIME, WEEKLY_TEXT = range(3)
REM_DEL_ID = 0

# --- Константы для голосований ---
POLL_DATE, POLL_QUESTION, POLL_OPTIONS = range(3)
DAILY_POLL_TIME, DAILY_POLL_QUESTION, DAILY_POLL_OPTIONS = range(3)
WEEKLY_POLL_DAY, WEEKLY_POLL_TIME, WEEKLY_POLL_QUESTION, WEEKLY_POLL_OPTIONS = range(4)
POLL_DEL_ID = 0

# --- Вспомогательные функции для хранения напоминаний (глобальный список) ---
def load_reminders():
    """
    Load reminders from the JSON file, returning an empty list if the file is missing,
    empty, or contains invalid JSON.
    """
    try:
        with open(REMINDERS_FILE, "r", encoding='utf-8') as f:
            data = f.read().strip()
            if not data:
                return []
            return json.loads(data)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_reminders(reminders):
    try:
        with open(REMINDERS_FILE, "w", encoding='utf-8') as f:
            json.dump(reminders, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Error saving reminders: {e}")

def get_next_reminder_id():
    """
    Генерирует следующий ID для напоминания
    """
    try:
        reminders = load_reminders()
        if not reminders:
            return "1"
        
        # Найти максимальный ID и добавить 1
        max_id = 0
        for reminder in reminders:
            try:
                reminder_id = int(reminder.get("id", "0"))
                if reminder_id > max_id:
                    max_id = reminder_id
            except ValueError:
                continue
        
        return str(max_id + 1)
    except Exception as e:
        logger.error(f"Error generating reminder ID: {e}")
        return "1"

# --- Вспомогательные функции для хранения голосований ---
def load_polls():
    """
    Загружает голосования из JSON файла, возвращая пустой список если файл отсутствует,
    пустой или содержит невалидный JSON.
    """
    try:
        with open(POLLS_FILE, "r", encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                logger.info("Polls file is empty, returning empty list")
                return []
            polls = json.loads(content)
            logger.info(f"Loaded {len(polls)} polls from {POLLS_FILE}")
            return polls
    except FileNotFoundError:
        logger.info(f"Polls file {POLLS_FILE} not found, returning empty list")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {POLLS_FILE}: {e}. Returning empty list.")
        return []
    except Exception as e:
        logger.error(f"Error loading polls: {e}")
        return []

def save_polls(polls):
    """Сохраняет голосования в JSON файл"""
    try:
        with open(POLLS_FILE, "w", encoding='utf-8') as f:
            json.dump(polls, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(polls)} polls to {POLLS_FILE}")
    except Exception as e:
        logger.error(f"Error saving polls: {e}")

def get_next_poll_id():
    """
    Генерирует следующий ID для голосования
    """
    try:
        polls = load_polls()
        
        # Также проверяем Google Sheets для получения максимального ID
        sheets_max_id = 0
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                sheets_max_id = sheets_manager.get_max_poll_id()
            except Exception as e:
                logger.warning(f"Could not get max ID from sheets: {e}")
        
        # Найти максимальный ID из локальных данных
        local_max_id = 0
        if polls:
            for poll in polls:
                try:
                    poll_id = int(poll.get("id", "0"))
                    if poll_id > local_max_id:
                        local_max_id = poll_id
                except ValueError:
                    continue
        
        # Используем максимальный ID из обеих источников
        max_id = max(local_max_id, sheets_max_id)
        return str(max_id + 1)
        
    except Exception as e:
        logger.error(f"Error generating poll ID: {e}")
        return "1"

# --- Обработчики добавления разового напоминания ---
def start_add_one_reminder(update: Update, context: CallbackContext):
    try:
        current_time = get_moscow_time().strftime("%Y-%m-%d %H:%M MSK")
        update.message.reply_text(f"📅 <b>Разовое напоминание</b>\n\nВведите дату и время в формате ГГГГ-ММ-ДД ЧЧ:ММ\nНапример: 2024-07-10 16:30\n\n<i>⏰ Сейчас: {current_time}</i>", parse_mode=ParseMode.HTML)
        return REMINDER_DATE
    except Exception as e:
        logger.error(f"Error in start_add_one_reminder: {e}")
        current_time = get_moscow_time().strftime("%Y-%m-%d %H:%M MSK")
        update.message.reply_text(f"📅 Разовое напоминание\n\nВведите дату и время в формате ГГГГ-ММ-ДД ЧЧ:ММ\nНапример: 2024-07-10 16:30\n\n⏰ Сейчас: {current_time}")
        return REMINDER_DATE

def receive_reminder_datetime(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        # Парсим введенное время как московское
        moscow_dt = datetime.strptime(text, "%Y-%m-%d %H:%M")
        moscow_dt = MOSCOW_TZ.localize(moscow_dt)
        
        # Проверяем, что время в будущем
        if moscow_dt < get_moscow_time():
            try:
                update.message.reply_text("⚠️ <b>Ошибка:</b> Дата и время уже прошли.\nВведите корректную дату и время в московском времени:", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("⚠️ Ошибка: Дата и время уже прошли.\nВведите корректную дату и время в московском времени:")
            return REMINDER_DATE
        
        context.user_data["reminder_datetime"] = text
        context.user_data["reminder_datetime_moscow"] = moscow_dt
        try:
            update.message.reply_text("✏️ <b>Текст напоминания</b>\n\nВведите текст напоминания (поддерживаются HTML теги и ссылки):", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("✏️ Текст напоминания\n\nВведите текст напоминания:")
        return REMINDER_TEXT
    except Exception:
        try:
            update.message.reply_text("❌ <b>Некорректный формат</b>\n\nВведите дату и время в формате ГГГГ-ММ-ДД ЧЧ:ММ (московское время):", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Некорректный формат\n\nВведите дату и время в формате ГГГГ-ММ-ДД ЧЧ:ММ (московское время):")
        return REMINDER_DATE

def receive_reminder_text(update: Update, context: CallbackContext):
    try:
        reminders = load_reminders()
        new_id = get_next_reminder_id()
        reminder_text = update.message.text_html if update.message.text_html else update.message.text.strip()
        
        # Безопасно обрабатываем HTML
        reminder_text = safe_html_escape(reminder_text)
        
        reminders.append({
            "id": new_id,
            "type": "once",
            "datetime": context.user_data["reminder_datetime"],
            "text": reminder_text
        })
        save_reminders(reminders)
        
        # ✅ ИНТЕГРАЦИЯ С GOOGLE SHEETS
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                # Логируем действие
                sheets_manager.log_reminder_action("CREATE", update.effective_user.id, username, chat_id, f"Created reminder: {reminder_text[:50]}...", new_id)
                
                # Синхронизируем напоминание
                reminder_data = {
                    "id": new_id,
                    "text": reminder_text,
                    "time": context.user_data["reminder_datetime"],
                    "type": "once",
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "created_at": get_moscow_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "username": username
                }
                sheets_manager.sync_reminder(reminder_data, "CREATE")
                
                # Обновляем количество напоминаний для чата
                sheets_manager.update_reminders_count(chat_id)
                
                logger.info(f"📊 Successfully synced reminder #{new_id} to Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error syncing reminder to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - reminder #{new_id} not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        else:
            logger.warning("📵 Google Sheets not available for reminder sync")
        
        # Планируем напоминание
        schedule_reminder(context.dispatcher.job_queue, reminders[-1])
        
        try:
            update.message.reply_text(
                f"✅ <b>Напоминание #{new_id} добавлено</b>\n\n"
                f"📅 <i>{context.user_data['reminder_datetime']}</i>\n"
                f"💬 {reminder_text}", 
                parse_mode=ParseMode.HTML
            )
        except:
            update.message.reply_text(f"✅ Напоминание #{new_id} добавлено: {context.user_data['reminder_datetime']}")
        
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_reminder_text: {e}")
        update.message.reply_text("❌ Ошибка при добавлении напоминания")
        return ConversationHandler.END

# --- Обработчики добавления ежедневного напоминания ---
def start_add_daily_reminder(update: Update, context: CallbackContext):
    try:
        current_time = get_moscow_time().strftime("%H:%M MSK")
        update.message.reply_text(f"🔄 <b>Ежедневное напоминание</b>\n\nВведите время в формате ЧЧ:ММ\nНапример: 08:00\n\n<i>⏰ Сейчас: {current_time}</i>", parse_mode=ParseMode.HTML)
        return DAILY_TIME
    except:
        current_time = get_moscow_time().strftime("%H:%M MSK")
        update.message.reply_text(f"🔄 Ежедневное напоминание\n\nВведите время в формате ЧЧ:ММ\nНапример: 08:00\n\n⏰ Сейчас: {current_time}")
        return DAILY_TIME

def receive_daily_time(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        time.strptime(text, "%H:%M")
        context.user_data["daily_time"] = text
        try:
            update.message.reply_text("✏️ <b>Текст ежедневного напоминания</b>\n\nВведите текст (поддерживаются HTML теги и ссылки):\n<i>⏰ Время указано московское (MSK)</i>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("✏️ Текст ежедневного напоминания\n\nВведите текст:\n⏰ Время указано московское (MSK)")
        return DAILY_TEXT
    except Exception:
        try:
            update.message.reply_text("❌ <b>Некорректный формат</b>\n\nВведите время в формате ЧЧ:ММ (московское время):", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Некорректный формат\n\nВведите время в формате ЧЧ:ММ (московское время):")
        return DAILY_TIME

def receive_daily_text(update: Update, context: CallbackContext):
    try:
        reminders = load_reminders()
        new_id = get_next_reminder_id()
        reminder_text = update.message.text_html if update.message.text_html else update.message.text.strip()
        reminder_text = safe_html_escape(reminder_text)
        
        reminders.append({
            "id": new_id,
            "type": "daily",
            "time": context.user_data["daily_time"],
            "text": reminder_text
        })
        save_reminders(reminders)
        
        # ✅ ИНТЕГРАЦИЯ С GOOGLE SHEETS
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                # Логируем действие
                sheets_manager.log_reminder_action("CREATE", update.effective_user.id, username, chat_id, f"Created daily reminder: {reminder_text[:50]}...", new_id)
                
                # Синхронизируем напоминание
                reminder_data = {
                    "id": new_id,
                    "text": reminder_text,
                    "time": context.user_data["daily_time"],
                    "type": "daily",
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "created_at": get_moscow_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "username": username
                }
                sheets_manager.sync_reminder(reminder_data, "CREATE")
                
                # Обновляем количество напоминаний для чата
                sheets_manager.update_reminders_count(chat_id)
                
                logger.info(f"📊 Successfully synced daily reminder #{new_id} to Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error syncing daily reminder to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - daily reminder #{new_id} not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        else:
            logger.warning("📵 Google Sheets not available for daily reminder sync")
        
        # Планируем напоминание
        schedule_reminder(context.dispatcher.job_queue, reminders[-1])
        
        try:
            update.message.reply_text(
                f"✅ <b>Ежедневное напоминание #{new_id} добавлено</b>\n\n"
                f"🕐 <i>Каждый день в {context.user_data['daily_time']}</i>\n"
                f"💬 {reminder_text}", 
                parse_mode=ParseMode.HTML
            )
        except:
            update.message.reply_text(f"✅ Ежедневное напоминание #{new_id} добавлено: {context.user_data['daily_time']}")
        
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_daily_text: {e}")
        update.message.reply_text("❌ Ошибка при добавлении напоминания")
        return ConversationHandler.END

# --- Обработчики добавления еженедельного напоминания ---
def start_add_weekly_reminder(update: Update, context: CallbackContext):
    try:
        current_time = get_moscow_time().strftime("%H:%M MSK")
        update.message.reply_text(f"📆 <b>Еженедельное напоминание</b>\n\nВведите день недели:\nПонедельник, Вторник, Среда, Четверг, Пятница, Суббота, Воскресенье\n\n<i>⏰ Сейчас: {current_time}</i>", parse_mode=ParseMode.HTML)
        return WEEKLY_DAY
    except:
        current_time = get_moscow_time().strftime("%H:%M MSK")
        update.message.reply_text(f"📆 Еженедельное напоминание\n\nВведите день недели:\nПонедельник, Вторник, Среда, Четверг, Пятница, Суббота, Воскресенье\n\n⏰ Сейчас: {current_time}")
        return WEEKLY_DAY

def receive_weekly_day(update: Update, context: CallbackContext):
    # ✅ ЗАЩИТА ОТ None - исправление AttributeError
    if not update.message or not update.message.text:
        try:
            update.message.reply_text("❌ <b>Сообщение не получено</b>\n\nПожалуйста, введите день недели текстом:", parse_mode=ParseMode.HTML)
        except:
            if update.message:
                update.message.reply_text("❌ Сообщение не получено. Введите день недели:")
        return WEEKLY_DAY
    
    text = update.message.text.strip().lower()
    days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
    if text not in days:
        try:
            update.message.reply_text("❌ <b>Некорректный день недели</b>\n\nВыберите один из:\nПонедельник, Вторник, Среда, Четверг, Пятница, Суббота, Воскресенье", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Некорректный день недели\n\nВыберите один из:\nПонедельник, Вторник, Среда, Четверг, Пятница, Суббота, Воскресенье")
        return WEEKLY_DAY
    context.user_data["weekly_day"] = text
    try:
        update.message.reply_text("🕐 <b>Время напоминания</b>\n\nВведите время в формате ЧЧ:ММ:", parse_mode=ParseMode.HTML)
    except:
        update.message.reply_text("🕐 Время напоминания\n\nВведите время в формате ЧЧ:ММ:")
    return WEEKLY_TIME

def receive_weekly_time(update: Update, context: CallbackContext):
    text = update.message.text.strip()
    try:
        time.strptime(text, "%H:%M")
        context.user_data["weekly_time"] = text
        try:
            update.message.reply_text("✏️ <b>Текст еженедельного напоминания</b>\n\nВведите текст (поддерживаются HTML теги и ссылки):\n<i>⏰ Время указано московское (MSK)</i>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("✏️ Текст еженедельного напоминания\n\nВведите текст:\n⏰ Время указано московское (MSK)")
        return WEEKLY_TEXT
    except Exception:
        try:
            update.message.reply_text("❌ <b>Некорректный формат</b>\n\nВведите время в формате ЧЧ:ММ (московское время):", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Некорректный формат\n\nВведите время в формате ЧЧ:ММ (московское время):")
        return WEEKLY_TIME

def receive_weekly_text(update: Update, context: CallbackContext):
    try:
        reminders = load_reminders()
        new_id = get_next_reminder_id()
        reminder_text = update.message.text_html if update.message.text_html else update.message.text.strip()
        reminder_text = safe_html_escape(reminder_text)
        
        reminders.append({
            "id": new_id,
            "type": "weekly",
            "day": context.user_data["weekly_day"],
            "time": context.user_data["weekly_time"],
            "text": reminder_text
        })
        save_reminders(reminders)
        
        # ✅ ИНТЕГРАЦИЯ С GOOGLE SHEETS
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                # Логируем действие
                sheets_manager.log_reminder_action("CREATE", update.effective_user.id, username, chat_id, f"Created weekly reminder: {reminder_text[:50]}...", new_id)
                
                # Синхронизируем напоминание
                reminder_data = {
                    "id": new_id,
                    "text": reminder_text,
                    "time": f"{context.user_data['weekly_day']} {context.user_data['weekly_time']}",
                    "type": "weekly",
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "created_at": get_moscow_time().strftime("%Y-%m-%d %H:%M:%S"),
                    "username": username,
                    "days_of_week": context.user_data["weekly_day"]
                }
                sheets_manager.sync_reminder(reminder_data, "CREATE")
                
                # Обновляем количество напоминаний для чата
                sheets_manager.update_reminders_count(chat_id)
                
                logger.info(f"📊 Successfully synced weekly reminder #{new_id} to Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error syncing weekly reminder to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - weekly reminder #{new_id} not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        else:
            logger.warning("📵 Google Sheets not available for weekly reminder sync")
        
        # Планируем напоминание
        schedule_reminder(context.dispatcher.job_queue, reminders[-1])
        
        try:
            update.message.reply_text(
                f"✅ <b>Еженедельное напоминание #{new_id} добавлено</b>\n\n"
                f"📅 <i>Каждый {context.user_data['weekly_day'].title()} в {context.user_data['weekly_time']}</i>\n"
                f"💬 {reminder_text}", 
                parse_mode=ParseMode.HTML
            )
        except:
            update.message.reply_text(f"✅ Еженедельное напоминание #{new_id} добавлено: {context.user_data['weekly_day'].title()} {context.user_data['weekly_time']}")
        
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in receive_weekly_text: {e}")
        update.message.reply_text("❌ Ошибка при добавлении напоминания")
        return ConversationHandler.END

# --- Список напоминаний ---
def list_reminders(update: Update, context: CallbackContext):
    try:
        reminders = load_reminders()
        if not reminders:
            try:
                update.message.reply_text("📭 <b>У вас нет активных напоминаний</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 У вас нет активных напоминаний")
            return
        
        lines = ["📋 Ваши напоминания:\n"]
        
        # Сортируем по ID для удобства
        reminders.sort(key=lambda x: int(x.get("id", "0")))
        
        for i, r in enumerate(reminders, 1):
            try:
                safe_text = safe_html_escape(r.get('text', ''))
                if r["type"] == "once":
                    lines.append(f"{i}. [📅 Разово] {r['datetime']}\n💬 {safe_text}\n")
                elif r["type"] == "daily":
                    lines.append(f"{i}. [🔄 Ежедневно] {r['time']}\n💬 {safe_text}\n")
                elif r["type"] == "weekly":
                    lines.append(f"{i}. [📆 Еженедельно] {r['day'].title()} {r['time']}\n💬 {safe_text}\n")
            except Exception as e:
                logger.error(f"Error formatting reminder {i}: {e}")
                lines.append(f"{i}. [Ошибка формата]\n")
        
        message_text = "\n".join(lines)
        
        # Telegram имеет лимит на длину сообщения
        if len(message_text) > 4000:
            # Разбиваем на части
            chunks = []
            current_chunk = "📋 Ваши напоминания:\n\n"
            
            for line in lines[1:]:  # Пропускаем заголовок
                if len(current_chunk + line) > 4000:
                    chunks.append(current_chunk)
                    current_chunk = line
                else:
                    current_chunk += line
            
            if current_chunk:
                chunks.append(current_chunk)
            
            for chunk in chunks:
                try:
                    update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
                except:
                    # Fallback без HTML
                    clean_chunk = chunk.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                    update.message.reply_text(clean_chunk)
        else:
            try:
                update.message.reply_text(message_text, parse_mode=ParseMode.HTML)
            except:
                # Fallback без HTML
                clean_text = message_text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                update.message.reply_text(clean_text)
                
    except Exception as e:
        logger.error(f"Error in list_reminders: {e}")
        update.message.reply_text("❌ Ошибка при загрузке списка напоминаний")

def list_polls(update: Update, context: CallbackContext):
    try:
        polls = load_polls()
        if not polls:
            try:
                update.message.reply_text("🗳️ <b>У вас нет активных голосований</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("🗳️ У вас нет активных голосований")
            return
        
        lines = ["🗳️ Ваши голосования:\n"]
        
        # Сортируем по ID для удобства
        polls.sort(key=lambda x: int(x.get("id", "0")))
        
        for i, p in enumerate(polls, 1):
            try:
                safe_question = safe_html_escape(p.get('question', ''))
                options_preview = ", ".join(p.get('options', [])[:2])  # Показываем первые 2 варианта
                if len(p.get('options', [])) > 2:
                    options_preview += "..."
                
                if p["type"] == "once" or p["type"] == "one_time":
                    lines.append(f"{i}. [📅 Разово] {p['datetime']}\n❓ {safe_question}\n🔘 {options_preview}\n")
                elif p["type"] == "daily" or p["type"] == "daily_poll":
                    lines.append(f"{i}. [🔄 Ежедневно] {p['time']}\n❓ {safe_question}\n🔘 {options_preview}\n")
                elif p["type"] == "weekly" or p["type"] == "weekly_poll":
                    day_str = str(p['day']).title() if p.get('day') else 'N/A'
                    lines.append(f"{i}. [📆 Еженедельно] {day_str} {p['time']}\n❓ {safe_question}\n🔘 {options_preview}\n")
            except Exception as e:
                logger.error(f"Error formatting poll {i}: {e}")
                lines.append(f"{i}. [Ошибка формата]\n")
        
        message_text = "\n".join(lines)
        
        # Telegram имеет лимит на длину сообщения
        if len(message_text) > 4000:
            # Разбиваем на части
            chunks = []
            current_chunk = "🗳️ Ваши голосования:\n\n"
            
            for line in lines[1:]:  # Пропускаем заголовок
                if len(current_chunk + line) > 4000:
                    chunks.append(current_chunk)
                    current_chunk = line
                else:
                    current_chunk += line
            
            if current_chunk:
                chunks.append(current_chunk)
            
            for chunk in chunks:
                try:
                    update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
                except:
                    # Fallback без HTML
                    clean_chunk = chunk.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                    update.message.reply_text(clean_chunk)
        else:
            try:
                update.message.reply_text(message_text, parse_mode=ParseMode.HTML)
            except:
                # Fallback без HTML
                clean_text = message_text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                update.message.reply_text(clean_text)
                
    except Exception as e:
        logger.error(f"Error in list_polls: {e}")
        update.message.reply_text("❌ Ошибка при загрузке списка голосований")

# --- Удаление напоминания ---
def start_delete_reminder(update: Update, context: CallbackContext):
    try:
        reminders = load_reminders()
        if not reminders:
            try:
                update.message.reply_text("📭 <b>У вас нет напоминаний для удаления</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 У вас нет напоминаний для удаления")
            return ConversationHandler.END
        
        lines = ["🗑 Выберите напоминание для удаления:\nВведите номер:\n"]
        
        # Сортируем по ID для удобства
        reminders.sort(key=lambda x: int(x.get("id", "0")))
        
        for i, r in enumerate(reminders, 1):
            try:
                text_preview = r.get('text', '')[:50]
                if len(r.get('text', '')) > 50:
                    text_preview += '...'
                    
                if r["type"] == "once":
                    lines.append(f"{i}. [📅 Разово] {r['datetime']}\n💬 {text_preview}")
                elif r["type"] == "daily":
                    lines.append(f"{i}. [🔄 Ежедневно] {r['time']}\n💬 {text_preview}")
                elif r["type"] == "weekly":
                    lines.append(f"{i}. [📆 Еженедельно] {r['day'].title()} {r['time']}\n💬 {text_preview}")
            except Exception as e:
                logger.error(f"Error formatting reminder for deletion {i}: {e}")
                lines.append(f"{i}. [Ошибка формата]")
        
        try:
            update.message.reply_text("\n\n".join(lines), parse_mode=ParseMode.HTML)
        except:
            # Fallback без HTML
            clean_text = "\n\n".join(lines).replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            update.message.reply_text(clean_text)
        
        return REM_DEL_ID
        
    except Exception as e:
        logger.error(f"Error in start_delete_reminder: {e}")
        update.message.reply_text("❌ Ошибка при загрузке напоминаний для удаления")
        return ConversationHandler.END

def confirm_delete_reminder(update: Update, context: CallbackContext):
    try:
        reminder_number = int(update.message.text.strip())
        reminders = load_reminders()
        
        if reminder_number < 1 or reminder_number > len(reminders):
            try:
                update.message.reply_text("❌ <b>Неверный номер</b>\n\nВведите номер от 1 до " + str(len(reminders)), parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text(f"❌ Неверный номер\n\nВведите номер от 1 до {len(reminders)}")
            return REM_DEL_ID
        
        # Сортируем по ID для удобства
        reminders.sort(key=lambda x: int(x.get("id", "0")))
        reminder_to_delete = reminders[reminder_number - 1]
        
        # ✅ СИНХРОНИЗАЦИЯ С GOOGLE SHEETS ПРИ УДАЛЕНИИ
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                # Логируем действие удаления
                sheets_manager.log_reminder_action("DELETE", update.effective_user.id, username, chat_id, f"Deleted reminder: {reminder_to_delete.get('text', '')[:50]}...", reminder_to_delete.get('id'))
                
                # Синхронизируем удаление - устанавливаем статус "Deleted"
                reminder_data = {
                    "id": reminder_to_delete.get('id'),
                    "text": reminder_to_delete.get('text', ''),
                    "time": reminder_to_delete.get('datetime') or reminder_to_delete.get('time', ''),
                    "type": reminder_to_delete.get('type', ''),
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "created_at": reminder_to_delete.get('created_at', ''),
                    "username": reminder_to_delete.get('username', username),
                    "last_sent": reminder_to_delete.get('last_sent', ''),
                    "days_of_week": reminder_to_delete.get('day', '') if reminder_to_delete.get('type') == 'weekly' else reminder_to_delete.get('days_of_week', '')
                }
                
                # ВАЖНО: Используем действие "DELETE" для установки статуса "Deleted"
                sheets_manager.sync_reminder(reminder_data, "DELETE")
                
                # Обновляем количество напоминаний для чата
                sheets_manager.update_reminders_count(chat_id)
                
                logger.info(f"📊 Successfully synced reminder #{reminder_to_delete.get('id')} deletion to Google Sheets (status: Deleted)")
                
            except Exception as e:
                logger.error(f"❌ Error syncing reminder deletion to Google Sheets: {e}")
                # Продолжаем удаление даже если синхронизация не удалась
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - reminder #{reminder_to_delete.get('id')} deletion not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        else:
            logger.warning("📵 Google Sheets not available for reminder deletion sync")
        
        # Удаляем напоминание из локального файла
        all_reminders = load_reminders()
        new_list = [r for r in all_reminders if r["id"] != reminder_to_delete["id"]]
        save_reminders(new_list)
        
        try:
            update.message.reply_text(f"✅ <b>Напоминание #{reminder_number} удалено</b>\n<i>Статус в Google Sheets изменен на Deleted</i>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text(f"✅ Напоминание #{reminder_number} удалено")
        
        # Перепланируем все напоминания
        reschedule_all_reminders(context.dispatcher.job_queue)
        
    except ValueError:
        try:
            update.message.reply_text("❌ <b>Введите номер напоминания</b>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Введите номер напоминания")
        return REM_DEL_ID
    except Exception as e:
        logger.error(f"Error in confirm_delete_reminder: {e}")
        update.message.reply_text("❌ Ошибка при удалении напоминания")
    
    return ConversationHandler.END

# --- Очистка всех напоминаний ---
def clear_reminders(update: Update, context: CallbackContext):
    try:
        # Получаем все текущие напоминания перед удалением для синхронизации
        all_reminders = load_reminders()
        reminders_count = len(all_reminders)
        
        # ✅ СИНХРОНИЗАЦИЯ С GOOGLE SHEETS ПРИ МАССОВОМ УДАЛЕНИИ
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized and all_reminders:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                # Отправляем сообщение о начале операции
                try:
                    progress_message = update.message.reply_text(
                        f"🔄 <b>Удаление всех напоминаний...</b>\n\n"
                        f"📊 Обновление Google Sheets для {reminders_count} напоминаний...",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    progress_message = update.message.reply_text(f"🔄 Удаление {reminders_count} напоминаний...")
                
                # Логируем начало массового удаления
                sheets_manager.log_reminder_action("CLEAR_ALL", update.effective_user.id, username, chat_id, f"Started mass deletion of {reminders_count} reminders", "")
                
                # Синхронизируем каждое напоминание - устанавливаем статус "Deleted"
                # Для больших количеств добавляем батчинг с перерывами
                synced_count = 0
                failed_count = 0
                batch_size = 5  # Обрабатываем по 5 напоминаний за раз
                batch_delay = 10.0  # 10 секунд между батчами
                
                for i, reminder in enumerate(all_reminders):
                    try:
                        # Проверяем, нужен ли перерыв между батчами
                        if i > 0 and i % batch_size == 0:
                            logger.info(f"📦 Completed batch {i//batch_size}, waiting {batch_delay}s before next batch...")
                            
                            # Обновляем сообщение о прогрессе между батчами
                            try:
                                context.bot.edit_message_text(
                                    chat_id=progress_message.chat_id,
                                    message_id=progress_message.message_id,
                                    text=f"🔄 <b>Пауза между батчами...</b>\n\n"
                                         f"✅ Обработано: {i}/{reminders_count}\n"
                                         f"✅ Успешно: {synced_count}\n"
                                         f"❌ Ошибки: {failed_count}\n\n"
                                         f"⏱️ Ожидание {batch_delay}s...",
                                    parse_mode=ParseMode.HTML
                                )
                            except:
                                pass
                            
                            time.sleep(batch_delay)
                        
                        reminder_data = {
                            "id": reminder.get('id'),
                            "text": reminder.get('text', ''),
                            "time": reminder.get('datetime') or reminder.get('time', ''),
                            "type": reminder.get('type', ''),
                            "chat_id": chat_id,
                            "chat_name": chat_name,
                            "created_at": reminder.get('created_at', ''),
                            "username": reminder.get('username', username),
                            "last_sent": reminder.get('last_sent', ''),
                            "days_of_week": reminder.get('day', '') if reminder.get('type') == 'weekly' else reminder.get('days_of_week', '')
                        }
                        
                        # ВАЖНО: Используем действие "DELETE" для установки статуса "Deleted"
                        success = sheets_manager.sync_reminder(reminder_data, "DELETE")
                        if success:
                            synced_count += 1
                        else:
                            failed_count += 1
                            logger.warning(f"⚠️ Failed to sync reminder #{reminder.get('id')} deletion")
                        
                        # Добавляем задержку между операциями для предотвращения rate limiting
                        # Увеличиваем задержку до 1-2 секунд для соблюдения лимита 60 запросов/минуту
                        if i < len(all_reminders) - 1:  # Не задерживаем после последнего
                            # Прогрессивная задержка: больше времени для более поздних операций
                            base_delay = 1.0  # Базовая задержка 1 секунда
                            progressive_delay = (i % batch_size) * 0.2  # Дополнительная задержка внутри батча
                            total_delay = base_delay + progressive_delay
                            time.sleep(total_delay)
                            logger.debug(f"⏱️ Waiting {total_delay:.1f}s before next sync operation ({i+2}/{len(all_reminders)})")
                        
                    except Exception as e:
                        logger.error(f"❌ Error syncing reminder #{reminder.get('id')} deletion: {e}")
                        failed_count += 1
                        # Продолжаем даже если одно напоминание не синхронизировалось
                
                # Обновляем сообщение о прогрессе после синхронизации напоминаний
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=f"🔄 <b>Обновление статистики...</b>\n\n"
                             f"✅ Напоминания: {synced_count}/{reminders_count}\n"
                             f"❌ Ошибки: {failed_count}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
                
                # Увеличенная задержка перед обновлением статистики
                time.sleep(2.0)  # Увеличиваем с 0.5 до 2 секунд
                
                # Обновляем количество напоминаний для чата (должно стать 0)
                count_update_success = sheets_manager.update_reminders_count(chat_id)
                
                # Финальное логирование
                sheets_manager.log_reminder_action("CLEAR_ALL_COMPLETE", update.effective_user.id, username, chat_id, f"Completed mass deletion. Synced: {synced_count}/{reminders_count}, Failed: {failed_count}", "")
                
                logger.info(f"📊 Mass deletion summary: {synced_count}/{reminders_count} reminders synced, {failed_count} failed")
                
                # Обновляем сообщение о прогрессе
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=f"🔄 <b>Завершение удаления...</b>\n\n"
                             f"✅ Google Sheets обновлен ({synced_count}/{reminders_count})"
                             f"\n{'⚠️ Статистика чатов: ошибка обновления' if not count_update_success else '✅ Статистика чатов обновлена'}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
                
            except Exception as e:
                logger.error(f"❌ Error syncing mass deletion to Google Sheets: {e}")
                # Продолжаем удаление даже если синхронизация не удалась
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - mass deletion of {reminders_count} reminders not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        elif not all_reminders:
            logger.info("📭 No reminders to delete")
        else:
            logger.warning("📵 Google Sheets not available for mass deletion sync")
        
        # Удаляем все напоминания из локального файла
        save_reminders([])
        
        # Останавливаем все задания
        job_queue = context.dispatcher.job_queue
        current_jobs = job_queue.jobs()
        for job in current_jobs:
            if hasattr(job, 'name') and job.name and job.name.startswith('reminder_'):
                job.schedule_removal()
        
        # Финальное сообщение пользователю
        if reminders_count > 0:
            try:
                # Если есть progress_message, обновляем его
                if 'progress_message' in locals():
                    # Формируем детальное сообщение с учетом успешности операций
                    if 'synced_count' in locals() and 'failed_count' in locals():
                        if failed_count == 0:
                            final_text = f"🗑 <b>Все напоминания удалены ({reminders_count})</b>\n<i>✅ Статус всех напоминаний в Google Sheets изменен на Deleted</i>"
                        else:
                            final_text = f"🗑 <b>Напоминания удалены ({reminders_count})</b>\n<i>✅ Синхронизировано: {synced_count}/{reminders_count}\n⚠️ Ошибок синхронизации: {failed_count}</i>"
                    else:
                        final_text = f"🗑 <b>Все напоминания удалены ({reminders_count})</b>\n<i>⚠️ Google Sheets недоступен</i>"
                    
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=final_text,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    update.message.reply_text(
                        f"🗑 <b>Все напоминания удалены ({reminders_count})</b>\n"
                        f"<i>Статус в Google Sheets изменен на Deleted</i>", 
                        parse_mode=ParseMode.HTML
                    )
            except:
                update.message.reply_text(f"🗑 Все напоминания удалены ({reminders_count})")
        else:
            try:
                update.message.reply_text("📭 <b>Напоминаний для удаления не найдено</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 Напоминаний для удаления не найдено")
            
    except Exception as e:
        logger.error(f"Error in clear_reminders: {e}")
        update.message.reply_text("❌ Ошибка при очистке напоминаний")

# --- Восстановление напоминаний и голосований из Google Sheets ---
def restore_reminders(update: Update, context: CallbackContext):
    """Восстановление активных напоминаний и голосований из Google Sheets"""
    try:
        # Проверяем доступность Google Sheets
        if not SHEETS_AVAILABLE or not sheets_manager:
            try:
                update.message.reply_text(
                    "❌ <b>Google Sheets недоступен</b>\n\n"
                    "📵 Интеграция с Google Sheets не настроена.\n"
                    "Обратитесь к администратору для настройки.",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text("❌ Google Sheets недоступен")
            return
        
        if not sheets_manager.is_initialized:
            try:
                update.message.reply_text(
                    "❌ <b>Google Sheets не инициализирован</b>\n\n"
                    "🔧 Проверьте переменные окружения:\n"
                    "• GOOGLE_SHEETS_ID\n"
                    "• GOOGLE_SHEETS_CREDENTIALS",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text("❌ Google Sheets не инициализирован")
            return
        
        # Отправляем сообщение о начале восстановления
        try:
            progress_message = update.message.reply_text(
                "🔄 <b>Восстановление данных...</b>\n\n"
                "📊 Получение данных из Google Sheets...\n"
                "🔄 Восстановление напоминаний, голосований и чатов...",
                parse_mode=ParseMode.HTML
            )
        except:
            progress_message = update.message.reply_text("🔄 Восстановление данных...")
        
        # Получаем информацию о пользователе для логирования
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name or "Unknown"
        
        # Логируем начало операции восстановления
        if sheets_manager.is_initialized:
            try:
                moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                sheets_manager.log_operation(
                    timestamp=moscow_time,
                    action="RESTORE_ALL_START",
                    user_id=str(user_id),
                    username=username,
                    chat_id=chat_id,
                    details="Manual restore reminders and chats command initiated",
                    reminder_id=""
                )
            except Exception as e:
                logger.error(f"Error logging restore start: {e}")
        
        # Обновляем сообщение о прогрессе
        try:
            context.bot.edit_message_text(
                chat_id=progress_message.chat_id,
                message_id=progress_message.message_id,
                text="🔄 <b>Восстановление данных...</b>\n\n"
                     "📱 Восстановление подписанных чатов...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        
        # ДОПОЛНИТЕЛЬНО: Восстанавливаем подписанные чаты
        chats_restored = False
        chats_count = 0
        chats_message = ""
        
        try:
            success_chats = sheets_manager.restore_subscribed_chats_file()
            if success_chats:
                # Получаем количество восстановленных чатов
                try:
                    with open("subscribed_chats.json", "r") as f:
                        restored_chats = json.load(f)
                        chats_count = len(restored_chats)
                        chats_restored = True
                        chats_message = f"Восстановлено чатов: {chats_count}"
                        logger.info(f"✅ Successfully restored {chats_count} chats for user {username}")
                except:
                    chats_message = "Чаты восстановлены (количество не определено)"
                    chats_restored = True
            else:
                chats_message = "Чаты не восстановлены (возможно, список пуст в Google Sheets)"
                logger.warning(f"⚠️ Failed to restore chats for user {username}")
        except Exception as e:
            chats_message = f"Ошибка восстановления чатов: {str(e)}"
            logger.error(f"❌ Error restoring chats for user {username}: {e}")
        
        # Обновляем сообщение о прогрессе
        try:
            context.bot.edit_message_text(
                chat_id=progress_message.chat_id,
                message_id=progress_message.message_id,
                text="🔄 <b>Восстановление данных...</b>\n\n"
                     "📋 Восстановление напоминаний...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        
        # Восстанавливаем напоминания
        success, message = sheets_manager.restore_reminders_from_sheets()
        
        # Обновляем сообщение о прогрессе для голосований
        try:
            context.bot.edit_message_text(
                chat_id=progress_message.chat_id,
                message_id=progress_message.message_id,
                text="🔄 <b>Восстановление данных...</b>\n\n"
                     "📊 Восстановление голосований...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        
        # Восстанавливаем голосования
        polls_success, polls_message = sheets_manager.restore_polls_from_sheets()
        
        if success or polls_success:
            # Перепланируем все напоминания и голосования
            if success:
                reschedule_all_reminders(context.dispatcher.job_queue)
            if polls_success:
                reschedule_all_polls(context.dispatcher.job_queue)
            
            # Получаем количество восстановленных напоминаний
            try:
                restored_reminders = load_reminders()
                reminders_count = len(restored_reminders)
                
                # Подсчитываем напоминания по типам
                once_count = sum(1 for r in restored_reminders if r.get('type') == 'once')
                daily_count = sum(1 for r in restored_reminders if r.get('type') == 'daily')
                weekly_count = sum(1 for r in restored_reminders if r.get('type') == 'weekly')
                
                # Получаем количество восстановленных голосований
                restored_polls = load_polls()
                polls_count = len(restored_polls)
                
                # Подсчитываем голосования по типам
                polls_once_count = sum(1 for p in restored_polls if p.get('type') == 'once')
                polls_daily_count = sum(1 for p in restored_polls if p.get('type') == 'daily')
                polls_weekly_count = sum(1 for p in restored_polls if p.get('type') == 'weekly')
                
                # Формируем итоговое сообщение
                final_message = (
                    f"✅ <b>Восстановление завершено успешно!</b>\n\n"
                    f"📋 <b>Восстановлено напоминаний: {reminders_count}</b>\n"
                    f"📅 Разовых: {once_count}\n"
                    f"🔄 Ежедневных: {daily_count}\n"
                    f"📆 Еженедельных: {weekly_count}\n\n"
                    f"📊 <b>Восстановлено голосований: {polls_count}</b>\n"
                    f"📅 Разовых: {polls_once_count}\n"
                    f"🔄 Ежедневных: {polls_daily_count}\n"
                    f"📆 Еженедельных: {polls_weekly_count}\n\n"
                    f"📱 <b>Подписанные чаты:</b>\n"
                    f"{'✅ ' + chats_message if chats_restored else '⚠️ ' + chats_message}\n\n"
                    f"⏰ Все напоминания и голосования перепланированы и активны!\n"
                    f"<i>Команды: /list_reminders, /list_polls для просмотра</i>"
                )
                
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=final_message,
                        parse_mode=ParseMode.HTML
                    )
                except:
                    # Fallback без HTML
                    clean_message = (
                        f"✅ Восстановление завершено успешно!\n\n"
                        f"📋 Восстановлено напоминаний: {reminders_count}\n"
                        f"📅 Разовых: {once_count}\n"
                        f"🔄 Ежедневных: {daily_count}\n"
                        f"📆 Еженедельных: {weekly_count}\n\n"
                        f"🗳️ Восстановлено голосований: {polls_count}\n"
                        f"📅 Разовых: {polls_once_count}\n"
                        f"🔄 Ежедневных: {polls_daily_count}\n"
                        f"📆 Еженедельных: {polls_weekly_count}\n\n"
                        f"📱 Подписанные чаты:\n"
                        f"{chats_message}\n\n"
                        f"⏰ Все напоминания и голосования перепланированы и активны!\n\n"
                        f"Используйте /list_reminders и /list_polls для просмотра."
                    )
                    update.message.reply_text(clean_message)
                
                logger.info(f"✅ Successfully restored {reminders_count} reminders, {polls_count} polls and {chats_count if chats_restored else 0} chats for user {username} (ID: {user_id})")
                
            except Exception as e:
                logger.error(f"Error getting restored data count: {e}")
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=f"✅ <b>Восстановление завершено!</b>\n\n"
                             f"📋 {message}\n"
                             f"📱 {chats_message}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    update.message.reply_text(f"✅ Восстановление завершено!\n\n📋 {message}\n📱 {chats_message}")
        
        else:
            # Ошибка восстановления данных
            try:
                context.bot.edit_message_text(
                    chat_id=progress_message.chat_id,
                    message_id=progress_message.message_id,
                    text=f"❌ <b>Ошибка восстановления данных</b>\n\n"
                         f"📋 {message}\n\n"
                         f"📱 <b>Подписанные чаты:</b>\n"
                         f"{'✅ ' + chats_message if chats_restored else '⚠️ ' + chats_message}\n\n"
                         f"💡 <i>Попробуйте:</i>\n"
                         f"• Проверить доступ к Google Sheets\n"
                         f"• Убедиться, что в листах есть активные напоминания и голосования\n"
                         f"• Обратиться к администратору",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text(f"❌ Ошибка восстановления данных\n\n📋 {message}\n📱 {chats_message}")
            
            logger.error(f"❌ Failed to restore data for user {username}: {message}")
        
        # Логируем завершение операции
        if sheets_manager.is_initialized:
            try:
                moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                sheets_manager.log_operation(
                    timestamp=moscow_time,
                    action="RESTORE_ALL_COMPLETE",
                    user_id=str(user_id),
                    username=username,
                    chat_id=chat_id,
                    details=f"Manual restore {'successful' if success else 'failed'}: Data: {message}, Chats: {chats_message}",
                    reminder_id=""
                )
            except Exception as e:
                logger.error(f"Error logging restore completion: {e}")
                
    except Exception as e:
        logger.error(f"Error in restore function: {e}")
        try:
            update.message.reply_text(
                "❌ <b>Критическая ошибка восстановления</b>\n\n"
                "Обратитесь к администратору системы.",
                parse_mode=ParseMode.HTML
            )
        except:
            update.message.reply_text("❌ Критическая ошибка восстановления")

def restore_polls(update: Update, context: CallbackContext):
    """Восстановление активных голосований из Google Sheets"""
    try:
        # Проверяем доступность Google Sheets
        if not SHEETS_AVAILABLE or not sheets_manager:
            try:
                update.message.reply_text(
                    "❌ <b>Google Sheets недоступен</b>\n\n"
                    "📵 Интеграция с Google Sheets не настроена.\n"
                    "Обратитесь к администратору для настройки.",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text("❌ Google Sheets недоступен")
            return
        
        if not sheets_manager.is_initialized:
            try:
                update.message.reply_text(
                    "❌ <b>Google Sheets не инициализирован</b>\n\n"
                    "🔧 Проверьте переменные окружения:\n"
                    "• GOOGLE_SHEETS_ID\n"
                    "• GOOGLE_SHEETS_CREDENTIALS",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text("❌ Google Sheets не инициализирован")
            return
        
        # Отправляем сообщение о начале восстановления
        try:
            progress_message = update.message.reply_text(
                "🔄 <b>Восстановление голосований...</b>\n\n"
                "📊 Получение данных из Google Sheets...",
                parse_mode=ParseMode.HTML
            )
        except:
            progress_message = update.message.reply_text("🔄 Восстановление голосований...")
        
        # Получаем информацию о пользователе для логирования
        chat_id = update.effective_chat.id
        user_id = update.effective_user.id
        username = update.effective_user.username or update.effective_user.first_name or "Unknown"
        
        # Логируем начало операции восстановления
        if sheets_manager.is_initialized:
            try:
                moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                sheets_manager.log_operation(
                    timestamp=moscow_time,
                    action="RESTORE_POLLS_START",
                    user_id=str(user_id),
                    username=username,
                    chat_id=chat_id,
                    details="Manual restore polls command initiated",
                    reminder_id=""
                )
            except Exception as e:
                logger.error(f"Error logging restore polls start: {e}")
        
        # Обновляем сообщение о прогрессе
        try:
            context.bot.edit_message_text(
                chat_id=progress_message.chat_id,
                message_id=progress_message.message_id,
                text="🔄 <b>Восстановление голосований...</b>\n\n"
                     "📊 Восстановление голосований...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        
        # Восстанавливаем голосования
        polls_success, polls_message = sheets_manager.restore_polls_from_sheets()
        
        if polls_success:
            # Перепланируем все голосования
            reschedule_all_polls(context.dispatcher.job_queue)
            
            # Получаем количество восстановленных голосований
            try:
                restored_polls = load_polls()
                polls_count = len(restored_polls)
                
                # Подсчитываем голосования по типам
                polls_once_count = sum(1 for p in restored_polls if p.get('type') == 'once')
                polls_daily_count = sum(1 for p in restored_polls if p.get('type') == 'daily')
                polls_weekly_count = sum(1 for p in restored_polls if p.get('type') == 'weekly')
                
                # Формируем итоговое сообщение
                final_message = (
                    f"✅ <b>Восстановление голосований завершено!</b>\n\n"
                    f"📊 <b>Восстановлено голосований: {polls_count}</b>\n"
                    f"📅 Разовых: {polls_once_count}\n"
                    f"🔄 Ежедневных: {polls_daily_count}\n"
                    f"📆 Еженедельных: {polls_weekly_count}\n\n"
                    f"⏰ Все голосования перепланированы и активны!\n"
                    f"<i>Команда: /list_polls для просмотра</i>"
                )
                
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=final_message,
                        parse_mode=ParseMode.HTML
                    )
                except:
                    # Fallback без HTML
                    clean_message = (
                        f"✅ Восстановление голосований завершено!\n\n"
                        f"📊 Восстановлено голосований: {polls_count}\n"
                        f"📅 Разовых: {polls_once_count}\n"
                        f"🔄 Ежедневных: {polls_daily_count}\n"
                        f"📆 Еженедельных: {polls_weekly_count}\n\n"
                        f"⏰ Все голосования перепланированы и активны!\n\n"
                        f"Используйте /list_polls для просмотра."
                    )
                    update.message.reply_text(clean_message)
                
                logger.info(f"✅ Successfully restored {polls_count} polls for user {username} (ID: {user_id})")
                
            except Exception as e:
                logger.error(f"Error getting restored polls count: {e}")
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=f"✅ <b>Восстановление голосований завершено!</b>\n\n"
                             f"📊 {polls_message}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    update.message.reply_text(f"✅ Восстановление голосований завершено!\n\n📊 {polls_message}")
        
        else:
            # Ошибка восстановления голосований
            try:
                context.bot.edit_message_text(
                    chat_id=progress_message.chat_id,
                    message_id=progress_message.message_id,
                    text=f"❌ <b>Ошибка восстановления голосований</b>\n\n"
                         f"📊 {polls_message}\n\n"
                         f"💡 Попробуйте позже или обратитесь к администратору.",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text(f"❌ Ошибка восстановления голосований\n\n📊 {polls_message}")
        
        # Логируем завершение операции
        if sheets_manager.is_initialized:
            try:
                moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                sheets_manager.log_operation(
                    timestamp=moscow_time,
                    action="RESTORE_POLLS_COMPLETE",
                    user_id=str(user_id),
                    username=username,
                    chat_id=chat_id,
                    details=f"Restore polls completed: {polls_message}",
                    reminder_id=""
                )
            except Exception as e:
                logger.error(f"Error logging restore polls complete: {e}")
        
    except Exception as e:
        logger.error(f"Error in restore_polls: {e}")
        try:
            update.message.reply_text(
                "❌ <b>Ошибка при восстановлении голосований</b>\n\n"
                "Произошла ошибка при восстановлении голосований.\n"
                "Попробуйте позже или обратитесь к администратору.",
                parse_mode=ParseMode.HTML
            )
        except:
            update.message.reply_text("❌ Ошибка при восстановлении голосований")

# --- Следующее напоминание ---
def next_notification(update: Update, context: CallbackContext):
    try:
        reminders = load_reminders()
        if not reminders:
            try:
                update.message.reply_text("📭 <b>Нет запланированных напоминаний</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 Нет запланированных напоминаний")
            return
        
        now_moscow = get_moscow_time()
        soonest = None
        soonest_time = None
        days = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
        
        for r in reminders:
            t = None
            if r["type"] == "once":
                try:
                    # Парсим как московское время
                    naive_dt = datetime.strptime(r["datetime"], "%Y-%m-%d %H:%M")
                    t = MOSCOW_TZ.localize(naive_dt)
                    if t < now_moscow:  # Пропускаем прошедшие разовые напоминания
                        continue
                except ValueError:
                    continue
            elif r["type"] == "daily":
                try:
                    h, m = map(int, r["time"].split(":"))
                    candidate = now_moscow.replace(hour=h, minute=m, second=0, microsecond=0)
                    if candidate < now_moscow:
                        candidate += timedelta(days=1)
                    t = candidate
                except ValueError:
                    continue
            elif r["type"] == "weekly":
                try:
                    weekday = days.index(r["day"])
                    h, m = map(int, r["time"].split(":"))
                    candidate = now_moscow.replace(hour=h, minute=m, second=0, microsecond=0)
                    days_ahead = (weekday - now_moscow.weekday() + 7) % 7
                    if days_ahead == 0 and candidate < now_moscow:
                        days_ahead = 7
                    t = candidate + timedelta(days=days_ahead)
                except (ValueError, IndexError):
                    continue
            
            if t and (soonest_time is None or t < soonest_time):
                soonest_time = t
                soonest = r
        
        if soonest is None:
            try:
                update.message.reply_text("📭 <b>Нет запланированных напоминаний</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 Нет запланированных напоминаний")
            return
        
        time_diff = soonest_time - now_moscow
        
        if time_diff.days > 0:
            time_str = f"через {time_diff.days} дн."
        elif time_diff.seconds > 3600:
            hours = time_diff.seconds // 3600
            time_str = f"через {hours} ч."
        elif time_diff.seconds > 60:
            minutes = time_diff.seconds // 60
            time_str = f"через {minutes} мин."
        else:
            time_str = "менее чем через минуту"
        
        safe_text = safe_html_escape(soonest.get('text', ''))
        current_time = now_moscow.strftime("%H:%M MSK")
        
        if soonest["type"] == "once":
            reminder_time = soonest_time.strftime("%Y-%m-%d %H:%M MSK")
            msg = f"📅 <b>Ближайшее напоминание</b>\n\n🕐 Разово: {reminder_time}\n⏰ {time_str}\n💬 {safe_text}\n\n<i>Сейчас: {current_time}</i>"
        elif soonest["type"] == "daily":
            reminder_time = soonest_time.strftime("%H:%M MSK")
            msg = f"🔄 <b>Ближайшее напоминание</b>\n\n🕐 Ежедневно: {reminder_time}\n⏰ {time_str}\n💬 {safe_text}\n\n<i>Сейчас: {current_time}</i>"
        elif soonest["type"] == "weekly":
            reminder_time = soonest_time.strftime("%H:%M MSK")
            msg = f"📆 <b>Ближайшее напоминание</b>\n\n🕐 Еженедельно: {soonest['day'].title()} {reminder_time}\n⏰ {time_str}\n💬 {safe_text}\n\n<i>Сейчас: {current_time}</i>"
        
        try:
            update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        except:
            # Fallback без HTML
            clean_msg = msg.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            update.message.reply_text(clean_msg)
            
    except Exception as e:
        logger.error(f"Error in next_notification: {e}")
        update.message.reply_text("❌ Ошибка при поиске ближайшего напоминания")

def cancel_reminder(update: Update, context: CallbackContext):
    """
    Отмена создания напоминания.
    """
    try:
        update.message.reply_text("❌ <b>Операция отменена</b>", parse_mode=ParseMode.HTML)
    except:
        update.message.reply_text("❌ Операция отменена")
    return ConversationHandler.END

# --- Poll management functions ---

def start_delete_poll(update: Update, context: CallbackContext):
    """Начало удаления голосования"""
    try:
        polls = load_polls()
        if not polls:
            try:
                update.message.reply_text("📭 <b>Нет голосований для удаления</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 Нет голосований для удаления")
            return ConversationHandler.END
        
        # Сортируем голосования по ID для удобства
        polls.sort(key=lambda x: x.get('id', 0))
        
        # Формируем список голосований
        poll_list = []
        for poll in polls:
            poll_id = poll.get('id', 'N/A')
            question = poll.get('question', 'Без вопроса')[:50]
            if len(poll.get('question', '')) > 50:
                question += '...'
            
            poll_type = poll.get('type', 'unknown')
            if poll_type == 'once':
                time_info = poll.get('datetime', 'N/A')
            elif poll_type == 'daily':
                time_info = f"Ежедневно в {poll.get('time', 'N/A')}"
            elif poll_type == 'weekly':
                day_str = str(poll.get('day', 'N/A')).title() if poll.get('day') else 'N/A'
                time_info = f"{day_str} в {poll.get('time', 'N/A')}"
            else:
                time_info = 'N/A'
            
            poll_list.append(f"#{poll_id}: {question} ({time_info})")
        
        # Разбиваем на части если список слишком длинный
        message_parts = []
        current_part = "🗑 <b>Выберите номер голосования для удаления:</b>\n\n"
        
        for poll_info in poll_list:
            test_part = current_part + poll_info + "\n"
            if len(test_part) > 3500:  # Оставляем запас для Telegram
                message_parts.append(current_part)
                current_part = poll_info + "\n"
            else:
                current_part = test_part
        
        if current_part.strip():
            message_parts.append(current_part)
        
        # Отправляем все части
        for i, part in enumerate(message_parts):
            if i == len(message_parts) - 1:  # Последняя часть
                part += "\n💡 <i>Введите номер голосования:</i>"
            
            try:
                update.message.reply_text(part, parse_mode=ParseMode.HTML)
            except:
                # Fallback без HTML
                clean_part = part.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                update.message.reply_text(clean_part)
        
        return POLL_DEL_ID
        
    except Exception as e:
        logger.error(f"Error in start_delete_poll: {e}")
        update.message.reply_text("❌ Ошибка при получении списка голосований")
        return ConversationHandler.END

def confirm_delete_poll(update: Update, context: CallbackContext):
    """Подтверждение удаления голосования"""
    try:
        poll_number = int(update.message.text.strip())
        
        polls = load_polls()
        poll_to_delete = None
        
        for poll in polls:
            if poll.get('id') == poll_number:
                poll_to_delete = poll
                break
        
        if not poll_to_delete:
            try:
                update.message.reply_text(f"❌ <b>Голосование #{poll_number} не найдено</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text(f"❌ Голосование #{poll_number} не найдено")
            return POLL_DEL_ID
        
        # Останавливаем задание для этого голосования
        job_queue = context.dispatcher.job_queue
        current_jobs = job_queue.jobs()
        for job in current_jobs:
            if hasattr(job, 'name') and job.name == f'poll_{poll_number}':
                job.schedule_removal()
                logger.info(f"🛑 Stopped job for poll #{poll_number}")
        
        # ✅ СИНХРОНИЗАЦИЯ С GOOGLE SHEETS ПРИ УДАЛЕНИИ
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                poll_data = {
                    "id": poll_to_delete.get('id'),
                    "question": poll_to_delete.get('question', ''),
                    "options": ', '.join(poll_to_delete.get('options', [])),
                    "time": poll_to_delete.get('datetime') or poll_to_delete.get('time', ''),
                    "type": poll_to_delete.get('type', ''),
                    "chat_id": chat_id,
                    "chat_name": chat_name,
                    "created_at": poll_to_delete.get('created_at', ''),
                    "username": poll_to_delete.get('username', username),
                    "last_sent": poll_to_delete.get('last_sent', ''),
                    "days_of_week": poll_to_delete.get('day', '') if poll_to_delete.get('type') == 'weekly' else poll_to_delete.get('days_of_week', '')
                }
                
                # ВАЖНО: Используем действие "DELETE" для установки статуса "Deleted"
                sheets_manager.sync_poll(poll_data, "DELETE")
                
                # Обновляем количество голосований для чата
                sheets_manager.update_polls_count(chat_id)
                
                logger.info(f"📊 Successfully synced poll #{poll_to_delete.get('id')} deletion to Google Sheets (status: Deleted)")
                
            except Exception as e:
                logger.error(f"❌ Error syncing poll deletion to Google Sheets: {e}")
                # Продолжаем удаление даже если синхронизация не удалась
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - poll #{poll_to_delete.get('id')} deletion not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        else:
            logger.warning("📵 Google Sheets not available for poll deletion sync")
        
        # Удаляем голосование из локального файла
        all_polls = load_polls()
        new_list = [p for p in all_polls if p["id"] != poll_to_delete["id"]]
        save_polls(new_list)
        
        try:
            update.message.reply_text(f"✅ <b>Голосование #{poll_number} удалено</b>\n<i>Статус в Google Sheets изменен на Deleted</i>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text(f"✅ Голосование #{poll_number} удалено")
        
        # Перепланируем все голосования
        reschedule_all_polls(context.dispatcher.job_queue)
        
    except ValueError:
        try:
            update.message.reply_text("❌ <b>Введите номер голосования</b>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Введите номер голосования")
        return POLL_DEL_ID
    except Exception as e:
        logger.error(f"Error in confirm_delete_poll: {e}")
        update.message.reply_text("❌ Ошибка при удалении голосования")
    
    return ConversationHandler.END

def clear_polls(update: Update, context: CallbackContext):
    """Очистка всех голосований"""
    try:
        # Получаем все текущие голосования перед удалением для синхронизации
        all_polls = load_polls()
        polls_count = len(all_polls)
        
        # ✅ СИНХРОНИЗАЦИЯ С GOOGLE SHEETS ПРИ МАССОВОМ УДАЛЕНИИ
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized and all_polls:
            try:
                chat_id = update.effective_chat.id
                chat = update.effective_chat
                chat_name = chat.title if chat.title else f"@{chat.username}" if chat.username else str(chat.first_name or "Private")
                username = update.effective_user.username or update.effective_user.first_name or "Unknown"
                
                # Отправляем сообщение о начале операции
                try:
                    progress_message = update.message.reply_text(
                        f"🔄 <b>Удаление всех голосований...</b>\n\n"
                        f"📊 Обновление Google Sheets для {polls_count} голосований...",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    progress_message = update.message.reply_text(f"🔄 Удаление {polls_count} голосований...")
                
                # Логируем начало массового удаления
                sheets_manager.log_poll_action("CLEAR_ALL", update.effective_user.id, username, chat_id, f"Started mass deletion of {polls_count} polls", "")
                
                # Синхронизируем каждое голосование - устанавливаем статус "Deleted"
                synced_count = 0
                failed_count = 0
                batch_size = 5  # Обрабатываем по 5 голосований за раз
                batch_delay = 10.0  # 10 секунд между батчами
                
                for i, poll in enumerate(all_polls):
                    try:
                        # Проверяем, нужен ли перерыв между батчами
                        if i > 0 and i % batch_size == 0:
                            logger.info(f"📦 Completed batch {i//batch_size}, waiting {batch_delay}s before next batch...")
                            
                            # Обновляем сообщение о прогрессе между батчами
                            try:
                                context.bot.edit_message_text(
                                    chat_id=progress_message.chat_id,
                                    message_id=progress_message.message_id,
                                    text=f"🔄 <b>Пауза между батчами...</b>\n\n"
                                         f"✅ Обработано: {i}/{polls_count}\n"
                                         f"✅ Успешно: {synced_count}\n"
                                         f"❌ Ошибки: {failed_count}\n\n"
                                         f"⏱️ Ожидание {batch_delay}s...",
                                    parse_mode=ParseMode.HTML
                                )
                            except:
                                pass
                            
                            time.sleep(batch_delay)
                        
                        poll_data = {
                            "id": poll.get('id'),
                            "question": poll.get('question', ''),
                            "options": ', '.join(poll.get('options', [])),
                            "time": poll.get('datetime') or poll.get('time', ''),
                            "type": poll.get('type', ''),
                            "chat_id": chat_id,
                            "chat_name": chat_name,
                            "created_at": poll.get('created_at', ''),
                            "username": poll.get('username', username),
                            "last_sent": poll.get('last_sent', ''),
                            "days_of_week": poll.get('day', '') if poll.get('type') == 'weekly' else poll.get('days_of_week', '')
                        }
                        
                        # ВАЖНО: Используем действие "DELETE" для установки статуса "Deleted"
                        success = sheets_manager.sync_poll(poll_data, "DELETE")
                        if success:
                            synced_count += 1
                        else:
                            failed_count += 1
                            logger.warning(f"⚠️ Failed to sync poll #{poll.get('id')} deletion")
                        
                        # Добавляем задержку между операциями для предотвращения rate limiting
                        if i < len(all_polls) - 1:  # Не задерживаем после последнего
                            base_delay = 1.0  # Базовая задержка 1 секунда
                            progressive_delay = (i % batch_size) * 0.2  # Дополнительная задержка внутри батча
                            total_delay = base_delay + progressive_delay
                            time.sleep(total_delay)
                            logger.debug(f"⏱️ Waiting {total_delay:.1f}s before next sync operation ({i+2}/{len(all_polls)})")
                        
                    except Exception as e:
                        logger.error(f"❌ Error syncing poll #{poll.get('id')} deletion: {e}")
                        failed_count += 1
                        # Продолжаем даже если одно голосование не синхронизировалось
                
                # Обновляем сообщение о прогрессе после синхронизации голосований
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=f"🔄 <b>Обновление статистики...</b>\n\n"
                             f"✅ Голосования: {synced_count}/{polls_count}\n"
                             f"❌ Ошибки: {failed_count}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
                
                # Увеличенная задержка перед обновлением статистики
                time.sleep(2.0)
                
                # Обновляем количество голосований для чата (должно стать 0)
                count_update_success = sheets_manager.update_polls_count(chat_id)
                
                # Финальное логирование
                sheets_manager.log_poll_action("CLEAR_ALL_COMPLETE", update.effective_user.id, username, chat_id, f"Completed mass deletion. Synced: {synced_count}/{polls_count}, Failed: {failed_count}", "")
                
                logger.info(f"📊 Mass deletion summary: {synced_count}/{polls_count} polls synced, {failed_count} failed")
                
                # Обновляем сообщение о прогрессе
                try:
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=f"🔄 <b>Завершение удаления...</b>\n\n"
                             f"✅ Google Sheets обновлен ({synced_count}/{polls_count})"
                             f"\n{'⚠️ Статистика чатов: ошибка обновления' if not count_update_success else '✅ Статистика чатов обновлена'}",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    pass
                
            except Exception as e:
                logger.error(f"❌ Error syncing mass deletion to Google Sheets: {e}")
                # Продолжаем удаление даже если синхронизация не удалась
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - mass deletion of {polls_count} polls not synced")
            logger.warning("   Check GOOGLE_SHEETS_ID and GOOGLE_SHEETS_CREDENTIALS environment variables")
        elif not all_polls:
            logger.info("📭 No polls to delete")
        else:
            logger.warning("📵 Google Sheets not available for mass deletion sync")
        
        # Удаляем все голосования из локального файла
        save_polls([])
        
        # Останавливаем все задания
        job_queue = context.dispatcher.job_queue
        current_jobs = job_queue.jobs()
        for job in current_jobs:
            if hasattr(job, 'name') and job.name and job.name.startswith('poll_'):
                job.schedule_removal()
        
        # Финальное сообщение пользователю
        if polls_count > 0:
            try:
                # Если есть progress_message, обновляем его
                if 'progress_message' in locals():
                    # Формируем детальное сообщение с учетом успешности операций
                    if 'synced_count' in locals() and 'failed_count' in locals():
                        if failed_count == 0:
                            final_text = f"🗑 <b>Все голосования удалены ({polls_count})</b>\n<i>✅ Статус всех голосований в Google Sheets изменен на Deleted</i>"
                        else:
                            final_text = f"🗑 <b>Голосования удалены ({polls_count})</b>\n<i>✅ Синхронизировано: {synced_count}/{polls_count}\n⚠️ Ошибок синхронизации: {failed_count}</i>"
                    else:
                        final_text = f"🗑 <b>Все голосования удалены ({polls_count})</b>\n<i>⚠️ Google Sheets недоступен</i>"
                    
                    context.bot.edit_message_text(
                        chat_id=progress_message.chat_id,
                        message_id=progress_message.message_id,
                        text=final_text,
                        parse_mode=ParseMode.HTML
                    )
                else:
                    update.message.reply_text(
                        f"🗑 <b>Все голосования удалены ({polls_count})</b>\n"
                        f"<i>Статус в Google Sheets изменен на Deleted</i>", 
                        parse_mode=ParseMode.HTML
                    )
            except:
                update.message.reply_text(f"🗑 Все голосования удалены ({polls_count})")
        else:
            try:
                update.message.reply_text("📭 <b>Голосований для удаления не найдено</b>", parse_mode=ParseMode.HTML)
            except:
                update.message.reply_text("📭 Голосований для удаления не найдено")
            
    except Exception as e:
        logger.error(f"Error in clear_polls: {e}")
        update.message.reply_text("❌ Ошибка при очистке голосований")

def cancel_poll(update: Update, context: CallbackContext):
    """
    Отмена создания голосования.
    """
    try:
        update.message.reply_text("❌ <b>Операция отменена</b>", parse_mode=ParseMode.HTML)
    except:
        update.message.reply_text("❌ Операция отменена")
    return ConversationHandler.END

# --- Poll handlers ---

def start_add_one_poll(update: Update, context: CallbackContext):
    """
    Начинает процесс создания одноразового голосования.
    """
    update.message.reply_text(
        "📊 <b>Создание одноразового голосования</b>\n\n"
        "Введите дату и время отправки голосования в формате:\n"
        "<code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n\n"
        "Например: <code>25.12.2024 15:30</code>\n\n"
        "⏰ Время указывается по Москве",
        parse_mode=ParseMode.HTML
    )
    return POLL_DATE

def receive_poll_datetime(update: Update, context: CallbackContext):
    """
    Получает дату и время для одноразового голосования.
    """
    try:
        datetime_str = update.message.text.strip()
        poll_datetime = datetime.strptime(datetime_str, "%d.%m.%Y %H:%M")
        moscow_tz = pytz.timezone('Europe/Moscow')
        poll_datetime = moscow_tz.localize(poll_datetime)
        
        # Проверяем, что время в будущем
        now_moscow = datetime.now(moscow_tz)
        if poll_datetime <= now_moscow:
            update.message.reply_text(
                "⚠️ <b>Ошибка!</b>\n\n"
                "Время голосования должно быть в будущем.\n"
                "Попробуйте еще раз.",
                parse_mode=ParseMode.HTML
            )
            return POLL_DATE
        
        context.user_data['poll_datetime'] = poll_datetime
        update.message.reply_text(
            "❓ <b>Введите вопрос для голосования</b>\n\n"
            "Например: <i>Какой фильм посмотрим сегодня?</i>",
            parse_mode=ParseMode.HTML
        )
        return POLL_QUESTION
        
    except ValueError:
        update.message.reply_text(
            "⚠️ <b>Неверный формат даты!</b>\n\n"
            "Используйте формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
            "Например: <code>25.12.2024 15:30</code>",
            parse_mode=ParseMode.HTML
        )
        return POLL_DATE

def receive_poll_question(update: Update, context: CallbackContext):
    """
    Получает вопрос для голосования.
    """
    question = update.message.text.strip()
    if len(question) > 300:
        update.message.reply_text(
            "⚠️ <b>Вопрос слишком длинный!</b>\n\n"
            "Максимальная длина вопроса: 300 символов.\n"
            "Попробуйте сократить вопрос.",
            parse_mode=ParseMode.HTML
        )
        return POLL_QUESTION
    
    context.user_data['poll_question'] = question
    update.message.reply_text(
        "📝 <b>Введите варианты ответов</b>\n\n"
        "Каждый вариант с новой строки.\n"
        "Максимум 12 вариантов.\n\n"
        "Пример:\n"
        "<code>Вариант 1\n"
        "Вариант 2\n"
        "Вариант 3</code>",
        parse_mode=ParseMode.HTML
    )
    return POLL_OPTIONS

def receive_poll_options(update: Update, context: CallbackContext):
    """
    Получает варианты ответов для голосования и создает голосование.
    """
    options_text = update.message.text.strip()
    options = [opt.strip() for opt in options_text.split('\n') if opt.strip()]
    
    if len(options) < 2:
        update.message.reply_text(
            "⚠️ <b>Недостаточно вариантов!</b>\n\n"
            "Минимум 2 варианта ответа.\n"
            "Попробуйте еще раз.",
            parse_mode=ParseMode.HTML
        )
        return POLL_OPTIONS
    
    if len(options) > 12:
        update.message.reply_text(
            "⚠️ <b>Слишком много вариантов!</b>\n\n"
            "Максимум 12 вариантов ответа.\n"
            "Попробуйте сократить список.",
            parse_mode=ParseMode.HTML
        )
        return POLL_OPTIONS
    
    # Проверяем длину каждого варианта
    for option in options:
        if len(option) > 100:
            update.message.reply_text(
                "⚠️ <b>Вариант слишком длинный!</b>\n\n"
                "Максимальная длина варианта: 100 символов.\n"
                "Попробуйте сократить варианты.",
                parse_mode=ParseMode.HTML
            )
            return POLL_OPTIONS
    
    try:
        polls = load_polls()
        poll_id = get_next_poll_id()
        
        new_poll = {
            "id": poll_id,
            "question": context.user_data['poll_question'],
            "options": options,
            "datetime": context.user_data['poll_datetime'].strftime("%Y-%m-%d %H:%M"),
            "type": "once",
            "chat_id": update.effective_chat.id,
            "chat_name": update.effective_chat.title or update.effective_user.first_name or "Unknown",
            "status": "Active",
            "created_at": get_moscow_time().strftime("%d.%m.%Y %H:%M:%S"),
            "username": update.effective_user.username or "Unknown",
            "last_sent": None,
            "days_of_week": None,
            "allow_multiple_answers": True
        }
        
        polls.append(new_poll)
        save_polls(polls)
        
        # Планируем голосование
        schedule_poll(context.job_queue, new_poll)
        
        # Логируем в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager:
            try:
                sheets_manager.log_poll_action(
                    action="CREATE",
                    user_id=update.effective_user.id,
                    username=update.effective_user.username or "Unknown",
                    chat_id=update.effective_chat.id,
                    details=f"One-time poll: {new_poll['question'][:50]}...",
                    poll_id=poll_id
                )
                sheets_manager.sync_poll(new_poll, 'CREATE')
            except Exception as e:
                logger.error(f"Failed to sync poll to sheets: {e}")
        
        moscow_time_str = context.user_data['poll_datetime'].strftime("%d.%m.%Y в %H:%M")
        update.message.reply_text(
            f"✅ <b>Голосование создано!</b>\n\n"
            f"📊 <b>Вопрос:</b> {safe_html_escape(new_poll['question'])}\n"
            f"📝 <b>Вариантов:</b> {len(options)}\n"
            f"⏰ <b>Время отправки:</b> {moscow_time_str} (МСК)\n"
            f"🆔 <b>ID:</b> {poll_id}",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error creating poll: {e}")
        update.message.reply_text(
            "❌ <b>Ошибка при создании голосования</b>\n\n"
            "Попробуйте еще раз позже.",
            parse_mode=ParseMode.HTML
        )
    
    return ConversationHandler.END

def cancel_poll(update: Update, context: CallbackContext):
    """
    Отмена создания голосования.
    """
    try:
        update.message.reply_text("❌ <b>Создание голосования отменено</b>", parse_mode=ParseMode.HTML)
    except:
        update.message.reply_text("❌ Создание голосования отменено")
    return ConversationHandler.END

def start_add_daily_poll(update: Update, context: CallbackContext):
    """
    Начинает процесс создания ежедневного голосования.
    """
    update.message.reply_text(
        "📊 <b>Создание ежедневного голосования</b>\n\n"
        "Введите время отправки голосования в формате:\n"
        "<code>ЧЧ:ММ</code>\n\n"
        "Например: <code>09:00</code> или <code>18:30</code>\n\n"
        "⏰ Время указывается по Москве",
        parse_mode=ParseMode.HTML
    )
    return DAILY_POLL_TIME

def receive_daily_poll_time(update: Update, context: CallbackContext):
    """
    Получает время для ежедневного голосования.
    """
    try:
        time_str = update.message.text.strip()
        poll_time = datetime.strptime(time_str, "%H:%M").time()
        
        context.user_data['daily_poll_time'] = poll_time
        update.message.reply_text(
            "❓ <b>Введите вопрос для ежедневного голосования</b>\n\n"
            "Например: <i>Что будем есть на обед?</i>",
            parse_mode=ParseMode.HTML
        )
        return DAILY_POLL_QUESTION
        
    except ValueError:
        update.message.reply_text(
            "⚠️ <b>Неверный формат времени!</b>\n\n"
            "Используйте формат: <code>ЧЧ:ММ</code>\n"
            "Например: <code>09:00</code> или <code>18:30</code>",
            parse_mode=ParseMode.HTML
        )
        return DAILY_POLL_TIME

def receive_daily_poll_question(update: Update, context: CallbackContext):
    """
    Получает вопрос для ежедневного голосования.
    """
    question = update.message.text.strip()
    if len(question) > 300:
        update.message.reply_text(
            "⚠️ <b>Вопрос слишком длинный!</b>\n\n"
            "Максимальная длина вопроса: 300 символов.\n"
            "Попробуйте сократить вопрос.",
            parse_mode=ParseMode.HTML
        )
        return DAILY_POLL_QUESTION
    
    context.user_data['daily_poll_question'] = question
    update.message.reply_text(
        "📝 <b>Введите варианты ответов</b>\n\n"
        "Каждый вариант с новой строки.\n"
        "Максимум 12 вариантов.\n\n"
        "Пример:\n"
        "<code>Вариант 1\n"
        "Вариант 2\n"
        "Вариант 3</code>",
        parse_mode=ParseMode.HTML
    )
    return DAILY_POLL_OPTIONS

def receive_daily_poll_options(update: Update, context: CallbackContext):
    """
    Получает варианты ответов для ежедневного голосования и создает голосование.
    """
    options_text = update.message.text.strip()
    options = [opt.strip() for opt in options_text.split('\n') if opt.strip()]
    
    if len(options) < 2:
        update.message.reply_text(
            "⚠️ <b>Недостаточно вариантов!</b>\n\n"
            "Минимум 2 варианта ответа.\n"
            "Попробуйте еще раз.",
            parse_mode=ParseMode.HTML
        )
        return DAILY_POLL_OPTIONS
    
    if len(options) > 12:
        update.message.reply_text(
            "⚠️ <b>Слишком много вариантов!</b>\n\n"
            "Максимум 12 вариантов ответа.\n"
            "Попробуйте сократить список.",
            parse_mode=ParseMode.HTML
        )
        return DAILY_POLL_OPTIONS
    
    # Проверяем длину каждого варианта
    for option in options:
        if len(option) > 100:
            update.message.reply_text(
                "⚠️ <b>Вариант слишком длинный!</b>\n\n"
                "Максимальная длина варианта: 100 символов.\n"
                "Попробуйте сократить варианты.",
                parse_mode=ParseMode.HTML
            )
            return DAILY_POLL_OPTIONS
    
    try:
        polls = load_polls()
        poll_id = get_next_poll_id()
        
        new_poll = {
            "id": poll_id,
            "question": context.user_data['daily_poll_question'],
            "options": options,
            "time": context.user_data['daily_poll_time'].strftime("%H:%M"),
            "type": "daily",
            "chat_id": update.effective_chat.id,
            "chat_name": update.effective_chat.title or update.effective_user.first_name or "Unknown",
            "status": "Active",
            "created_at": get_moscow_time().strftime("%d.%m.%Y %H:%M:%S"),
            "username": update.effective_user.username or "Unknown",
            "last_sent": None,
            "days_of_week": None,
            "allow_multiple_answers": True
        }
        
        polls.append(new_poll)
        save_polls(polls)
        
        # Планируем голосование
        schedule_poll(context.job_queue, new_poll)
        
        # Логируем в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager:
            try:
                sheets_manager.log_poll_action(
                    action="CREATE",
                    user_id=update.effective_user.id,
                    username=update.effective_user.username or "Unknown",
                    chat_id=update.effective_chat.id,
                    details=f"Daily poll: {new_poll['question'][:50]}...",
                    poll_id=poll_id
                )
                sheets_manager.sync_poll(new_poll, 'CREATE')
            except Exception as e:
                logger.error(f"Failed to sync poll to sheets: {e}")
        
        time_str = context.user_data['daily_poll_time'].strftime("%H:%M")
        update.message.reply_text(
            f"✅ <b>Ежедневное голосование создано!</b>\n\n"
            f"📊 <b>Вопрос:</b> {safe_html_escape(new_poll['question'])}\n"
            f"📝 <b>Вариантов:</b> {len(options)}\n"
            f"⏰ <b>Время отправки:</b> каждый день в {time_str} (МСК)\n"
            f"🆔 <b>ID:</b> {poll_id}",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error creating daily poll: {e}")
        update.message.reply_text(
            "❌ <b>Ошибка при создании ежедневного голосования</b>\n\n"
            "Попробуйте еще раз позже.",
            parse_mode=ParseMode.HTML
        )
    
    return ConversationHandler.END

def start_add_weekly_poll(update: Update, context: CallbackContext):
    """
    Начинает процесс создания еженедельного голосования.
    """
    keyboard = [
        [InlineKeyboardButton("Понедельник", callback_data="poll_day_monday")],
        [InlineKeyboardButton("Вторник", callback_data="poll_day_tuesday")],
        [InlineKeyboardButton("Среда", callback_data="poll_day_wednesday")],
        [InlineKeyboardButton("Четверг", callback_data="poll_day_thursday")],
        [InlineKeyboardButton("Пятница", callback_data="poll_day_friday")],
        [InlineKeyboardButton("Суббота", callback_data="poll_day_saturday")],
        [InlineKeyboardButton("Воскресенье", callback_data="poll_day_sunday")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    update.message.reply_text(
        "📊 <b>Создание еженедельного голосования</b>\n\n"
        "Выберите день недели для отправки голосования:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )
    return WEEKLY_POLL_DAY

def receive_weekly_poll_day(update: Update, context: CallbackContext):
    """
    Получает день недели для еженедельного голосования.
    """
    query = update.callback_query
    query.answer()
    
    day_mapping = {
        "poll_day_monday": (0, "Понедельник"),
        "poll_day_tuesday": (1, "Вторник"),
        "poll_day_wednesday": (2, "Среда"),
        "poll_day_thursday": (3, "Четверг"),
        "poll_day_friday": (4, "Пятница"),
        "poll_day_saturday": (5, "Суббота"),
        "poll_day_sunday": (6, "Воскресенье")
    }
    
    if query.data in day_mapping:
        day_num, day_name = day_mapping[query.data]
        context.user_data['weekly_poll_day'] = day_num
        context.user_data['weekly_poll_day_name'] = day_name
        
        query.edit_message_text(
            f"✅ <b>Выбран день:</b> {day_name}\n\n"
            "Введите время отправки голосования в формате:\n"
            "<code>ЧЧ:ММ</code>\n\n"
            "Например: <code>09:00</code> или <code>18:30</code>\n\n"
            "⏰ Время указывается по Москве",
            parse_mode=ParseMode.HTML
        )
        return WEEKLY_POLL_TIME
    
    return WEEKLY_POLL_DAY

def receive_weekly_poll_time(update: Update, context: CallbackContext):
    """
    Получает время для еженедельного голосования.
    """
    try:
        time_str = update.message.text.strip()
        poll_time = datetime.strptime(time_str, "%H:%M").time()
        
        context.user_data['weekly_poll_time'] = poll_time
        update.message.reply_text(
            "❓ <b>Введите вопрос для еженедельного голосования</b>\n\n"
            "Например: <i>Какие планы на выходные?</i>",
            parse_mode=ParseMode.HTML
        )
        return WEEKLY_POLL_QUESTION
        
    except ValueError:
        update.message.reply_text(
            "⚠️ <b>Неверный формат времени!</b>\n\n"
            "Используйте формат: <code>ЧЧ:ММ</code>\n"
            "Например: <code>09:00</code> или <code>18:30</code>",
            parse_mode=ParseMode.HTML
        )
        return WEEKLY_POLL_TIME

def receive_weekly_poll_question(update: Update, context: CallbackContext):
    """
    Получает вопрос для еженедельного голосования.
    """
    question = update.message.text.strip()
    if len(question) > 300:
        update.message.reply_text(
            "⚠️ <b>Вопрос слишком длинный!</b>\n\n"
            "Максимальная длина вопроса: 300 символов.\n"
            "Попробуйте сократить вопрос.",
            parse_mode=ParseMode.HTML
        )
        return WEEKLY_POLL_QUESTION
    
    context.user_data['weekly_poll_question'] = question
    update.message.reply_text(
        "📝 <b>Введите варианты ответов</b>\n\n"
        "Каждый вариант с новой строки.\n"
        "Максимум 12 вариантов.\n\n"
        "Пример:\n"
        "<code>Вариант 1\n"
        "Вариант 2\n"
        "Вариант 3</code>",
        parse_mode=ParseMode.HTML
    )
    return WEEKLY_POLL_OPTIONS

def receive_weekly_poll_options(update: Update, context: CallbackContext):
    """
    Получает варианты ответов для еженедельного голосования и создает голосование.
    """
    options_text = update.message.text.strip()
    options = [opt.strip() for opt in options_text.split('\n') if opt.strip()]
    
    if len(options) < 2:
        update.message.reply_text(
            "⚠️ <b>Недостаточно вариантов!</b>\n\n"
            "Минимум 2 варианта ответа.\n"
            "Попробуйте еще раз.",
            parse_mode=ParseMode.HTML
        )
        return WEEKLY_POLL_OPTIONS
    
    if len(options) > 12:
        update.message.reply_text(
            "⚠️ <b>Слишком много вариантов!</b>\n\n"
            "Максимум 12 вариантов ответа.\n"
            "Попробуйте сократить список.",
            parse_mode=ParseMode.HTML
        )
        return WEEKLY_POLL_OPTIONS
    
    # Проверяем длину каждого варианта
    for option in options:
        if len(option) > 100:
            update.message.reply_text(
                "⚠️ <b>Вариант слишком длинный!</b>\n\n"
                "Максимальная длина варианта: 100 символов.\n"
                "Попробуйте сократить варианты.",
                parse_mode=ParseMode.HTML
            )
            return WEEKLY_POLL_OPTIONS
    
    try:
        polls = load_polls()
        poll_id = get_next_poll_id()
        
        new_poll = {
            "id": poll_id,
            "question": context.user_data['weekly_poll_question'],
            "options": options,
            "time": context.user_data['weekly_poll_time'].strftime("%H:%M"),
            "day": context.user_data['weekly_poll_day'],
            "type": "weekly",
            "chat_id": update.effective_chat.id,
            "chat_name": update.effective_chat.title or update.effective_user.first_name or "Unknown",
            "status": "Active",
            "created_at": get_moscow_time().strftime("%d.%m.%Y %H:%M:%S"),
            "username": update.effective_user.username or "Unknown",
            "last_sent": None,
            "days_of_week": [context.user_data['weekly_poll_day']],
            "allow_multiple_answers": True
        }
        
        polls.append(new_poll)
        save_polls(polls)
        
        # Планируем голосование
        schedule_poll(context.job_queue, new_poll)
        
        # Логируем в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager:
            try:
                sheets_manager.log_poll_action(
                    action="CREATE",
                    user_id=update.effective_user.id,
                    username=update.effective_user.username or "Unknown",
                    chat_id=update.effective_chat.id,
                    details=f"Weekly poll: {new_poll['question'][:50]}...",
                    poll_id=poll_id
                )
                sheets_manager.sync_poll(new_poll, 'CREATE')
            except Exception as e:
                logger.error(f"Failed to sync poll to sheets: {e}")
        
        day_name = context.user_data['weekly_poll_day_name']
        time_str = context.user_data['weekly_poll_time'].strftime("%H:%M")
        update.message.reply_text(
            f"✅ <b>Еженедельное голосование создано!</b>\n\n"
            f"📊 <b>Вопрос:</b> {safe_html_escape(new_poll['question'])}\n"
            f"📝 <b>Вариантов:</b> {len(options)}\n"
            f"⏰ <b>Время отправки:</b> каждый {day_name} в {time_str} (МСК)\n"
            f"🆔 <b>ID:</b> {poll_id}",
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logger.error(f"Error creating weekly poll: {e}")
        update.message.reply_text(
            "❌ <b>Ошибка при создании еженедельного голосования</b>\n\n"
            "Попробуйте еще раз позже.",
            parse_mode=ParseMode.HTML
        )
    
    return ConversationHandler.END

# --- Scheduling helpers ---

def send_poll(context: CallbackContext):
    """
    Отправляет голосование всем подписанным чатам.
    """
    # 🚨 КРИТИЧЕСКОЕ ЛОГИРОВАНИЕ НАЧАЛА ВЫПОЛНЕНИЯ
    moscow_time_start = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S MSK")
    utc_time_start = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    logger.info(f"🎯 POLL EXECUTION STARTED at {moscow_time_start} ({utc_time_start})")
    logger.info(f"📋 Job context: {context.job.context if context.job else 'NO JOB CONTEXT'}")
    
    try:
        poll = context.job.context
        
        # 🔍 ДЕТАЛЬНАЯ ПРОВЕРКА КОНТЕКСТА ГОЛОСОВАНИЯ
        if not poll:
            logger.error(f"❌ CRITICAL: No poll context found in job!")
            return
        
        poll_id = poll.get('id', 'UNKNOWN')
        poll_type = poll.get('type', 'UNKNOWN')
        poll_question = poll.get('question', 'NO QUESTION')[:50]
        
        logger.info(f"📊 Poll details: ID={poll_id}, Type={poll_type}, Question='{poll_question}...'")
        logger.info(f"🔧 Full poll context: {poll}")
        
        # Пытаемся загрузить чаты с автовосстановлением
        try:
            with open("subscribed_chats.json", "r") as f:
                chats = json.load(f)
                if not chats or len(chats) == 0:
                    raise ValueError("Empty chats list")
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            logger.warning(f"⚠️ Problem with subscribed_chats.json: {e}")
            logger.info("🔧 Attempting emergency restore...")
            if ensure_subscribed_chats_file():
                try:
                    with open("subscribed_chats.json", "r") as f:
                        chats = json.load(f)
                    logger.info(f"✅ Emergency restore successful, loaded {len(chats)} chats")
                except:
                    logger.error("❌ Emergency restore failed, no polls will be sent")
                    return
            else:
                logger.error("❌ Emergency restore failed, no polls will be sent")
                return
        
        # 🆕 ОБРАБОТКА СЛУЧАЯ "НЕТ АКТИВНЫХ ЧАТОВ"
        if not chats or len(chats) == 0:
            moscow_time = get_moscow_time().strftime("%H:%M MSK")
            utc_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            poll_id = poll.get('id', 'unknown')
            
            logger.warning(f"⚠️ No active chats available for poll #{poll_id}")
            logger.info(f"📋 Poll details: {poll.get('type')} - '{poll.get('question', '')[:50]}...'")
            
            # 📊 Логируем в Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    sheets_manager.log_poll_sent(
                    poll_id=poll_id,
                    chat_id="NO_CHATS",
                    status="NO_RECIPIENTS",
                    error="No active chats available for delivery",
                    question_preview=poll.get('question', '')[:50] + "..." if len(poll.get('question', '')) > 50 else poll.get('question', '')
                )
                    logger.info(f"📊 Logged 'no recipients' status for poll #{poll_id}")
                except Exception as e:
                    logger.error(f"❌ Error logging 'no recipients' to Google Sheets: {e}")
            
            # 🚮 АВТОМАТИЧЕСКОЕ УДАЛЕНИЕ РАЗОВЫХ ГОЛОСОВАНИЙ БЕЗ ПОЛУЧАТЕЛЕЙ
            if poll.get("type") == "once" or poll.get("type") == "one_time":
                moscow_sent_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                
                # Обновляем данные голосования перед удалением
                updated_poll = poll.copy()
                updated_poll['last_sent'] = moscow_sent_time
                updated_poll['delivery_status'] = "No recipients available - auto-deleted"
                
                # Удаляем из локального файла
                polls = load_polls()
                polls = [p for p in polls if p.get("id") != poll.get("id")]
                save_polls(polls)
                logger.info(f"🗑️ One-time poll #{poll_id} auto-deleted: no recipients available")
                
                # 📊 СИНХРОНИЗИРУЕМ УДАЛЕНИЕ В GOOGLE SHEETS
                if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                    try:
                        # Обновляем информацию о попытке отправки
                        sheets_manager.sync_poll(updated_poll, "UPDATE")
                        logger.info(f"📊 Updated poll #{poll_id} with 'no recipients' info")
                        
                        # Затем помечаем как удаленное
                        sheets_manager.sync_poll(updated_poll, "DELETE")
                        logger.info(f"📊 Marked poll #{poll_id} as 'Deleted' (no recipients)")
                        
                        # Логируем завершение обработки
                        sheets_manager.log_poll_action(
                            "ONCE_AUTO_DELETED", 
                            "SYSTEM", 
                            "NoRecipients", 
                            0, 
                            f"One-time poll auto-deleted: no active chats available for delivery",
                            poll_id
                        )
                        logger.info(f"✅ One-time poll #{poll_id} marked as completed: no recipients")
                        
                    except Exception as e:
                        logger.error(f"❌ Error syncing 'no recipients' deletion to Google Sheets: {e}")
                        
                else:
                    logger.warning(f"📵 Google Sheets not available - poll #{poll_id} removed locally only")
                
                logger.info(f"✅ One-time poll #{poll_id} processing completed: no recipients available")
                    
            else:
                # Для повторяющихся голосований просто логируем
                logger.info(f"📅 Recurring poll #{poll_id} ({poll.get('type')}) - will retry on next schedule")
                
            return  # Завершаем выполнение функции
        
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        utc_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        poll_id = poll.get('id', 'unknown')
        
        # 📊 Логируем начало отправки в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                sheets_manager.log_poll_sent(
                    poll_id=poll_id,
                    chat_id="ALL",
                    status="SENDING",
                    error="",
                    question_preview=poll.get('question', '')[:50] + "..." if len(poll.get('question', '')) > 50 else poll.get('question', '')
                )
                logger.info(f"📊 Logged poll sending start for #{poll_id} in Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error logging send start to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - poll #{poll_id} sending start not logged")
        
        # Отправляем каждому чату
        total_sent = 0
        total_failed = 0
        blocked_chats = []  # 🆕 Список заблокированных чатов для удаления
        
        for cid in chats[:]:  # Используем срез для безопасной итерации
            delivery_status = "SUCCESS"
            error_details = ""
            
            try:
                # Отправляем голосование
                poll_message = context.bot.send_poll(
                    chat_id=cid,
                    question=poll.get('question', ''),
                    options=poll.get('options', []),
                    is_anonymous=False,  # Неанонимное голосование
                    allows_multiple_answers=poll.get('allow_multiple_answers', True)  # Множественный выбор по умолчанию
                )
                
                # Кнопка "Результаты" убрана по запросу пользователя
                
                logger.info(f"✅ Poll sent to chat {cid} at {moscow_time}")
                total_sent += 1
                
            except Exception as e:
                error_str = str(e)
                logger.error(f"❌ Failed to send poll to chat {cid}: {e}")
                error_details = error_str
                delivery_status = "FAILED"
                
                # 🆕 ПРОВЕРЯЕМ НА БЛОКИРОВКУ БОТА
                if "Forbidden: bot was blocked by the user" in error_str or \
                   "Forbidden: user is deactivated" in error_str or \
                   "Forbidden: the group chat was deleted" in error_str or \
                   "Bad Request: chat not found" in error_str:
                    
                    logger.warning(f"🚫 Chat {cid} blocked bot or deleted - adding to removal list")
                    blocked_chats.append(cid)
                    delivery_status = "BLOCKED_AUTO_REMOVE"
                    error_details = f"Auto-removed due to: {error_str}"
                    total_failed += 1
                    continue  # Пропускаем fallback для заблокированных чатов
                
                total_failed += 1
                
            # 📊 Логируем результат отправки в каждый чат
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    sheets_manager.log_poll_sent(
                        poll_id=poll_id,
                        chat_id=str(cid),
                        status=delivery_status,
                        error=error_details,
                        question_preview=poll.get('question', '')[:50] + "..." if len(poll.get('question', '')) > 50 else poll.get('question', '')
                    )
                except Exception as e:
                    logger.error(f"❌ Error logging poll delivery to Google Sheets: {e}")
        
        # 🚮 УДАЛЯЕМ ЗАБЛОКИРОВАННЫЕ ЧАТЫ
        if blocked_chats:
            logger.info(f"🧹 Removing {len(blocked_chats)} blocked/deleted chats from subscription list")
            
            # Удаляем из локального файла
            updated_chats = [cid for cid in chats if cid not in blocked_chats]
            save_chats(updated_chats)
            
            # 📊 Синхронизируем с Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    sheets_manager.sync_subscribed_chats_to_sheets()
                    logger.info(f"📊 Synced blocked chats removal to Google Sheets")
                except Exception as e:
                    logger.error(f"❌ Error syncing blocked chats removal: {e}")
            
            for blocked_chat in blocked_chats:
                logger.info(f"🚫 Removed blocked chat {blocked_chat} from subscription list")
        
        # 📊 ОБНОВЛЯЕМ ИНФОРМАЦИЮ О ПОСЛЕДНЕЙ ОТПРАВКЕ
        moscow_sent_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
        
        # Обновляем данные голосования
        updated_poll = poll.copy()
        updated_poll['last_sent'] = moscow_sent_time
        updated_poll['delivery_status'] = f"Sent to {total_sent} chats, {total_failed} failed"
        
        # Сохраняем обновленные данные
        polls = load_polls()
        for i, p in enumerate(polls):
            if p.get('id') == poll.get('id'):
                polls[i] = updated_poll
                break
        save_polls(polls)
        
        # 📊 Синхронизируем с Google Sheets
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                sheets_manager.sync_poll(updated_poll, "UPDATE")
                logger.info(f"📊 Updated poll #{poll_id} delivery info in Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error updating poll delivery info in Google Sheets: {e}")
        
        # 🗑️ УДАЛЯЕМ РАЗОВЫЕ ГОЛОСОВАНИЯ ПОСЛЕ ОТПРАВКИ
        if poll.get("type") == "once" or poll.get("type") == "one_time":
            polls = load_polls()
            polls = [p for p in polls if p.get("id") != poll.get("id")]
            save_polls(polls)
            logger.info(f"🗑️ One-time poll #{poll_id} removed after sending")
            
            # 📊 Помечаем как удаленное в Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    sheets_manager.sync_poll(updated_poll, "DELETE")
                    sheets_manager.log_poll_action(
                        "ONCE_COMPLETED", 
                        "SYSTEM", 
                        "AutoDelete", 
                        0, 
                        f"One-time poll completed and auto-deleted after sending to {total_sent} chats",
                        poll_id
                    )
                    logger.info(f"📊 Marked one-time poll #{poll_id} as completed in Google Sheets")
                except Exception as e:
                    logger.error(f"❌ Error marking one-time poll as completed in Google Sheets: {e}")
        
        logger.info(f"✅ Poll #{poll_id} sent to {total_sent} chats, {total_failed} failed at {moscow_time}")
        
    except Exception as e:
        logger.error(f"❌ Critical error in send_poll: {e}")
        poll_id = context.job.context.get('id', 'unknown') if context.job.context else 'unknown'
        
        # 📊 Логируем критическую ошибку
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                utc_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                moscow_time = get_moscow_time().strftime("%H:%M MSK")
                sheets_manager.log_poll_sent(
                    poll_id=poll_id,
                    chat_id="ERROR",
                    status="CRITICAL_ERROR",
                    error=str(e),
                    question_preview="Critical error occurred"
                )
                logger.info(f"📊 Logged critical poll error to Google Sheets")
            except Exception as sheet_error:
                logger.error(f"❌ Error logging critical poll error to Google Sheets: {sheet_error}")


def send_reminder(context: CallbackContext):
    """
    Отправляет текст напоминания всем подписанным чатам.
    """
    try:
        reminder = context.job.context
        
        # Пытаемся загрузить чаты с автовосстановлением
        try:
            with open("subscribed_chats.json", "r") as f:
                chats = json.load(f)
                if not chats or len(chats) == 0:
                    raise ValueError("Empty chats list")
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
            logger.warning(f"⚠️ Problem with subscribed_chats.json: {e}")
            logger.info("🔧 Attempting emergency restore...")
            if ensure_subscribed_chats_file():
                try:
                    with open("subscribed_chats.json", "r") as f:
                        chats = json.load(f)
                    logger.info(f"✅ Emergency restore successful, loaded {len(chats)} chats")
                except:
                    logger.error("❌ Emergency restore failed, no reminders will be sent")
                    return
            else:
                logger.error("❌ Emergency restore failed, no reminders will be sent")
                return
        
        # 🆕 ОБРАБОТКА СЛУЧАЯ "НЕТ АКТИВНЫХ ЧАТОВ"
        if not chats or len(chats) == 0:
            moscow_time = get_moscow_time().strftime("%H:%M MSK")
            utc_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            reminder_id = reminder.get('id', 'unknown')
            
            logger.warning(f"⚠️ No active chats available for reminder #{reminder_id}")
            logger.info(f"📋 Reminder details: {reminder.get('type')} - '{reminder.get('text', '')[:50]}...'")
            
            # 📊 Логируем в Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    sheets_manager.log_send_history(
                        utc_time=utc_time,
                        moscow_time=moscow_time,
                        reminder_id=reminder_id,
                        chat_id="NO_CHATS",
                        status="NO_RECIPIENTS",
                        error="No active chats available for delivery",
                        text_preview=reminder.get('text', '')[:50] + "..." if len(reminder.get('text', '')) > 50 else reminder.get('text', '')
                    )
                    logger.info(f"📊 Logged 'no recipients' status for reminder #{reminder_id}")
                except Exception as e:
                    logger.error(f"❌ Error logging 'no recipients' to Google Sheets: {e}")
            
            # 🚮 АВТОМАТИЧЕСКОЕ УДАЛЕНИЕ РАЗОВЫХ НАПОМИНАНИЙ БЕЗ ПОЛУЧАТЕЛЕЙ
            if reminder.get("type") == "once":
                moscow_sent_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                
                # Обновляем данные напоминания перед удалением
                updated_reminder = reminder.copy()
                updated_reminder['last_sent'] = moscow_sent_time
                updated_reminder['delivery_status'] = "No recipients available - auto-deleted"
                
                # Удаляем из локального файла
                reminders = load_reminders()
                reminders = [r for r in reminders if r.get("id") != reminder.get("id")]
                save_reminders(reminders)
                logger.info(f"🗑️ One-time reminder #{reminder_id} auto-deleted: no recipients available")
                
                # 📊 СИНХРОНИЗИРУЕМ УДАЛЕНИЕ В GOOGLE SHEETS
                if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                    try:
                        # Обновляем информацию о попытке отправки
                        sheets_manager.sync_reminder(updated_reminder, "UPDATE")
                        logger.info(f"📊 Updated reminder #{reminder_id} with 'no recipients' info")
                        
                        # Затем помечаем как удаленное
                        sheets_manager.sync_reminder(updated_reminder, "DELETE")
                        logger.info(f"📊 Marked reminder #{reminder_id} as 'Deleted' (no recipients)")
                        
                        # Логируем завершение обработки
                        sheets_manager.log_reminder_action(
                            "ONCE_AUTO_DELETED", 
                            "SYSTEM", 
                            "NoRecipients", 
                            0, 
                            f"One-time reminder auto-deleted: no active chats available for delivery",
                            reminder_id
                        )
                        logger.info(f"✅ One-time reminder #{reminder_id} marked as completed: no recipients")
                        
                    except Exception as e:
                        logger.error(f"❌ Error syncing 'no recipients' deletion to Google Sheets: {e}")
                        
                else:
                    logger.warning(f"📵 Google Sheets not available - reminder #{reminder_id} removed locally only")
                
                logger.info(f"✅ One-time reminder #{reminder_id} processing completed: no recipients available")
                    
            else:
                # Для повторяющихся напоминаний просто логируем
                logger.info(f"📅 Recurring reminder #{reminder_id} ({reminder.get('type')}) - will retry on next schedule")
                
            return  # Завершаем выполнение функции
        
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        utc_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        reminder_text = f"🔔 <b>НАПОМИНАНИЕ</b> <i>({moscow_time})</i>\n\n{reminder.get('text', '')}"
        reminder_id = reminder.get('id', 'unknown')
        
        # 📊 Логируем начало отправки в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                sheets_manager.log_send_history(
                    utc_time=utc_time,
                    moscow_time=moscow_time,
                    reminder_id=reminder_id,
                    chat_id="ALL",
                    status="SENDING",
                    error="",
                    text_preview=reminder.get('text', '')[:50] + "..." if len(reminder.get('text', '')) > 50 else reminder.get('text', '')
                )
                logger.info(f"📊 Logged reminder sending start for #{reminder_id} in Google Sheets")
            except Exception as e:
                logger.error(f"❌ Error logging send start to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - reminder #{reminder_id} sending start not logged")
        
        # Отправляем каждому чату
        total_sent = 0
        total_failed = 0
        blocked_chats = []  # 🆕 Список заблокированных чатов для удаления
        
        for cid in chats[:]:  # Используем срез для безопасной итерации
            delivery_status = "SUCCESS"
            error_details = ""
            
            try:
                # 🆕 Определяем тип чата для INLINE кнопки
                try:
                    chat_info = context.bot.get_chat(cid)
                    is_private_chat = chat_info.type == 'private'
                except:
                    # Если не можем получить информацию о чате, считаем что это личка (для безопасности)
                    is_private_chat = True
                
                # 🆕 Создаем INLINE кнопку "Отписаться" только для личных чатов
                reply_markup = None
                if is_private_chat:
                    keyboard = [[InlineKeyboardButton("🚫 Отписаться от бота", callback_data="unsubscribe")]]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                
                context.bot.send_message(
                    chat_id=cid, 
                    text=reminder_text, 
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup
                )
                logger.info(f"✅ Reminder sent to chat {cid} at {moscow_time}")
                total_sent += 1
                
            except Exception as e:
                error_str = str(e)
                logger.error(f"❌ Failed to send reminder to chat {cid}: {e}")
                error_details = error_str
                delivery_status = "FAILED"
                
                # 🆕 ПРОВЕРЯЕМ НА БЛОКИРОВКУ БОТА
                if "Forbidden: bot was blocked by the user" in error_str or \
                   "Forbidden: user is deactivated" in error_str or \
                   "Forbidden: the group chat was deleted" in error_str or \
                   "Bad Request: chat not found" in error_str:
                    
                    logger.warning(f"🚫 Chat {cid} blocked bot or deleted - adding to removal list")
                    blocked_chats.append(cid)
                    delivery_status = "BLOCKED_AUTO_REMOVE"
                    error_details = f"Auto-removed due to: {error_str}"
                    total_failed += 1
                    continue  # Пропускаем fallback для заблокированных чатов
                
                # Fallback без HTML для остальных ошибок
                try:
                    clean_text = reminder_text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
                    
                    # Определяем тип чата для fallback
                    try:
                        chat_info = context.bot.get_chat(cid)
                        is_private_chat = chat_info.type == 'private'
                    except:
                        is_private_chat = True
                    
                    # Создаем INLINE кнопку для fallback
                    reply_markup = None
                    if is_private_chat:
                        keyboard = [[InlineKeyboardButton("🚫 Отписаться от бота", callback_data="unsubscribe")]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    context.bot.send_message(
                        chat_id=cid, 
                        text=clean_text,
                        reply_markup=reply_markup
                    )
                    logger.info(f"✅ Fallback reminder sent to chat {cid} at {moscow_time}")
                    delivery_status = "SUCCESS_FALLBACK"
                    error_details = f"HTML failed: {str(e)}, sent as plain text"
                    total_sent += 1
                    
                except Exception as e2:
                    error_str2 = str(e2)
                    
                    # 🆕 ПРОВЕРЯЕМ НА БЛОКИРОВКУ И В FALLBACK
                    if "Forbidden: bot was blocked by the user" in error_str2 or \
                       "Forbidden: user is deactivated" in error_str2 or \
                       "Forbidden: the group chat was deleted" in error_str2 or \
                       "Bad Request: chat not found" in error_str2:
                        
                        logger.warning(f"🚫 Chat {cid} blocked bot (fallback) - adding to removal list")
                        blocked_chats.append(cid)
                        delivery_status = "BLOCKED_AUTO_REMOVE"
                        error_details = f"Auto-removed due to: {error_str2}"
                    else:
                        logger.error(f"❌ Failed to send fallback reminder to chat {cid}: {e2}")
                        error_details = f"HTML failed: {str(e)}, Plain text failed: {str(e2)}"
                    
                    total_failed += 1
            
            # 📊 Логируем каждую отправку в Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    sheets_manager.log_send_history(
                        utc_time=utc_time,
                        moscow_time=moscow_time,
                        reminder_id=reminder_id,
                        chat_id=str(cid),
                        status=delivery_status,
                        error=error_details,
                        text_preview=reminder.get('text', '')[:50] + "..." if len(reminder.get('text', '')) > 50 else reminder.get('text', '')
                    )
                except Exception as e:
                    logger.error(f"❌ Error logging send to Google Sheets for chat {cid}: {e}")
        
        # 🆕 АВТОМАТИЧЕСКОЕ УДАЛЕНИЕ ЗАБЛОКИРОВАННЫХ ЧАТОВ
        if blocked_chats:
            logger.info(f"🚫 Processing {len(blocked_chats)} blocked chats for auto-removal")
            
            # Обновляем локальный файл
            updated_chats = [cid for cid in chats if cid not in blocked_chats]
            save_chats(updated_chats)
            
            # Обновляем Google Sheets
            for blocked_chat_id in blocked_chats:
                try:
                    success, result = unsubscribe_user(blocked_chat_id, "BlockedUser", "AUTO_BLOCKED")
                    if success:
                        logger.info(f"✅ Auto-removed blocked chat {blocked_chat_id}")
                    else:
                        logger.warning(f"⚠️ Could not auto-remove blocked chat {blocked_chat_id}: {result}")
                except Exception as e:
                    logger.error(f"❌ Error auto-removing blocked chat {blocked_chat_id}: {e}")
            
            logger.info(f"🧹 Auto-removal completed: {len(blocked_chats)} blocked chats removed")
        
        # 📊 Итоговый лог в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                final_status = "COMPLETED" if total_failed == 0 else f"PARTIAL ({total_sent}/{total_sent + total_failed})"
                if blocked_chats:
                    final_status += f", REMOVED {len(blocked_chats)} BLOCKED"
                
                sheets_manager.log_send_history(
                    utc_time=utc_time,
                    moscow_time=moscow_time,
                    reminder_id=reminder_id,
                    chat_id="SUMMARY",
                    status=final_status,
                    error=f"Sent: {total_sent}, Failed: {total_failed}, Blocked: {len(blocked_chats)}",
                    text_preview=f"Total chats: {len(chats)}"
                )
                logger.info(f"📊 Logged final summary for reminder #{reminder_id}: {total_sent} sent, {total_failed} failed, {len(blocked_chats)} auto-removed")
            except Exception as e:
                logger.error(f"❌ Error logging final summary to Google Sheets: {e}")
        elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not initialized - final summary for reminder #{reminder_id} not logged")
        
        logger.info(f"📈 Reminder #{reminder_id} delivery summary: {total_sent} sent, {total_failed} failed, {len(blocked_chats)} auto-removed")
        
        # 🆕 УЛУЧШЕННОЕ УДАЛЕНИЕ РАЗОВЫХ НАПОМИНАНИЙ ПОСЛЕ ОТПРАВКИ
        if reminder.get("type") == "once":
            moscow_sent_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
            
            # Обновляем данные напоминания перед удалением
            updated_reminder = reminder.copy()
            updated_reminder['last_sent'] = moscow_sent_time
            updated_reminder['delivery_status'] = f"Sent to {total_sent} chats, failed to {total_failed} chats, removed {len(blocked_chats)} blocked"
            
            # Удаляем из локального файла
            reminders = load_reminders()
            reminders = [r for r in reminders if r.get("id") != reminder.get("id")]
            save_reminders(reminders)
            logger.info(f"🗑️ One-time reminder #{reminder_id} removed from local storage after successful delivery")
            
            # 📊 СИНХРОНИЗИРУЕМ УДАЛЕНИЕ В GOOGLE SHEETS
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    # Сначала обновляем информацию о последней отправке
                    sheets_manager.sync_reminder(updated_reminder, "UPDATE")
                    logger.info(f"📊 Updated last_sent info for reminder #{reminder_id} in Google Sheets")
                    
                    # Затем помечаем как удаленное
                    sheets_manager.sync_reminder(updated_reminder, "DELETE")
                    logger.info(f"📊 Successfully marked reminder #{reminder_id} as 'Deleted' in Google Sheets")
                    
                    # Логируем завершение обработки разового напоминания
                    sheets_manager.log_reminder_action(
                        "ONCE_COMPLETED", 
                        "SYSTEM", 
                        "AutoDelete", 
                        0, 
                        f"One-time reminder completed and auto-deleted. Sent: {total_sent}, Failed: {total_failed}, Blocked: {len(blocked_chats)}",
                        reminder_id
                    )
                    
                except Exception as e:
                    logger.error(f"❌ Error syncing one-time reminder #{reminder_id} deletion to Google Sheets: {e}")
                    # Даже если синхронизация не удалась, локальное удаление уже выполнено
                    
            elif SHEETS_AVAILABLE and sheets_manager and not sheets_manager.is_initialized:
                logger.warning(f"📵 Google Sheets not initialized - reminder #{reminder_id} deletion not synced")
                logger.warning("   One-time reminder removed locally but Google Sheets status not updated")
            else:
                logger.warning(f"📵 Google Sheets not available - reminder #{reminder_id} removed locally only")
            
            logger.info(f"✅ One-time reminder #{reminder_id} processing completed: delivered and removed")
        
    except Exception as e:
        logger.error(f"❌ Critical error in send_reminder: {e}")
        
        # 📊 Логируем критическую ошибку в Google Sheets
        if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
            try:
                utc_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                moscow_time = get_moscow_time().strftime("%H:%M MSK")
                sheets_manager.log_send_history(
                    utc_time=utc_time,
                    moscow_time=moscow_time,
                    reminder_id=reminder.get('id', 'unknown') if 'reminder' in locals() else 'unknown',
                    chat_id="ERROR",
                    status="CRITICAL_ERROR",
                    error=str(e),
                    text_preview="Critical error in send_reminder function"
                )
            except:
                pass  # Не логируем ошибку логирования, чтобы не создать бесконечный цикл

def schedule_reminder(job_queue, reminder):
    """
    Добавляет задание в JobQueue для данного напоминания с учетом московского времени.
    """
    try:
        # Сначала удаляем существующее задание с таким же ID, если есть
        current_jobs = job_queue.jobs()
        for job in current_jobs:
            if hasattr(job, 'name') and job.name == f"reminder_{reminder.get('id')}":
                job.schedule_removal()
        
        if reminder["type"] == "once":
            # Парсим как московское время и конвертируем в UTC для планировщика
            moscow_dt = datetime.strptime(reminder["datetime"], "%Y-%m-%d %H:%M")
            moscow_dt = MOSCOW_TZ.localize(moscow_dt)
            utc_dt = moscow_dt.astimezone(pytz.UTC).replace(tzinfo=None)
            
            if moscow_dt > get_moscow_time():  # Планируем только будущие напоминания
                job_queue.run_once(send_reminder, utc_dt, context=reminder, name=f"reminder_{reminder.get('id')}")
                logger.info(f"Scheduled one-time reminder {reminder.get('id')} for {moscow_dt.strftime('%Y-%m-%d %H:%M MSK')}")
                
        elif reminder["type"] == "daily":
            h, m = map(int, reminder["time"].split(":"))
            # Создаем время в московском часовом поясе, затем конвертируем в UTC
            moscow_time = dt_time(hour=h, minute=m)
            # Для ежедневных напоминаний нужно учесть смещение UTC
            utc_hour = (h - 3) % 24  # MSK = UTC+3
            utc_time = dt_time(hour=utc_hour, minute=m)
            
            job_queue.run_daily(send_reminder, utc_time, context=reminder, name=f"reminder_{reminder.get('id')}")
            logger.info(f"Scheduled daily reminder {reminder.get('id')} for {h:02d}:{m:02d} MSK (UTC: {utc_hour:02d}:{m:02d})")
            
        elif reminder["type"] == "weekly":
            days_map = {
                "понедельник": 0, "вторник": 1, "среда": 2,
                "четверг": 3, "пятница": 4, "суббота": 5, "воскресенье": 6
            }
            weekday = days_map[reminder["day"].lower()]
            h, m = map(int, reminder["time"].split(":"))
            
            # Конвертируем московское время в UTC
            utc_hour = (h - 3) % 24  # MSK = UTC+3
            utc_time = dt_time(hour=utc_hour, minute=m)
            
            job_queue.run_daily(
                send_reminder,
                utc_time,
                context=reminder,
                days=(weekday,),
                name=f"reminder_{reminder.get('id')}"
            )
            logger.info(f"Scheduled weekly reminder {reminder.get('id')} for {reminder['day']} {h:02d}:{m:02d} MSK")
            
    except Exception as e:
        logger.error(f"Error scheduling reminder {reminder.get('id', 'unknown')}: {e}")

def schedule_poll(job_queue, poll):
    """
    Добавляет задание в JobQueue для данного голосования с учетом московского времени.
    """
    try:
        poll_id = poll.get('id', 'unknown')
        poll_type = poll.get('type', 'unknown')
        logger.info(f"🔄 Attempting to schedule poll #{poll_id} (type: {poll_type})")
        
        # Сначала удаляем существующее задание с таким же ID, если есть
        current_jobs = job_queue.jobs()
        removed_jobs = 0
        for job in current_jobs:
            if hasattr(job, 'name') and job.name == f"poll_{poll_id}":
                job.schedule_removal()
                removed_jobs += 1
        
        if removed_jobs > 0:
            logger.info(f"🗑️ Removed {removed_jobs} existing job(s) for poll #{poll_id}")
        
        if poll["type"] == "once" or poll["type"] == "one_time":
            # Парсим как московское время и конвертируем в UTC для планировщика
            datetime_str = poll.get("datetime", "")
            logger.info(f"📅 Processing one-time poll #{poll_id} with datetime: {datetime_str}")
            
            moscow_dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
            moscow_dt = MOSCOW_TZ.localize(moscow_dt)
            utc_dt = moscow_dt.astimezone(pytz.UTC).replace(tzinfo=None)
            
            current_moscow_time = get_moscow_time()
            time_diff = (moscow_dt - current_moscow_time).total_seconds()
            
            logger.info(f"⏰ Poll #{poll_id} scheduled for: {moscow_dt.strftime('%Y-%m-%d %H:%M MSK')}")
            logger.info(f"⏰ Current Moscow time: {current_moscow_time.strftime('%Y-%m-%d %H:%M MSK')}")
            logger.info(f"⏰ Time difference: {time_diff:.0f} seconds ({time_diff/60:.1f} minutes)")
            
            if moscow_dt > current_moscow_time:  # Планируем будущие голосования
                job_queue.run_once(send_poll, utc_dt, context=poll, name=f"poll_{poll_id}")
                logger.info(f"✅ Scheduled one-time poll #{poll_id} for {moscow_dt.strftime('%Y-%m-%d %H:%M MSK')}")
            elif moscow_dt <= current_moscow_time and time_diff >= -3600:  # Пропущенные голосования (в течение часа)
                # Отправляем пропущенное голосование немедленно
                logger.warning(f"⚠️ Missed poll #{poll_id} scheduled for {moscow_dt.strftime('%Y-%m-%d %H:%M MSK')}, sending immediately")
                job_queue.run_once(send_poll, datetime.utcnow() + timedelta(seconds=5), context=poll, name=f"poll_{poll_id}_missed")
            else:
                logger.warning(f"❌ Skipping old poll #{poll_id} scheduled for {moscow_dt.strftime('%Y-%m-%d %H:%M MSK')} (too old: {-time_diff/3600:.1f} hours ago)")
                
        elif poll["type"] == "daily" or poll["type"] == "daily_poll":
            time_str = poll.get("time", "")
            logger.info(f"📅 Processing daily poll #{poll_id} with time: {time_str}")
            
            h, m = map(int, time_str.split(":"))
            # Создаем время в московском часовом поясе, затем конвертируем в UTC
            moscow_time = dt_time(hour=h, minute=m)
            # Для ежедневных голосований нужно учесть смещение UTC
            utc_hour = (h - 3) % 24  # MSK = UTC+3
            utc_time = dt_time(hour=utc_hour, minute=m)
            
            job_queue.run_daily(send_poll, utc_time, context=poll, name=f"poll_{poll_id}")
            logger.info(f"✅ Scheduled daily poll #{poll_id} for {h:02d}:{m:02d} MSK (UTC: {utc_hour:02d}:{m:02d})")
            
        elif poll["type"] == "weekly" or poll["type"] == "weekly_poll":
            days_map = {
                "понедельник": 0, "вторник": 1, "среда": 2,
                "четверг": 3, "пятница": 4, "суббота": 5, "воскресенье": 6
            }
            day_str = poll.get("day", "")
            time_str = poll.get("time", "")
            logger.info(f"📅 Processing weekly poll #{poll_id} for {day_str} at {time_str}")
            
            weekday = days_map[day_str.lower()]
            h, m = map(int, time_str.split(":"))
            
            # Конвертируем московское время в UTC
            utc_hour = (h - 3) % 24  # MSK = UTC+3
            utc_time = dt_time(hour=utc_hour, minute=m)
            
            job_queue.run_daily(
                send_poll,
                utc_time,
                context=poll,
                days=(weekday,),
                name=f"poll_{poll_id}"
            )
            logger.info(f"✅ Scheduled weekly poll #{poll_id} for {day_str} {h:02d}:{m:02d} MSK")
        else:
            logger.error(f"❌ Unknown poll type '{poll_type}' for poll #{poll_id}")
            
    except Exception as e:
        logger.error(f"Error scheduling poll {poll.get('id', 'unknown')}: {e}")

def schedule_all_polls(job_queue):
    """
    Планирует все активные голосования из файла.
    """
    polls = load_polls()
    logger.info(f"🔍 schedule_all_polls: Loaded {len(polls)} polls from file")
    
    active_polls = []
    for poll in polls:
        poll_id = poll.get('id', 'unknown')
        poll_status = poll.get('status', 'unknown')
        logger.info(f"🔍 Poll #{poll_id}: status='{poll_status}'")
        
        # Строгая проверка статуса - принимаем только 'Active'
        if poll.get("status") == "Active":
            logger.info(f"✅ Poll #{poll_id} is active (status: '{poll_status}'), calling schedule_poll")
            active_polls.append(poll)
            schedule_poll(job_queue, poll)
        else:
            logger.info(f"❌ Poll #{poll_id} is not active (status: '{poll_status}') - only 'Active' polls are scheduled")
    
    logger.info(f"📊 schedule_all_polls: Processed {len(active_polls)} active polls out of {len(polls)} total")

def reschedule_all_polls(job_queue):
    """
    Перепланирует все голосования (удаляет старые задания и создает новые).
    """
    # Удаляем все существующие задания голосований
    current_jobs = job_queue.jobs()
    for job in current_jobs:
        if hasattr(job, 'name') and job.name and job.name.startswith('poll_'):
            job.schedule_removal()
    
    # Планируем заново
    schedule_all_polls(job_queue)
    logger.info("All polls rescheduled")

def schedule_all_reminders(job_queue):
    """
    Загружает все напоминания и запланировывает их.
    """
    try:
        reminders = load_reminders()
        for reminder in reminders:
            schedule_reminder(job_queue, reminder)
    except Exception as e:
        logger.error(f"Error scheduling all reminders: {e}")

def reschedule_all_reminders(job_queue):
    """
    Перепланирует все напоминания (используется после удаления)
    """
    try:
        # Останавливаем все текущие задания
        current_jobs = job_queue.jobs()
        for job in current_jobs:
            if hasattr(job, 'name') and job.name and job.name.startswith('reminder_'):
                job.schedule_removal()
        
        # Планируем заново
        schedule_all_reminders(job_queue)
    except Exception as e:
        logger.error(f"Error rescheduling reminders: {e}")

# --- Функции автовосстановления подписок ---

def ensure_subscribed_chats_file():
    """Проверяет и восстанавливает subscribed_chats.json при необходимости"""
    try:
        # Проверяем существует ли файл и не пустой ли он
        with open("subscribed_chats.json", "r") as f:
            chats = json.load(f)
            if chats and len(chats) > 0:
                logger.info(f"✅ Found {len(chats)} existing subscribed chats")
                return True  # Файл в порядке
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        pass  # Файл отсутствует или поврежден
    
    # Детальная диагностика доступности Google Sheets
    logger.warning("⚠️ subscribed_chats.json is missing or empty. Attempting restore from Google Sheets...")
    logger.info(f"🔍 Google Sheets availability check:")
    logger.info(f"   SHEETS_AVAILABLE: {SHEETS_AVAILABLE}")
    logger.info(f"   sheets_manager exists: {sheets_manager is not None}")
    
    if sheets_manager:
        logger.info(f"   sheets_manager.is_initialized: {sheets_manager.is_initialized}")
        
        # Проверяем переменные окружения
        sheets_id = os.environ.get('GOOGLE_SHEETS_ID')
        sheets_creds = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
        logger.info(f"   GOOGLE_SHEETS_ID present: {bool(sheets_id)}")
        logger.info(f"   GOOGLE_SHEETS_CREDENTIALS present: {bool(sheets_creds)}")
        
        if sheets_id:
            logger.info(f"   Using Sheet ID: {sheets_id[:20]}...{sheets_id[-10:] if len(sheets_id) > 30 else sheets_id}")
    
    if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
        if sheets_manager.restore_subscribed_chats_file():
            logger.info("✅ Successfully restored subscribed chats from Google Sheets")
            return True
        else:
            logger.error("❌ Failed to restore from Google Sheets")
    else:
        logger.warning("📵 Google Sheets not available for restoration")
        logger.warning("   This means:")
        logger.warning("   1. Check GOOGLE_SHEETS_ID environment variable")
        logger.warning("   2. Check GOOGLE_SHEETS_CREDENTIALS environment variable") 
        logger.warning("   3. Verify Google Sheets API access")
    
    # Создаем пустой файл как fallback с подробным объяснением
    logger.warning("📝 Creating empty subscribed_chats.json as fallback")
    logger.warning("⚠️  ВНИМАНИЕ: Бот не сможет отправлять напоминания без подписанных чатов!")
    logger.warning("   Для работы бота нужно:")
    logger.warning("   1. Запустить команду /start в Telegram чатах")
    logger.warning("   2. Настроить Google Sheets интеграцию")
    
    with open("subscribed_chats.json", "w") as f:
        json.dump([], f)
    
    return False

def ensure_reminders_file():
    """🆕 Проверяет и восстанавливает reminders.json при необходимости"""
    try:
        # Проверяем существует ли файл и не пустой ли он
        existing_reminders = load_reminders()
        if existing_reminders and len(existing_reminders) > 0:
            logger.info(f"✅ Found {len(existing_reminders)} existing reminders")
            return True, len(existing_reminders)  # Файл в порядке
    except Exception:
        pass  # Файл отсутствует или поврежден
    
    # Детальная диагностика доступности Google Sheets для напоминаний
    logger.warning("⚠️ reminders.json is missing, empty or corrupted. Attempting restore from Google Sheets...")
    logger.info(f"🔍 Google Sheets reminders restore check:")
    
    if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
        logger.info("   ✅ Google Sheets available for reminders restore")
        try:
            success, message = sheets_manager.restore_reminders_from_sheets()
            if success:
                restored_reminders = load_reminders()
                restored_count = len(restored_reminders)
                logger.info(f"✅ Successfully restored {restored_count} reminders from Google Sheets")
                return True, restored_count
            else:
                logger.error(f"❌ Failed to restore reminders from Google Sheets: {message}")
        except Exception as e:
            logger.error(f"❌ Exception during reminders restore: {e}")
    else:
        logger.warning("📵 Google Sheets not available for reminders restoration")
        logger.warning("   This means:")
        logger.warning("   1. Check GOOGLE_SHEETS_ID environment variable")
        logger.warning("   2. Check GOOGLE_SHEETS_CREDENTIALS environment variable") 
        logger.warning("   3. Verify Google Sheets API access")
        logger.warning("   4. Ensure reminders exist in Google Sheets with 'Active' status")
    
    # Создаем пустой файл как fallback
    logger.warning("📝 Creating empty reminders.json as fallback")
    logger.warning("⚠️ ВНИМАНИЕ: Бот не сможет отправлять напоминания без активных заданий!")
    logger.warning("   Для работы бота нужно:")
    logger.warning("   1. Создать напоминания командами /remind, /remind_daily, /remind_weekly")
    logger.warning("   2. Или восстановить из Google Sheets командой /restore_reminders")
    
    save_reminders([])
    return False, 0

def ensure_polls_file():
    """🆕 Проверяет и восстанавливает polls.json, всегда синхронизируется с Google Sheets"""
    try:
        # Проверяем существует ли локальный файл
        existing_polls = load_polls()
        local_count = len(existing_polls) if existing_polls else 0
        logger.info(f"📋 Found {local_count} existing local polls")
    except Exception:
        existing_polls = []
        local_count = 0
        logger.warning("⚠️ polls.json is missing or corrupted")
    
    # ВСЕГДА пытаемся синхронизироваться с Google Sheets при запуске
    logger.info(f"🔄 Attempting to sync polls from Google Sheets on startup...")
    
    if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
        logger.info("   ✅ Google Sheets available for polls sync")
        try:
            success, message = sheets_manager.restore_polls_from_sheets()
            if success:
                synced_polls = load_polls()
                synced_count = len(synced_polls)
                
                if synced_count != local_count:
                    logger.info(f"🔄 Startup sync: Updated polls {local_count} → {synced_count}")
                    logger.info(f"✅ Successfully synced {synced_count} polls from Google Sheets")
                else:
                    logger.info(f"✅ Polls already in sync ({synced_count} items)")
                
                return True, synced_count
            else:
                logger.error(f"❌ Failed to sync polls from Google Sheets: {message}")
                # Используем локальные данные если синхронизация не удалась
                if local_count > 0:
                    logger.info(f"📋 Using {local_count} local polls as fallback")
                    return True, local_count
        except Exception as e:
            logger.error(f"❌ Exception during polls sync: {e}")
            # Используем локальные данные при ошибке
            if local_count > 0:
                logger.info(f"📋 Using {local_count} local polls as fallback")
                return True, local_count
    else:
        logger.warning("📵 Google Sheets not available for polls sync")
        logger.warning("   This means:")
        logger.warning("   1. Check GOOGLE_SHEETS_ID environment variable")
        logger.warning("   2. Check GOOGLE_SHEETS_CREDENTIALS environment variable") 
        logger.warning("   3. Verify Google Sheets API access")
        logger.warning("   4. Ensure polls exist in Google Sheets with 'Active' status")
        
        # Используем локальные данные если Google Sheets недоступен
        if local_count > 0:
            logger.info(f"📋 Using {local_count} local polls (Google Sheets unavailable)")
            return True, local_count
    
    # Создаем пустой файл только если нет ни локальных данных, ни Google Sheets
    logger.warning("📝 Creating empty polls.json as fallback")
    logger.warning("⚠️ ВНИМАНИЕ: Бот не сможет отправлять голосования без активных заданий!")
    logger.warning("   Для работы бота нужно:")
    logger.warning("   1. Создать голосования командами /poll, /poll_daily, /poll_weekly")
    logger.warning("   2. Или восстановить из Google Sheets командой /restore_polls")
    
    save_polls([])
    return False, 0

def auto_sync_subscribed_chats(context: CallbackContext):
    """Автоматическая синхронизация subscribed_chats.json с Google Sheets каждый час"""
    try:
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        logger.info(f"🔄 Starting hourly sync at {moscow_time}")
        
        if SHEETS_AVAILABLE and sheets_manager:
            success = sheets_manager.sync_subscribed_chats_from_sheets()
            if success:
                logger.info(f"✅ Hourly sync completed successfully at {moscow_time}")
            else:
                logger.warning(f"⚠️ Hourly sync had issues at {moscow_time}")
        else:
            logger.warning(f"📵 Google Sheets not available for sync at {moscow_time}")
            
    except Exception as e:
        logger.error(f"❌ Error in hourly sync: {e}")

def auto_sync_reminders(context: CallbackContext):
    """🆕 Автоматическая синхронизация напоминаний с Google Sheets каждые 2 часа"""
    try:
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        logger.info(f"🔄 Starting reminders auto-sync at {moscow_time}")
        
        if not SHEETS_AVAILABLE or not sheets_manager or not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not available for reminders sync at {moscow_time}")
            return
        
        try:
            # Проверяем есть ли локальные напоминания
            current_reminders = load_reminders()
            current_count = len(current_reminders)
            
            logger.info(f"📋 Current local reminders: {current_count}")
            
            # Пытаемся получить напоминания из Google Sheets
            success, message = sheets_manager.restore_reminders_from_sheets()
            
            if success:
                synced_reminders = load_reminders()
                synced_count = len(synced_reminders)
                
                if synced_count != current_count:
                    logger.info(f"🔄 Auto-sync: Updated reminders {current_count} → {synced_count}")
                    logger.info(f"🛡️ File completely overwritten - no duplicates possible")
                    
                    # Перепланируем все напоминания
                    reschedule_all_reminders(context.dispatcher.job_queue)
                    logger.info(f"✅ Reminders rescheduled after auto-sync at {moscow_time}")
                    
                    # Проверяем активные задания после перепланирования
                    active_jobs_after = check_active_jobs(context.dispatcher.job_queue)
                    logger.info(f"📊 Active jobs after auto-sync: {active_jobs_after}")
                    
                    # Логируем в Google Sheets
                    if sheets_manager.is_initialized:
                        try:
                            sheets_manager.log_operation(
                                timestamp=moscow_time,
                                action="AUTO_SYNC_REMINDERS",
                                user_id="SYSTEM",
                                username="AutoSync",
                                chat_id=0,
                                details=f"Auto-sync updated reminders: {current_count} → {synced_count}, active jobs: {active_jobs_after}, no duplicates",
                                reminder_id=""
                            )
                        except:
                            pass
                else:
                    logger.info(f"✅ Auto-sync: Reminders already in sync ({current_count} items) at {moscow_time}")
                    logger.info(f"🛡️ No changes needed - all reminders unique")
            else:
                logger.warning(f"⚠️ Auto-sync reminders failed at {moscow_time}: {message}")
                
                # При неудаче автосинхронизации проверяем есть ли хотя бы локальные напоминания
                if current_count == 0:
                    logger.warning("🚨 CRITICAL: No local reminders AND auto-sync failed!")
                    logger.warning("   This means NO reminders will be sent until manual intervention")
                    logger.warning("   Recommended action: use /restore_reminders command")
                
        except Exception as e:
            logger.error(f"❌ Error during reminders auto-sync: {e}")
            
            # Проверяем критическое состояние
            try:
                current_reminders = load_reminders()
                if len(current_reminders) == 0:
                    logger.error("🚨 CRITICAL ERROR: No reminders available after auto-sync failure!")
            except:
                logger.error("🚨 CRITICAL ERROR: Cannot access reminders file!")
            
    except Exception as e:
        logger.error(f"❌ Critical error in auto_sync_reminders: {e}")

def auto_sync_polls(context: CallbackContext):
    """🆕 Автоматическая синхронизация голосований с Google Sheets каждые 5 минут"""
    try:
        moscow_time = get_moscow_time().strftime("%H:%M MSK")
        logger.info(f"🔄 Starting polls auto-sync at {moscow_time}")
        
        if not SHEETS_AVAILABLE or not sheets_manager or not sheets_manager.is_initialized:
            logger.warning(f"📵 Google Sheets not available for polls sync at {moscow_time}")
            return
        
        try:
            # Проверяем есть ли локальные голосования
            current_polls = load_polls()
            current_count = len(current_polls)
            
            logger.info(f"📋 Current local polls: {current_count}")
            
            # Пытаемся получить голосования из Google Sheets
            success, message = sheets_manager.restore_polls_from_sheets()
            
            if success:
                synced_polls = load_polls()
                synced_count = len(synced_polls)
                
                if synced_count != current_count:
                    logger.info(f"🔄 Auto-sync: Updated polls {current_count} → {synced_count}")
                    logger.info(f"🛡️ File completely overwritten - no duplicates possible")
                    
                    # Перепланируем все голосования
                    reschedule_all_polls(context.dispatcher.job_queue)
                    logger.info(f"✅ Polls rescheduled after auto-sync at {moscow_time}")
                    
                    # Проверяем активные задания после перепланирования
                    active_jobs_after = check_active_jobs(context.dispatcher.job_queue)
                    logger.info(f"📊 Active jobs after polls auto-sync: {active_jobs_after}")
                    
                    # Логируем в Google Sheets
                    if sheets_manager.is_initialized:
                        try:
                            sheets_manager.log_operation(
                                timestamp=moscow_time,
                                action="AUTO_SYNC_POLLS",
                                user_id="SYSTEM",
                                username="AutoSync",
                                chat_id=0,
                                details=f"Auto-sync updated polls: {current_count} → {synced_count}, active jobs: {active_jobs_after}, no duplicates",
                                reminder_id=""
                            )
                        except:
                            pass
                else:
                    logger.info(f"✅ Auto-sync: Polls already in sync ({current_count} items) at {moscow_time}")
                    logger.info(f"🛡️ No changes needed - all polls unique")
            else:
                logger.warning(f"⚠️ Auto-sync polls failed at {moscow_time}: {message}")
                
                # При неудаче автосинхронизации проверяем есть ли хотя бы локальные голосования
                if current_count == 0:
                    logger.warning("🚨 CRITICAL: No local polls AND auto-sync failed!")
                    logger.warning("   This means NO polls will be sent until manual intervention")
                    logger.warning("   Recommended action: use /restore_polls command")
                
        except Exception as e:
            logger.error(f"❌ Error during polls auto-sync: {e}")
            
            # Проверяем критическое состояние
            try:
                current_polls = load_polls()
                if len(current_polls) == 0:
                    logger.error("🚨 CRITICAL ERROR: No polls available after auto-sync failure!")
            except:
                logger.error("🚨 CRITICAL ERROR: Cannot access polls file!")
            
    except Exception as e:
        logger.error(f"❌ Critical error in auto_sync_polls: {e}")


def check_active_jobs(job_queue):
    """🆕 Проверяет активные задания напоминаний и голосований и выводит статистику"""
    try:
        current_jobs = job_queue.jobs()
        reminder_jobs = [job for job in current_jobs if hasattr(job, 'name') and job.name and job.name.startswith('reminder_')]
        poll_jobs = [job for job in current_jobs if hasattr(job, 'name') and job.name and job.name.startswith('poll_')]
        
        logger.info(f"📊 Active reminder jobs: {len(reminder_jobs)}")
        logger.info(f"📊 Active poll jobs: {len(poll_jobs)}")
        
        if len(reminder_jobs) > 0:
            logger.info("📋 Active reminder jobs list:")
            for job in reminder_jobs:
                # 🔧 ИСПРАВЛЕНО: безопасная проверка атрибута next_run
                try:
                        # Универсальная проверка времени следующего выполнения
                        next_run = None
                        if hasattr(job, 'next_run_time') and job.next_run_time:
                            next_run = job.next_run_time
                        elif hasattr(job, 'next_run') and job.next_run:
                            next_run = job.next_run
                        elif hasattr(job, 'trigger'):
                            try:
                                from datetime import datetime
                                import pytz
                                utc_now = datetime.now(pytz.UTC)
                                next_run = job.trigger.get_next_fire_time(None, utc_now)
                            except: pass
                        
                        if next_run:
                            next_run_moscow = utc_to_moscow_time(next_run)
                            logger.info(f"   • {job.name}: next run at {next_run_moscow}")
                        else:
                            logger.info(f"   • {job.name}: scheduled (time info unavailable)")
                except Exception as attr_error:
                    logger.info(f"   • {job.name}: scheduled (next_run attribute error)")
        else:
            logger.warning("⚠️ NO ACTIVE REMINDER JOBS FOUND!")
            logger.warning("   This means reminders will not be sent!")
            logger.warning("   Possible reasons:")
            logger.warning("   1. reminders.json is empty")
            logger.warning("   2. All reminders are in the past")
            logger.warning("   3. Scheduling failed")
            
        if len(poll_jobs) > 0:
            logger.info("📋 Active poll jobs list:")
            for job in poll_jobs:
                try:
                        # Универсальная проверка времени следующего выполнения
                        next_run = None
                        if hasattr(job, 'next_run_time') and job.next_run_time:
                            next_run = job.next_run_time
                        elif hasattr(job, 'next_run') and job.next_run:
                            next_run = job.next_run
                        elif hasattr(job, 'trigger'):
                            try:
                                from datetime import datetime
                                import pytz
                                utc_now = datetime.now(pytz.UTC)
                                next_run = job.trigger.get_next_fire_time(None, utc_now)
                            except: pass
                        
                        if next_run:
                            next_run_moscow = utc_to_moscow_time(next_run)
                            logger.info(f"   • {job.name}: next run at {next_run_moscow}")
                        else:
                            logger.info(f"   • {job.name}: scheduled (time info unavailable)")
                except Exception as attr_error:
                    logger.info(f"   • {job.name}: scheduled (next_run attribute error)")
        else:
            logger.warning("⚠️ NO ACTIVE POLL JOBS FOUND!")
            logger.warning("   This means polls will not be sent!")
            logger.warning("   Possible reasons:")
            logger.warning("   1. polls.json is empty")
            logger.warning("   2. All polls are in the past")
            logger.warning("   3. Scheduling failed")
            
        return len(reminder_jobs), len(poll_jobs)
        
    except Exception as e:
        logger.error(f"❌ Error checking active jobs: {e}")
        return 0, 0

def emergency_restore_subscribed_chats(context: CallbackContext):
    """Экстренное восстановление при критической ошибке отправки"""
    try:
        logger.warning("🚨 Emergency restore triggered - checking subscribed_chats.json")
        
        # Проверяем текущий файл
        try:
            with open("subscribed_chats.json", "r") as f:
                chats = json.load(f)
                if chats and len(chats) > 0:
                    logger.info(f"📋 Current file contains {len(chats)} chats - no restore needed")
                    return
        except:
            pass
        
        # Файл поврежден или пуст - восстанавливаем
        logger.warning("🔧 Attempting emergency restore from Google Sheets")
        ensure_subscribed_chats_file()
        
    except Exception as e:
        logger.error(f"❌ Error in emergency restore: {e}")

def bot_status(update: Update, context: CallbackContext):
    """🆕 Диагностика состояния бота"""
    try:
        current_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S MSK")
        
        # Рассчитываем время работы бота
        uptime_info = ""
        if BOT_START_TIME:
            uptime_delta = get_moscow_time() - BOT_START_TIME
            hours = uptime_delta.seconds // 3600
            minutes = (uptime_delta.seconds % 3600) // 60
            if uptime_delta.days > 0:
                uptime_info = f"⏱️ <i>Работает: {uptime_delta.days}д {hours}ч {minutes}м</i>\n"
            else:
                uptime_info = f"⏱️ <i>Работает: {hours}ч {minutes}м</i>\n"
        
        # Проверяем локальные файлы
        try:
            reminders = load_reminders()
            reminders_count = len(reminders)
        except:
            reminders_count = 0
            
        try:
            with open("subscribed_chats.json", "r") as f:
                chats = json.load(f)
                chats_count = len(chats)
        except:
            chats_count = 0
        
        # Проверяем активные задания
        current_jobs = context.dispatcher.job_queue.jobs()
        reminder_jobs = [job for job in current_jobs if hasattr(job, 'name') and job.name and job.name.startswith('reminder_')]
        active_jobs_count = len(reminder_jobs)
        
        # Проверяем Google Sheets
        sheets_status = "❌ Недоступен"
        sheets_details = "Не инициализирован"
        
        if SHEETS_AVAILABLE and sheets_manager:
            if sheets_manager.is_initialized:
                sheets_status = "✅ Подключен"
                sheets_details = "Готов к работе"
            else:
                sheets_status = "⚠️ Не инициализирован"
                sheets_details = "Проверьте переменные окружения"
        
        # Подсчитываем типы напоминаний
        once_count = sum(1 for r in reminders if r.get('type') == 'once')
        daily_count = sum(1 for r in reminders if r.get('type') == 'daily')
        weekly_count = sum(1 for r in reminders if r.get('type') == 'weekly')
        
        # Формируем сообщение
        status_msg = (
            f"🤖 <b>Статус бота</b>\n"
            f"⏰ <i>{current_time}</i>\n"
            f"{uptime_info}\n"
            
            f"📋 <b>Локальные данные:</b>\n"
            f"• Напоминания: {reminders_count}\n"
            f"  📅 Разовых: {once_count}\n"
            f"  🔄 Ежедневных: {daily_count}\n"
            f"  📆 Еженедельных: {weekly_count}\n"
            f"• Подписанные чаты: {chats_count}\n\n"
            
            f"⚙️ <b>Планировщик заданий:</b>\n"
            f"• Активные задания: {active_jobs_count}\n"
            f"• Состояние: {'✅ Работает' if active_jobs_count > 0 else '❌ Нет заданий!'}\n\n"
            
            f"📊 <b>Google Sheets:</b>\n"
            f"• Статус: {sheets_status}\n"
            f"• Детали: {sheets_details}\n\n"
            
            f"🔧 <b>Диагностика:</b>\n"
        )
        
        # 🆕 Информация об автосинхронизации
        sync_info = ""
        try:
            now_moscow = get_moscow_time()
            sync_jobs = []
            
            for job in current_jobs:
                if hasattr(job, 'callback') and job.callback:
                    if job.callback.__name__ == 'auto_sync_subscribed_chats':
                        sync_jobs.append(('chats', job, '🔄 Чаты', 'каждые 5 мин'))
                    elif job.callback.__name__ == 'auto_sync_reminders':
                        sync_jobs.append(('reminders', job, '📋 Напоминания', 'каждые 5 мин'))
                    elif job.callback.__name__ == 'ping_self':
                        sync_jobs.append(('ping', job, '🏓 Ping', 'каждые 5 мин'))
            
            if sync_jobs:
                sync_info += f"🔄 <b>Автосинхронизация:</b>\n"
                
                for sync_type, job, name, period in sync_jobs:
                    try:
                        # Улучшенная проверка времени следующего выполнения
                        next_run = None
                        
                        # Пробуем разные способы получить время следующего выполнения
                        if hasattr(job, 'next_run_time') and job.next_run_time:
                            next_run = job.next_run_time
                        elif hasattr(job, 'next_run') and job.next_run:
                            next_run = job.next_run
                        elif hasattr(job, 'trigger') and hasattr(job.trigger, 'get_next_fire_time'):
                            try:
                                from datetime import datetime
                                import pytz
                                utc_now = datetime.now(pytz.UTC)
                                next_run = job.trigger.get_next_fire_time(None, utc_now)
                            except: pass
                        
                        if next_run:
                            next_run_moscow = utc_to_moscow_time(next_run)
                            time_diff = next_run_moscow - now_moscow
                            
                            # Форматируем время до следующей синхронизации
                            if time_diff.total_seconds() < 0:
                                time_str = "сейчас"
                            elif time_diff.total_seconds() < 60:
                                seconds = int(time_diff.total_seconds())
                                time_str = f"через {seconds}с"
                            elif time_diff.total_seconds() < 3600:
                                minutes = int(time_diff.total_seconds() // 60)
                                seconds = int(time_diff.total_seconds() % 60)
                                time_str = f"через {minutes}м {seconds}с"
                            else:
                                hours = int(time_diff.total_seconds() // 3600)
                                minutes = int((time_diff.total_seconds() % 3600) // 60)
                                time_str = f"через {hours}ч {minutes}м"
                            
                            sync_info += f"• {name}: {time_str} ({period})\n"
                        else:
                            sync_info += f"• {name}: запланировано ({period})\n"
                    except Exception:
                        sync_info += f"• {name}: активно ({period})\n"
                
                sync_info += "\n"
        except Exception as e:
            logger.error(f"Error getting sync info: {e}")
            sync_info = "🔄 <b>Автосинхронизация:</b> ошибка получения данных\n\n"
        
        # Добавляем рекомендации
        if reminders_count == 0:
            status_msg += "⚠️ Нет напоминаний - создайте их или используйте /restore_reminders\n"
        
        if active_jobs_count == 0:
            status_msg += "🚨 КРИТИЧНО: Нет активных заданий! Напоминания не будут отправляться!\n"
            status_msg += "💡 Решение: /restore_reminders для восстановления\n"
        
        if chats_count == 0:
            status_msg += "📭 Нет подписанных чатов - напоминания некому отправлять\n"
            
        if not SHEETS_AVAILABLE or not sheets_manager or not sheets_manager.is_initialized:
            status_msg += "📵 Google Sheets недоступен - автовосстановление отключено\n"
        
        # Добавляем информацию о синхронизации ПЕРЕД ближайшими заданиями
        status_msg += f"\n{sync_info}"
        
        # Информация о ближайших заданиях
        if active_jobs_count > 0:
            status_msg += f"\n📅 <b>Ближайшие задания:</b>\n"
            jobs_info = []
            for job in reminder_jobs[:3]:  # Показываем только 3 ближайших
                # 🔧 ИСПРАВЛЕНО: безопасная проверка атрибута next_run
                try:
                    if hasattr(job, 'next_run') and job.next_run:
                        next_run_moscow = utc_to_moscow_time(next_run)
                        job_name = job.name.replace('reminder_', '#')
                        jobs_info.append(f"• {job_name}: {next_run_moscow.strftime('%d.%m %H:%M')}")
                    else:
                        job_name = job.name.replace('reminder_', '#')
                        jobs_info.append(f"• {job_name}: запланировано")
                except Exception:
                    job_name = job.name.replace('reminder_', '#')
                    jobs_info.append(f"• {job_name}: запланировано")
            
            if jobs_info:
                status_msg += "\n".join(jobs_info)
                if active_jobs_count > 3:
                    status_msg += f"\n• ... и ещё {active_jobs_count - 3} заданий"
        
        try:
            update.message.reply_text(status_msg, parse_mode=ParseMode.HTML)
        except:
            # Fallback без HTML
            clean_msg = status_msg.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            update.message.reply_text(clean_msg)
            
    except Exception as e:
        logger.error(f"Error in bot_status: {e}")
        try:
            update.message.reply_text("❌ <b>Ошибка получения статуса бота</b>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Ошибка получения статуса бота")

def about_bot(update: Update, context: CallbackContext):
    """Информация о боте"""
    try:
        about_text = (
            "🤖 <b>Telegram Бот-Напоминалка 2.0</b> 📅\n"
            "Улучшенная версия прошлого бота для управления напоминаниями с интеграцией Google Sheets для резервного копирования напоминаний в случае рестарта сервера или других проблем.\n\n"
            
            "🔧 <b>Основные возможности:</b>\n"
            "✅ Разовые напоминания — точная дата и время\n"
            "🔄 Ежедневные напоминания — каждый день в указанное время\n"
            "📆 Еженедельные напоминания — конкретный день недели\n"
            "🌍 Московское время (MSK) — автоматическая синхронизация\n"
            "📱 Работа в группах и личных чатах\n"
            "🔗 Поддержка HTML-разметки и ссылок\n\n"
            
            "⁉️ <b>Что изменилось?</b>\n"
            "— Добавлена интеграция с сервисом Google для хранения данных (у бота нет доступа к сообщениям в группах и т. п.), а также автовосстановление данных при внештатных ситуациях.\n"
            "— Улучшена отказоустойчивость бота: все напоминания теперь дублируются ещё и в Google-таблице, и в случае перезапуска хостинга (такое бывает, так как он бесплатный), бот автоматически восстановит все напоминания и все подписки, и продолжит работу в штатном режиме.\n\n"
        )
        
        commands_text = (
            "📋 <b>Команды бота:</b>\n\n"
            
            "📝 <b>Создание напоминаний:</b>\n"
            "/remind — разовое напоминание\n"
            "/remind_daily — ежедневное напоминание\n"
            "/remind_weekly — еженедельное напоминание\n\n"
            
            "📊 <b>Управление:</b>\n"
            "/list_reminders — просмотр всех напоминаний\n"
            "/next — ближайшее напоминание\n"
            "/del_reminder — удалить одно напоминание\n"
            "/clear_reminders — удалить все напоминания\n\n"
            
            "⚙️ <b>Сервисные:</b>\n"
            "/start — активация бота в чате\n"
            "/test — проверка работы бота\n"
            "/status — диагностика состояния\n"
            "/restore_reminders — восстановление из резервной копии (необходимо при этом в Google-таблице поставить статус Active на нужные уведомления, которые нужно восстановить)\n"
            "/about — информация о боте\n"
            "/cancel — отмена операции\n\n"
        )
        
        features_text = (
            "🛡️ <b>Надёжность:</b>\n"
            "📊 Автосинхронизация с Google Sheets каждые 5 минут\n"
            "🔄 Автовосстановление при сбоях\n"
            "📈 Детальная диагностика состояния системы\n"
            "🔒 Защита от потери данных\n\n"
            
            "🚀 <b>Быстрый старт:</b>\n"
            "Добавьте бота в чат\n"
            "Введите /start для активации\n"
            "Создайте первое напоминание командой /remind\n"
            "Проверьте список командой /list_reminders\n\n"
            
            "🚨 <b>Важно:</b> отправляет уведомления во все лички и группы, где он был активирован, вне зависимости от того, кто и где создавал напоминания!\n\n"
            
            "💡 <b>Пример использования:</b>\n"
            "/remind\n"
            "2025-07-15 09:00\n"
            "Запланирован еженедельный мит с отделом в 18:00\n\n"
            
            "Создание всех напоминаний идёт пошагово с подсказками — достаточно просто нажать на нужную команду, бот подскажет, что и как ввести.\n"
            "Если что-то не так - пишите..\n"
            "Просьба не добавлять ссылки ведущие на сайты МБ БА и тп, а также любые упоминания об этих ресурсах, из-за этого бота блокируют за нарушение правил Telegram!\n\n"
            
            "🤖 <b>Бот работает 24/7 и гарантирует доставку ваших напоминаний!</b>"
        )
        
        # Отправляем сообщение частями, чтобы не превысить лимит Telegram
        try:
            update.message.reply_text(about_text, parse_mode=ParseMode.HTML)
            update.message.reply_text(commands_text, parse_mode=ParseMode.HTML)
            update.message.reply_text(features_text, parse_mode=ParseMode.HTML)
        except Exception as e:
            # Fallback без HTML если возникла ошибка
            logger.error(f"Error sending about message with HTML: {e}")
            clean_about = about_text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            clean_commands = commands_text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            clean_features = features_text.replace('<b>', '').replace('</b>', '').replace('<i>', '').replace('</i>', '')
            
            update.message.reply_text(clean_about)
            update.message.reply_text(clean_commands)
            update.message.reply_text(clean_features)
            
    except Exception as e:
        logger.error(f"Error in about_bot command: {e}")
        try:
            update.message.reply_text("❌ <b>Ошибка получения информации о боте</b>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Ошибка получения информации о боте")

def unsubscribe_user(chat_id, user_name="Unknown", reason="USER_REQUEST"):
    """
    Удаляет пользователя из рассылки (локально и в Google Sheets)
    """
    try:
        # Загружаем текущий список чатов
        try:
            with open("subscribed_chats.json", "r") as f:
                chats = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            chats = []
        
        # Проверяем, был ли пользователь подписан
        if chat_id in chats:
            # Удаляем из локального файла
            chats.remove(chat_id)
            save_chats(chats)
            
            logger.info(f"🚫 User {chat_id} ({user_name}) unsubscribed: {reason}")
            
            # Удаляем из Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    # Обновляем статус чата в Google Sheets
                    moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S")
                    sheets_manager.log_operation(
                        timestamp=moscow_time,
                        action="CHAT_UNSUBSCRIBE",
                        user_id="SYSTEM",
                        username="AutoUnsubscribe",
                        chat_id=chat_id,
                        details=f"User unsubscribed: {user_name}, Reason: {reason}",
                        reminder_id=""
                    )
                    
                    # Обновляем список подписанных чатов в Google Sheets
                    sheets_manager.sync_subscribed_chats_to_sheets(chats)
                    
                    # Помечаем чат как неактивный в статистике
                    sheets_manager.update_chat_stats(chat_id, user_name, "unsubscribed", None, status="Unsubscribed")
                    
                    logger.info(f"📊 Successfully synced unsubscription of {chat_id} to Google Sheets")
                    
                except Exception as e:
                    logger.error(f"❌ Error syncing unsubscription to Google Sheets: {e}")
                    # Продолжаем, даже если синхронизация не удалась
            
            return True, "SUCCESS"
        else:
            logger.warning(f"⚠️ User {chat_id} was not subscribed (unsubscribe attempt)")
            return False, "NOT_SUBSCRIBED"
            
    except Exception as e:
        logger.error(f"❌ Error unsubscribing user {chat_id}: {e}")
        return False, f"ERROR: {str(e)}"

def unsubscribe_command(update: Update, context: CallbackContext):
    """
    Команда /unsubscribe для отписки от бота
    """
    try:
        chat_id = update.effective_chat.id
        user = update.effective_user
        user_name = user.username or user.first_name or "Unknown"
        
        success, result = unsubscribe_user(chat_id, user_name, "COMMAND")
        
        if success:
            try:
                update.message.reply_text(
                    "✅ <b>Вы успешно отписались от бота</b>\n\n"
                    "🚫 Вы больше не будете получать напоминания\n"
                    "💬 Чтобы снова подписаться, используйте команду /start",
                    parse_mode=ParseMode.HTML
                )
            except:
                update.message.reply_text(
                    "✅ Вы успешно отписались от бота\n\n"
                    "🚫 Вы больше не будете получать напоминания\n"
                    "💬 Чтобы снова подписаться, используйте команду /start"
                )
        else:
            if result == "NOT_SUBSCRIBED":
                try:
                    update.message.reply_text(
                        "ℹ️ <b>Вы уже не подписаны на бота</b>\n\n"
                        "💬 Чтобы подписаться, используйте команду /start",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    update.message.reply_text(
                        "ℹ️ Вы уже не подписаны на бота\n\n"
                        "💬 Чтобы подписаться, используйте команду /start"
                    )
            else:
                try:
                    update.message.reply_text(
                        "❌ <b>Ошибка при отписке</b>\n\n"
                        "Обратитесь к администратору бота",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    update.message.reply_text("❌ Ошибка при отписке")
                    
    except Exception as e:
        logger.error(f"Error in unsubscribe command: {e}")
        try:
            update.message.reply_text("❌ <b>Ошибка команды отписки</b>", parse_mode=ParseMode.HTML)
        except:
            update.message.reply_text("❌ Ошибка команды отписки")

def handle_unsubscribe_button(update: Update, context: CallbackContext):
    """
    Обработчик INLINE кнопки "Отписаться"
    """
    try:
        query = update.callback_query
        query.answer()  # Подтверждаем нажатие кнопки
        
        chat_id = query.from_user.id  # ID пользователя, который нажал кнопку
        user_name = query.from_user.username or query.from_user.first_name or "Unknown"
        
        success, result = unsubscribe_user(chat_id, user_name, "INLINE_BUTTON")
        
        if success:
            try:
                query.edit_message_text(
                    "✅ <b>Вы успешно отписались от бота</b>\n\n"
                    "🚫 Вы больше не будете получать напоминания\n"
                    "💬 Чтобы снова подписаться, используйте команду /start",
                    parse_mode=ParseMode.HTML
                )
            except:
                query.edit_message_text(
                    "✅ Вы успешно отписались от бота\n\n"
                    "🚫 Вы больше не будете получать напоминания\n"
                    "💬 Чтобы снова подписаться, используйте команду /start"
                )
        else:
            if result == "NOT_SUBSCRIBED":
                try:
                    query.edit_message_text(
                        "ℹ️ <b>Вы уже не подписаны на бота</b>\n\n"
                        "💬 Чтобы подписаться, используйте команду /start",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    query.edit_message_text(
                        "ℹ️ Вы уже не подписаны на бота\n\n"
                        "💬 Чтобы подписаться, используйте команду /start"
                    )
            else:
                try:
                    query.edit_message_text(
                        "❌ <b>Ошибка при отписке</b>\n\n"
                        "Обратитесь к администратору бота",
                        parse_mode=ParseMode.HTML
                    )
                except:
                    query.edit_message_text("❌ Ошибка при отписке")
                    
    except Exception as e:
        logger.error(f"Error in unsubscribe button handler: {e}")
        try:
            update.callback_query.answer("❌ Ошибка при отписке", show_alert=True)
        except:
            pass

# Функция handle_poll_results_button удалена по запросу пользователя

def monitor_scheduler_health(context: CallbackContext):
    """
    Мониторинг здоровья планировщика - проверяет активные задачи каждые 10 минут
    и пытается восстановить голосования из Google Sheets при их отсутствии
    """
    try:
        moscow_time = get_moscow_time().strftime("%Y-%m-%d %H:%M:%S MSK")
        logger.info(f"🔍 Scheduler health check started at {moscow_time}")
        
        # Проверяем активные задачи
        job_queue = context.job_queue
        active_jobs = check_active_jobs(job_queue)
        
        reminder_jobs = active_jobs.get('reminder_jobs', 0)
        poll_jobs = active_jobs.get('poll_jobs', 0)
        
        logger.info(f"📊 Current active jobs: {reminder_jobs} reminders, {poll_jobs} polls")
        
        # Если нет активных задач голосований, пытаемся восстановить
        if poll_jobs == 0:
            logger.warning(f"⚠️ No active poll jobs detected! Attempting emergency restore...")
            
            # Пытаемся восстановить из Google Sheets
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    logger.info(f"🔧 Attempting emergency poll restore from Google Sheets...")
                    
                    # Загружаем активные голосования из Google Sheets
                    active_polls = sheets_manager.get_active_polls()
                    
                    if active_polls:
                        logger.info(f"📊 Found {len(active_polls)} active polls in Google Sheets")
                        
                        # Сохраняем в локальный файл
                        save_polls(active_polls)
                        
                        # Перепланируем все голосования
                        reschedule_all_polls(job_queue)
                        
                        # Проверяем результат
                        updated_jobs = check_active_jobs(job_queue)
                        new_poll_jobs = updated_jobs.get('poll_jobs', 0)
                        
                        logger.info(f"✅ Emergency restore completed: {new_poll_jobs} poll jobs scheduled")
                        
                        # Логируем в Google Sheets
                        sheets_manager.log_poll_action(
                            "EMERGENCY_RESTORE", 
                            "SYSTEM", 
                            "HealthMonitor", 
                            0, 
                            f"Emergency restore: {new_poll_jobs} polls restored from Google Sheets",
                            "HEALTH_CHECK"
                        )
                        
                    else:
                        logger.info(f"📭 No active polls found in Google Sheets")
                        
                except Exception as e:
                    logger.error(f"❌ Emergency poll restore failed: {e}")
            else:
                logger.warning(f"📵 Google Sheets not available for emergency restore")
        else:
            logger.info(f"✅ Scheduler health check passed: {poll_jobs} poll jobs active")
            
    except Exception as e:
        logger.error(f"❌ Error in scheduler health monitor: {e}")

def main():
    try:
        global BOT_START_TIME
        BOT_START_TIME = get_moscow_time()
        
        token = os.environ['BOT_TOKEN']
        port = int(os.environ.get('PORT', 8000))
        updater = Updater(token=token, use_context=True)
        
        # Reset any existing webhook so polling can start cleanly
        try:
            res = updater.bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook deleted: %s", res)
        except Exception as e:
            logger.error("Error deleting webhook: %s", e)
        
        dp = updater.dispatcher
        
        # ✅ ПРОВЕРЯЕМ И ВОССТАНАВЛИВАЕМ ПОДПИСКИ ПРИ ЗАПУСКЕ
        logger.info("🔧 Checking subscribed_chats.json...")
        ensure_subscribed_chats_file()
        
        # 🆕 ПРОВЕРЯЕМ И ВОССТАНАВЛИВАЕМ НАПОМИНАНИЯ ПРИ ЗАПУСКЕ
        logger.info("🔧 Checking reminders.json...")
        reminders_restored, reminders_count = ensure_reminders_file()
        if reminders_restored:
            logger.info(f"✅ Reminders status: {reminders_count} reminders ready for scheduling")
        else:
            logger.warning(f"⚠️ Reminders status: starting with empty reminders list")
            logger.warning("💡 TIP: Use /restore_reminders command to recover data from Google Sheets")
        
        # 🆕 ПРОВЕРЯЕМ И ВОССТАНАВЛИВАЕМ ГОЛОСОВАНИЯ ПРИ ЗАПУСКЕ
        logger.info("🔧 Checking polls.json...")
        polls_restored, polls_count = ensure_polls_file()
        if polls_restored:
            logger.info(f"✅ Polls status: {polls_count} polls ready for scheduling")
        else:
            logger.warning(f"⚠️ Polls status: starting with empty polls list")
            logger.warning("💡 TIP: Use /restore_polls command to recover data from Google Sheets")
        
        # Добавляем обработчики команд ПЕРВЫМИ
        dp.add_handler(CommandHandler("start", start))
        dp.add_handler(CommandHandler("test", test))
        dp.add_handler(CommandHandler("about", about_bot))  # Добавляем новую команду
        
        conv = ConversationHandler(
            entry_points=[CommandHandler("remind", start_add_one_reminder)],
            states={
                REMINDER_DATE: [MessageHandler(Filters.text & ~Filters.command, receive_reminder_datetime)],
                REMINDER_TEXT: [MessageHandler(Filters.text & ~Filters.command, receive_reminder_text)],
            },
            fallbacks=[CommandHandler("cancel", cancel_reminder)],
            allow_reentry=True,
        )
        dp.add_handler(conv)
        
        conv_daily = ConversationHandler(
            entry_points=[CommandHandler("remind_daily", start_add_daily_reminder)],
            states={
                DAILY_TIME: [MessageHandler(Filters.text & ~Filters.command, receive_daily_time)],
                DAILY_TEXT: [MessageHandler(Filters.text & ~Filters.command, receive_daily_text)],
            },
            fallbacks=[CommandHandler("cancel", cancel_reminder)],
            allow_reentry=True,
        )
        dp.add_handler(conv_daily)

        conv_weekly = ConversationHandler(
            entry_points=[CommandHandler("remind_weekly", start_add_weekly_reminder)],
            states={
                WEEKLY_DAY: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_day)],
                WEEKLY_TIME: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_time)],
                WEEKLY_TEXT: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_text)],
            },
            fallbacks=[CommandHandler("cancel", cancel_reminder)],
            allow_reentry=True,
        )
        dp.add_handler(conv_weekly)
        
        # Обработчики голосований
        conv_poll = ConversationHandler(
            entry_points=[CommandHandler("poll", start_add_one_poll)],
            states={
                POLL_DATE: [MessageHandler(Filters.text & ~Filters.command, receive_poll_datetime)],
                POLL_QUESTION: [MessageHandler(Filters.text & ~Filters.command, receive_poll_question)],
                POLL_OPTIONS: [MessageHandler(Filters.text & ~Filters.command, receive_poll_options)],
            },
            fallbacks=[CommandHandler("cancel", cancel_poll)],
            allow_reentry=True,
        )
        dp.add_handler(conv_poll)
        
        conv_daily_poll = ConversationHandler(
            entry_points=[CommandHandler("daily_poll", start_add_daily_poll)],
            states={
                DAILY_POLL_TIME: [MessageHandler(Filters.text & ~Filters.command, receive_daily_poll_time)],
                DAILY_POLL_QUESTION: [MessageHandler(Filters.text & ~Filters.command, receive_daily_poll_question)],
                DAILY_POLL_OPTIONS: [MessageHandler(Filters.text & ~Filters.command, receive_daily_poll_options)],
            },
            fallbacks=[CommandHandler("cancel", cancel_poll)],
            allow_reentry=True,
        )
        dp.add_handler(conv_daily_poll)
        
        conv_weekly_poll = ConversationHandler(
            entry_points=[CommandHandler("weekly_poll", start_add_weekly_poll)],
            states={
                WEEKLY_POLL_DAY: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_poll_day)],
                WEEKLY_POLL_TIME: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_poll_time)],
                WEEKLY_POLL_QUESTION: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_poll_question)],
                WEEKLY_POLL_OPTIONS: [MessageHandler(Filters.text & ~Filters.command, receive_weekly_poll_options)],
            },
            fallbacks=[CommandHandler("cancel", cancel_poll)],
            allow_reentry=True,
        )
        dp.add_handler(conv_weekly_poll)
        
        dp.add_handler(CommandHandler("list_reminders", list_reminders))
        dp.add_handler(CommandHandler("list_polls", list_polls))
        
        conv_del = ConversationHandler(
            entry_points=[CommandHandler("del_reminder", start_delete_reminder)],
            states={REM_DEL_ID: [MessageHandler(Filters.text & ~Filters.command, confirm_delete_reminder)]},
            fallbacks=[CommandHandler("cancel", cancel_reminder)],
            allow_reentry=True,
        )
        dp.add_handler(conv_del)
        
        conv_del_poll = ConversationHandler(
            entry_points=[CommandHandler("del_poll", start_delete_poll)],
            states={POLL_DEL_ID: [MessageHandler(Filters.text & ~Filters.command, confirm_delete_poll)]},
            fallbacks=[CommandHandler("cancel", cancel_poll)],
            allow_reentry=True,
        )
        dp.add_handler(conv_del_poll)
        
        dp.add_handler(CommandHandler("clear_reminders", clear_reminders))
        dp.add_handler(CommandHandler("clear_polls", clear_polls))
        dp.add_handler(CommandHandler("delete_all_polls", clear_polls))
        dp.add_handler(CommandHandler("restore", restore_reminders))
        dp.add_handler(CommandHandler("restore_polls", restore_polls))
        dp.add_handler(CommandHandler("next", next_notification))
        dp.add_handler(CommandHandler("status", bot_status))
        dp.add_handler(CommandHandler("unsubscribe", unsubscribe_command))  # 🆕 Команда отписки
        
        # 🆕 Обработчик INLINE кнопок
        dp.add_handler(CallbackQueryHandler(handle_unsubscribe_button, pattern="^unsubscribe$"))
        # Обработчик кнопки результатов удален по запросу пользователя

        # Добавляем обработчик ошибок
        dp.add_error_handler(error_handler)

        # Запланировать все сохранённые напоминания
        logger.info("📋 Scheduling all reminders...")
        schedule_all_reminders(updater.job_queue)
        
        # Запланировать все сохранённые голосования
        logger.info("🗳️ Scheduling all polls...")
        schedule_all_polls(updater.job_queue)
        
        # 🆕 ПРОВЕРЯЕМ АКТИВНЫЕ ЗАДАНИЯ ПОСЛЕ ПЛАНИРОВАНИЯ
        active_reminder_jobs, active_poll_jobs = check_active_jobs(updater.job_queue)
        if active_reminder_jobs == 0:
            logger.warning("⚠️ CRITICAL: No active reminder jobs scheduled!")
            logger.warning("   Attempting immediate reminders restore...")
            
            # Попытка экстренного восстановления
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    success, message = sheets_manager.restore_reminders_from_sheets()
                    if success:
                        logger.info("✅ Emergency restore successful, rescheduling...")
                        reschedule_all_reminders(updater.job_queue)
                        final_reminder_jobs, final_poll_jobs = check_active_jobs(updater.job_queue)
                        logger.info(f"🔄 After emergency restore: {final_reminder_jobs} reminder jobs, {final_poll_jobs} poll jobs")
                    else:
                        logger.error(f"❌ Emergency restore failed: {message}")
                except Exception as e:
                    logger.error(f"❌ Exception during emergency restore: {e}")
        
        if active_poll_jobs == 0:
            logger.warning("⚠️ CRITICAL: No active poll jobs scheduled!")
            logger.warning("   Attempting immediate polls restore...")
            
            # Попытка экстренного восстановления голосований
            if SHEETS_AVAILABLE and sheets_manager and sheets_manager.is_initialized:
                try:
                    success, message = sheets_manager.restore_polls_from_sheets()
                    if success:
                        logger.info("✅ Emergency polls restore successful, rescheduling...")
                        reschedule_all_polls(updater.job_queue)
                        final_reminder_jobs, final_poll_jobs = check_active_jobs(updater.job_queue)
                        logger.info(f"🔄 After emergency polls restore: {final_reminder_jobs} reminder jobs, {final_poll_jobs} poll jobs")
                    else:
                        logger.error(f"❌ Emergency polls restore failed: {message}")
                except Exception as e:
                    logger.error(f"❌ Exception during emergency polls restore: {e}")
            else:
                logger.warning("📵 Google Sheets not available for emergency polls restore")
                logger.warning("   This means polls will not be sent!")
                logger.warning("   Check if polls.json contains valid future polls")
        
        # Добавляем ping каждые 5 минут для предотвращения засыпания на Render
        updater.job_queue.run_repeating(ping_self, interval=300, first=30)
        
        # 🚨 КРИТИЧЕСКИ ВАЖНЫЙ МОНИТОРИНГ АКТИВНОСТИ ПЛАНИРОВЩИКА
        # Запускаем мониторинг здоровья планировщика каждые 10 минут
        updater.job_queue.run_repeating(monitor_scheduler_health, interval=600, first=60)
        logger.info("🔍 Scheduler health monitoring enabled (every 10 minutes)")
        
        # Мониторинг здоровья планировщика уже запущен выше
        
        # ✅ АВТОМАТИЧЕСКАЯ СИНХРОНИЗАЦИЯ ПОДПИСОК КАЖДЫЕ 5 МИНУТ
        updater.job_queue.run_repeating(auto_sync_subscribed_chats, interval=300, first=300)  # Каждые 5 минут, первый через 5 мин
        logger.info("🔄 Scheduled 5-minute subscribed chats sync")
        
        # 🆕 АВТОМАТИЧЕСКАЯ СИНХРОНИЗАЦИЯ НАПОМИНАНИЙ КАЖДЫЕ 5 МИНУТ
        updater.job_queue.run_repeating(auto_sync_reminders, interval=300, first=300)  # Каждые 5 минут, первый через 5 мин
        logger.info("🔄 Scheduled 5-minute reminders auto-sync")
        
        # 🆕 АВТОМАТИЧЕСКАЯ СИНХРОНИЗАЦИЯ ГОЛОСОВАНИЙ КАЖДЫЕ 5 МИНУТ
        updater.job_queue.run_repeating(auto_sync_polls, interval=300, first=300)  # Каждые 5 минут, первый через 5 мин
        logger.info("🔄 Scheduled 5-minute polls auto-sync")

        # Health check server for Render free tier
        threading.Thread(target=start_health_server, daemon=True).start()
        
        # 🚨 КРИТИЧЕСКИ ВАЖНАЯ ЗАЩИТА ОТ МНОЖЕСТВЕННЫХ ЭКЗЕМПЛЯРОВ
        logger.info("🚀 Starting bot with AGGRESSIVE conflict prevention...")
        
        # Множественные попытки очистки webhook для предотвращения конфликтов
        for attempt in range(3):
            try:
                result = updater.bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"✅ Webhook deletion attempt {attempt + 1}: {result}")
                time.sleep(2)  # Пауза между попытками
            except Exception as e:
                logger.warning(f"⚠️ Webhook deletion attempt {attempt + 1} failed: {e}")
                time.sleep(1)
        
        # Дополнительная пауза для полного завершения предыдущих экземпляров
        logger.info("⏳ Waiting for previous bot instances to terminate...")
        time.sleep(10)
        
        # Попытка запуска с расширенными параметрами для предотвращения конфликтов
        max_start_attempts = 3
        for start_attempt in range(max_start_attempts):
            try:
                logger.info(f"🚀 Bot start attempt {start_attempt + 1}/{max_start_attempts}")
                updater.start_polling(
                    drop_pending_updates=True, 
                    timeout=20,  # Увеличенный таймаут
                    read_latency=10,  # Увеличенная задержка чтения
                    bootstrap_retries=3  # Повторные попытки при ошибках
                )
                logger.info("✅ Bot started successfully in polling mode")
                break
            except Conflict as conflict_error:
                logger.error(f"🚨 Conflict on start attempt {start_attempt + 1}: {conflict_error}")
                if start_attempt < max_start_attempts - 1:
                    wait_time = (start_attempt + 1) * 15  # Увеличивающаяся задержка
                    logger.warning(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ All start attempts failed due to conflicts!")
                    raise
            except Exception as e:
                logger.error(f"❌ Start attempt {start_attempt + 1} failed: {e}")
                if start_attempt < max_start_attempts - 1:
                    time.sleep(5)
                else:
                    raise
            
        # Финальная проверка состояния через 30 секунд
        time.sleep(30)
        final_reminder_jobs, final_poll_jobs = check_active_jobs(updater.job_queue)
        logger.info(f"🔍 Final status check: {final_reminder_jobs} reminder jobs active")
        logger.info(f"🔍 Final status check: {final_poll_jobs} poll jobs active")
        
        # Проверяем подписанные чаты
        try:
            with open("subscribed_chats.json", "r") as f:
                final_chats = json.load(f)
                logger.info(f"📱 Final chats check: {len(final_chats)} subscribed chats")
        except:
            logger.warning("⚠️ Final chats check: subscribed_chats.json not accessible")
        
        # Проверяем напоминания  
        try:
            final_reminders = load_reminders()
            logger.info(f"📋 Final reminders check: {len(final_reminders)} reminders loaded")
        except:
            logger.warning("⚠️ Final reminders check: reminders.json not accessible")
        
        # Проверяем голосования
        try:
            final_polls = load_polls()
            logger.info(f"🗳️ Final polls check: {len(final_polls)} polls loaded")
        except:
            logger.warning("⚠️ Final polls check: polls.json not accessible")
        
        logger.info("🚀 Bot startup completed successfully!")
        
        updater.idle()
        
    except Exception as e:
        logger.error(f"Critical error in main: {e}")
        
if __name__ == "__main__":
    main()
