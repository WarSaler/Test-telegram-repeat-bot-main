# sheets_integration.py

import os
import json
import logging
from datetime import datetime
import pytz
from typing import Dict, List, Any, Optional
import gspread
from google.oauth2.service_account import Credentials
import time
import random

# Константы
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
logger = logging.getLogger(__name__)

def handle_rate_limit_with_retry(func, max_retries: int = 3, base_delay: float = 1.0):
    """
    Обработка rate limiting с экспоненциальной задержкой и jitter
    
    Args:
        func: Функция для выполнения
        max_retries: Максимальное количество попыток
        base_delay: Базовая задержка в секундах
    """
    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            
            # Проверяем на ошибку rate limiting
            if "429" in error_str or "RATE_LIMIT_EXCEEDED" in error_str or "Quota exceeded" in error_str:
                if attempt < max_retries:
                    # Более агрессивная экспоненциальная задержка для rate limiting
                    if attempt == 0:
                        delay = base_delay + random.uniform(0.5, 1.5)  # 1.5-2.5 секунд
                    elif attempt == 1:
                        delay = base_delay * 3 + random.uniform(1.0, 2.0)  # 4-5 секунд  
                    else:
                        delay = base_delay * 6 + random.uniform(2.0, 4.0)  # 8-10 секунд
                    
                    logger.warning(f"⏱️ Rate limit exceeded. Retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries + 1})")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"❌ Rate limit exceeded after {max_retries + 1} attempts. Giving up.")
                    raise
            else:
                # Не rate limiting ошибка - пробрасываем сразу
                raise
    
    return None

class SheetsManager:
    def __init__(self):
        self.credentials = None
        self.client = None
        self.spreadsheet = None
        self.sheet_id = None
        self.is_initialized = False
        self._init_sheets()
    
    def _init_sheets(self):
        """Инициализация Google Sheets"""
        try:
            # Получаем данные из переменных окружения
            sheets_creds = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
            self.sheet_id = os.environ.get('GOOGLE_SHEETS_ID')
            
            if not sheets_creds or not self.sheet_id:
                logger.warning("Google Sheets credentials or ID not found in environment variables")
                return
            
            # Парсим JSON credentials
            creds_data = json.loads(sheets_creds)
            
            # Настраиваем scopes
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Создаем credentials
            self.credentials = Credentials.from_service_account_info(creds_data, scopes=scopes)
            self.client = gspread.authorize(self.credentials)
            self.spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # Создаем необходимые листы
            self._setup_sheets()
            self.is_initialized = True
            logger.info("Google Sheets integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets: {e}")
            self.is_initialized = False
    
    def _setup_sheets(self):
        """Создание и настройка необходимых листов"""
        try:
            # Список необходимых листов с их заголовками
            sheets_config = {
                'Reminders': [
                    'ID', 'Text', 'Time_MSK', 'Type', 'Chat_ID', 'Chat_Name', 
                    'Status', 'Created_At', 'Username', 'Last_Sent', 'Days_Of_Week'
                ],
                'Send_History': [
                    'Timestamp_UTC', 'Timestamp_MSK', 'Reminder_ID', 'Chat_ID', 
                    'Status', 'Error', 'Text_Preview'
                ],
                'Chat_Stats': [
                    'Chat_ID', 'Chat_Name', 'Chat_Type', 'Reminders_Count', 
                    'Last_Activity', 'Members_Count', 'First_Seen', 'Status'
                ],
                'Operation_Logs': [
                    'Timestamp_UTC', 'Timestamp_MSK', 'Action', 'User_ID', 
                    'Username', 'Chat_ID', 'Details', 'Reminder_ID'
                ],
                'Polls': [
                    'ID', 'Question', 'Options', 'Time_MSK', 'Type', 'Chat_ID', 'Chat_Name',
                    'Status', 'Created_At', 'Username', 'Last_Sent', 'Days_Of_Week', 'Allow_Multiple_Answers'
                ],
                'Poll_Send_History': [
                    'Timestamp_UTC', 'Timestamp_MSK', 'Poll_ID', 'Chat_ID',
                    'Status', 'Error', 'Question_Preview'
                ]
            }
            
            existing_sheets = [sheet.title for sheet in self.spreadsheet.worksheets()]
            
            for sheet_name, headers in sheets_config.items():
                if sheet_name not in existing_sheets:
                    # Создаем новый лист
                    worksheet = self.spreadsheet.add_worksheet(
                        title=sheet_name, 
                        rows=1000, 
                        cols=len(headers)
                    )
                    # Добавляем заголовки
                    worksheet.append_row(headers)
                    logger.info(f"Created sheet: {sheet_name}")
                else:
                    # Проверяем и обновляем заголовки существующего листа
                    worksheet = self.spreadsheet.worksheet(sheet_name)
                    try:
                        current_headers = worksheet.row_values(1)
                        # Если заголовки не совпадают или лист пустой
                        if not current_headers or current_headers != headers:
                            worksheet.clear()
                            worksheet.append_row(headers)
                            logger.info(f"Updated headers for sheet: {sheet_name}")
                    except Exception as e:
                        # Если не удается получить заголовки, очищаем и создаем заново
                        logger.warning(f"Could not read headers for {sheet_name}, recreating: {e}")
                        worksheet.clear()
                        worksheet.append_row(headers)
                        logger.info(f"Recreated headers for sheet: {sheet_name}")
                        
        except Exception as e:
            logger.error(f"Error setting up sheets: {e}")
    
    def log_reminder_action(self, action: str, user_id: int, username: str, 
                          chat_id: int, details: str, reminder_id: int = None):
        """Логирование действий с напоминаниями с обработкой rate limiting"""
        if not self.is_initialized:
            return
        
        def _log_operation():
            worksheet = self.spreadsheet.worksheet('Operation_Logs')
            
            # Московское время
            moscow_time = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')
            
            row = [
                moscow_time,
                action,
                user_id,
                username,
                chat_id,
                details,
                reminder_id or ''
            ]
            
            worksheet.append_row(row)
            logger.info(f"Logged action: {action} by {username}")
        
        try:
            # Используем retry механизм для обработки rate limiting
            handle_rate_limit_with_retry(_log_operation, max_retries=2, base_delay=0.5)
            
        except Exception as e:
            logger.error(f"Error logging reminder action: {e}")
            return False
        
        return True
    
    def sync_reminder(self, reminder: Dict[str, Any], action: str = 'UPDATE'):
        """Синхронизация напоминания с Google Sheets с обработкой rate limiting"""
        if not self.is_initialized:
            return
        
        def _sync_operation():
            worksheet = self.spreadsheet.worksheet('Reminders')
            
            # Подготавливаем данные для записи
            row_data = [
                reminder.get('id', ''),
                reminder.get('text', ''),
                reminder.get('time', ''),
                reminder.get('type', ''),
                reminder.get('chat_id', ''),
                reminder.get('chat_name', ''),
                'Active' if action != 'DELETE' else 'Deleted',
                reminder.get('created_at', ''),
                reminder.get('username', ''),
                reminder.get('last_sent', ''),
                str(reminder.get('days_of_week', ''))
            ]
            
            if action == 'CREATE':
                # Добавляем новую запись
                worksheet.append_row(row_data)
            elif action == 'UPDATE' or action == 'DELETE':
                # Безопасно получаем записи
                try:
                    records = worksheet.get_all_records()
                except Exception as e:
                    logger.warning(f"Could not get records from Reminders, sheet may be empty: {e}")
                    records = []
                
                row_to_update = None
                
                for i, record in enumerate(records):
                    if str(record.get('ID', '')) == str(reminder.get('id')):
                        row_to_update = i + 2  # +2 потому что индексы с 1 и есть заголовок
                        break
                
                if row_to_update:
                    # Обновляем существующую строку
                    for col, value in enumerate(row_data, 1):
                        worksheet.update_cell(row_to_update, col, value)
                else:
                    # Если не найдена, добавляем новую
                    worksheet.append_row(row_data)
            
            logger.info(f"Synced reminder {reminder.get('id')} with action {action}")
        
        try:
            # Используем retry механизм для обработки rate limiting с увеличенными параметрами
            handle_rate_limit_with_retry(_sync_operation, max_retries=5, base_delay=2.0)
            
        except Exception as e:
            logger.error(f"Error syncing reminder: {e}")
            # Возвращаем False чтобы вызывающий код знал об ошибке
            return False
        
        return True
    
    def log_reminder_sent(self, reminder_id: int, chat_id: int, status: str, 
                         error: str = None, text_preview: str = ''):
        """Логирование отправленных напоминаний"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Send_History')
            now_utc = datetime.now(pytz.UTC)
            now_msk = now_utc.astimezone(MOSCOW_TZ)
            
            row = [
                now_utc.strftime('%Y-%m-%d %H:%M:%S'),
                now_msk.strftime('%Y-%m-%d %H:%M:%S'),
                reminder_id,
                chat_id,
                status,
                error or '',
                text_preview[:50] + '...' if len(text_preview) > 50 else text_preview
            ]
            
            worksheet.append_row(row)
            logger.info(f"Logged reminder sent: {reminder_id} to {chat_id}")
            
        except Exception as e:
            logger.error(f"Error logging reminder sent: {e}")
    
    def update_chat_stats(self, chat_id: int, chat_name: str, chat_type: str, 
                         members_count: int = None, status: str = "Active"):
        """Обновление статистики чатов с поддержкой статуса"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Chat_Stats')
            
            # Безопасно получаем записи
            try:
                records = worksheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get records from Chat_Stats, sheet may be empty: {e}")
                records = []
            
            # Ищем существующую запись
            row_to_update = None
            existing_record = None
            
            for i, record in enumerate(records):
                if str(record.get('Chat_ID', '')) == str(chat_id):
                    row_to_update = i + 2  # +2 для учета заголовка и индексации с 1
                    existing_record = record
                    break
            
            now_msk = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')
            
            if row_to_update:
                # Обновляем существующую запись
                worksheet.update_cell(row_to_update, 2, chat_name)  # Chat_Name
                worksheet.update_cell(row_to_update, 3, chat_type)  # Chat_Type
                worksheet.update_cell(row_to_update, 5, now_msk)    # Last_Activity
                if members_count is not None:
                    worksheet.update_cell(row_to_update, 6, members_count)  # Members_Count
                
                # 🆕 Обновляем статус (добавляем новый столбец если нужно)
                try:
                    worksheet.update_cell(row_to_update, 8, status)  # Status (столбец 8)
                except Exception as e:
                    # Возможно, столбец Status еще не существует, добавляем заголовок
                    try:
                        headers = worksheet.row_values(1)
                        if 'Status' not in headers:
                            # Добавляем заголовок Status в столбец 8
                            worksheet.update_cell(1, 8, 'Status')
                        worksheet.update_cell(row_to_update, 8, status)
                    except Exception as e2:
                        logger.warning(f"Could not update status column: {e2}")
                
                logger.info(f"📊 Updated existing chat {chat_id} in Google Sheets with status: {status}")
            else:
                # Создаем новую запись
                row = [
                    chat_id,
                    chat_name,
                    chat_type,
                    0,  # Reminders_Count - будет обновляться отдельно
                    now_msk,  # Last_Activity
                    members_count or 0,  # Members_Count
                    now_msk,   # First_Seen
                    status     # 🆕 Status
                ]
                
                # Проверяем, есть ли заголовок Status
                try:
                    headers = worksheet.row_values(1)
                    if len(headers) < 8 or headers[7] != 'Status':
                        # Добавляем заголовок Status
                        worksheet.update_cell(1, 8, 'Status')
                except:
                    pass
                
                worksheet.append_row(row)
                logger.info(f"📊 Added new chat {chat_id} to Google Sheets with status: {status}")
            
        except Exception as e:
            logger.error(f"Error updating chat stats: {e}")
    
    def update_reminders_count(self, chat_id: int):
        """Обновление количества напоминаний для чата с обработкой rate limiting"""
        if not self.is_initialized:
            return
        
        def _update_operation():
            # Подсчитываем активные напоминания для чата
            reminders_sheet = self.spreadsheet.worksheet('Reminders')
            try:
                reminders = reminders_sheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get reminders, sheet may be empty: {e}")
                reminders = []
            
            active_count = sum(1 for r in reminders 
                             if str(r.get('Chat_ID', '')) == str(chat_id) 
                             and r.get('Status') == 'Active')
            
            # Обновляем статистику чата
            chat_stats_sheet = self.spreadsheet.worksheet('Chat_Stats')
            try:
                records = chat_stats_sheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get chat stats, sheet may be empty: {e}")
                records = []
            
            updated = False
            for i, record in enumerate(records):
                if str(record.get('Chat_ID', '')) == str(chat_id):
                    chat_stats_sheet.update_cell(i + 2, 4, active_count)  # Reminders_Count
                    updated = True
                    break
            
            if updated:
                logger.info(f"📊 Updated reminders count for chat {chat_id}: {active_count}")
            else:
                logger.warning(f"⚠️ Chat {chat_id} not found in Chat_Stats for reminders count update")
        
        try:
            # Используем retry механизм для обработки rate limiting с увеличенными параметрами
            handle_rate_limit_with_retry(_update_operation, max_retries=5, base_delay=2.0)
            
        except Exception as e:
            logger.error(f"Error updating reminders count: {e}")
            return False
        
        return True
    
    def update_polls_count(self, chat_id: int):
        """Обновление количества голосований для чата с обработкой rate limiting"""
        if not self.is_initialized:
            return
        
        def _update_operation():
            # Подсчитываем активные голосования для чата
            polls_sheet = self.spreadsheet.worksheet('Polls')
            try:
                polls = polls_sheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get polls, sheet may be empty: {e}")
                polls = []
            
            active_count = sum(1 for p in polls 
                             if str(p.get('Chat_ID', '')) == str(chat_id) 
                             and p.get('Status') == 'Active')
            
            # Обновляем статистику чата
            chat_stats_sheet = self.spreadsheet.worksheet('Chat_Stats')
            try:
                records = chat_stats_sheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get chat stats, sheet may be empty: {e}")
                records = []
            
            updated = False
            for i, record in enumerate(records):
                if str(record.get('Chat_ID', '')) == str(chat_id):
                    # Предполагаем, что колонка Polls_Count находится в позиции 5
                    chat_stats_sheet.update_cell(i + 2, 5, active_count)
                    updated = True
                    break
            
            if updated:
                logger.info(f"📊 Updated polls count for chat {chat_id}: {active_count}")
            else:
                logger.warning(f"⚠️ Chat {chat_id} not found in Chat_Stats for polls count update")
        
        try:
            # Используем retry механизм для обработки rate limiting с увеличенными параметрами
            handle_rate_limit_with_retry(_update_operation, max_retries=5, base_delay=2.0)
            
        except Exception as e:
            logger.error(f"Error updating polls count: {e}")
            return False
        
        return True
    
    def get_polls_count(self, chat_id: int) -> int:
        """Получение количества активных голосований для чата"""
        if not self.is_initialized:
            return 0
        
        try:
            polls_sheet = self.spreadsheet.worksheet('Polls')
            try:
                polls = polls_sheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get polls, sheet may be empty: {e}")
                return 0
            
            active_count = sum(1 for p in polls 
                             if str(p.get('Chat_ID', '')) == str(chat_id) 
                             and p.get('Status') == 'Active')
            
            return active_count
            
        except Exception as e:
            logger.error(f"Error getting polls count: {e}")
            return 0
    
    def backup_all_reminders(self, reminders: List[Dict[str, Any]]):
        """Полное резервное копирование всех напоминаний"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Reminders')
            
            # Очищаем лист (кроме заголовков)
            worksheet.clear()
            headers = [
                'ID', 'Text', 'Time_MSK', 'Type', 'Chat_ID', 'Chat_Name', 
                'Status', 'Created_At', 'Username', 'Last_Sent', 'Days_Of_Week'
            ]
            worksheet.append_row(headers)
            
            # Добавляем все напоминания
            for reminder in reminders:
                row_data = [
                    reminder.get('id', ''),
                    reminder.get('text', ''),
                    reminder.get('time', ''),
                    reminder.get('type', ''),
                    reminder.get('chat_id', ''),
                    reminder.get('chat_name', ''),
                    'Active',
                    reminder.get('created_at', ''),
                    reminder.get('username', ''),
                    reminder.get('last_sent', ''),
                    str(reminder.get('days_of_week', ''))
                ]
                worksheet.append_row(row_data)
            
            logger.info(f"Backed up {len(reminders)} reminders to Google Sheets")
            
        except Exception as e:
            logger.error(f"Error backing up reminders: {e}")

    def restore_reminders_from_sheets(self, target_file="reminders.json"):
        """Восстановление активных напоминаний из Google Sheets"""
        if not self.is_initialized:
            logger.warning("Google Sheets not available for reminders restoration")
            return False, "Google Sheets не инициализирован"
        
        try:
            worksheet = self.spreadsheet.worksheet('Reminders')
            
            # Безопасно получаем записи
            try:
                records = worksheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get records from Reminders, sheet may be empty: {e}")
                return False, "Не удалось получить данные из Google Sheets (лист может быть пустым)"
            
            if not records:
                logger.warning("No records found in Reminders sheet")
                return False, "В листе Reminders нет записей"
            
            # Фильтруем только активные напоминания
            active_reminders = []
            seen_ids = set()  # 🆕 Отслеживаем уже обработанные ID
            
            for record in records:
                try:
                    # Проверяем статус
                    status = record.get('Status', '').strip()
                    if status.lower() != 'active':
                        logger.debug(f"Skipping reminder {record.get('ID')} with status: {status}")
                        continue
                    
                    # 🆕 Проверяем уникальность ID
                    reminder_id = str(record.get('ID', '')).strip()
                    if not reminder_id or reminder_id in seen_ids:
                        if not reminder_id:
                            logger.warning(f"Skipping reminder with empty ID: {record}")
                        else:
                            logger.warning(f"Skipping duplicate reminder ID: {reminder_id}")
                        continue
                    
                    seen_ids.add(reminder_id)  # 🆕 Запоминаем ID
                    
                    # Конвертируем в формат бота
                    reminder_type = record.get('Type', '').strip().lower()
                    
                    if reminder_type == 'once':
                        # Разовое напоминание
                        restored_reminder = {
                            "id": reminder_id,  # 🆕 Используем проверенный ID
                            "type": "once",
                            "datetime": record.get('Time_MSK', ''),
                            "text": record.get('Text', ''),
                            "chat_id": record.get('Chat_ID', ''),
                            "chat_name": record.get('Chat_Name', ''),
                            "created_at": record.get('Created_At', ''),
                            "username": record.get('Username', ''),
                            "last_sent": record.get('Last_Sent', '')
                        }
                        
                    elif reminder_type == 'daily':
                        # Ежедневное напоминание
                        restored_reminder = {
                            "id": reminder_id,  # 🆕 Используем проверенный ID
                            "type": "daily",
                            "time": record.get('Time_MSK', ''),
                            "text": record.get('Text', ''),
                            "chat_id": record.get('Chat_ID', ''),
                            "chat_name": record.get('Chat_Name', ''),
                            "created_at": record.get('Created_At', ''),
                            "username": record.get('Username', ''),
                            "last_sent": record.get('Last_Sent', '')
                        }
                        
                    elif reminder_type == 'weekly':
                        # Еженедельное напоминание
                        days_of_week = record.get('Days_Of_Week', '').strip()
                        time_parts = record.get('Time_MSK', '').strip().split()
                        
                        if len(time_parts) >= 2:
                            # Формат: "понедельник 10:00"
                            day_name = time_parts[0].lower()
                            time_str = time_parts[1]
                        else:
                            # Используем Days_Of_Week и Time_MSK отдельно
                            day_name = days_of_week.lower() if days_of_week else 'понедельник'
                            time_str = record.get('Time_MSK', '10:00')
                        
                        restored_reminder = {
                            "id": reminder_id,  # 🆕 Используем проверенный ID
                            "type": "weekly",
                            "day": day_name,
                            "time": time_str,
                            "text": record.get('Text', ''),
                            "chat_id": record.get('Chat_ID', ''),
                            "chat_name": record.get('Chat_Name', ''),
                            "created_at": record.get('Created_At', ''),
                            "username": record.get('Username', ''),
                            "last_sent": record.get('Last_Sent', ''),
                            "days_of_week": day_name
                        }
                        
                    else:
                        logger.warning(f"Unknown reminder type: {reminder_type} for ID {record.get('ID')}")
                        continue
                    
                    # Валидация обязательных полей
                    if not restored_reminder.get('id') or not restored_reminder.get('text'):
                        logger.warning(f"Invalid reminder data: ID={restored_reminder.get('id')}, Text={restored_reminder.get('text')}")
                        continue
                    
                    active_reminders.append(restored_reminder)
                    logger.debug(f"Restored reminder: {restored_reminder['id']} ({restored_reminder['type']})")
                    
                except Exception as e:
                    logger.error(f"Error processing reminder record: {e}")
                    continue
            
            if not active_reminders:
                logger.warning("No active reminders found in Google Sheets")
                return False, "В Google Sheets не найдено активных напоминаний"
            
            # 🆕 Дополнительная статистика восстановления
            total_processed = len(records)
            duplicates_skipped = total_processed - len(seen_ids) - len([r for r in records if r.get('Status', '').lower() != 'active'])
            invalid_skipped = len(seen_ids) - len(active_reminders)
            
            logger.info(f"📊 Restore statistics:")
            logger.info(f"   Total records in Google Sheets: {total_processed}")
            logger.info(f"   Active reminders found: {len(active_reminders)}")
            logger.info(f"   Duplicates skipped: {duplicates_skipped}")
            logger.info(f"   Invalid records skipped: {invalid_skipped}")
            logger.info(f"   Non-active records skipped: {total_processed - len(seen_ids)}")
            
            # Сохраняем восстановленные напоминания
            try:
                import json
                with open(target_file, "w", encoding='utf-8') as f:
                    json.dump(active_reminders, f, ensure_ascii=False, indent=2)
                
                logger.info(f"✅ Successfully restored {len(active_reminders)} active reminders from Google Sheets to {target_file}")
                logger.info(f"🔄 File completely overwritten - no duplicates possible")
                
                # Логируем операцию восстановления
                moscow_time = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')
                self.log_operation(
                    timestamp=moscow_time,
                    action="RESTORE_REMINDERS",
                    user_id="SYSTEM",
                    username="AutoRestore",
                    chat_id=0,
                    details=f"Restored {len(active_reminders)} active reminders from Google Sheets (duplicates: {duplicates_skipped}, invalid: {invalid_skipped})",
                    reminder_id=""
                )
                
                return True, f"Успешно восстановлено {len(active_reminders)} активных напоминаний (без дубликатов)"
                
            except Exception as e:
                logger.error(f"Error saving restored reminders to {target_file}: {e}")
                return False, f"Ошибка сохранения в файл: {e}"
                
        except Exception as e:
            logger.error(f"Error restoring reminders from Google Sheets: {e}")
            return False, f"Ошибка восстановления из Google Sheets: {e}"

    def get_subscribed_chats(self):
        """Получение списка АКТИВНЫХ подписанных чатов из Google Sheets (исключая отписавшихся)"""
        if not self.is_initialized:
            return []
        
        try:
            worksheet = self.spreadsheet.worksheet('Chat_Stats')
            
            # Безопасно получаем записи
            try:
                records = worksheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get records from Chat_Stats, sheet may be empty: {e}")
                return []
            
            # 🆕 Возвращаем только АКТИВНЫЕ чаты (исключаем отписавшихся)
            chat_ids = []
            unsubscribed_count = 0
            for record in records:
                try:
                    chat_id_value = record.get('Chat_ID')
                    status = record.get('Status', 'Active').strip()  # По умолчанию Active для совместимости
                    
                    if chat_id_value:
                        chat_id = int(chat_id_value)
                        if chat_id != 0:  # Исключаем 0 и пустые значения
                            # 🚫 ФИЛЬТРУЕМ ПО СТАТУСУ - исключаем отписавшихся
                            if status.lower() in ['unsubscribed', 'blocked', 'deleted']:
                                unsubscribed_count += 1
                                logger.debug(f"🚫 Excluding chat {chat_id} with status: {status}")
                                continue
                            
                            chat_ids.append(chat_id)
                except (ValueError, TypeError):
                    continue
            
            if unsubscribed_count > 0:
                logger.info(f"🔄 Retrieved {len(chat_ids)} active chats from Google Sheets (excluded {unsubscribed_count} unsubscribed)")
            else:
                logger.info(f"🔄 Retrieved {len(chat_ids)} active chats from Google Sheets")
            
            return chat_ids
            
        except Exception as e:
            logger.error(f"Error retrieving subscribed chats from Google Sheets: {e}")
            return []
    
    def restore_subscribed_chats_file(self, target_file="subscribed_chats.json"):
        """Восстановление файла subscribed_chats.json из Google Sheets"""
        if not self.is_initialized:
            logger.warning("Google Sheets not available for chat restoration")
            return False
        
        try:
            # Получаем чаты из Google Sheets
            chat_ids = self.get_subscribed_chats()
            
            if not chat_ids:
                logger.warning("No chats found in Google Sheets for restoration")
                return False
            
            # Записываем в локальный файл
            with open(target_file, "w") as f:
                json.dump(chat_ids, f)
            
            logger.info(f"✅ Successfully restored {len(chat_ids)} chats to {target_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error restoring subscribed chats file: {e}")
            return False
    
    def sync_subscribed_chats_from_sheets(self, target_file="subscribed_chats.json"):
        """Синхронизация subscribed_chats.json с Google Sheets (безопасное обновление)"""
        if not self.is_initialized:
            return False
        
        try:
            # Получаем текущие чаты из локального файла
            current_chats = []
            try:
                with open(target_file, "r") as f:
                    current_chats = json.load(f)
                    if not isinstance(current_chats, list):
                        current_chats = []
            except (FileNotFoundError, json.JSONDecodeError):
                current_chats = []
            
            # Получаем чаты из Google Sheets
            sheets_chats = self.get_subscribed_chats()
            
            if not sheets_chats:
                logger.warning("No chats in Google Sheets, keeping current local file")
                return True
            
            # Сравниваем и обновляем только если есть изменения
            current_set = set(current_chats)
            sheets_set = set(sheets_chats)
            
            if current_set != sheets_set:
                # Есть изменения - обновляем файл
                with open(target_file, "w") as f:
                    json.dump(sheets_chats, f)
                
                added = sheets_set - current_set
                removed = current_set - sheets_set
                
                logger.info(f"🔄 Synced subscribed chats: +{len(added)} -{len(removed)} (total: {len(sheets_chats)})")
                if added:
                    logger.info(f"  Added chats: {list(added)}")
                if removed:
                    logger.info(f"  Removed chats: {list(removed)}")
            else:
                logger.info(f"✅ Subscribed chats already in sync ({len(sheets_chats)} chats)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing subscribed chats: {e}")
            return False

    def log_send_history(self, utc_time: str, moscow_time: str, reminder_id: str, 
                        chat_id: str, status: str, error: str = "", text_preview: str = ""):
        """Детальное логирование истории отправки напоминаний"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Send_History')
            
            row = [
                utc_time,
                moscow_time,
                reminder_id,
                chat_id,
                status,
                error or '',
                text_preview[:50] + '...' if len(text_preview) > 50 else text_preview
            ]
            
            worksheet.append_row(row)
            logger.debug(f"Logged send history: {reminder_id} -> {chat_id} ({status})")
            
        except Exception as e:
            logger.error(f"Error logging send history: {e}")
    
    def log_operation(self, timestamp: str, action: str, user_id: str, username: str,
                     chat_id: int, details: str, reminder_id: str = ""):
        """Общее логирование операций системы"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Operation_Logs')
            
            row = [
                timestamp,
                timestamp,  # Может быть изменено на UTC если нужно
                action,
                user_id,
                username or 'Unknown',
                chat_id,
                details,
                reminder_id or ''
            ]
            
            worksheet.append_row(row)
            logger.debug(f"Logged operation: {action} by {username}")
            
        except Exception as e:
            logger.error(f"Error logging operation: {e}")
    
    def sync_subscribed_chats_to_sheets(self, chat_ids: List[int]):
        """Синхронизация локального списка чатов в Google Sheets"""
        if not self.is_initialized:
            return False
        
        try:
            # Обновляем информацию о чатах в Chat_Stats
            # Этот метод уже обновляет Google Sheets через update_chat_stats
            # при вызове subscribe_chat, поэтому здесь просто логируем
            
            logger.info(f"📊 Local chats list synced to Google Sheets: {len(chat_ids)} chats")
            
            # Дополнительно можем залогировать операцию синхронизации
            moscow_time = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')
            self.log_operation(
                timestamp=moscow_time,
                action="SYNC_CHATS",
                user_id="SYSTEM",
                username="AutoSync",
                chat_id=0,
                details=f"Synchronized {len(chat_ids)} chats to Google Sheets",
                reminder_id=""
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing chats to sheets: {e}")
            return False
    
    # ========== ФУНКЦИИ ДЛЯ РАБОТЫ С ГОЛОСОВАНИЯМИ ==========
    
    def log_poll_action(self, action: str, user_id: int, username: str, 
                       chat_id: int, details: str, poll_id: int = None):
        """Логирование действий с голосованиями с обработкой rate limiting"""
        if not self.is_initialized:
            return
        
        def _log_operation():
            worksheet = self.spreadsheet.worksheet('Operation_Logs')
            
            # Московское время
            moscow_time = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')
            
            row = [
                moscow_time,
                action,
                user_id,
                username,
                chat_id,
                details,
                poll_id or ''
            ]
            
            worksheet.append_row(row)
            logger.info(f"Logged poll action: {action} by {username}")
        
        try:
            # Используем retry механизм для обработки rate limiting
            handle_rate_limit_with_retry(_log_operation, max_retries=2, base_delay=0.5)
            
        except Exception as e:
            logger.error(f"Error logging poll action: {e}")
            return False
        
        return True
    
    def sync_poll(self, poll: Dict[str, Any], action: str = 'UPDATE'):
        """Синхронизация голосования с Google Sheets с обработкой rate limiting"""
        if not self.is_initialized:
            return
        
        def _sync_operation():
            worksheet = self.spreadsheet.worksheet('Polls')
            
            # Подготавливаем данные для записи
            options_str = '|'.join(poll.get('options', [])) if poll.get('options') else ''
            
            row_data = [
                poll.get('id', ''),
                poll.get('question', ''),
                options_str,
                poll.get('time', ''),
                poll.get('type', ''),
                poll.get('chat_id', ''),
                poll.get('chat_name', ''),
                'Active' if action != 'DELETE' else 'Deleted',
                poll.get('created_at', ''),
                poll.get('username', ''),
                poll.get('last_sent', ''),
                str(poll.get('days_of_week', '')),
                poll.get('allows_multiple_answers', True)
            ]
            
            if action == 'CREATE':
                # Добавляем новую запись
                worksheet.append_row(row_data)
            elif action == 'UPDATE' or action == 'DELETE':
                # Безопасно получаем записи
                try:
                    records = worksheet.get_all_records()
                except Exception as e:
                    logger.warning(f"Could not get records from Polls, sheet may be empty: {e}")
                    records = []
                
                row_to_update = None
                
                for i, record in enumerate(records):
                    if str(record.get('ID', '')) == str(poll.get('id')):
                        row_to_update = i + 2  # +2 потому что индексы с 1 и есть заголовок
                        break
                
                if row_to_update:
                    # Обновляем существующую строку
                    for col, value in enumerate(row_data, 1):
                        worksheet.update_cell(row_to_update, col, value)
                else:
                    # Если не найдена, добавляем новую
                    worksheet.append_row(row_data)
            
            logger.info(f"Synced poll {poll.get('id')} with action {action}")
        
        try:
            # Используем retry механизм для обработки rate limiting с увеличенными параметрами
            handle_rate_limit_with_retry(_sync_operation, max_retries=5, base_delay=2.0)
            
        except Exception as e:
            logger.error(f"Error syncing poll: {e}")
            # Возвращаем False чтобы вызывающий код знал об ошибке
            return False
        
        return True
    
    def log_poll_sent(self, poll_id: int, chat_id: int, status: str, 
                     error: str = None, question_preview: str = ''):
        """Логирование отправленных голосований"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Poll_Send_History')
            now_utc = datetime.now(pytz.UTC)
            now_msk = now_utc.astimezone(MOSCOW_TZ)
            
            row = [
                now_utc.strftime('%Y-%m-%d %H:%M:%S'),
                now_msk.strftime('%Y-%m-%d %H:%M:%S'),
                poll_id,
                chat_id,
                status,
                error or '',
                question_preview[:50] + '...' if len(question_preview) > 50 else question_preview
            ]
            
            worksheet.append_row(row)
            logger.info(f"Logged poll sent: {poll_id} to {chat_id}")
            
        except Exception as e:
            logger.error(f"Error logging poll sent: {e}")
    
    def backup_all_polls(self, polls: List[Dict[str, Any]]):
        """Полное резервное копирование всех голосований"""
        if not self.is_initialized:
            return
        
        try:
            worksheet = self.spreadsheet.worksheet('Polls')
            
            # Очищаем лист (кроме заголовков)
            worksheet.clear()
            headers = [
                'ID', 'Question', 'Options', 'Time_MSK', 'Type', 'Chat_ID', 'Chat_Name',
                'Status', 'Created_At', 'Username', 'Last_Sent', 'Days_Of_Week', 'Allow_Multiple_Answers'
            ]
            worksheet.append_row(headers)
            
            # Добавляем все голосования
            for poll in polls:
                options_str = '|'.join(poll.get('options', [])) if poll.get('options') else ''
                row_data = [
                    poll.get('id', ''),
                    poll.get('question', ''),
                    options_str,
                    poll.get('time', ''),
                    poll.get('type', ''),
                    poll.get('chat_id', ''),
                    poll.get('chat_name', ''),
                    'Active',
                    poll.get('created_at', ''),
                    poll.get('username', ''),
                    poll.get('last_sent', ''),
                    str(poll.get('days_of_week', '')),
                    poll.get('allows_multiple_answers', True)
                ]
                worksheet.append_row(row_data)
            
            logger.info(f"Backed up {len(polls)} polls to Google Sheets")
            
        except Exception as e:
            logger.error(f"Error backing up polls: {e}")
    
    def get_max_poll_id(self) -> int:
        """Получение максимального ID голосования из Google Sheets"""
        if not self.is_initialized:
            return 0
        
        try:
            worksheet = self.spreadsheet.worksheet('Polls')
            
            # Безопасно получаем записи
            try:
                records = worksheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get records from Polls, sheet may be empty: {e}")
                return 0
            
            if not records:
                return 0
            
            max_id = 0
            for record in records:
                try:
                    poll_id = int(record.get('ID', '0'))
                    if poll_id > max_id:
                        max_id = poll_id
                except (ValueError, TypeError):
                    continue
            
            logger.debug(f"Max poll ID from Google Sheets: {max_id}")
            return max_id
            
        except Exception as e:
            logger.error(f"Error getting max poll ID from Google Sheets: {e}")
            return 0
    
    def restore_polls_from_sheets(self, target_file="polls.json"):
        """Восстановление активных голосований из Google Sheets"""
        if not self.is_initialized:
            logger.warning("Google Sheets not available for polls restoration")
            return False, "Google Sheets не инициализирован"
        
        try:
            worksheet = self.spreadsheet.worksheet('Polls')
            
            # Безопасно получаем записи
            try:
                records = worksheet.get_all_records()
            except Exception as e:
                logger.warning(f"Could not get records from Polls, sheet may be empty: {e}")
                return False, "Не удалось получить данные из Google Sheets (лист может быть пустым)"
            
            if not records:
                logger.warning("No records found in Polls sheet")
                return False, "В листе Polls нет записей"
            
            # Фильтруем только активные голосования
            active_polls = []
            seen_ids = set()  # Отслеживаем уже обработанные ID
            
            for record in records:
                try:
                    # Проверяем статус
                    status = record.get('Status', '').strip()
                    if status.lower() != 'active':
                        logger.debug(f"Skipping poll {record.get('ID')} with status: {status}")
                        continue
                    
                    # Проверяем уникальность ID
                    poll_id = str(record.get('ID', '')).strip()
                    if not poll_id or poll_id in seen_ids:
                        if not poll_id:
                            logger.warning(f"Skipping poll with empty ID: {record}")
                        else:
                            logger.warning(f"Skipping duplicate poll ID: {poll_id}")
                        continue
                    
                    seen_ids.add(poll_id)
                    
                    # Конвертируем в формат бота
                    poll_type = record.get('Type', '').strip().lower()
                    options_str = record.get('Options', '')
                    options = options_str.split('|') if options_str else []
                    
                    if poll_type == 'once':
                        # Разовое голосование
                        restored_poll = {
                            "id": poll_id,
                            "type": "once",
                            "datetime": record.get('Time_MSK', ''),
                            "question": record.get('Question', ''),
                            "options": options,
                            "allows_multiple_answers": record.get('Allow_Multiple_Answers', True),
                            "chat_id": record.get('Chat_ID', ''),
                            "chat_name": record.get('Chat_Name', ''),
                            "created_at": record.get('Created_At', ''),
                            "username": record.get('Username', ''),
                            "last_sent": record.get('Last_Sent', '')
                        }
                        
                    elif poll_type == 'daily':
                        # Ежедневное голосование
                        restored_poll = {
                            "id": poll_id,
                            "type": "daily",
                            "time": record.get('Time_MSK', ''),
                            "question": record.get('Question', ''),
                            "options": options,
                            "allows_multiple_answers": record.get('Allow_Multiple_Answers', True),
                            "chat_id": record.get('Chat_ID', ''),
                            "chat_name": record.get('Chat_Name', ''),
                            "created_at": record.get('Created_At', ''),
                            "username": record.get('Username', ''),
                            "last_sent": record.get('Last_Sent', '')
                        }
                        
                    elif poll_type == 'weekly':
                        # Еженедельное голосование
                        days_of_week = record.get('Days_Of_Week', '').strip()
                        time_parts = record.get('Time_MSK', '').strip().split()
                        
                        if len(time_parts) >= 2:
                            # Формат: "понедельник 10:00"
                            day_name = time_parts[0].lower()
                            time_str = time_parts[1]
                        else:
                            # Используем Days_Of_Week и Time_MSK отдельно
                            day_name = days_of_week.lower() if days_of_week else 'понедельник'
                            time_str = record.get('Time_MSK', '10:00')
                        
                        restored_poll = {
                            "id": poll_id,
                            "type": "weekly",
                            "day": day_name,
                            "time": time_str,
                            "question": record.get('Question', ''),
                            "options": options,
                            "allows_multiple_answers": record.get('Allow_Multiple_Answers', True),
                            "chat_id": record.get('Chat_ID', ''),
                            "chat_name": record.get('Chat_Name', ''),
                            "created_at": record.get('Created_At', ''),
                            "username": record.get('Username', ''),
                            "last_sent": record.get('Last_Sent', ''),
                            "days_of_week": day_name
                        }
                        
                    else:
                        logger.warning(f"Unknown poll type: {poll_type} for ID {record.get('ID')}")
                        continue
                    
                    # Валидация обязательных полей
                    if not restored_poll.get('id') or not restored_poll.get('question') or not restored_poll.get('options'):
                        logger.warning(f"Invalid poll data: ID={restored_poll.get('id')}, Question={restored_poll.get('question')}, Options={restored_poll.get('options')}")
                        continue
                    
                    active_polls.append(restored_poll)
                    logger.debug(f"Restored poll: {restored_poll['id']} ({restored_poll['type']})")
                    
                except Exception as e:
                    logger.error(f"Error processing poll record: {e}")
                    continue
            
            if not active_polls:
                logger.warning("No active polls found in Google Sheets")
                return False, "В Google Sheets не найдено активных голосований"
            
            # Дополнительная статистика восстановления
            total_processed = len(records)
            duplicates_skipped = total_processed - len(seen_ids) - len([r for r in records if r.get('Status', '').lower() != 'active'])
            invalid_skipped = len(seen_ids) - len(active_polls)
            
            logger.info(f"📊 Poll restore statistics:")
            logger.info(f"   Total records in Google Sheets: {total_processed}")
            logger.info(f"   Active polls found: {len(active_polls)}")
            logger.info(f"   Duplicates skipped: {duplicates_skipped}")
            logger.info(f"   Invalid records skipped: {invalid_skipped}")
            logger.info(f"   Non-active records skipped: {total_processed - len(seen_ids)}")
            
            # Сохраняем восстановленные голосования
            try:
                import json
                with open(target_file, "w", encoding='utf-8') as f:
                    json.dump(active_polls, f, ensure_ascii=False, indent=2)
                
                logger.info(f"✅ Successfully restored {len(active_polls)} active polls from Google Sheets to {target_file}")
                logger.info(f"🔄 File completely overwritten - no duplicates possible")
                
                # Логируем операцию восстановления
                moscow_time = datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M:%S')
                self.log_operation(
                    timestamp=moscow_time,
                    action="RESTORE_POLLS",
                    user_id="SYSTEM",
                    username="AutoRestore",
                    chat_id=0,
                    details=f"Restored {len(active_polls)} active polls from Google Sheets (duplicates: {duplicates_skipped}, invalid: {invalid_skipped})",
                    reminder_id=""
                )
                
                return True, f"Успешно восстановлено {len(active_polls)} активных голосований (без дубликатов)"
                
            except Exception as e:
                logger.error(f"Error saving restored polls to {target_file}: {e}")
                return False, f"Ошибка сохранения в файл: {e}"
                
        except Exception as e:
            logger.error(f"Error restoring polls from Google Sheets: {e}")
            return False, f"Ошибка восстановления из Google Sheets: {e}"


# Глобальный экземпляр
sheets_manager = SheetsManager()
