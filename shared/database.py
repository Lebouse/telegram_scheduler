# shared/database.py
import sqlite3
import threading
import datetime
import logging
import os
from contextlib import contextmanager
from typing import Optional, List, Tuple, Any

from config import DATABASE_PATH, TIMEZONE

logger = logging.getLogger(__name__)

# Глобальный lock для SQLite (на случай многопоточности)
_db_lock = threading.RLock()

def ensure_db_directory():
    """
    Гарантирует существование директории для базы данных.
    Создаёт директорию, если её нет, и проверяет права на запись.
    """
    db_dir = os.path.dirname(DATABASE_PATH)
    
    # Создаём директорию если не существует
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"✅ Создана директория для БД: {db_dir}")
        except Exception as e:
            logger.error(f"❌ Ошибка создания директории {db_dir}: {e}")
            raise
    
    # Проверяем права на запись
    if db_dir:
        test_file = os.path.join(db_dir, "test_write.tmp")
        try:
            with open(test_file, "w") as f:
                f.write("test")
            os.remove(test_file)
            logger.info(f"✅ Права на запись в {db_dir} подтверждены")
        except Exception as e:
            logger.error(f"❌ Ошибка записи в {db_dir}: {e}")
            # Попробуем изменить права на директорию
            try:
                os.chmod(db_dir, 0o755)
                logger.info(f"🔄 Права на {db_dir} изменены на 755")
            except Exception as chmod_e:
                logger.error(f"❌ Не удалось изменить права: {chmod_e}")
                raise

def init_db():
    """
    Инициализирует базу данных и создаёт таблицу при необходимости.
    Вызывает ensure_db_directory() для гарантии существования путей.
    """
    ensure_db_directory()
    
    try:
        with get_db_connection() as conn:
            # Включаем WAL для улучшения concurrency
            conn.execute('PRAGMA journal_mode=WAL;')
            conn.execute('PRAGMA foreign_keys = ON;')
            
            # Создаём таблицу
            conn.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    text TEXT,
                    photo_file_id TEXT,
                    document_file_id TEXT,
                    caption TEXT,
                    publish_at TEXT NOT NULL,
                    original_publish_at TEXT NOT NULL,
                    recurrence TEXT NOT NULL DEFAULT 'once',
                    pin BOOLEAN NOT NULL DEFAULT 0,
                    notify BOOLEAN NOT NULL DEFAULT 1,
                    delete_after_days INTEGER,
                    active BOOLEAN NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    max_end_date TEXT,
                    task_hash TEXT
                )
            ''')
            conn.commit()
            logger.info("✅ База данных инициализирована")
    except Exception as e:
        logger.critical(f"❌ Критическая ошибка при инициализации БД: {e}")
        raise

@contextmanager
def get_db_connection():
    """
    Контекстный менеджер для безопасного подключения к SQLite.
    Обеспечивает потокобезопасность через глобальный lock.
    """
    with _db_lock:
        conn = None
        try:
            conn = sqlite3.connect(
                DATABASE_PATH,
                check_same_thread=False,
                timeout=30  # Увеличиваем таймаут для надёжности
            )
            # Настраиваем параметры соединения
            conn.execute('PRAGMA busy_timeout = 30000;')  # 30 секунд ожидания
            conn.execute('PRAGMA synchronous = NORMAL;')  # Баланс скорости и надёжности
            conn.row_factory = sqlite3.Row  # Возвращаем результаты как словари
            yield conn
        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as close_e:
                    logger.warning(f"⚠️ Ошибка закрытия соединения: {close_e}")

def add_scheduled_message(data: dict) -> int:
    """
    Добавляет новую запланированную задачу.
    Автоматически добавляет недостающие столбцы при первой миграции.
    
    Args:
        data: Словарь с параметрами задачи
        
    Returns:
        ID созданной задачи
        
    Raises:
        ValueError: Если задача с таким хэшем уже существует
        Exception: При других ошибках
    """
    # Подготавливаем данные
    created_at = datetime.datetime.now(TIMEZONE).replace(tzinfo=None).isoformat()
    max_end_date = (datetime.datetime.now(TIMEZONE).replace(tzinfo=None) + 
                   datetime.timedelta(days=365)).isoformat()
    
    # Формируем SQL-запрос
    columns = [
        'chat_id', 'text', 'photo_file_id', 'document_file_id', 'caption',
        'publish_at', 'original_publish_at', 'recurrence', 'pin', 'notify',
        'delete_after_days', 'active', 'created_at', 'max_end_date'
    ]
    placeholders = ','.join(['?'] * len(columns))
    values = [
        data['chat_id'], data.get('text'), data.get('photo_file_id'), 
        data.get('document_file_id'), data.get('caption'), data['publish_at'],
        data['publish_at'], data['recurrence'], 
        int(data.get('pin', False)), int(data.get('notify', True)),
        data.get('delete_after_days'), 1,  # active = 1 (true)
        created_at, max_end_date
    ]
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Пытаемся вставить данные
            cursor.execute(f'''
                INSERT INTO scheduled_messages (
                    {','.join(columns)}
                ) VALUES ({placeholders})
            ''', values)
            
            msg_id = cursor.lastrowid
            conn.commit()
            logger.info(f"✅ Создана задача ID={msg_id} для чата {data['chat_id']}")
            return msg_id
            
    except sqlite3.OperationalError as e:
        if "no such column" in str(e):
            logger.warning(f"🔄 Обнаружена устаревшая схема БД: {e}. Выполняем миграцию...")
            _migrate_database()
            return add_scheduled_message(data)
        else:
            logger.error(f"❌ Ошибка при добавлении задачи: {e}")
            raise
    except sqlite3.IntegrityError as e:
        logger.error(f"❌ Ошибка целостности данных: {e}")
        raise
    except Exception as e:
        logger.exception(f"❌ Неожиданная ошибка при добавлении задачи: {e}")
        raise

def _migrate_database():
    """Выполняет миграцию базы данных при изменении схемы."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Проверяем существование столбцов и добавляем при необходимости
            cursor.execute("PRAGMA table_info(scheduled_messages)")
            columns = {row[1] for row in cursor.fetchall()}
            
            # Добавляем недостающие столбцы
            if 'created_at' not in columns:
                cursor.execute("ALTER TABLE scheduled_messages ADD COLUMN created_at TEXT DEFAULT (datetime('now'))")
                logger.info("➕ Добавлен столбец created_at")
                
            if 'max_end_date' not in columns:
                cursor.execute("ALTER TABLE scheduled_messages ADD COLUMN max_end_date TEXT")
                logger.info("➕ Добавлен столбец max_end_date")
                
            if 'task_hash' not in columns:
                cursor.execute("ALTER TABLE scheduled_messages ADD COLUMN task_hash TEXT")
                logger.info("➕ Добавлен столбец task_hash")
                
            conn.commit()
            logger.info("✅ Миграция базы данных завершена")
    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")
        raise

def get_all_active_messages() -> List[sqlite3.Row]:
    """
    Возвращает все активные задачи, отсортированные по времени публикации.
    
    Returns:
        Список строк из базы данных
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    id, chat_id, text, photo_file_id, document_file_id, caption,
                    publish_at, original_publish_at, recurrence, pin, notify,
                    delete_after_days, active, created_at, max_end_date, task_hash
                FROM scheduled_messages
                WHERE active = 1
                ORDER BY publish_at ASC
            """)
            rows = cursor.fetchall()
            logger.debug(f"📥 Загружено {len(rows)} активных задач")
            return rows
    except Exception as e:
        logger.error(f"❌ Ошибка получения активных задач: {e}")
        return []

def get_message_by_id(msg_id: int) -> Optional[sqlite3.Row]:
    """
    Возвращает задачу по ID.
    
    Args:
        msg_id: ID задачи
        
    Returns:
        Строка из базы данных или None
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    id, chat_id, text, photo_file_id, document_file_id, caption,
                    publish_at, original_publish_at, recurrence, pin, notify,
                    delete_after_days, active, created_at, max_end_date, task_hash
                FROM scheduled_messages
                WHERE id = ?
            """, (msg_id,))
            return cursor.fetchone()
    except Exception as e:
        logger.error(f"❌ Ошибка получения задачи {msg_id}: {e}")
        return None

def deactivate_message(msg_id: int) -> bool:
    """
    Деактивирует задачу (логическое удаление).
    
    Args:
        msg_id: ID задачи
        
    Returns:
        True если задача была деактивирована, False если не найдена
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scheduled_messages 
                SET active = 0 
                WHERE id = ?
            """, (msg_id,))
            
            if cursor.rowcount == 0:
                logger.warning(f"⚠️ Задача {msg_id} не найдена для деактивации")
                return False
                
            conn.commit()
            logger.info(f"⏹️ Задача {msg_id} деактивирована")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка деактивации задачи {msg_id}: {e}")
        return False

def update_scheduled_message(
    msg_id: int,
    chat_id: int,
    text: Optional[str],
    photo_file_id: Optional[str],
    document_file_id: Optional[str],
    caption: Optional[str],
    publish_at: str,
    recurrence: str,
    pin: bool,
    notify: bool,
    delete_after_days: Optional[int]
) -> bool:
    """
    Обновляет существующую задачу.
    
    Args:
        msg_id: ID задачи
        chat_id: ID чата
        text: Текст сообщения
        photo_file_id: ID фото
        document_file_id: ID документа
        caption: Подпись к медиа
        publish_at: Время публикации в ISO формате
        recurrence: Периодичность
        pin: Закреплять ли сообщение
        notify: Отправлять ли уведомление
        delete_after_days: Удалять через N дней
        
    Returns:
        True если задача обновлена, False если не найдена
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Обновляем max_end_date (сбрасываем срок действия)
            max_end_date = (datetime.datetime.now(TIMEZONE).replace(tzinfo=None) + 
                           datetime.timedelta(days=365)).isoformat()
            
            cursor.execute('''
                UPDATE scheduled_messages SET
                    chat_id = ?, text = ?, photo_file_id = ?, document_file_id = ?,
                    caption = ?, publish_at = ?, recurrence = ?, pin = ?, notify = ?,
                    delete_after_days = ?, max_end_date = ?
                WHERE id = ?
            ''', (
                chat_id, text, photo_file_id, document_file_id,
                caption, publish_at, recurrence, int(pin), int(notify),
                delete_after_days, max_end_date, msg_id
            ))
            
            if cursor.rowcount == 0:
                logger.warning(f"⚠️ Задача {msg_id} не найдена для обновления")
                return False
                
            conn.commit()
            logger.info(f"✏️ Задача {msg_id} обновлена")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка обновления задачи {msg_id}: {e}")
        return False

def update_next_publish_time(msg_id: int, next_time_iso: str) -> bool:
    """
    Обновляет время следующей публикации для задачи.
    
    Args:
        msg_id: ID задачи
        next_time_iso: Новое время в ISO формате
        
    Returns:
        True если обновлено успешно
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE scheduled_messages 
                SET publish_at = ? 
                WHERE id = ? AND active = 1
            """, (next_time_iso, msg_id))
            
            updated = cursor.rowcount > 0
            if updated:
                conn.commit()
                logger.debug(f"⏰ Задача {msg_id}: следующая публикация назначена на {next_time_iso}")
            else:
                logger.debug(f"ℹ️ Задача {msg_id} неактивна или не найдена, обновление пропущено")
                
            return updated
    except Exception as e:
        logger.error(f"❌ Ошибка обновления времени публикации для задачи {msg_id}: {e}")
        return False

def cleanup_old_tasks(max_age_days: int = 30) -> int:
    """
    Удаляет неактивные задачи старше max_age_days.
    
    Args:
        max_age_days: Максимальный возраст задач в днях
        
    Returns:
        Количество удалённых записей
    """
    cutoff = datetime.datetime.now(TIMEZONE).replace(tzinfo=None) - datetime.timedelta(days=max_age_days)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM scheduled_messages 
                WHERE active = 0 AND created_at < ?
            """, (cutoff.isoformat(),))
            
            deleted = cursor.rowcount
            if deleted > 0:
                conn.commit()
                logger.info(f"🧹 Очистка: удалено {deleted} старых задач (старше {max_age_days} дней)")
            return deleted
    except Exception as e:
        logger.error(f"❌ Ошибка очистки старых задач: {e}")
        return 0

def get_pending_messages() -> List[sqlite3.Row]:
    """
    Возвращает задачи, которые нужно опубликовать сейчас или в прошлом.
    
    Returns:
        Список задач для публикации
    """
    now = datetime.datetime.utcnow().isoformat()
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    id, chat_id, text, photo_file_id, document_file_id, caption,
                    publish_at, recurrence, pin, notify, delete_after_days
                FROM scheduled_messages
                WHERE active = 1 AND publish_at <= ?
                ORDER BY publish_at ASC
            """, (now,))
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"❌ Ошибка получения ожидающих задач: {e}")
        return []

def health_check() -> dict:
    """
    Проверяет здоровье базы данных.
    
    Returns:
        Словарь с информацией о состоянии БД
    """
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            integrity = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM scheduled_messages WHERE active = 1")
            active_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM scheduled_messages WHERE active = 0")
            inactive_count = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA page_count")
            page_count = cursor.fetchone()[0]
            
            cursor.execute("PRAGMA page_size")
            page_size = cursor.fetchone()[0]
            
            db_size = page_count * page_size
            
            return {
                "status": "ok",
                "integrity": integrity,
                "active_tasks": active_count,
                "inactive_tasks": inactive_count,
                "db_size_bytes": db_size,
                "db_path": DATABASE_PATH
            }
    except Exception as e:
        logger.error(f"❌ Ошибка проверки здоровья БД: {e}")
        return {
            "status": "error",
            "error": str(e),
            "db_path": DATABASE_PATH
        }
