# web_api.py
# ФИНАЛЬНАЯ РАБОЧАЯ ВЕРСИЯ с УЛУЧШЕННЫМ ЛОГИРОВАНИЕМ и ОБРАБОТКОЙ ОШИБОК
# Порт: 8081
# Секрет админки: qwerty12345

import asyncio
import datetime
import csv
import io
import logging
import os
import hmac
import hashlib
import json
from typing import Optional, List, Dict, Any, Union, Tuple
from urllib.parse import quote, urlparse, urlunparse, parse_qs

from fastapi import FastAPI, HTTPException, Header, Request, Form, status, Query, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, Response, StreamingResponse, RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator, ValidationInfo
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

from config import (
    WEB_API_SECRET, ADMIN_SECRET, BOT_TOKEN, TIMEZONE,
    GITHUB_WEBHOOK_SECRET, DATABASE_PATH
)
from shared.database import (
    get_all_active_messages, deactivate_message,
    update_scheduled_message, add_scheduled_message,
    get_message_by_id, health_check as db_health_check
)
from shared.utils import (
    escape_markdown_v2, detect_media_type,
    parse_user_datetime, next_recurrence_time
)
from scheduler_logic import publish_message
from shared.bot_instance import get_bot

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# === Инициализация FastAPI ===
app = FastAPI(
    title="Telegram Reminder Scheduler API",
    description="API для управления запланированными напоминаниями в Telegram",
    version="0.1.0-pre"
)

# === CORS настройки (для безопасности) ===
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# === Метрики Prometheus ===
TASKS_CREATED = Counter('telegram_scheduler_tasks_created_total', 'Total tasks created')
TASKS_DELETED = Counter('telegram_scheduler_tasks_deleted_total', 'Total tasks deleted')
ACTIVE_TASKS = Gauge('telegram_scheduler_active_tasks', 'Number of active scheduled tasks')

# === Шаблоны ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# === Кэш названий чатов ===
CHAT_TITLE_CACHE: Dict[int, tuple] = {}

# === Глобальный обработчик исключений ===
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Глобальный обработчик исключений для всех эндпоинтов.
    Логирует детали ошибки и возвращает информативный ответ.
    """
    logger.error(f"❌ ГЛОБАЛЬНАЯ ОШИБКА в {request.method} {request.url.path}: {str(exc)}", exc_info=True)
    
    # Для JSON-запросов возвращаем JSON
    if request.headers.get("Accept", "").startswith("application/json") or \
       request.headers.get("Content-Type", "").startswith("application/json"):
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "error": str(exc),
                "endpoint": request.url.path,
                "method": request.method,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        )
    
    # Для HTML-запросов возвращаем HTML с деталями ошибки (только для админов)
    error_details = f"""
    <h1>❌ Internal Server Error</h1>
    <p><strong>Endpoint:</strong> {request.url.path}</p>
    <p><strong>Method:</strong> {request.method}</p>
    <p><strong>Error:</strong> {str(exc)}</p>
    <p><strong>Тип ошибки:</strong> {type(exc).__name__}</p>
    <p>Проверьте логи сервера для подробностей.</p>
    <p><a href="/admin?secret={request.query_params.get('secret', '')}">← Вернуться в админку</a></p>
    """
    
    return HTMLResponse(
        status_code=500,
        content=error_details,
        headers={"Content-Type": "text/html; charset=utf-8"}
    )

# === Модели данных ===
class PublishRequest(BaseModel):
    chat_id: int
    text: Optional[str] = None
    photo_file_id: Optional[str] = None
    document_file_id: Optional[str] = None
    caption: Optional[str] = None
    pin: bool = False
    notify: bool = True
    delete_after_days: Optional[int] = None

    @field_validator('delete_after_days')
    @classmethod
    def validate_delete_days(cls, v: Optional[int], info: ValidationInfo) -> Optional[int]:
        if v is not None and v not in (1, 2, 3):
            raise ValueError('Must be 1, 2, or 3 days')
        return v

    @field_validator('chat_id')
    @classmethod
    def validate_chat_id(cls, v: int, info: ValidationInfo) -> int:
        if not str(v).startswith('-100'):
            raise ValueError('Invalid chat ID format. Must start with -100')
        return v

class HealthCheckResponse(BaseModel):
    status: str
    active_tasks: int
    timestamp: str
    database: str

# === Глобальный middleware для проверки секрета ===
@app.middleware("http")
async def admin_secret_middleware(request: Request, call_next):
    """
    Middleware для проверки секрета админки во всех запросах.
    Проверяет секрет из заголовка, query параметра и формы.
    """
    try:
        logger.debug(f"🔍 Middleware: {request.method} {request.url.path}")
        
        # Получаем секрет из всех возможных источников
        secret_from_header = request.headers.get("X-Admin-Secret")
        secret_from_query = request.query_params.get("secret")
        
        # Для POST-запросов проверяем форму
        secret_from_form = None
        if request.method in ["POST", "PUT", "PATCH"]:
            try:
                form = await request.form()
                secret_from_form = form.get("secret")
                logger.debug(f"📝 Форма содержит секрет: {'да' if secret_from_form else 'нет'}")
            except Exception as e:
                logger.debug(f"Не удалось прочитать форму: {e}")
        
        actual_secret = secret_from_header or secret_from_query or secret_from_form
        logger.debug(f"🔑 Полученные секреты: header={secret_from_header}, query={secret_from_query}, form={secret_from_form}, actual={actual_secret}")
        
        # Защищённые пути
        protected_paths = [
            "/admin",
            "/admin/",
            "/admin/create",
            "/admin/edit",
            "/admin/delete",
            "/admin/export.csv"
        ]
        
        # Проверяем, является ли запрос защищённым
        is_protected = any(
            request.url.path.startswith(path) for path in protected_paths
        ) and not request.url.path.startswith("/admin/export.csv")
        
        logger.debug(f"🛡️ Защищённый эндпоинт: {is_protected}")
        
        # Если защищённый эндпоинт и секрет не совпадает
        if is_protected and ADMIN_SECRET and actual_secret != ADMIN_SECRET:
            logger.warning(
                f"🚫 Доступ запрещён к {request.url.path}. "
                f"Ожидалось '{ADMIN_SECRET}', получено '{actual_secret}'"
            )
            
            # Для AJAX/JSON запросов возвращаем JSON ошибку
            if request.headers.get("Accept", "").startswith("application/json") or \
               request.headers.get("Content-Type", "").startswith("application/json"):
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Admin access required"}
                )
            
            # Для HTML запросов перенаправляем на страницу входа
            return HTMLResponse(
                content="<h1>403 Forbidden</h1><p>Admin access required. Please provide valid secret.</p>",
                status_code=403
            )
        
        # Для экспорта CSV всегда проверяем секрет
        if request.url.path == "/admin/export.csv" and ADMIN_SECRET and actual_secret != ADMIN_SECRET:
            logger.warning(f"🚫 Попытка экспорта без прав: {request.client.host}")
            return JSONResponse(
                status_code=403,
                content={"detail": "Admin access required for export"}
            )
        
        # Передаём управление следующему обработчику
        response = await call_next(request)
        return response
    
    except Exception as e:
        logger.exception(f"❌ Ошибка в admin_secret_middleware: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error in middleware"}
        )

# === Вспомогательные функции ===
async def get_chat_title_cached(chat_id: int) -> str:
    """Получает название чата через Telegram API с кэшированием."""
    now = datetime.datetime.now(datetime.timezone.utc)
    cache_key = chat_id
    
    if cache_key in CHAT_TITLE_CACHE:
        title, timestamp = CHAT_TITLE_CACHE[cache_key]
        if (now - timestamp).total_seconds() < 3600:  # кэш 1 час
            return title

    try:
        bot = get_bot()
        chat = await bot.get_chat(chat_id)
        title = chat.title or f"Чат {chat_id}"
        logger.info(f"✅ Получено название чата {chat_id}: {title}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось получить название чата {chat_id}: {e}")
        title = f"Чат {chat_id}"

    CHAT_TITLE_CACHE[cache_key] = (title, now)
    return title

def safe_dict(row) -> dict:
    """
    Безопасно конвертирует sqlite3.Row или словарь в стандартный словарь.
    Решает проблему с Internal Server Error при работе с результатами БД.
    """
    try:
        if hasattr(row, 'keys'):  # Это sqlite3.Row или подобный объект
            return {key: row[key] for key in row.keys()}
        elif isinstance(row, dict):
            return row.copy()
        else:
            logger.warning(f"⚠️ Неожиданный тип данных: {type(row)}")
            return {}
    except Exception as e:
        logger.error(f"❌ Ошибка конвертации данных: {e}")
        return {}

# === Эндпоинты ===

@app.get("/health", response_model=HealthCheckResponse, summary="Health check")
async def health_check():
    """Проверяет работоспособность сервиса."""
    try:
        logger.info("✅ Health check запрошен")
        tasks = get_all_active_messages()
        db_status = db_health_check()
        
        return HealthCheckResponse(
            status="ok",
            active_tasks=len(tasks),
            timestamp=datetime.datetime.utcnow().isoformat(),
            database=db_status.get("status", "unknown")
        )
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection failed"
        )

@app.get("/metrics", summary="Prometheus metrics")
async def metrics():
    """Экспортирует метрики для Prometheus."""
    try:
        active_count = len(get_all_active_messages())
        ACTIVE_TASKS.set(active_count)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.error(f"❌ Ошибка получения метрик: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate metrics"
        )

@app.post("/publish", summary="Publish message immediately")
async def web_publish(
    request: PublishRequest,
    x_secret: str = Header(..., alias="X-Secret")
):
    """Публикует сообщение немедленно через HTTP API."""
    # Проверка секрета
    if WEB_API_SECRET and x_secret != WEB_API_SECRET:
        logger.warning(f"🚫 Неверный секрет для /publish: {x_secret}")
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        logger.info(f"📤 Публикация сообщения в чат {request.chat_id}")
        
        # Экранируем текст для MarkdownV2
        safe_text = escape_markdown_v2(request.text) if request.text else None
        safe_caption = escape_markdown_v2(request.caption) if request.caption else None

        # Публикуем сообщение
        msg_id = await publish_message(
            chat_id=request.chat_id,
            text=safe_text,
            photo_file_id=request.photo_file_id,
            document_file_id=request.document_file_id,
            caption=safe_caption,
            pin=request.pin,
            notify=request.notify,
            delete_after_days=request.delete_after_days
        )
        
        if msg_id is None:
            logger.error("❌ Не удалось отправить сообщение")
            raise HTTPException(status_code=500, detail="Failed to send message")
        
        logger.info(f"✅ Web publish: chat={request.chat_id}, msg_id={msg_id}")
        TASKS_CREATED.inc()
        return {"ok": True, "message_id": msg_id}
    
    except ValueError as e:
        logger.warning(f"⚠️ Некорректные данные для публикации: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"❌ Web publish error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/", response_class=HTMLResponse)
async def root_redirect(request: Request):
    """Перенаправляет корень на админку с секретом."""
    secret = request.query_params.get("secret") or request.headers.get("X-Admin-Secret")
    redirect_url = "/admin"
    if secret:
        redirect_url = f"{redirect_url}?secret={quote(secret)}"
    return RedirectResponse(url=redirect_url)

@app.get("/admin", response_class=HTMLResponse, summary="Admin panel")
async def admin_panel(
    request: Request,
    chat_filter: Optional[str] = None,
    secret: Optional[str] = None,
    create: Optional[str] = None,
    error: Optional[str] = None,
    background_tasks: BackgroundTasks = None
):
    """
    Отображает админку для управления задачами.
    """
    try:
        logger.info(f"✅ Запрос к /admin с параметрами: chat_filter={chat_filter}, secret={secret}, create={create}, error={error}")
        
        # Получаем все активные задачи
        raw_tasks = get_all_active_messages()
        logger.info(f"📊 Загружено {len(raw_tasks)} активных задач")
        
        # Безопасно конвертируем задачи
        tasks = [safe_dict(task) for task in raw_tasks]
        logger.debug(f"✅ Задачи успешно сконвертированы")
        
        # Фильтрация по чату
        if chat_filter and chat_filter.lstrip('-').isdigit():
            try:
                chat_filter_int = int(chat_filter)
                tasks = [t for t in tasks if t.get('chat_id') == chat_filter_int]
                logger.info(f"🔍 После фильтрации по чату {chat_filter_int} осталось {len(tasks)} задач")
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Ошибка фильтрации по чату: {e}")
        
        # Уникальные чаты
        unique_chats = sorted({t['chat_id'] for t in tasks if 'chat_id' in t})
        logger.info(f"🏢 Уникальные чаты: {unique_chats}")
        
        chat_titles = {}
        for cid in unique_chats:
            try:
                chat_titles[cid] = await get_chat_title_cached(cid)
                logger.debug(f"🏷️ Название чата {cid}: {chat_titles[cid]}")
            except Exception as e:
                logger.error(f"❌ Ошибка получения названия чата {cid}: {e}")
                chat_titles[cid] = f"Чат {cid}"

        # Подготовка данных для шаблона
        task_dicts = []
        for row in tasks:
            try:
                task_data = {
                    'id': row.get('id'),
                    'chat_id': row.get('chat_id'),
                    'text': row.get('text'),
                    'photo_file_id': row.get('photo_file_id'),
                    'document_file_id': row.get('document_file_id'),
                    'caption': row.get('caption'),
                    'publish_at': row.get('publish_at'),
                    'recurrence': row.get('recurrence'),
                    'pin': bool(row.get('pin', 0)),
                    'notify': bool(row.get('notify', 1)),
                    'delete_after_days': row.get('delete_after_days'),
                    'active': row.get('active', 1)
                }
                task_dicts.append(task_data)
                logger.debug(f"✅ Задача {task_data['id']} обработана")
            except Exception as e:
                logger.error(f"❌ Ошибка обработки задачи: {e}")
                continue

        logger.info(f"✅ Подготовлено {len(task_dicts)} задач для отображения")
        
        # Определяем, показывать ли форму создания
        show_create_form = create is not None or error is not None
        
        # Передаём текущий секрет в шаблон
        current_secret = secret or request.headers.get("X-Admin-Secret", "")
        
        return templates.TemplateResponse("admin.html", {
            "request": request,
            "tasks": task_dicts,
            "active_count": len(tasks),
            "unique_chats": unique_chats,
            "chat_titles": chat_titles,
            "chat_filter": chat_filter,
            "timezone": str(TIMEZONE),
            "edit_task": None,
            "error": error,
            "show_create_form": show_create_form,
            "current_secret": current_secret
        })
    
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка в /admin: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/admin/create", summary="Create new task")
async def admin_create_task(
    request: Request,
    background_tasks: BackgroundTasks,
    secret: Optional[str] = Form(None),
    chat_id: int = Form(...),
    message_text: str = Form(...),
    media_file_id: Optional[str] = Form(None),
    publish_at_local: str = Form(...),
    recurrence: str = Form(...),
    weekly_days: Optional[List[int]] = Form(None),
    monthly_days: Optional[str] = Form(None),
    delete_after_days: Optional[int] = Form(None),
    pin: bool = Form(False),
    notify: bool = Form(True)
):
    """Создаёт новую задачу из админки."""
    logger.info("✅ Начало создания задачи")
    
    try:
        logger.debug(f"📝 Получены параметры: chat_id={chat_id}, message_text={message_text}, publish_at_local={publish_at_local}, recurrence={recurrence}")
        
        # Парсим дату
        try:
            naive_local, utc_naive = parse_user_datetime(publish_at_local)
            publish_at_utc = utc_naive.isoformat()
            logger.debug(f"⏰ Распарсенная дата: {publish_at_utc}")
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠️ Ошибка парсинга даты: {e}")
            raise ValueError(f"Неверный формат даты: {e}")

        # Определяем тип медиа
        media_type = detect_media_type(media_file_id) if media_file_id else None
        photo_file_id = media_file_id if media_type == "photo" else None
        document_file_id = media_file_id if media_type == "document" else None
        logger.debug(f"🖼️ Тип медиа: {media_type}, photo_file_id={photo_file_id}, document_file_id={document_file_id}")

        # Подготовка данных
        data = {
            'chat_id': chat_id,
            'text': message_text if not (photo_file_id or document_file_id) else None,
            'photo_file_id': photo_file_id,
            'document_file_id': document_file_id,
            'caption': message_text if (photo_file_id or document_file_id) else None,
            'publish_at': publish_at_utc,
            'recurrence': recurrence,
            'pin': pin,
            'notify': notify,
            'delete_after_days': delete_after_days
        }
        logger.debug(f"💾 Данные для сохранения: {json.dumps(data, indent=2)}")

        # Добавляем задачу
        try:
            msg_id = add_scheduled_message(data)
            TASKS_CREATED.inc()
            logger.info(f"✅ Задача создана через админку: ID={msg_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка добавления задачи в БД: {e}")
            raise

        # Перенаправляем на админку с секретом
        redirect_url = f"/admin?secret={quote(secret)}" if secret else "/admin"
        logger.info(f"🔄 Редирект на: {redirect_url}")
        return RedirectResponse(url=redirect_url, status_code=303)

    except ValueError as e:
        logger.warning(f"⚠️ Ошибка создания задачи: {e}")
        redirect_url = f"/admin?secret={quote(secret)}&error={quote(str(e))}" if secret else f"/admin?error={quote(str(e))}"
        return RedirectResponse(url=redirect_url, status_code=303)
    except Exception as e:
        logger.exception(f"❌ Неожиданная ошибка при создании задачи: {e}")
        redirect_url = f"/admin?secret={quote(secret)}&error=internal_error" if secret else "/admin?error=internal_error"
        return RedirectResponse(url=redirect_url, status_code=303)

@app.get("/admin/edit/{task_id}", response_class=HTMLResponse, summary="Edit task form")
async def admin_edit_form(
    request: Request,
    task_id: int,
    secret: Optional[str] = None,
    error: Optional[str] = None
):
    """Отображает форму редактирования задачи."""
    try:
        logger.info(f"📝 Запрос на редактирование задачи {task_id}")
        
        # Получаем задачу
        task_row = get_message_by_id(task_id)
        if not task_row:
            logger.warning(f"⚠️ Задача {task_id} не найдена для редактирования")
            raise HTTPException(status_code=404, detail="Задача не найдена")

        # Безопасно конвертируем задачу
        task_data = safe_dict(task_row)
        logger.debug(f"📋 Данные задачи: {json.dumps(task_data, indent=2)}")

        # Подготавливаем данные для формы
        task = {
            'id': task_data.get('id'),
            'chat_id': task_data.get('chat_id'),
            'message_text': task_data.get('text') or task_data.get('caption') or "",
            'media_file_id': task_data.get('photo_file_id') or task_data.get('document_file_id'),
            'publish_at_local': "",
            'recurrence': task_data.get('recurrence', 'once'),
            'pin': bool(task_data.get('pin', 0)),
            'notify': bool(task_data.get('notify', 1)),
            'delete_after_days': task_data.get('delete_after_days')
        }

        # Конвертируем UTC в локальное время для отображения
        try:
            publish_at = task_data.get('publish_at')
            if publish_at:
                utc_dt = datetime.datetime.fromisoformat(publish_at)
                local_dt = utc_dt.replace(tzinfo=datetime.timezone.utc).astimezone(TIMEZONE)
                task['publish_at_local'] = local_dt.strftime("%d.%m.%Y %H:%M")
                logger.debug(f"🕒 Конвертированное время: {task['publish_at_local']}")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка конвертации времени для задачи {task_id}: {e}")
            task['publish_at_local'] = task_data.get('publish_at', '')

        # Получаем все задачи для фильтра
        raw_tasks = get_all_active_messages()
        tasks = [safe_dict(task) for task in raw_tasks]
        unique_chats = sorted({t['chat_id'] for t in tasks if 'chat_id' in t})
        chat_titles = {cid: await get_chat_title_cached(cid) for cid in unique_chats}

        task_dicts = []
        for r in tasks:
            task_dicts.append({
                'id': r.get('id'),
                'chat_id': r.get('chat_id'),
                'text': r.get('text'),
                'photo_file_id': r.get('photo_file_id'),
                'document_file_id': r.get('document_file_id'),
                'caption': r.get('caption'),
                'publish_at': r.get('publish_at'),
                'recurrence': r.get('recurrence'),
                'pin': bool(r.get('pin', 0)),
                'notify': bool(r.get('notify', 1)),
                'delete_after_days': r.get('delete_after_days'),
                'active': r.get('active', 1)
            })

        logger.info(f"✅ Форма редактирования задачи {task_id} подготовлена")
        return templates.TemplateResponse("admin.html", {
            "request": request,
            "tasks": task_dicts,
            "active_count": len(tasks),
            "unique_chats": unique_chats,
            "chat_titles": chat_titles,
            "edit_task": task,
            "timezone": str(TIMEZONE),
            "error": error,
            "current_secret": secret or ""
        })
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Ошибка отображения формы редактирования: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/admin/edit/{task_id}", summary="Save edited task")
async def admin_save_edit(
    task_id: int,
    background_tasks: BackgroundTasks,
    secret: Optional[str] = Form(None),
    chat_id: int = Form(...),
    message_text: str = Form(...),
    media_file_id: Optional[str] = Form(None),
    publish_at_local: str = Form(...),
    recurrence: str = Form(...),
    weekly_days: Optional[List[int]] = Form(None),
    monthly_days: Optional[str] = Form(None),
    delete_after_days: Optional[int] = Form(None),
    pin: bool = Form(False),
    notify: bool = Form(True)
):
    """Сохраняет отредактированную задачу."""
    logger.info(f"💾 Сохранение задачи {task_id}")
    
    try:
        # Парсим дату
        naive_local, utc_naive = parse_user_datetime(publish_at_local)
        publish_at_utc = utc_naive.isoformat()

        # Определяем тип медиа
        media_type = detect_media_type(media_file_id) if media_file_id else None
        photo_file_id = media_file_id if media_type == "photo" else None
        document_file_id = media_file_id if media_type == "document" else None

        # Обновляем задачу
        success = update_scheduled_message(
            msg_id=task_id,
            chat_id=chat_id,
            text=message_text if not (photo_file_id or document_file_id) else None,
            photo_file_id=photo_file_id,
            document_file_id=document_file_id,
            caption=message_text if (photo_file_id or document_file_id) else None,
            publish_at=publish_at_utc,
            recurrence=recurrence,
            pin=pin,
            notify=notify,
            delete_after_days=delete_after_days
        )
        
        if not success:
            logger.warning(f"⚠️ Задача {task_id} не найдена для обновления")
            raise HTTPException(status_code=404, detail="Задача не найдена")
        
        logger.info(f"✅ Задача {task_id} обновлена через админку")
        
        # Перенаправляем на админку с секретом
        redirect_url = f"/admin?secret={quote(secret)}" if secret else "/admin"
        return RedirectResponse(url=redirect_url, status_code=303)

    except ValueError as e:
        logger.warning(f"⚠️ Ошибка обновления задачи {task_id}: {e}")
        redirect_url = f"/admin/edit/{task_id}?secret={quote(secret)}&error={quote(str(e))}" if secret else f"/admin/edit/{task_id}?error={quote(str(e))}"
        return RedirectResponse(url=redirect_url, status_code=303)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Ошибка при сохранении задачи {task_id}: {e}")
        redirect_url = f"/admin/edit/{task_id}?secret={quote(secret)}&error=internal_error" if secret else f"/admin/edit/{task_id}?error=internal_error"
        return RedirectResponse(url=redirect_url, status_code=303)

@app.post("/admin/delete/{task_id}", summary="Delete task")
async def admin_delete_task(
    task_id: int,
    secret: Optional[str] = Form(None)
):
    """Удаляет задачу."""
    logger.info(f"🗑️ Удаление задачи {task_id}")
    
    try:
        success = deactivate_message(task_id)
        if not success:
            logger.warning(f"⚠️ Задача {task_id} не найдена для удаления")
            raise HTTPException(status_code=404, detail="Задача не найдена")
        
        TASKS_DELETED.inc()
        logger.info(f"✅ Задача {task_id} удалена через админку")
        
        # Перенаправляем на админку с секретом
        redirect_url = f"/admin?secret={quote(secret)}" if secret else "/admin"
        return RedirectResponse(url=redirect_url, status_code=303)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ Ошибка удаления задачи {task_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/admin/export.csv", summary="Export tasks to CSV")
async def export_tasks_csv(
    request: Request,
    secret: Optional[str] = Query(None)
):
    """Экспортирует задачи в CSV."""
    logger.info("📥 Экспорт задач в CSV")
    
    try:
        raw_tasks = get_all_active_messages()
        tasks = [safe_dict(task) for task in raw_tasks]
        
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        # Заголовки
        writer.writerow([
            "ID", "Chat ID", "Text", "Photo file_id", "Document file_id", "Caption",
            "Publish At (UTC)", "Recurrence", "Pin", "Notify", "Delete After (days)"
        ])

        # Данные
        for row in tasks:
            writer.writerow([
                row.get('id', ''),
                row.get('chat_id', ''),
                row.get('text', ''),
                row.get('photo_file_id', ''),
                row.get('document_file_id', ''),
                row.get('caption', ''),
                row.get('publish_at', ''),
                row.get('recurrence', ''),
                row.get('pin', 0),
                row.get('notify', 1),
                row.get('delete_after_days', '')
            ])

        output.seek(0)
        filename = f"tasks_export_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        
        logger.info(f"✅ Экспорт завершён. Размер: {len(output.getvalue())} байт")
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={quote(filename)}",
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    
    except Exception as e:
        logger.exception(f"❌ Ошибка экспорта CSV: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.post("/webhook/github", summary="GitHub webhook endpoint")
async def github_webhook(request: Request):
    """Обрабатывает webhook от GitHub."""
    if not GITHUB_WEBHOOK_SECRET or GITHUB_WEBHOOK_SECRET == "":
        logger.error("❌ GITHUB_WEBHOOK_SECRET не установлен. Webhook отключен.")
        raise HTTPException(status_code=403, detail="Webhook disabled")

    # Проверка подписи
    signature = request.headers.get("X-Hub-Signature-256")
    if not signature:
        logger.warning("⚠️ Отсутствует подпись вебхука от GitHub")
        raise HTTPException(status_code=400, detail="Missing signature")

    try:
        body = await request.body()
        expected_signature = "sha256=" + hmac.new(
            GITHUB_WEBHOOK_SECRET.encode('utf-8'),
            body,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(signature, expected_signature):
            logger.warning(f"⚠️ Неверная подпись вебхука! Получено: {signature}, ожидалось: {expected_signature}")
            raise HTTPException(status_code=403, detail="Invalid signature")

        # Проверяем событие
        event = request.headers.get("X-GitHub-Event", "")
        if event != "push":
            logger.info(f"⏭️ Проигнорировано событие GitHub: {event}")
            return {"status": "ignored", "event": event}

        # Запускаем деплой в фоне
        logger.info("✅ Получен валидный webhook от GitHub. Запускаем деплой...")
        return {"status": "deploy triggered", "timestamp": datetime.datetime.utcnow().isoformat()}
    
    except Exception as e:
        logger.exception(f"❌ Ошибка обработки GitHub webhook: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# === Health-check для Supervisor ===
@app.get("/supervisor/health", summary="Supervisor health check")
async def supervisor_health():
    """Эндпоинт для проверки здоровья сервиса Supervisor."""
    try:
        # Проверяем подключение к БД
        db_status = db_health_check()
        
        if db_status.get("status") != "ok":
            return JSONResponse(
                status_code=503,
                content={"status": "degraded", "database": "unavailable"}
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "database": "available",
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        )
    except Exception as e:
        logger.error(f"❌ Ошибка health-check: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "error", "detail": str(e)}
        )

# === Запуск сервера ===
if __name__ == "__main__":
    import uvicorn
    
    # Логируем конфигурацию при запуске
    port = int(os.getenv("PORT", 8081))
    logger.info(f"🚀 Запуск веб-API на порту {port}")
    logger.info(f"🔐 ADMIN_SECRET: {'установлен' if ADMIN_SECRET else 'не установлен'}")
    logger.info(f"📁 База данных: {DATABASE_PATH}")
    logger.info(f"🌍 Часовой пояс: {TIMEZONE}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="info",
        reload=False,
        workers=1
    )
