# web_api.py

import asyncio
import datetime
import csv
import io
import logging
from typing import Optional, List, Dict, Any
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Header, Request, Form, status
from fastapi.responses import JSONResponse, Response, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, validator
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

from config import WEB_API_SECRET, ADMIN_SECRET, BOT_TOKEN, TIMEZONE
from shared.database import (
    get_all_active_messages, deactivate_message,
    update_scheduled_message, add_scheduled_message
)
from shared.utils import (
    escape_markdown_v2, detect_media_type,
    parse_user_datetime, next_recurrence_time
)
from scheduler_logic import publish_message

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# === Инициализация FastAPI ===
app = FastAPI(title="Telegram Reminder Scheduler API")

# === Метрики Prometheus ===
TASKS_CREATED = Counter('telegram_scheduler_tasks_created_total', 'Total tasks created')
TASKS_DELETED = Counter('telegram_scheduler_tasks_deleted_total', 'Total tasks deleted')
ACTIVE_TASKS = Gauge('telegram_scheduler_active_tasks', 'Number of active scheduled tasks')

# === Шаблоны ===
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# === Кэш названий чатов ===
CHAT_TITLE_CACHE: Dict[int, tuple] = {}

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

    @validator('delete_after_days')
    def validate_delete_days(cls, v):
        if v is not None and v not in (1, 2, 3):
            raise ValueError('Must be 1, 2, or 3')
        return v

class TaskCreateForm(BaseModel):
    chat_id: int
    message_text: str
    media_file_id: Optional[str] = None
    publish_at_local: str  # ДД.ММ.ГГГГ ЧЧ:ММ
    recurrence: str
    weekly_days: Optional[List[int]] = None
    monthly_days: Optional[str] = None
    delete_after_days: Optional[int] = None
    pin: bool = False
    notify: bool = True

    @validator('chat_id')
    def validate_chat_id(cls, v):
        if not str(v).startswith('-100'):
            raise ValueError('Invalid chat ID format')
        return v

    @validator('delete_after_days')
    def validate_delete_days_form(cls, v):
        if v is not None and v not in (1, 2, 3):
            raise ValueError('Must be 1, 2, or 3')
        return v

# === Вспомогательные функции ===
async def get_chat_title(chat_id: int) -> str:
    """Получает название чата через Telegram API с кэшированием."""
    now = datetime.datetime.now()
    if chat_id in CHAT_TITLE_CACHE:
        title, timestamp = CHAT_TITLE_CACHE[chat_id]
        if (now - timestamp).total_seconds() < 3600:  # кэш 1 час
            return title

    try:
        from telegram import Bot
        bot = Bot(token=BOT_TOKEN)
        chat = await bot.get_chat(chat_id)
        title = chat.title or f"Чат {chat_id}"
    except Exception as e:
        logger.warning(f"Не удалось получить название чата {chat_id}: {e}")
        title = f"Чат {chat_id}"

    CHAT_TITLE_CACHE[chat_id] = (title, now)
    return title

def parse_weekly_days(days_str: Optional[str]) -> List[int]:
    """Парсит строку дней недели (например, '0,2,4')."""
    if not days_str:
        return []
    try:
        return [int(d.strip()) for d in days_str.split(',') if d.strip().isdigit()]
    except:
        return []

# === Эндпоинты ===

@app.get("/health", summary="Health check")
async def health_check():
    """Проверяет работоспособность сервиса."""
    try:
        tasks = get_all_active_messages()
        return JSONResponse({
            "status": "ok",
            "active_tasks": len(tasks),
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            {"status": "error", "detail": str(e)},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

@app.get("/metrics", summary="Prometheus metrics")
async def metrics():
    """Экспортирует метрики для Prometheus."""
    active_count = len(get_all_active_messages())
    ACTIVE_TASKS.set(active_count)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/publish", summary="Publish message immediately")
async def web_publish(request: PublishRequest, x_secret: str = Header(...)):
    """Публикует сообщение немедленно через HTTP API."""
    if WEB_API_SECRET and x_secret != WEB_API_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret")

    try:
        # Экранируем текст для MarkdownV2
        safe_text = escape_markdown_v2(request.text) if request.text else None
        safe_caption = escape_markdown_v2(request.caption) if request.caption else None

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
            raise HTTPException(status_code=500, detail="Failed to send message")
        logger.info(f"Web publish: chat={request.chat_id}, msg_id={msg_id}")
        return {"ok": True, "message_id": msg_id}
    except Exception as e:
        logger.exception("Web publish error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin", summary="Admin panel")
async def admin_panel(
    request: Request,
    chat_filter: Optional[str] = None,
    x_admin_secret: str = Header(None)
):
    """Отображает админку для управления задачами."""
    if ADMIN_SECRET and x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Admin access required")

    tasks = get_all_active_messages()

    # Фильтрация по чату
    if chat_filter and chat_filter.lstrip('-').isdigit():
        chat_filter = int(chat_filter)
        tasks = [t for t in tasks if t[1] == chat_filter]

    # Уникальные чаты
    unique_chats = sorted({t[1] for t in tasks})
    chat_titles = {}
    for cid in unique_chats:
        chat_titles[cid] = await get_chat_title(cid)

    # Преобразуем кортежи в словари
    task_dicts = []
    for row in tasks:
        task_dicts.append({
            'id': row[0],
            'chat_id': row[1],
            'text': row[2],
            'photo_file_id': row[3],
            'document_file_id': row[4],
            'caption': row[5],
            'publish_at': row[6],
            'recurrence': row[8],
            'pin': bool(row[9]),
            'notify': bool(row[10]),
            'delete_after_days': row[11],
            'active': row[12]
        })

    return templates.TemplateResponse("admin.html", {
        "request": request,
        "tasks": task_dicts,
        "active_count": len(tasks),
        "unique_chats": unique_chats,
        "chat_titles": chat_titles,
        "chat_filter": chat_filter,
        "timezone": str(TIMEZONE)
    })

@app.post("/admin/create", summary="Create new task")
async def admin_create_task(
    request: Request,
    chat_id: int = Form(...),
    message_text: str = Form(...),
    media_file_id: Optional[str] = Form(None),
    publish_at_local: str = Form(...),
    recurrence: str = Form(...),
    weekly_days: Optional[List[int]] = Form(None),
    monthly_days: Optional[str] = Form(None),
    delete_after_days: Optional[int] = Form(None),
    pin: bool = Form(False),
    notify: bool = Form(True),
    x_admin_secret: str = Header(None)
):
    """Создаёт новую задачу из админки."""
    if ADMIN_SECRET and x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)

    try:
        # Парсим дату
        naive_local, utc_naive = parse_user_datetime(publish_at_local)
        publish_at_utc = utc_naive.isoformat()

        # Определяем тип медиа
        media_type = detect_media_type(media_file_id) if media_file_id else None
        photo_file_id = media_file_id if media_type == "photo" else None
        document_file_id = media_file_id if media_type == "document" else None

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

        msg_id = add_scheduled_message(data)
        TASKS_CREATED.inc()
        logger.info(f"Задача создана через админку: ID={msg_id}")
        return RedirectResponse(url="/admin", status_code=303)

    except ValueError as e:
        logger.warning(f"Ошибка создания задачи: {e}")
        return RedirectResponse(url=f"/admin?error={quote(str(e))}", status_code=303)
    except Exception as e:
        logger.exception("Неожиданная ошибка при создании задачи")
        return RedirectResponse(url="/admin?error=internal_error", status_code=303)

@app.get("/admin/edit/{task_id}", summary="Edit task form")
async def admin_edit_form(
    request: Request,
    task_id: int,
    x_admin_secret: str = Header(None)
):
    """Отображает форму редактирования задачи."""
    if ADMIN_SECRET and x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)

    row = get_all_active_messages()
    task_row = None
    for r in row:
        if r[0] == task_id:
            task_row = r
            break

    if not task_row:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    task = {
        'id': task_row[0],
        'chat_id': task_row[1],
        'message_text': task_row[2] or task_row[5] or "",
        'media_file_id': task_row[3] or task_row[4],
        'publish_at_local': "",  # Будет заполнено ниже
        'recurrence': task_row[8],
        'pin': bool(task_row[9]),
        'notify': bool(task_row[10]),
        'delete_after_days': task_row[11]
    }

    # Конвертируем UTC в локальное время для отображения
    try:
        utc_dt = datetime.datetime.fromisoformat(task_row[6])
        local_dt = utc_dt.replace(tzinfo=datetime.timezone.utc).astimezone(TIMEZONE)
        task['publish_at_local'] = local_dt.strftime("%d.%m.%Y %H:%M")
    except:
        task['publish_at_local'] = task_row[6]

    # Загружаем все задачи для фильтра
    tasks = get_all_active_messages()
    unique_chats = sorted({t[1] for t in tasks})
    chat_titles = {cid: await get_chat_title(cid) for cid in unique_chats}

    task_dicts = []
    for r in tasks:
        task_dicts.append({
            'id': r[0],
            'chat_id': r[1],
            'text': r[2],
            'photo_file_id': r[3],
            'document_file_id': r[4],
            'caption': r[5],
            'publish_at': r[6],
            'recurrence': r[8],
            'pin': bool(r[9]),
            'notify': bool(r[10]),
            'delete_after_days': r[11],
            'active': r[12]
        })

    return templates.TemplateResponse("admin.html", {
        "request": request,
        "tasks": task_dicts,
        "active_count": len(tasks),
        "unique_chats": unique_chats,
        "chat_titles": chat_titles,
        "edit_task": task,
        "timezone": str(TIMEZONE)
    })

@app.post("/admin/edit/{task_id}", summary="Save edited task")
async def admin_save_edit(
    task_id: int,
    chat_id: int = Form(...),
    message_text: str = Form(...),
    media_file_id: Optional[str] = Form(None),
    publish_at_local: str = Form(...),
    recurrence: str = Form(...),
    weekly_days: Optional[List[int]] = Form(None),
    monthly_days: Optional[str] = Form(None),
    delete_after_days: Optional[int] = Form(None),
    pin: bool = Form(False),
    notify: bool = Form(True),
    x_admin_secret: str = Header(None)
):
    """Сохраняет отредактированную задачу."""
    if ADMIN_SECRET and x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)

    try:
        naive_local, utc_naive = parse_user_datetime(publish_at_local)
        publish_at_utc = utc_naive.isoformat()

        media_type = detect_media_type(media_file_id) if media_file_id else None
        photo_file_id = media_file_id if media_type == "photo" else None
        document_file_id = media_file_id if media_type == "document" else None

        update_scheduled_message(
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
        logger.info(f"Задача {task_id} обновлена через админку")
        return RedirectResponse(url="/admin", status_code=303)

    except Exception as e:
        logger.exception("Ошибка при сохранении задачи")
        return RedirectResponse(url=f"/admin/edit/{task_id}?error={quote(str(e))}", status_code=303)

@app.post("/admin/delete/{task_id}", summary="Delete task")
async def admin_delete_task(task_id: int, x_admin_secret: str = Header(None)):
    """Удаляет задачу."""
    if ADMIN_SECRET and x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)

    deactivate_message(task_id)
    TASKS_DELETED.inc()
    logger.info(f"Задача {task_id} удалена через админку")
    return RedirectResponse(url="/admin", status_code=303)

@app.get("/admin/export.csv", summary="Export tasks to CSV")
async def export_tasks_csv(x_admin_secret: str = Header(None)):
    """Экспортирует задачи в CSV."""
    if ADMIN_SECRET and x_admin_secret != ADMIN_SECRET:
        raise HTTPException(status_code=403)

    tasks = get_all_active_messages()
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "ID", "Chat ID", "Text", "Photo file_id", "Document file_id", "Caption",
        "Publish At (UTC)", "Recurrence", "Pin", "Notify", "Delete After (days)"
    ])

    for row in tasks:
        writer.writerow([
            row[0], row[1], row[2], row[3], row[4], row[5],
            row[6], row[8], row[9], row[10], row[11]
        ])

    output.seek(0)
    filename = f"tasks_export_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={quote(filename)}"}
    )

# === Запуск сервера ===
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
