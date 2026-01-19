# telegram_bot.py

import asyncio
import signal
import sys
import datetime
import re
import logging
from typing import Optional

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, ChatMember
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes,
    ConversationHandler, filters, CallbackQueryHandler, ChatMemberHandler
)
from telegram.constants import ChatType
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import UTC

from config import BOT_TOKEN, AUTHORIZED_USER_IDS, TIMEZONE
from shared.database import (
    init_db, add_scheduled_message, get_all_active_messages,
    deactivate_message, cleanup_old_tasks
)
from shared.utils import parse_user_datetime, next_recurrence_time, detect_media_type
from scheduler_logic import publish_and_reschedule

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# === Константы состояний ===
(
    WAITING_CONTENT, SELECT_CHAT, INPUT_DATE, SELECT_RECURRENCE,
    SELECT_PIN, SELECT_NOTIFY, SELECT_DELETE_DAYS
) = range(7)

user_sessions = {}
shutdown_event = asyncio.Event()

# === Файлы для хранения данных ===
TRUSTED_CHATS_FILE = "/data/trusted_chats.txt"

def load_trusted_chats():
    """Загружает список чатов, куда добавлен бот."""
    try:
        with open(TRUSTED_CHATS_FILE, "r") as f:
            return {int(line.strip()) for line in f if line.strip().isdigit()}
    except FileNotFoundError:
        return set()

def save_trusted_chats(chats):
    """Сохраняет список доверенных чатов."""
    with open(TRUSTED_CHATS_FILE, "w") as f:
        for chat_id in sorted(chats):
            f.write(f"{chat_id}\n")

# === Декоратор авторизации ===
def check_auth(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id not in AUTHORIZED_USER_IDS:
            await update.message.reply_text("❌ Доступ запрещён.")
            return
        return await func(update, context)
    return wrapper

# === Обработка добавления/удаления из чата ===
async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Срабатывает при изменении статуса бота в чате."""
    my_chat_member = update.my_chat_member
    if not my_chat_member:
        return

    chat = my_chat_member.chat
    new_status = my_chat_member.new_chat_member.status
    old_status = my_chat_member.old_chat_member.status

    trusted = load_trusted_chats()

    if new_status in ("member", "administrator"):
        if chat.type in (ChatType.GROUP, ChatType.SUPERGROUP):
            trusted.add(chat.id)
            save_trusted_chats(trusted)
            logger.info(f"Бот добавлен в чат {chat.id} ({chat.title})")
    elif new_status in ("left", "kicked"):
        trusted.discard(chat.id)
        save_trusted_chats(trusted)
        logger.info(f"Бот удалён из чата {chat.id}")

# === Валидация чата ===
async def validate_chat_id(chat_id: int) -> bool:
    """Проверяет, имеет ли бот доступ к чату."""
    from shared.bot_instance import get_bot
    bot = get_bot()
    try:
        await bot.get_chat(chat_id)
        return True
    except Exception as e:
        logger.warning(f"Ошибка проверки чата {chat_id}: {e}")
        return False

# === Обработка медиа от админа ===
@check_auth
async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Позволяет админу получить file_id для использования в админке."""
    if update.message.photo:
        file_id = update.message.photo[-1].file_id
        await update.message.reply_text(
            f"✅ Photo file_id:\n<code>{file_id}</code>",
            parse_mode="HTML"
        )
    elif update.message.document:
        mime = update.message.document.mime_type
        if mime in ('application/pdf', 'image/jpeg', 'image/png'):
            file_id = update.message.document.file_id
            await update.message.reply_text(
                f"✅ Document file_id:\n<code>{file_id}</code>",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text("Поддерживаются только PDF и изображения.")
    else:
        await update.message.reply_text("Отправьте фото или PDF.")

# === Диалог планирования ===
@check_auth
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📩 Отправьте сообщение (текст, фото или PDF), которое нужно запланировать как напоминание."
    )
    return WAITING_CONTENT

async def receive_content(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USER_IDS:
        return

    session = {
        'text': None,
        'photo_file_id': None,
        'document_file_id': None,
        'caption': None
    }

    if update.message.text:
        session['text'] = update.message.text
    elif update.message.photo:
        session['photo_file_id'] = update.message.photo[-1].file_id
        session['caption'] = update.message.caption
    elif update.message.document:
        mime = update.message.document.mime_type
        if mime in ('application/pdf', 'image/jpeg', 'image/png'):
            session['document_file_id'] = update.message.document.file_id
            session['caption'] = update.message.caption
        else:
            await update.message.reply_text("Поддерживаются только PDF и изображения.")
            return WAITING_CONTENT
    else:
        await update.message.reply_text("Пожалуйста, отправьте текст, фото или PDF.")
        return WAITING_CONTENT

    user_sessions[user_id] = session

    # Загружаем доверенные чаты
    trusted_chats = load_trusted_chats()
    if not trusted_chats:
        await update.message.reply_text(
            "Бот не добавлен ни в один чат. Сначала добавьте его в группу как администратора."
        )
        return ConversationHandler.END

    # Формируем кнопки
    buttons = []
    for chat_id in trusted_chats:
        buttons.append([InlineKeyboardButton(f"Чат {chat_id}", callback_data=str(chat_id))])
    await update.message.reply_text(
        "Выберите чат для публикации:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    return SELECT_CHAT

async def select_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    chat_id = int(query.data)

    if not await validate_chat_id(chat_id):
        await query.edit_message_text("Бот не имеет доступа к этому чату.")
        return ConversationHandler.END

    user_sessions[user_id]['chat_id'] = chat_id
    await query.edit_message_text("Введите дату и время первого напоминания (формат: ДД.ММ.ГГГГ ЧЧ:ММ):")
    return INPUT_DATE

async def input_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    try:
        naive_local, utc_naive = parse_user_datetime(text)
    except ValueError as e:
        await update.message.reply_text(str(e))
        return INPUT_DATE

    # Проверка максимального срока (365 дней)
    max_allowed = datetime.datetime.utcnow() + datetime.timedelta(days=365)
    if utc_naive > max_allowed:
        await update.message.reply_text("❌ Максимальный срок публикации — 1 год от сегодняшнего дня.")
        return INPUT_DATE

    if utc_naive <= datetime.datetime.utcnow():
        await update.message.reply_text("Дата должна быть в будущем!")
        return INPUT_DATE

    user_sessions[user_id]['publish_at'] = utc_naive.isoformat()

    keyboard = [
        [InlineKeyboardButton("Один раз", callback_data="once")],
        [InlineKeyboardButton("Ежедневно", callback_data="daily")],
        [InlineKeyboardButton("Еженедельно", callback_data="weekly")],
        [InlineKeyboardButton("Ежемесячно", callback_data="monthly")]
    ]
    await update.message.reply_text("Выберите периодичность:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_RECURRENCE

async def select_recurrence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_sessions[user_id]['recurrence'] = query.data
    keyboard = [
        [InlineKeyboardButton("Да", callback_data="1"), InlineKeyboardButton("Нет", callback_data="0")]
    ]
    await query.edit_message_text("Закрепить сообщение?", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_PIN

async def select_pin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_sessions[user_id]['pin'] = bool(int(query.data))
    keyboard = [
        [InlineKeyboardButton("Да", callback_data="1"), InlineKeyboardButton("Нет", callback_data="0")]
    ]
    await query.edit_message_text("Оповестить участников?", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_NOTIFY

async def select_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_sessions[user_id]['notify'] = bool(int(query.data))
    keyboard = [
        [InlineKeyboardButton("1 день", callback_data="1")],
        [InlineKeyboardButton("2 дня", callback_data="2")],
        [InlineKeyboardButton("3 дня", callback_data="3")],
        [InlineKeyboardButton("Никогда", callback_data="0")]
    ]
    await query.edit_message_text("Удалить напоминание через:", reply_markup=InlineKeyboardMarkup(keyboard))
    return SELECT_DELETE_DAYS

async def select_delete_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    days = int(query.data)
    user_sessions[user_id]['delete_after_days'] = days if days > 0 else None

    # Сохраняем в БД
    data = user_sessions[user_id]
    try:
        msg_id = add_scheduled_message(data)
        await query.edit_message_text(f"✅ Напоминание запланировано! ID задачи: {msg_id}")
        schedule_all_jobs(context.application.job_queue)
    except ValueError as e:
        await query.edit_message_text(f"⚠️ {e}")
    except Exception as e:
        logger.exception("Ошибка при сохранении задачи")
        await query.edit_message_text("❌ Произошла ошибка. Попробуйте позже.")

    del user_sessions[user_id]
    return ConversationHandler.END

@check_auth
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in user_sessions:
        del user_sessions[user_id]
    await update.message.reply_text("Операция отменена.")
    return ConversationHandler.END

@check_auth
async def list_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tasks = get_all_active_messages()
    if not tasks:
        await update.message.reply_text("Нет активных напоминаний.")
        return
    text = "\n".join([
        f"ID: {t[0]}, Чат: {t[1]}, Публикация: {t[6][:16]}, Контент: {(t[2] or t[5] or 'медиа')[:30]}..."
        for t in tasks[:10]
    ])
    await update.message.reply_text(f"Активные напоминания:\n\n{text}")

# === Планировщик задач ===
def schedule_all_jobs(job_queue):
    """Перепланирует все активные задачи."""
    job_queue.scheduler.remove_all_jobs()
    messages = get_all_active_messages()
    for row in messages:
        msg_id, chat_id, text, photo, doc, caption, publish_at_str, _, recurrence, pin, notify, del_days, _, _, _ = row
        publish_at = datetime.datetime.fromisoformat(publish_at_str)
        if publish_at > datetime.datetime.utcnow():
            job_queue.run_once(
                lambda ctx, r=row: publish_and_reschedule(
                    r[0], r[1], r[2], r[3], r[4], r[5], r[8], r[9], r[10], r[11], r[6]
                ),
                publish_at
            )

# === Проверка истекающих задач ===
async def check_expiring_tasks(context: ContextTypes.DEFAULT_TYPE):
    """Отправляет напоминание админам за 7 дней до окончания срока."""
    from shared.database import get_all_active_messages
    bot = context.bot
    now = datetime.datetime.utcnow()
    week_from_now = now + datetime.timedelta(days=7)

    tasks = get_all_active_messages()
    for row in tasks:
        msg_id, chat_id, text, photo, doc, caption, pub_at, orig_pub_at, recurrence, pin, notify, del_days, active, created_at, max_end_date = row[:15]
        
        if not max_end_date:
            continue
            
        try:
            end_date = datetime.datetime.fromisoformat(max_end_date)
        except ValueError:
            continue

        if now < end_date <= week_from_now:
            message = (
                f"⚠️ Напоминание: задача ID={msg_id} в чате {chat_id} "
                f"закончится {end_date.strftime('%d.%m.%Y')}.\n"
                f"Контент: {(text or caption or 'медиа')[:50]}...\n"
                f"Используйте веб-админку для продления."
            )
            for admin_id in AUTHORIZED_USER_IDS:
                try:
                    await bot.send_message(admin_id, message)
                except Exception as e:
                    logger.warning(f"Не удалось отправить напоминание админу {admin_id}: {e}")

# === Graceful shutdown ===
def signal_handler():
    logger.info("Получен сигнал завершения. Ожидание завершения...")
    shutdown_event.set()

async def main():
    init_db()
    cleanup_old_tasks(max_age_days=30)

    app = Application.builder().token(BOT_TOKEN).build()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.start()

    # Регистрация хендлеров
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            WAITING_CONTENT: [MessageHandler(filters.ALL & ~filters.COMMAND, receive_content)],
            SELECT_CHAT: [CallbackQueryHandler(select_chat)],
            INPUT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, input_date)],
            SELECT_RECURRENCE: [CallbackQueryHandler(select_recurrence)],
            SELECT_PIN: [CallbackQueryHandler(select_pin)],
            SELECT_NOTIFY: [CallbackQueryHandler(select_notify)],
            SELECT_DELETE_DAYS: [CallbackQueryHandler(select_delete_days)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("list", list_tasks))
    app.add_handler(MessageHandler(
        filters.PHOTO | filters.Document.ALL & ~filters.COMMAND,
        handle_media
    ))
    app.add_handler(ChatMemberHandler(on_chat_member_update, ChatMemberHandler.MY_CHAT_MEMBER))

    app.job_queue.scheduler = scheduler
    schedule_all_jobs(app.job_queue)

    # Ежедневная проверка истекающих задач
    app.job_queue.run_daily(check_expiring_tasks, time=datetime.time(9, 0, tzinfo=UTC))

    # Graceful shutdown
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, signal_handler)

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    await shutdown_event.wait()

    # Остановка
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    scheduler.shutdown()
    logger.info("Бот остановлен.")

if __name__ == "__main__":
    asyncio.run(main())
