# telegram_bot.py

import asyncio
import signal
import sys
import datetime
import re
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes,
    ConversationHandler, filters, CallbackQueryHandler
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pytz import UTC

from config import BOT_TOKEN, AUTHORIZED_USER_IDS, TIMEZONE
from shared.database import init_db, add_scheduled_message, get_all_active_messages, deactivate_message
from scheduler_logic import publish_and_reschedule

# === Константы ===
(
    WAITING_CONTENT, SELECT_CHAT, INPUT_DATE, SELECT_RECURRENCE,
    SELECT_PIN, SELECT_NOTIFY, SELECT_DELETE_DAYS
) = range(7)

user_sessions = {}
shutdown_event = asyncio.Event()

# === Вспомогательные функции ===
def check_auth(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id not in AUTHORIZED_USER_IDS:
            await update.message.reply_text("❌ Доступ запрещён.")
            return
        return await func(update, context)
    return wrapper

async def validate_chat_id(chat_id: int) -> bool:
    """Проверяет, может ли бот писать в чат."""
    from telegram import Bot
    bot = Bot(token=BOT_TOKEN)
    try:
        await bot.get_chat(chat_id)
        return True
    except:
        return False

# === Диалог планирования ===
@check_auth
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отправьте сообщение для планирования.")
    return WAITING_CONTENT

# ... (реализуйте receive_content, select_chat и т.д. — как в предыдущих версиях)

async def select_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        chat_id = int(update.message.text.strip())
        if not await validate_chat_id(chat_id):
            await update.message.reply_text("Бот не имеет доступа к этому чату. Убедитесь, что он добавлен как админ.")
            return SELECT_CHAT
        user_sessions[user_id]['chat_id'] = chat_id
    except ValueError:
        await update.message.reply_text("Неверный ID чата.")
        return SELECT_CHAT
    await update.message.reply_text("Дата публикации (ДД.ММ.ГГГГ ЧЧ:ММ):")
    return INPUT_DATE

async def input_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})", text)
    if not match:
        await update.message.reply_text("Формат: ДД.ММ.ГГГГ ЧЧ:ММ")
        return INPUT_DATE

    day, month, year, hour, minute = map(int, match.groups())
    try:
        naive_dt = datetime.datetime(year, month, day, hour, minute)
        local_dt = TIMEZONE.localize(naive_dt)
        utc_dt = local_dt.astimezone(UTC).replace(tzinfo=None)
        if utc_dt <= datetime.datetime.now(UTC):
            await update.message.reply_text("Дата должна быть в будущем!")
            return INPUT_DATE
        user_sessions[user_id]['publish_at'] = utc_dt.isoformat()
    except ValueError as e:
        await update.message.reply_text(f"Ошибка: {e}")
        return INPUT_DATE

    # ... продолжение диалога

# === Планировщик ===
def schedule_all_jobs(job_queue):
    job_queue.scheduler.remove_all_jobs()
    messages = get_all_active_messages()
    for row in messages:
        msg_id, chat_id, text, photo, doc, caption, publish_at_str, _, recurrence, pin, notify, del_days, _ = row
        publish_at = datetime.datetime.fromisoformat(publish_at_str)
        if publish_at > datetime.datetime.now(UTC):
            job_queue.run_once(
                lambda ctx, r=row: publish_and_reschedule(
                    r[0], r[1], r[2], r[3], r[4], r[5], r[8], r[9], r[10], r[11], r[6]
                ),
                publish_at
            )

# === Graceful shutdown ===
def signal_handler():
    print("Получен сигнал завершения. Ожидание завершения...")
    shutdown_event.set()

async def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.start()

    # Регистрация хендлеров...
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={...},  # ← ваш код диалога
        fallbacks=[CommandHandler("cancel", lambda u, c: u.message.reply_text("Отменено."))]
    )
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("list", list_tasks))
    app.add_handler(CommandHandler("delete", delete_task))

    app.job_queue.scheduler = scheduler
    schedule_all_jobs(app.job_queue)

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
    print("Бот остановлен.")

if __name__ == "__main__":
    asyncio.run(main())
