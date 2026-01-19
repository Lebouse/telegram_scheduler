# scheduler_logic.py

import asyncio
import datetime
from telegram import Bot
from config import BOT_TOKEN, TIMEZONE
from shared.database import update_next_publish_time, deactivate_message

bot = Bot(token=BOT_TOKEN)

def next_recurrence_time(original: datetime.datetime, recurrence: str, last: datetime.datetime) -> datetime.datetime:
    if recurrence == 'once':
        return None
    elif recurrence == 'daily':
        return last + datetime.timedelta(days=1)
    elif recurrence == 'weekly':
        return last + datetime.timedelta(weeks=1)
    elif recurrence == 'monthly':
        year = last.year + (last.month // 12)
        month = (last.month % 12) + 1
        day = min(last.day, [31,
            29 if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
        try:
            return last.replace(year=year, month=month, day=day)
        except ValueError:
            return last.replace(year=year, month=month, day=1) + datetime.timedelta(days=31)
    return None

async def publish_message(
    chat_id, text=None, photo_file_id=None, document_file_id=None,
    caption=None, pin=False, notify=True, delete_after_days=None
):
    try:
        if photo_file_id:
            msg = await bot.send_photo(chat_id, photo_file_id, caption=caption or text, disable_notification=not notify)
        elif document_file_id:
            msg = await bot.send_document(chat_id, document_file_id, caption=caption or text, disable_notification=not notify)
        else:
            msg = await bot.send_message(chat_id, text or "", disable_notification=not notify)

        if pin:
            await bot.pin_chat_message(chat_id, msg.message_id)

        if delete_after_days and delete_after_days > 0:
            asyncio.create_task(schedule_deletion(chat_id, msg.message_id, delete_after_days))

        return msg.message_id
    except Exception as e:
        print(f"Ошибка публикации: {e}")
        return None

async def schedule_deletion(chat_id, message_id, days):
    await asyncio.sleep(days * 24 * 3600)
    try:
        await bot.delete_message(chat_id, message_id)
    except:
        pass

async def publish_and_reschedule(
    msg_id, chat_id, text, photo_file_id, document_file_id, caption,
    recurrence, pin, notify, delete_after_days, current_publish_at_str
):
    await publish_message(chat_id, text, photo_file_id, document_file_id, caption, pin, notify, delete_after_days)

    if recurrence != 'once':
        current = datetime.datetime.fromisoformat(current_publish_at_str)
        next_time = next_recurrence_time(current, recurrence, current)
        if next_time:
            from pytz import UTC
            next_utc = TIMEZONE.localize(next_time).astimezone(UTC).replace(tzinfo=None)
            update_next_publish_time(msg_id, next_utc.isoformat())
        else:
            deactivate_message(msg_id)
    else:
        deactivate_message(msg_id)
