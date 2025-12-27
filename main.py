import os
import sqlite3
import requests
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
import asyncio
import logging
import math
from aiogram.types import PreCheckoutQuery
import hashlib
import re
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import random
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
import aiohttp
import tzdata
import string
from aiogram import Router, F
from aiogram.types import CallbackQuery

# Создаем роутер для магазина
shop_router = Router()

BOT_START_TIME = datetime.now()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
BOT_USERNAME = "CryptoMiner_sBot"

# Список администраторов бота
ADMINS = [5929120983, 7338817463, 8315604670]

# Путь к папке с баннерами (работает на любом хосте)
BANNER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'baners')

# Кеш для file_id баннеров (для быстрой загрузки)
SHOP_BANNER_FILE_ID = None

def format_number(num, is_price=False):
    if isinstance(num, (int, float)):
        if is_price:
            # Для цен убираем .00 в конце
            num_str = "{:,.0f}".format(int(num)).replace(",", ".")
            return num_str
        if isinstance(num, float) and num.is_integer():
            return "{:,.0f}".format(int(num)).replace(",", ".")
        if abs(num) < 0.0001:
            return "{:.5f}".format(num).rstrip('0').rstrip('.')
        if abs(num) < 0.01:
            return "{:.5f}".format(num).rstrip('0').rstrip('.')
        if abs(num) < 1:
            return "{:.5f}".format(num).rstrip('0').rstrip('.')
        return "{:,.2f}".format(num).replace(",", ".")
    return str(num)

API_TOKEN = "8022498920:AAHwijIPn3LnxQIys5PETOCyCEUCBJhollA" # original
#API_TOKEN = "8050994385:AAHTvNvBZmY9_9vLsfQs-zc57WWdGN7I6hg" # global test
#API_TOKEN = "7951423512:AAEs3oYKfJnWIQugOF3BAhvW0RyMix0pQE0" # local test
#API_TOKEN = "8376943234:AAF7AA4kSXrdPsuGj7JgXib2zqOLI-SpGP4" # test bot

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# ===== MIDDLEWARE ДЛЯ ПРОВЕРКИ БАНА =====
@dp.update.outer_middleware()
async def ban_check_middleware(handler, event, data):
    """Middleware для проверки глобального бана пользователя"""
    user_id = None
    if hasattr(event, 'from_user') and event.from_user:
        user_id = event.from_user.id
    elif hasattr(event, 'message') and event.message and hasattr(event.message, 'from_user'):
        user_id = event.message.from_user.id

    if user_id and user_id not in ADMINS:
        is_banned, reason = check_ban(user_id)
        if is_banned:
            try:
                if hasattr(event, 'answer'):
                    await event.answer(
                        f'🚫 Вы заблокированы\nПричина: {reason}\n\nВы не можете использовать бота.',
                        show_alert=True
                    )
                elif hasattr(event, 'message'):
                    await event.message.answer(
                        f'🚫 Вы заблокированы\nПричина: {reason}\n\nВы не можете использовать бота.'
                    )
            except:
                pass
            return

    return await handler(event, data)

dp.include_router(shop_router)
# Подключение к базе данных
import os
os.makedirs('data', exist_ok=True)
conn = sqlite3.connect('data/miner.db', check_same_thread=False)
cursor = conn.cursor()

from datetime import datetime, timedelta
from aiogram import Bot, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, PreCheckoutQuery, LabeledPrice
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

        
# Создание таблиц
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    usd_balance REAL DEFAULT 10000,
    btc_balance REAL DEFAULT 0,
    income_btc REAL DEFAULT 0,
    expansion INTEGER DEFAULT 1,
    last_income_time TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS automation_users (
    user_id INTEGER PRIMARY KEY,
    automation_until TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS antivirus_users (
    user_id INTEGER PRIMARY KEY,
    antivirus_until TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS banned_users (
    user_id INTEGER PRIMARY KEY,
    banned_at TEXT DEFAULT CURRENT_TIMESTAMP,
    banned_by INTEGER,
    reason TEXT DEFAULT 'Глобальный бан'
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_boosters (
    user_id INTEGER,
    booster_type TEXT,
    until TEXT,
    bonus REAL DEFAULT 0,
    PRIMARY KEY (user_id, booster_type)
)
''')

# Добавим после других CREATE TABLE
cursor.execute('''
CREATE TABLE IF NOT EXISTS promo_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE,
    creator_id INTEGER,
    uses_left INTEGER,
    max_uses INTEGER,
    income_multiplier REAL,
    created_at TEXT,
    FOREIGN KEY(creator_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS promo_activations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    promo_id INTEGER,
    activated_at TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(promo_id) REFERENCES promo_codes(id)
)
''')


cursor.execute('''
CREATE TABLE IF NOT EXISTS user_upgrades (
    user_id INTEGER PRIMARY KEY,
    wiring INTEGER DEFAULT 0,
    ventilation INTEGER DEFAULT 0,
    traffic INTEGER DEFAULT 0,
    software INTEGER DEFAULT 0,
    cooling INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_wipes (
    user_id INTEGER PRIMARY KEY,
    wipe_count INTEGER DEFAULT 0,
    total_wipe_bonus REAL DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')


cursor.execute('''
CREATE TABLE IF NOT EXISTS chat_income_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    user_id INTEGER,
    btc_income REAL,
    timestamp TEXT
)
''')


cursor.execute('''
CREATE TABLE IF NOT EXISTS user_cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    card_id INTEGER,
    count INTEGER DEFAULT 1,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS chat_stats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    weekly_btc_earned REAL DEFAULT 0,
    members_count INTEGER DEFAULT 0,
    last_updated TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS chat_members (
    user_id INTEGER,
    chat_id INTEGER,
    PRIMARY KEY (user_id, chat_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS premium_users (
    user_id INTEGER PRIMARY KEY,
    premium_until TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_badges (
    user_id INTEGER PRIMARY KEY,
    badge_id INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

# Добавим после других CREATE TABLE
cursor.execute('''
CREATE TABLE IF NOT EXISTS lottery_tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    ticket_count INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS lottery_winners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    prize_amount REAL,
    draw_date TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS auction_cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER,
    price INTEGER,
    timestamp TEXT,
    FOREIGN KEY(card_id) REFERENCES GRAPHICS_CARDS(id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_businesses (
    user_id INTEGER,
    business_id INTEGER,
    level INTEGER DEFAULT 1,
    last_income_time TEXT,
    PRIMARY KEY (user_id, business_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
) 
''')



cursor.execute('''
CREATE TABLE IF NOT EXISTS bank_deposits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    deposit_type INTEGER,
    amount REAL,
    interest_rate REAL,
    start_date TEXT,
    end_date TEXT,
    status TEXT DEFAULT 'active',
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS bank_loans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    interest_rate REAL,
    start_date TEXT,
    next_payment_date TEXT,
    status TEXT DEFAULT 'active',
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS bank_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    transaction_type TEXT,
    amount REAL,
    description TEXT,
    timestamp TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

conn.commit()                 

cursor.execute('''
CREATE TABLE IF NOT EXISTS server_fund (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    current_goal REAL DEFAULT 0,
    current_amount REAL DEFAULT 0,
    reward_type TEXT DEFAULT 'card',
    reward_value INTEGER DEFAULT 0,
    reward_duration INTEGER DEFAULT 0,
    start_date TEXT,
    end_date TEXT,
    status TEXT DEFAULT 'active'
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS server_fund_contributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    contribution_date TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS server_fund_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    goal REAL,
    amount_collected REAL,
    reward_type TEXT,
    reward_value INTEGER,
    completion_date TEXT,
    participants_count INTEGER
)
''')

conn.commit()

# Добавляем после других CREATE TABLE
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_admin_badges (
    user_id INTEGER,
    badge_id INTEGER,
    assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
    assigned_by INTEGER,
    PRIMARY KEY (user_id, badge_id),
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS custom_badges (
    badge_id INTEGER PRIMARY KEY,
    badge_name TEXT NOT NULL,
    created_by INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
)
''')
conn.commit()

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_viruses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    start_time TEXT,
    duration_hours INTEGER,
    income_penalty REAL DEFAULT 0.3,
    status TEXT DEFAULT 'active',
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()

def add_virus_to_user(user_id: int, duration_hours: int) -> bool:
    """Добавляет вирус пользователю"""
    try:
        start_time = datetime.now().isoformat()
        cursor.execute('''
        INSERT INTO user_viruses (user_id, start_time, duration_hours, status)
        VALUES (?, ?, ?, 'active')
        ''', (user_id, start_time, duration_hours))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding virus to user {user_id}: {e}")
        conn.rollback()
        return False

def get_active_virus(user_id: int) -> Optional[Dict]:
    """Получает активный вирус пользователя"""
    try:
        cursor.execute('''
        SELECT id, start_time, duration_hours, income_penalty 
        FROM user_viruses 
        WHERE user_id = ? AND status = 'active'
        ''', (user_id,))
        result = cursor.fetchone()
        if result:
            return {
                "id": result[0],
                "start_time": result[1],
                "duration_hours": result[2],
                "penalty": result[3]
            }
        return None
    except Exception as e:
        logger.error(f"Error getting active virus: {e}")
        return None


# Таблицы для автоматизации
cursor.execute('''
CREATE TABLE IF NOT EXISTS automation_access (
    user_id INTEGER PRIMARY KEY,
    access_until TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_automation (
    user_id INTEGER PRIMARY KEY,
    auto_taxes BOOLEAN DEFAULT FALSE,
    auto_work BOOLEAN DEFAULT FALSE,
    last_tax_payment TEXT,
    last_work_time TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()



def update_automation_setting(user_id: int, setting: str, value: bool) -> bool:
    """Обновить настройку автоматизации"""
    try:
        if not check_automation_access(user_id):
            return False
            
        if setting == "taxes":
            cursor.execute('UPDATE user_automation SET auto_taxes = ? WHERE user_id = ?', (value, user_id))
        elif setting == "work":
            cursor.execute('UPDATE user_automation SET auto_work = ? WHERE user_id = ?', (value, user_id))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating automation setting: {e}")
        conn.rollback()
        return False

@dp.message(Command("auto"))
async def auto_command(message: Message):
    """Управление автоматизацией"""
    user_id = message.from_user.id
    status = get_automation_status(user_id)
    
    text = "🤖 <b>Автоматизация</b>\n\n"
    
    if status["has_access"] and status["access_until"]:
        # Умное форматирование времени
        now = datetime.now()
        time_left = status["access_until"] - now
        
        if time_left.total_seconds() <= 0:
            text += "🔴 <b>Срок истек</b>\n"
        else:
            total_seconds = int(time_left.total_seconds())
            
            if total_seconds > 2592000:  # 30 дней
                # Больше 30 дней - показываем дату
                text += f"🟢 <b>Срок:</b> до {status['access_until'].strftime('%d.%m.%Y')}\n"
            elif total_seconds > 86400:  # Больше 1 дня
                days = total_seconds // 86400
                text += f"🟢 <b>Срок:</b> {days} дней\n"
            elif total_seconds > 3600:  # Больше 1 часа
                hours = total_seconds // 3600
                text += f"🟡 <b>Срок:</b> {hours} часов\n"
            elif total_seconds > 60:  # Больше 1 минуты
                minutes = total_seconds // 60
                text += f"🟠 <b>Срок:</b> {minutes} минут\n"
            else:
                text += f"🔴 <b>Срок:</b> {total_seconds} секунд\n"
        
        # Статус функций
        text += "\n<b>Функции:</b>\n"
        text += f"💰 Налоги: {'🟢 ВКЛ' if status['auto_taxes'] else '🔴 ВЫКЛ'}\n"
        text += f"💼 Работа: {'🟢 ВКЛ' if status['auto_work'] else '🔴 ВЫКЛ'}\n"
        
        # Команды управления
        text += "\n<b>Управление:</b>\n"
        text += "🔄 /auto_toggle - переключить все\n"
        text += "⚙️ /auto_taxes - переключить налоги\n"
        text += "💼 /auto_work - переключить работу\n"
            
    else:
        text += "🔴 <b>Нет доступа к автоматизации</b>\n\n"
        text += "💡 Для получения доступа обратитесь к администратору или приобретите автоматизацию в магазине"
    
    await message.answer(text, parse_mode='HTML')

@dp.message(Command("auto_toggle"))
async def auto_toggle(message: Message):
    """Переключить все функции автоматизации"""
    user_id = message.from_user.id
    
    has_access, _ = check_automation_access(user_id)
    if not has_access:
        await message.answer("❌ Нет доступа к автоматизации")
        return
    
    status = get_automation_status(user_id)
    
    # Если обе функции выключены или включены - включаем обе
    # Если разные - включаем обе
    new_taxes = not (status["auto_taxes"] and status["auto_work"])
    new_work = not (status["auto_taxes"] and status["auto_work"])
    
    update_automation_setting(user_id, "taxes", new_taxes)
    update_automation_setting(user_id, "work", new_work)
    
    if new_taxes:
        await message.answer("✅ Все функции автоматизации включены")
    else:
        await message.answer("✅ Все функции автоматизации выключены")

@dp.message(Command("auto_taxes"))
async def auto_taxes_toggle(message: Message):
    """Переключить авто-налоги"""
    user_id = message.from_user.id
    
    has_access, _ = check_automation_access(user_id)
    if not has_access:
        await message.answer("❌ Нет доступа к автоматизации")
        return
    
    status = get_automation_status(user_id)
    new_value = not status["auto_taxes"]
    
    update_automation_setting(user_id, "taxes", new_value)
    
    if new_value:
        await message.answer("✅ Авто-налоги включены")
    else:
        await message.answer("✅ Авто-налоги выключены")

@dp.message(Command("auto_work"))
async def auto_work_toggle(message: Message):
    """Переключить авто-работу"""
    user_id = message.from_user.id
    
    has_access, _ = check_automation_access(user_id)
    if not has_access:
        await message.answer("❌ Нет доступа к автоматизации")
        return
    
    status = get_automation_status(user_id)
    new_value = not status["auto_work"]
    
    update_automation_setting(user_id, "work", new_value)
    
    if new_value:
        await message.answer("✅ Авто-работа включена")
    else:
        await message.answer("✅ Авто-работа выключена")
        
@dp.message(Command("grant_auto"))
async def grant_auto_command(message: Message):
    """Выдать доступ к автоматизации"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return
    
    try:
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer("Формат: /grant_auto [user_id] [дни]")
            return
        
        target_user_id = int(parts[1])
        days = int(parts[2])
        
        if grant_automation_access(target_user_id, days):
            await message.answer(f"✅ Доступ выдан на {days}д")
        else:
            await message.answer("❌ Ошибка")
            
    except Exception as e:
        await message.answer("❌ Ошибка формата")

@dp.message(Command("ban"))
async def ban_command(message: Message):
    """Глобальный бан пользователя (только для админов)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return

    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer('⚠️ Используйте: /ban (ID пользователя) [причина]')
            return

        user_id = int(args[1])
        reason = ' '.join(args[2:]) if len(args) > 2 else "Глобальный бан"

        if user_id in ADMINS:
            await message.answer('❌ Нельзя забанить администратора')
            return

        # Проверяем, не забанен ли уже
        cursor.execute('SELECT user_id FROM banned_users WHERE user_id = ?', (user_id,))
        if cursor.fetchone():
            await message.answer('⚠️ Пользователь уже забанен')
            return

        # Баним пользователя
        cursor.execute(
            'INSERT INTO banned_users (user_id, banned_by, reason) VALUES (?, ?, ?)',
            (user_id, message.from_user.id, reason)
        )

        # Обнуляем все данные пользователя
        cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        cursor.execute('DELETE FROM user_cards WHERE user_id = ?', (user_id,))
        cursor.execute('DELETE FROM user_businesses WHERE user_id = ?', (user_id,))
        cursor.execute('DELETE FROM user_work_stats WHERE user_id = ?', (user_id,))
        cursor.execute('DELETE FROM user_bp_progress WHERE user_id = ?', (user_id,))
        conn.commit()

        await message.answer(
            f'✅ Пользователь {user_id} забанен\n'
            f'Причина: {reason}\n'
            f'Все данные удалены'
        )

        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                f'🚫 Вы заблокированы\n'
                f'Причина: {reason}\n\n'
                f'Все ваши данные удалены. Вы не можете использовать бота.'
            )
        except:
            pass

    except ValueError:
        await message.answer('❌ ID должен быть числом')
    except Exception as e:
        logger.error(f"Error in ban_command: {e}")
        await message.answer(f'❌ Ошибка: {str(e)}')

@dp.message(Command("unban"))
async def unban_command(message: Message):
    """Разбан пользователя (только для админов)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return

    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer('⚠️ Используйте: /unban (ID пользователя)')
            return

        user_id = int(args[1])

        # Проверяем, забанен ли пользователь
        cursor.execute('SELECT user_id, reason FROM banned_users WHERE user_id = ?', (user_id,))
        if not cursor.fetchone():
            await message.answer('⚠️ Пользователь не забанен')
            return

        # Разбаниваем
        cursor.execute('DELETE FROM banned_users WHERE user_id = ?', (user_id,))
        conn.commit()

        await message.answer(f'✅ Пользователь {user_id} разбанен')

        # Уведомляем пользователя
        try:
            await bot.send_message(
                user_id,
                '✅ Вы разблокированы!\n'
                'Теперь вы можете снова использовать бота.\n'
                'Начните с /start'
            )
        except:
            pass

    except ValueError:
        await message.answer('❌ ID должен быть числом')
    except Exception as e:
        logger.error(f"Error in unban_command: {e}")
        await message.answer(f'❌ Ошибка: {str(e)}')

# Функции автоматических операций
async def process_auto_taxes(user_id: int) -> bool:
    """Автоматически оплатить налоги"""
    try:
        user = get_user(user_id)
        if not user:
            return False
        
        tax_info = get_user_tax_info(user_id)
        if not tax_info:
            return True
        
        business_tax = tax_info.get("business_tax_debt", 0)
        farm_tax = tax_info.get("farm_tax_debt", 0)
        total_tax = business_tax + farm_tax
        
        if total_tax <= 0:
            return True
        
        if user[2] >= total_tax:
            new_usd = user[2] - total_tax
            update_balance(user_id, usd=new_usd)
            update_tax_info(user_id, business_tax=0, farm_tax=0, farm_penalty=False)
            remove_farm_penalty(user_id)
            cursor.execute('UPDATE user_automation SET last_tax_payment = ? WHERE user_id = ?', (datetime.now().isoformat(), user_id))
            conn.commit()
            return True
        return False
        
    except Exception as e:
        logger.error(f"Error processing auto taxes: {e}")
        return False

async def process_auto_work(user_id: int) -> bool:
    """Автоматически выполнить работу"""
    try:
        experience, last_work_time = get_user_work_stats(user_id)
        
        if last_work_time:
            next_work_time = last_work_time + timedelta(hours=1)
            if datetime.now() < next_work_time:
                return False
        
        current_job = None
        for job in reversed(WORK_JOBS):
            if job['min_exp'] <= experience:
                current_job = job
                break

        if not current_job:
            return False
        
        reward = current_job['reward']
        user = get_user(user_id)
        new_usd = user[2] + reward
        
        cursor.execute('UPDATE users SET usd_balance = ? WHERE user_id = ?', (new_usd, user_id))
        cursor.execute('UPDATE user_work_stats SET total_experience = total_experience + 1, last_work_time = ? WHERE user_id = ?', 
                     (datetime.now().isoformat(), user_id))
        cursor.execute('UPDATE user_automation SET last_work_time = ? WHERE user_id = ?', (datetime.now().isoformat(), user_id))
        conn.commit()
        return True
        
    except Exception as e:
        logger.error(f"Error processing auto work: {e}")
        return False

async def automation_scheduler():
    """Планировщик автоматизации с проверкой истекшего доступа"""
    while True:
        try:
            now = datetime.now()
            current_minute = now.minute
            
            process_taxes = current_minute in [1, 31]
            process_work = current_minute == 1
            
            if process_taxes or process_work:
                # Сначала проверяем истекшие доступы и отключаем их
                cursor.execute('''
                SELECT aa.user_id 
                FROM automation_access aa
                JOIN user_automation ua ON aa.user_id = ua.user_id
                WHERE aa.access_until < ? AND (ua.auto_taxes = TRUE OR ua.auto_work = TRUE)
                ''', (now.isoformat(),))
                
                expired_users = cursor.fetchall()
                
                for (user_id,) in expired_users:
                    try:
                        # Отключаем все функции автоматизации
                        cursor.execute('''
                        UPDATE user_automation 
                        SET auto_taxes = FALSE, auto_work = FALSE 
                        WHERE user_id = ?
                        ''', (user_id,))
                        
                        # Удаляем доступ (опционально - можно оставить для истории)
                        cursor.execute('DELETE FROM automation_access WHERE user_id = ?', (user_id,))
                        
                        logger.info(f"Automation access expired for user {user_id}, functions disabled")
                        
                        # Отправляем уведомление пользователю
                        try:
                            await bot.send_message(
                                chat_id=user_id,
                                text="🔴 <b>Срок доступа к автоматизации истек</b>\n\n"
                                     "Все функции автоматизации были отключены.\n"
                                     "Для продления доступа обратитесь к администратору.",
                                parse_mode='HTML'
                            )
                        except Exception as e:
                            logger.error(f"Error sending expiration notification to user {user_id}: {e}")
                            
                    except Exception as e:
                        logger.error(f"Error processing expired automation for user {user_id}: {e}")
                        continue
                
                conn.commit()
                
                # Теперь обрабатываем активных пользователей
                cursor.execute('''
                SELECT ua.user_id 
                FROM user_automation ua
                JOIN automation_access aa ON ua.user_id = aa.user_id
                WHERE aa.access_until >= ? 
                AND ((ua.auto_taxes = TRUE AND ? = 1) OR (ua.auto_work = TRUE AND ? = 1))
                ''', (now.isoformat(), process_taxes, process_work))
                
                for (user_id,) in cursor.fetchall():
                    try:
                        if process_taxes:
                            await process_auto_taxes(user_id)
                        if process_work:
                            await process_auto_work(user_id)
                        await asyncio.sleep(0.3)
                    except Exception as e:
                        logger.error(f"Error processing user {user_id}: {e}")
                        continue
            
            # Ждем до следующей минуты
            next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            sleep_seconds = (next_run - now).total_seconds()
            await asyncio.sleep(sleep_seconds)
            
        except Exception as e:
            logger.error(f"Error in automation scheduler: {e}")
            await asyncio.sleep(60)
            
async def check_expired_automation_access():
    """Проверяет и отключает истекшие доступы к автоматизации"""
    try:
        now = datetime.now()
        
        cursor.execute('''
        SELECT aa.user_id 
        FROM automation_access aa
        JOIN user_automation ua ON aa.user_id = ua.user_id
        WHERE aa.access_until < ? AND (ua.auto_taxes = TRUE OR ua.auto_work = TRUE)
        ''', (now.isoformat(),))
        
        expired_users = cursor.fetchall()
        
        disabled_count = 0
        for (user_id,) in expired_users:
            try:
                # Отключаем все функции автоматизации
                cursor.execute('''
                UPDATE user_automation 
                SET auto_taxes = FALSE, auto_work = FALSE 
                WHERE user_id = ?
                ''', (user_id,))
                
                # Удаляем доступ
                cursor.execute('DELETE FROM automation_access WHERE user_id = ?', (user_id,))
                
                disabled_count += 1
                logger.info(f"Automation access expired for user {user_id}, functions disabled")
                
            except Exception as e:
                logger.error(f"Error disabling automation for user {user_id}: {e}")
                continue
        
        conn.commit()
        
        if disabled_count > 0:
            logger.info(f"Expired automation access: {disabled_count} users disabled")
            
        return disabled_count
        
    except Exception as e:
        logger.error(f"Error checking expired automation access: {e}")
        return 0

async def start_automation_scheduler():
    """Запустить планировщик"""
    asyncio.create_task(automation_scheduler())

@dp.message(Command("give_money"))
async def give_money_command(message: Message):
    """Команда для выдачи денег пользователю (только для админа)"""
    # Проверяем, что команду вызывает админ
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Проверяем, есть ли ответ на сообщение
        if not message.reply_to_message:
            await message.answer(
                "❌ Используйте команду в ответ на сообщение пользователя, которому хотите выдать деньги\n\n"
                "Формат:\n"
                "/give_money [сумма]\n\n"
                "Пример:\n"
                "/give_money 1000000"
            )
            return
        
        # Получаем ID пользователя, которому выдаем деньги
        target_user_id = message.reply_to_message.from_user.id
        
        # Проверяем, зарегистрирован ли пользователь
        target_user = get_user(target_user_id)
        if not target_user:
            await message.answer("❌ Пользователь не найден. Сначала он должен зарегистрироваться с помощью /start")
            return
        
        # Получаем сумму из команды
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer(
                "❌ Не указана сумма\n\n"
                "Формат:\n"
                "/give_money [сумма]\n\n"
                "Пример:\n"
                "/give_money 1000000"
            )
            return
        
        try:
            amount = float(parts[1])
            if amount <= 0:
                await message.answer("❌ Сумма должна быть положительной")
                return
                
            if amount > 1000000000000:  # Ограничение на максимальную сумму (1 триллион)
                await message.answer("❌ Слишком большая сумма")
                return
                
        except ValueError:
            await message.answer("❌ Неверный формат суммы. Используйте число")
            return
        
        # Выдаем деньги пользователю
        current_balance = target_user[2]
        new_balance = current_balance + amount
        update_balance(target_user_id, usd=new_balance)
        
        # Получаем информацию о пользователе для красивого ответа
        target_user_info = await bot.get_chat(target_user_id)
        target_name = target_user_info.full_name
        
        # Формируем ответ
        success_message = (
            f"✅ <b>Деньги успешно выданы!</b>\n\n"
            f"👤 Получатель: {target_name}\n"
            f"🆔 ID: {target_user_id}\n"
            f"💰 Сумма: ${format_number(amount, True)}\n"
            f"💳 Новый баланс: ${format_number(new_balance, True)}"
        )
        
        await message.answer(success_message, parse_mode='HTML')
        
        # Также уведомляем получателя, если это не тот же пользователь
        if target_user_id != message.from_user.id:
            try:
                user_notification = (
                    f"🎉 <b>Вам выданы деньги!</b>\n\n"
                    f"💰 Сумма: ${format_number(amount, True)}\n"
                    f"💳 Ваш баланс: ${format_number(new_balance, True)}\n\n"
                    f"💸 Приятного использования!"
                )
                await bot.send_message(chat_id=target_user_id, text=user_notification, parse_mode='HTML')
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
    except Exception as e:
        logger.error(f"Error in give_money command: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")
        
def remove_expired_viruses():
    """Удаляет истекшие вирусы"""
    try:
        now = datetime.now()
        cursor.execute('''
        SELECT id, user_id, start_time, duration_hours 
        FROM user_viruses 
        WHERE status = 'active'
        ''')
        viruses = cursor.fetchall()
        
        for virus_id, user_id, start_time, duration_hours in viruses:
            start_dt = datetime.fromisoformat(start_time)
            end_dt = start_dt + timedelta(hours=duration_hours)
            
            if now >= end_dt:
                cursor.execute('''
                UPDATE user_viruses 
                SET status = 'expired' 
                WHERE id = ?
                ''', (virus_id,))
                logger.info(f"Virus expired for user {user_id}")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error removing expired viruses: {e}")
        conn.rollback()

def calculate_virus_penalty(user_id: int) -> float:
    """Рассчитывает штраф от вируса (0.0 - нет штрафа, 0.3 - 30% штраф)"""
    virus = get_active_virus(user_id)
    if virus:
        return virus["penalty"]
    return 0.0

def get_virus_time_remaining(user_id: int) -> Optional[Tuple[int, int]]:
    """Возвращает оставшееся время вируса в часах и минутах"""
    virus = get_active_virus(user_id)
    if not virus:
        return None
    
    start_dt = datetime.fromisoformat(virus["start_time"])
    end_dt = start_dt + timedelta(hours=virus["duration_hours"])
    now = datetime.now()
    
    if now >= end_dt:
        return None
    
    remaining = end_dt - now
    hours = remaining.seconds // 3600
    minutes = (remaining.seconds % 3600) // 60
    
    return hours, minutes

async def try_activate_virus(user_id: int, guaranteed: bool = False) -> bool:
    """Пытается активировать вирус с шансом 3% или гарантированно"""
    try:
        # Проверяем, нет ли уже активного вируса
        if get_active_virus(user_id):
            return False  # Пропускаем, если вирус уже активен
        
        # Гарантированная активация или 3% шанс
        if guaranteed or random.random() <= 0.02:
            # Случайная продолжительность 1-6 часов
            duration_hours = random.randint(1, 6)
            start_time = datetime.now().isoformat()
            
            # Добавляем вирус (штраф по умолчанию 30%)
            cursor.execute('''
            INSERT INTO user_viruses (user_id, start_time, duration_hours, income_penalty, status)
            VALUES (?, ?, ?, 0.3, 'active')
            ''', (user_id, start_time, duration_hours))
            conn.commit()
            
            # Пересчитываем доход
            calculate_income(user_id)
            
            # Отправляем уведомление пользователю
            try:
                virus_message = (
                    "🦠 <b>ВНИМАНИЕ! Обнаружен вирус!</b>\n\n"
                    f"Ваша ферма заражена! В течение {duration_hours} часов "
                    f"доход будет снижен на 30%.\n\n"
                    "💡 Вирус исчезнет автоматически по истечении времени."
                )
                await bot.send_message(chat_id=user_id, text=virus_message, parse_mode='HTML')
            except Exception as e:
                logger.error(f"Error sending virus notification: {e}")
            
            logger.info(f"Virus activated for user {user_id}, duration: {duration_hours}h")
            return True
        
        return False
    except Exception as e:
        logger.error(f"Error in try_activate_virus: {e}")
        return False

def check_expired_viruses():
    """Проверяет истекшие вирусы и меняет их статус"""
    try:
        # Находим истекшие вирусы
        cursor.execute('''
        SELECT id, user_id, start_time, duration_hours 
        FROM user_viruses 
        WHERE status = 'active'
        ''')
        
        active_viruses = cursor.fetchall()
        expired_count = 0
        
        for virus_id, user_id, start_time, duration_hours in active_viruses:
            try:
                # Проверяем, истек ли вирус
                start_dt = datetime.fromisoformat(start_time)
                end_dt = start_dt + timedelta(hours=duration_hours)
                
                if datetime.now() >= end_dt:
                    # Меняем статус вируса на expired
                    cursor.execute('''
                    UPDATE user_viruses SET status = 'expired' WHERE id = ?
                    ''', (virus_id,))
                    
                    # Пересчитываем доход пользователя
                    calculate_income(user_id)
                    expired_count += 1
                    
                    logger.info(f"Virus {virus_id} expired for user {user_id}")
                    
            except Exception as e:
                logger.error(f"Error processing virus {virus_id}: {e}")
                continue
        
        conn.commit()
        logger.info(f"Expired viruses check completed: {expired_count} viruses expired")
        
    except Exception as e:
        logger.error(f"Error checking expired viruses: {e}")

async def virus_checker():
    """Фоновая задача для проверки и активации вирусов каждый час"""
    while True:
        try:
            # Проверяем истекшие вирусы
            check_expired_viruses()
            
            # Удаляем пользователей с истекшим антивирусом
            cursor.execute('''
                DELETE FROM antivirus_users 
                WHERE antivirus_until < datetime('now')
            ''')
            deleted_antivirus_count = cursor.rowcount
            if deleted_antivirus_count > 0:
                conn.commit()
                logger.info(f"Removed {deleted_antivirus_count} expired antivirus users")
            
            # Получаем всех активных пользователей (были активны в последние 24 часа)
            twenty_four_hours_ago = (datetime.now() - timedelta(hours=24)).isoformat()
            cursor.execute('''
            SELECT DISTINCT user_id FROM users 
            WHERE last_income_time > ? OR last_income_time IS NOT NULL
            ''', (twenty_four_hours_ago,))
            
            active_users = cursor.fetchall()
            
            logger.info(f"Hourly virus check: processing {len(active_users)} active users")
            
            virus_activated_count = 0
            
            # Для каждого активного пользователя пытаемся активировать вирус с шансом 3%
            for (user_id,) in active_users:
                try:
                    # Пропускаем пользователей с активным антивирусом
                    if has_active_antivirus(user_id):
                        continue
                    
                    # Пытаемся активировать вирус с 3% шансом
                    success = await try_activate_virus(user_id, guaranteed=False)
                    if success:
                        virus_activated_count += 1
                    
                    # Небольшая задержка между пользователями
                    await asyncio.sleep(0.05)
                    
                except Exception as e:
                    logger.error(f"Error processing virus for user {user_id}: {e}")
                    continue
            
            logger.info(f"Virus check completed. Viruses activated: {virus_activated_count}/{len(active_users)}")
            
        except Exception as e:
            logger.error(f"Error in virus checker: {e}")
        
        # Ждем 1 час до следующей проверки
        await asyncio.sleep(3600)

async def start_virus_checker():
    """Запускает фоновую задачу проверки вирусов"""
    asyncio.create_task(virus_checker())

    
@dp.message(Command("virus"))
async def virus_status(message: Message):
    """Показывает статус вируса пользователя"""
    user_id = message.from_user.id
    
    # Проверяем активный антивирус
    has_antivirus, antivirus_until = check_antivirus_access(user_id)
    
    if has_antivirus:
        # Пользователь с антивирусом
        remaining = antivirus_until - datetime.now()
        days = remaining.days
        hours = remaining.seconds // 3600
        minutes = (remaining.seconds % 3600) // 60
        
        text = (
            "✅ <b>СТАТУС ВИРУСА</b>\n\n"
            "🟢 У Вас куплен антивирус, поэтому, Ваша ферма защищена\n"
            f"⏰ Срок антивируса: {days}д {hours}ч {minutes}м\n\n"
            "ℹ️ Каждый час есть 3% шанс поймать вирус, который снизит доход на 30% на 1-12 часов."
        )
    else:
        # Проверяем активный вирус
        virus = get_active_virus(user_id)
        virus_time = get_virus_time_remaining(user_id)
        
        if virus and virus_time:
            hours, minutes = virus_time
            start_dt = datetime.fromisoformat(virus["start_time"])
            end_dt = start_dt + timedelta(hours=virus["duration_hours"])
            
            text = (
                "🦠 <b>СТАТУС ВИРУСА</b>\n\n"
                f"⚠️ <b>Ваша ферма заражена!</b>\n\n"
                f"📉 Штраф к доходу: {int(virus['penalty'] * 100)}%\n"
                f"⏰ Осталось времени: {hours}ч {minutes}м\n"
                f"🕐 Начало: {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
                f"⏳ Конец: {end_dt.strftime('%d.%m.%Y %H:%M')}\n\n"
                f"💡 Вирус исчезнет автоматически по истечении времени."
            )
        else:
            text = (
                "✅ <b>СТАТУС ВИРУСА</b>\n\n"
                "🟢 Ваша ферма чиста! Вирусов не обнаружено.\n\n"
                "ℹ️ Каждый час есть 10% шанс поймать вирус, который снизит доход на 30% на 1-12 часов."
            )
    
    await message.answer(text, parse_mode='HTML')
    
@dp.message(Command("test_virus"))
async def test_virus(message: Message):
    """Тестовая команда для активации вируса (только для админа)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        duration = random.randint(1, 12)
        
        if add_virus_to_user(user_id, duration):
            calculate_income(user_id)
            await message.answer(f"✅ Вирус активирован на {duration} часов")
        else:
            await message.answer("❌ Ошибка активации вируса")
            
    except Exception as e:
        logger.error(f"Error in test_virus: {e}")
        await message.answer("❌ Произошла ошибка")

# Добавляем таблицу для отслеживания визитов скупщика
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_scavenger_visits (
    user_id INTEGER PRIMARY KEY,
    last_visit_date TEXT,
    visits_today INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()


#НАЧАЛО ИВЕНТА

# Добавляем после других CREATE TABLE
cursor.execute('''
CREATE TABLE IF NOT EXISTS event_currency (
    user_id INTEGER PRIMARY KEY,
    balance INTEGER DEFAULT 0,
    earned_balance INTEGER DEFAULT 0,
    last_updated TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS event_rewards_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    reward_type TEXT,
    reward_value TEXT,
    amount INTEGER,
    created_at TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS event_top_winners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    place INTEGER,
    season INTEGER DEFAULT 1,
    created_at TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

conn.commit()


# Функции для работы с ивент валютой
def get_event_currency(user_id: int) -> int:
    """Получить баланс ивент валюты пользователя"""
    try:
        cursor.execute('SELECT balance, earned_balance FROM event_currency WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            return result[0]  # balance
        else:
            # Создаем запись если нет
            cursor.execute('INSERT INTO event_currency (user_id, balance, earned_balance) VALUES (?, 0, 0)', (user_id,))
            conn.commit()
            return 0
    except Exception as e:
        logger.error(f"Error getting event currency: {e}")
        return 0

def get_earned_event_currency(user_id: int) -> int:
    """Получить заработанную валюту (для топа)"""
    try:
        cursor.execute('SELECT earned_balance FROM event_currency WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error getting earned event currency: {e}")
        return 0

def add_event_currency(user_id: int, amount: int, is_earned: bool = True):
    """Добавить ивент валюту"""
    try:
        current_balance = get_event_currency(user_id)
        new_balance = current_balance + amount
        
        if is_earned:
            current_earned = get_earned_event_currency(user_id)
            new_earned = current_earned + amount
            cursor.execute('''
            INSERT OR REPLACE INTO event_currency (user_id, balance, earned_balance, last_updated)
            VALUES (?, ?, ?, ?)
            ''', (user_id, new_balance, new_earned, datetime.now().isoformat()))
        else:
            cursor.execute('''
            INSERT OR REPLACE INTO event_currency (user_id, balance, earned_balance, last_updated)
            VALUES (?, ?, COALESCE((SELECT earned_balance FROM event_currency WHERE user_id = ?), 0), ?)
            ''', (user_id, new_balance, user_id, datetime.now().isoformat()))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding event currency: {e}")
        conn.rollback()
        return False

def spend_event_currency(user_id: int, amount: int) -> bool:
    """Потратить ивент валюту"""
    try:
        current_balance = get_event_currency(user_id)
        if current_balance < amount:
            return False
        
        new_balance = current_balance - amount
        cursor.execute('''
        UPDATE event_currency 
        SET balance = ?, last_updated = ?
        WHERE user_id = ?
        ''', (new_balance, datetime.now().isoformat(), user_id))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error spending event currency: {e}")
        conn.rollback()
        return False

def try_give_event_currency(user_id: int, chance: float, amount: int = 1) -> bool:
    """Попытаться выдать ивент валюту с указанным шансом"""
    # Всегда возвращаем False - ивент завершен
    return False

# Специальные функции для разных шансов
def try_give_100percent(user_id: int, amount: int = 1) -> bool:
    """0% шанс выдачи валюты"""
    return False  # Всегда возвращаем False

def try_give_5percent(user_id: int, amount: int = 1) -> bool:
    """0% шанс выдачи валюты"""
    return False  # Всегда возвращаем False

def try_give_1percent(user_id: int, amount: int = 1) -> bool:
    """0% шанс выдачи валюты"""
    return False  # Всегда возвращаем False

def distribute_event_top_rewards():
    """Выдать награды топу игроков"""
    try:
        top_players = get_event_top(10)
        now = datetime.now().isoformat()
        
        for i, (user_id, username, earned) in enumerate(top_players, 1):
            if i == 1:
                # 1 место: 7 дней премиума + префикс
                premium_until = (datetime.now() + timedelta(days=7)).isoformat()
                cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)', 
                             (user_id, premium_until))
                # Записываем в историю победителей
                cursor.execute('INSERT INTO event_top_winners (user_id, place, created_at) VALUES (?, ?, ?)',
                             (user_id, 1, now))
                
            elif i == 2:
                # 2 место: 4 дня премиума + префикс
                premium_until = (datetime.now() + timedelta(days=4)).isoformat()
                cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)',
                             (user_id, premium_until))
                cursor.execute('INSERT INTO event_top_winners (user_id, place, created_at) VALUES (?, ?, ?)',
                             (user_id, 2, now))
                
            elif i == 3:
                # 3 место: 2 дня премиума + префикс
                premium_until = (datetime.now() + timedelta(days=2)).isoformat()
                cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)',
                             (user_id, premium_until))
                cursor.execute('INSERT INTO event_top_winners (user_id, place, created_at) VALUES (?, ?, ?)',
                             (user_id, 3, now))
                
            else:
                # 4-10 места: 1 день премиума
                premium_until = (datetime.now() + timedelta(days=1)).isoformat()
                cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)',
                             (user_id, premium_until))
                cursor.execute('INSERT INTO event_top_winners (user_id, place, created_at) VALUES (?, ?, ?)',
                             (user_id, i, now))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error distributing event top rewards: {e}")
        conn.rollback()
        return False
    
def open_event_case(user_id: int) -> Tuple[bool, str]:
    """Открыть ивент кейс"""
    try:
        # Проверяем баланс
        if get_event_currency(user_id) < 5:
            return False, "У вас недостаточно 🍁"
        
        # Списываем валюту
        if not spend_event_currency(user_id, 5):
            return False, "Ошибка при списании валюты"
        
        # Определяем награду
        reward = calculate_case_reward(user_id)
        
        # Выдаем награду
        success, reward_message = give_case_reward(user_id, reward)
        
        if success:
            # Логируем награду
            cursor.execute('''
            INSERT INTO event_rewards_log (user_id, reward_type, reward_value, amount, created_at)
            VALUES (?, ?, ?, ?, ?)
            ''', (user_id, reward['type'], str(reward['value']), 1, datetime.now().isoformat()))
            conn.commit()
            
            return True, f"Вы открыли осенний кейс и выбили с него:\n\n{reward_message}"
        else:
            # Возвращаем валюту если ошибка
            add_event_currency(user_id, 5, is_earned=False)
            return False, "Произошла ошибка при выдаче награды"
            
    except Exception as e:
        logger.error(f"Error opening event case: {e}")
        return False, "Произошла ошибка при открытии кейса"

def calculate_case_reward(user_id: int) -> Dict:
    """Рассчитать награду из кейса"""
    rand = random.random()
    
    if rand <= 0.30:  # 30% - доход фермы
        hours = random.randint(1, 24)
        return {"type": "farm_income", "value": hours}
    
    elif rand <= 0.90:  # 30% - доход работы
        hours = random.randint(1, 24)
        return {"type": "work_income", "value": hours}
    
    elif rand <= 0.93:  # 3% - обнуление бонуса
        return {"type": "reset_bonus", "value": 1}
    
    elif rand <= 0.95:  # 2% - последняя видеокарта
        return {"type": "last_card", "value": 1}
    
    elif rand <= 0.97:  # 2% - ивент валюта
        amount = random.randint(1, 10)
        return {"type": "event_currency", "value": amount}
    
    elif rand <= 0.98:  # 1% - премиум
        days = random.randint(1, 3)
        return {"type": "premium", "value": days}
    
    elif rand <= 0.99:  # 1% - уборщик
        days = random.randint(1, 3)
        return {"type": "cleaner", "value": days}
    
    else:  # 1% - инвестор
        days = random.randint(1, 3)
        return {"type": "investor", "value": days}

def give_case_reward(user_id: int, reward: Dict) -> Tuple[bool, str]:
    """Выдать награду из кейса"""
    try:
        reward_type = reward['type']
        value = reward['value']
        
        if reward_type == "farm_income":
            return give_farm_income_reward(user_id, value)
        elif reward_type == "work_income":
            return give_work_income_reward(user_id, value)
        elif reward_type == "reset_bonus":
            return reset_daily_bonus(user_id)
        elif reward_type == "last_card":
            return give_last_card_or_money(user_id)
        elif reward_type == "event_currency":
            add_event_currency(user_id, value, is_earned=False)
            return True, f"{value} 🍁"
        elif reward_type == "premium":
            return give_premium_reward(user_id, value)
        elif reward_type == "cleaner":
            return give_cleaner_reward(user_id, value)
        elif reward_type == "investor":
            return give_investor_reward(user_id, value)
        else:
            return False, "Неизвестный тип награды"
    except Exception as e:
        logger.error(f"Error giving case reward: {e}")
        return False, "Ошибка выдачи награды"

# Функции для конкретных наград
def give_farm_income_reward(user_id: int, hours: int) -> Tuple[bool, str]:
    """Выдать доход фермы за N часов"""
    try:
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        farm_income = calculate_income(user_id) * 6 * hours
        new_btc = user[3] + farm_income
        update_balance(user_id, btc=new_btc, btc_delta=farm_income)

        return True, f"{hours} часов дохода фермы ({format_number(farm_income)} BTC)"
    except Exception as e:
        logger.error(f"Error giving farm income reward: {e}")
        return False, "Ошибка выдачи дохода фермы"

def give_business_income_reward(user_id: int, hours: int) -> Tuple[bool, str]:
    """Выдать доход бизнесов за N часов"""
    try:
        businesses = get_user_businesses(user_id)
        if not businesses:
            return False, "У вас нет бизнесов"
        
        total_income = 0
        for business in businesses:
            business_id, level, last_income_time = business
            biz_info = next((b for b in BUSINESSES if b['id'] == business_id), None)
            if biz_info:
                income = calculate_business_income(business_id, level)
                total_income += income * hours
        
        user = get_user(user_id)
        new_usd = user[2] + total_income
        update_balance(user_id, usd=new_usd, usd_delta=total_income)

        return True, f"{hours} часов дохода бизнесов (${format_number(total_income, True)})"
    except Exception as e:
        logger.error(f"Error giving business income reward: {e}")
        return False, "Ошибка выдачи дохода бизнесов"

def give_work_income_reward(user_id: int, hours: int) -> Tuple[bool, str]:
    """Выдать доход работы за N часов"""
    try:
        experience, _ = get_user_work_stats(user_id)
        current_job = None

        for job in reversed(WORK_JOBS):
            if job['min_exp'] <= experience:
                current_job = job
                break

        if not current_job:
            return False, "У вас нет доступной работы"
        
        total_income = current_job['reward'] * hours
        user = get_user(user_id)
        new_usd = user[2] + total_income
        update_balance(user_id, usd=new_usd, usd_delta=total_income)

        return True, f"{hours} часов дохода работы ({current_job['name']} - ${format_number(total_income, True)})"
    except Exception as e:
        logger.error(f"Error giving work income reward: {e}")
        return False, "Ошибка выдачи дохода работы"

def reset_daily_bonus(user_id: int) -> Tuple[bool, str]:
    """Обнулить время ежедневного бонуса"""
    try:
        cursor.execute('DELETE FROM daily_bonus_claims WHERE user_id = ?', (user_id,))
        conn.commit()
        return True, "Обнуление времени ежедневного бонуса"
    except Exception as e:
        logger.error(f"Error resetting daily bonus: {e}")
        return False, "Ошибка обнуления бонуса"

def give_last_card_or_money(user_id: int) -> Tuple[bool, str]:
    """Выдать последнюю доступную видеокарту или деньги"""
    try:
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        expansion_info = EXPANSIONS[user[5] - 1]
        last_card_id = expansion_info["last_available_card"]
        last_card = next((card for card in GRAPHICS_CARDS if card["id"] == last_card_id), None)
        
        if not last_card:
            return False, "Карта не найдена"
        
        # Проверяем свободные слоты
        user_cards, total_cards = get_user_cards(user_id)
        max_cards = expansion_info['max_cards']
        
        if total_cards < max_cards:
            # Есть место - даем карту
            add_user_card(user_id, last_card_id)
            calculate_income(user_id)
            return True, f"Видеокарта: {last_card['name']}"
        else:
            # Нет места - начисляем 70% стоимости
            compensation = int(last_card['price'] * 0.7)
            current_balance = get_user(user_id)[2]
            new_balance = current_balance + compensation
            update_balance(user_id, usd=new_balance)
            return True, f"Компенсация за карту: ${format_number(compensation, True)}"
            
    except Exception as e:
        logger.error(f"Error giving last card: {e}")
        return False, "Ошибка выдачи видеокарты"

def give_premium_reward(user_id: int, days: int) -> Tuple[bool, str]:
    """Выдать премиум статус"""
    try:
        cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            current_until = datetime.fromisoformat(result[0])
            new_until = current_until + timedelta(days=days)
        else:
            new_until = datetime.now() + timedelta(days=days)
        
        premium_until = new_until.isoformat()
        cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)', 
                     (user_id, premium_until))
        conn.commit()
        return True, f"Премиум статус на {days} дней"
    except Exception as e:
        logger.error(f"Error giving premium reward: {e}")
        return False, "Ошибка выдачи премиума"

def give_cleaner_reward(user_id: int, days: int) -> Tuple[bool, str]:
    """Выдать уборщика фермы"""
    try:
        cursor.execute('SELECT until FROM user_boosters WHERE user_id = ? AND booster_type = "cleaner"', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            current_until = datetime.fromisoformat(result[0])
            new_until = current_until + timedelta(days=days)
        else:
            new_until = datetime.now() + timedelta(days=days)
        
        cleaner_until = new_until.isoformat()
        cursor.execute('INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until) VALUES (?, "cleaner", ?)', 
                     (user_id, cleaner_until))
        conn.commit()
        return True, f"Уборщик фермы на {days} дней"
    except Exception as e:
        logger.error(f"Error giving cleaner reward: {e}")
        return False, "Ошибка выдачи уборщика"

def give_investor_reward(user_id: int, days: int) -> Tuple[bool, str]:
    """Выдать инвестора бизнесов"""
    try:
        cursor.execute('SELECT until FROM user_boosters WHERE user_id = ? AND booster_type = "investor"', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            current_until = datetime.fromisoformat(result[0])
            new_until = current_until + timedelta(days=days)
        else:
            new_until = datetime.now() + timedelta(days=days)
        
        investor_until = new_until.isoformat()
        cursor.execute('INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until) VALUES (?, "investor", ?)', 
                     (user_id, investor_until))
        conn.commit()
        return True, f"Инвестор бизнесов на {days} дней"
    except Exception as e:
        logger.error(f"Error giving investor reward: {e}")
        return False, "Ошибка выдачи инвестора"

def get_event_top(limit: int = 10) -> List[Tuple]:
    """Получить топ игроков по заработанной валюте"""
    try:
        cursor.execute('''
        SELECT u.user_id, u.username, ec.earned_balance 
        FROM event_currency ec
        JOIN users u ON ec.user_id = u.user_id
        WHERE ec.earned_balance > 0
        ORDER BY ec.earned_balance DESC
        LIMIT ?
        ''', (limit,))
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting event top: {e}")
        return []

# @dp.message(Command("top_ivent"))
async def top_ivent_command(message: Message):
    """Показать топ игроков по ивент валюте"""
    try:
        top_players = get_event_top(10)
        
        if not top_players:
            await message.answer("🏆 Топ лучших игроков по заработку 🎃\n\nПока никто не заработал ивент валюту!")
            return
        
        text = "🏆 Топ лучших игроков по заработку 🎃\n\n"
        
        medals = ["🥇", "🥈", "🥉"]
        for i, (user_id, username, earned) in enumerate(top_players, 1):
            # Получаем полное имя пользователя из Telegram
            try:
                user_chat = await bot.get_chat(user_id)
                display_name = user_chat.full_name
            except Exception:
                # Если не удалось получить имя, используем username или создаем анонимное имя
                if username:
                    display_name = f"{username}"
                else:
                    display_name = "Анонимный майнер"
            
            if i <= 3:
                medal = medals[i-1]
                text += f"{medal} {display_name} - {earned} 🎃\n"
            else:
                text += f"{i}. {display_name} - {earned} 🎃\n"
        
        # Получаем информацию о текущем пользователе для отображения его места
        user_rank = get_user_event_rank(message.from_user.id)
        user_earned = get_earned_event_currency(message.from_user.id)
        
        text += f"\n🎯 Ваше место: #{user_rank if user_rank > 0 else 'пока нет'}"
        text += f"\n💰 Ваша валюта за все время: {user_earned} 🎃"
        
        await message.answer(text)
        
    except Exception as e:
        logger.error(f"Error in top_ivent command: {e}")
        await message.answer("❌ Произошла ошибка при получении топа")
        
# @dp.message(Command("case_1"))
async def case_1_command(message: Message):
    """Открыть ивент кейс"""
    user_id = message.from_user.id
    
    success, result = open_event_case(user_id)
    if success:
        await message.answer(result)
    else:
        await message.answer(result)

def get_user_event_rank(user_id: int) -> int:
    """Получить место пользователя в топе"""
    try:
        cursor.execute('''
        SELECT rank FROM (
            SELECT user_id, RANK() OVER (ORDER BY earned_balance DESC) as rank
            FROM event_currency
            WHERE earned_balance > 0
        ) WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error getting user event rank: {e}")
        return 0




class ScavengerStates(StatesGroup):
    waiting_for_response = State()

# Константы для системы скупщика
SCAVENGER_CHANCE_SECOND_VISIT = 100  # 5% шанс на второй визит в день
SCAVENGER_GOOD_DEAL_CHANCE = 35   # 35% шанс на хорошую сделку (x2 цены)
SCAVENGER_BAD_DEAL_CHANCE = 65    # 65% шанс на плохую сделку (50% цены)

async def send_scavenger_visit(user_id: int):
    """Отправляет уведомление о визите скупщика"""
    try:
        # Проверяем, есть ли у пользователя видеокарты
        user_cards, total_cards = get_user_cards(user_id)
        if not user_cards or total_cards == 0:
            return False
        
        # Выбираем случайную видеокарту из фермы пользователя
        # Создаем список, где каждая карта представлена количеством раз, равным ее количеству у пользователя
        cards_pool = []
        for card_id, count in user_cards:
            cards_pool.extend([card_id] * count)
        
        if not cards_pool:
            return False
        
        selected_card_id = random.choice(cards_pool)
        card_info = next((c for c in GRAPHICS_CARDS if c['id'] == selected_card_id), None)
        
        if not card_info:
            return False
        
        # Создаем клавиатуру с кнопками
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да", callback_data=f"scavenger_yes_{user_id}_{selected_card_id}"),
                    InlineKeyboardButton(text="❌ Нет", callback_data=f"scavenger_no_{user_id}")
                ]
            ]
        )
        
        # Отправляем сообщение
        await bot.send_message(
            chat_id=user_id,
            text=f"🛒 К Вам пришёл скупщик и предлагает купить Вашу видеокарту <b>{card_info['name']}</b>. Хотите её продать?",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        # Обновляем статистику визитов
        update_scavenger_visit(user_id)
        
        return True
        
    except Exception as e:
        logger.error(f"Error sending scavenger visit: {e}")
        return False

def update_scavenger_visit(user_id: int):
    """Обновляет статистику визитов скупщика"""
    try:
        today = datetime.now().date().isoformat()
        
        cursor.execute('''
        SELECT last_visit_date, visits_today 
        FROM user_scavenger_visits 
        WHERE user_id = ?
        ''', (user_id,))
        
        result = cursor.fetchone()
        
        if result:
            last_visit_date, visits_today = result
            if last_visit_date == today:
                # Уже был визит сегодня
                new_visits = visits_today + 1
            else:
                # Новый день
                new_visits = 1
        else:
            # Первый визит
            new_visits = 1
        
        cursor.execute('''
        INSERT OR REPLACE INTO user_scavenger_visits 
        (user_id, last_visit_date, visits_today)
        VALUES (?, ?, ?)
        ''', (user_id, today, new_visits))
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error updating scavenger visit: {e}")
        conn.rollback()

def can_receive_scavenger_visit(user_id: int) -> bool:
    """Проверяет, может ли пользователь получить визит скупщика"""
    try:
        today = datetime.now().date().isoformat()
        
        cursor.execute('''
        SELECT last_visit_date, visits_today 
        FROM user_scavenger_visits 
        WHERE user_id = ?
        ''', (user_id,))
        
        result = cursor.fetchone()
        
        if not result:
            return True  # Никогда не получал визитов
        
        last_visit_date, visits_today = result
        
        if last_visit_date != today:
            return True  # Новый день - сбрасываем счетчик
        
        # Увеличиваем лимиты: 3 визита в день вместо 2
        if visits_today == 0:
            return True  # Может получить первый визит
        elif visits_today == 1:
            # 80% шанс на второй визит
            return random.randint(1, 100) <= 80
        elif visits_today == 2:
            # 50% шанс на третий визит
            return random.randint(1, 100) <= 50
        else:
            return False  # Максимум 3 визита в день
        
    except Exception as e:
        logger.error(f"Error checking scavenger visit possibility: {e}")
        return False
        
@dp.message(Command("scavenger_stats"))
async def scavenger_stats(message: Message):
    """Показывает статистику работы скупщика с временем до следующей рассылки"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        today = datetime.now().date().isoformat()
        now = datetime.now()
        
        # Статистика за сегодня
        cursor.execute('''
        SELECT COUNT(*) as total_visits, 
               COUNT(DISTINCT user_id) as unique_users,
               AVG(visits_today) as avg_visits_per_user
        FROM user_scavenger_visits 
        WHERE last_visit_date = ?
        ''', (today,))
        
        today_stats = cursor.fetchone()
        
        # Пользователи, которые еще не получили визит сегодня
        cursor.execute('''
        SELECT COUNT(*) 
        FROM users u
        WHERE u.last_income_time IS NOT NULL 
        AND datetime(u.last_income_time) > datetime('now', '-30 days')
        AND NOT EXISTS (
            SELECT 1 FROM user_scavenger_visits usv 
            WHERE usv.user_id = u.user_id AND usv.last_visit_date = ?
        )
        ''', (today,))
        
        users_without_visit = cursor.fetchone()[0]
        
        # Общая статистика
        cursor.execute('SELECT COUNT(*) FROM user_scavenger_visits')
        total_visits_all_time = cursor.fetchone()[0]
        
        # Время до следующей рассылки (каждые 30 минут)
        next_run_minutes = 30 - (now.minute % 30)
        next_run_seconds = 60 - now.second
        total_seconds = next_run_minutes * 60 + next_run_seconds
        
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        if hours > 0:
            next_run_str = f"{hours}ч {minutes}м {seconds}с"
        else:
            next_run_str = f"{minutes}м {seconds}с"
        
        text = (
            "📊 <b>Статистика скупщика</b>\n\n"
            f"📅 <b>Сегодня ({today}):</b>\n"
            f"   • Всего визитов: {today_stats[0] or 0}\n"
            f"   • Уникальных пользователей: {today_stats[1] or 0}\n"
            f"   • Среднее визитов на пользователя: {today_stats[2] or 0:.1f}\n"
            f"   • Ожидают визита: {users_without_visit}\n\n"
            f"📈 <b>За все время:</b>\n"
            f"   • Всего визитов: {total_visits_all_time}\n\n"
            f"🕒 <b>Следующая рассылка:</b> через {next_run_str}\n"
            f"⏰ <b>Расписание:</b> каждые 30 минут\n"
            f"👥 <b>Охват:</b> все активные пользователи"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in scavenger stats: {e}")
        await message.answer(f"❌ Ошибка при получении статистики: {str(e)}")
        
@dp.message(Command("scavenger_force_send"))
async def scavenger_force_send(message: Message):
    """Немедленная принудительная рассылка скупщика ВСЕМ пользователям"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Получаем всех активных пользователей
        cursor.execute('''
        SELECT user_id FROM users 
        WHERE last_income_time IS NOT NULL 
        AND datetime(last_income_time) > datetime('now', '-30 days')
        ''')
        active_users = cursor.fetchall()
        
        total_users = len(active_users)
        successful_visits = 0
        failed_visits = 0
        
        status_msg = await message.answer(
            f"🛒 <b>ЗАПУСК ПРИНУДИТЕЛЬНОЙ РАССЫЛКИ</b>\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"⏳ Начинаю отправку...",
            parse_mode='HTML'
        )
        
        # Обрабатываем всех пользователей БЕЗ задержек
        for i, (user_id,) in enumerate(active_users, 1):
            try:
                success = await send_scavenger_visit(user_id)
                if success:
                    successful_visits += 1
                else:
                    failed_visits += 1
                
                # Обновляем статус каждые 10 пользователей
                if i % 10 == 0:
                    progress = (i / total_users) * 100
                    await status_msg.edit_text(
                        f"🛒 <b>ПРИНУДИТЕЛЬНАЯ РАССЫЛКА</b>\n\n"
                        f"📊 Прогресс: {i}/{total_users} ({progress:.1f}%)\n"
                        f"✅ Успешно: {successful_visits}\n"
                        f"❌ Ошибок: {failed_visits}",
                        parse_mode='HTML'
                    )
                
                # Минимальная задержка чтобы не спамить
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error sending scavenger to user {user_id}: {e}")
                failed_visits += 1
                continue
        
        # Финальный отчет
        result_text = (
            f"🎯 <b>ПРИНУДИТЕЛЬНАЯ РАССЫЛКА ЗАВЕРШЕНА!</b>\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"✅ Успешных визитов: {successful_visits}\n"
            f"❌ Неудачных: {failed_visits}\n"
            f"📊 Эффективность: {(successful_visits/total_users*100):.1f}%\n\n"
            f"⏰ <b>Следующая автоматическая рассылка:</b> через 30 минут"
        )
        
        await status_msg.edit_text(result_text, parse_mode='HTML')
        
        # Логируем результат
        logger.info(f"🛒 FORCE SEND COMPLETED: {successful_visits}/{total_users} successful")
        
    except Exception as e:
        logger.error(f"Error in scavenger force send: {e}")
        await message.answer(f"❌ Ошибка при рассылке: {str(e)}")

async def process_scavenger_deal(user_id: int, card_id: int, accept: bool):
    """Обрабатывает сделку со скупщиком"""
    try:
        if not accept:
            # Пользователь отказался
            await bot.send_message(
                chat_id=user_id,
                text="😔 Вы отказались продавать свою видеокарту Скупщику!"
            )
            return
        
        # Пользователь согласился
        card_info = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        if not card_info:
            await bot.send_message(user_id, "❌ Ошибка: видеокарта не найдена")
            return
        
        # Проверяем, есть ли у пользователя эта видеокарта
        cursor.execute('SELECT id, count FROM user_cards WHERE user_id = ? AND card_id = ?', (user_id, card_id))
        card_data = cursor.fetchone()
        
        if not card_data or card_data[1] <= 0:
            await bot.send_message(user_id, "❌ Ошибка: у вас нет этой видеокарты")
            return
        
        # Определяем тип сделки
        deal_type = "good" if random.randint(1, 100) <= SCAVENGER_GOOD_DEAL_CHANCE else "bad"
        
        # Рассчитываем цену
        if deal_type == "good":
            # Хорошая сделка - x2 цены
            price = card_info['price'] * 2
            message = f"😮 Ого! Вам повезло и Скупщик купил видеокарту в два раза дороже от её начальной стоимости. Поздравляем🔥"
        else:
            # Плохая сделка - 50% цены
            price = card_info['price'] * 0.5
            message = f"😢 О нет! Скупщик обманул Вас и вы получили 50% от начальной стоимости видеокарты"
        
        # Округляем цену
        price = int(price)
        
        # Удаляем одну видеокарту у пользователя
        with conn:
            if card_data[1] > 1:
                cursor.execute('UPDATE user_cards SET count = count - 1 WHERE id = ?', (card_data[0],))
            else:
                cursor.execute('DELETE FROM user_cards WHERE id = ?', (card_data[0],))
            
            # Начисляем деньги
            user = get_user(user_id)
            new_usd = user[2] + price
            update_balance(user_id, usd=new_usd)
            
            # Пересчитываем доход
            calculate_income(user_id)
        
        # Добавляем информацию о выручке
        message += f"\n\n💰 Вы получили: ${format_number(price, True)}"
        message += f"\n\n💵 Новый баланс: ${format_number(new_usd, True)}"
        message += f"\n\n🎮 Удалена видеокарта: {card_info['name']}"
        
        await bot.send_message(user_id, message)
        
    except Exception as e:
        logger.error(f"Error processing scavenger deal: {e}")
        await bot.send_message(user_id, "❌ Произошла ошибка при обработке сделки")

# Обработчики callback'ов для кнопок
@dp.callback_query(F.data.startswith("scavenger_yes_"))
async def handle_scavenger_yes(callback: CallbackQuery):
    await callback.answer()
    try:
        parts = callback.data.split('_')
        user_id = int(parts[2])
        card_id = int(parts[3])

        if callback.from_user.id != user_id:
            return
        
        # Удаляем клавиатуру
        await callback.message.edit_reply_markup(reply_markup=None)
        
        # Обрабатываем сделку
        await process_scavenger_deal(user_id, card_id, True)
        
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in scavenger_yes handler: {e}")

@dp.callback_query(F.data.startswith("scavenger_no_"))
async def handle_scavenger_no(callback: CallbackQuery):
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[2])

        if callback.from_user.id != user_id:
            return
        
        # Удаляем клавиатуру
        await callback.message.edit_reply_markup(reply_markup=None)
        
        # Отправляем сообщение об отказе
        await callback.message.answer("😔 Вы отказались продавать свою видеокарту Скупщику!")
        
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in scavenger_no handler: {e}")

async def scavenger_visits_scheduler():
    """Планировщик визитов скупщика - исправленная версия"""
    while True:
        try:
            now = datetime.now()
            logger.info(f"Scavenger scheduler running at {now}")
            
            # Более широкое окно активности (с 8 утра до 2 ночи)
            if 8 <= now.hour <= 26:  # 26 = 2 часа ночи следующего дня
                # Получаем ВСЕХ активных пользователей (кто был активен за последние 30 дней)
                cursor.execute('''
                SELECT user_id FROM users 
                WHERE last_income_time IS NOT NULL 
                AND datetime(last_income_time) > datetime('now', '-30 days')
                ''')
                active_users = cursor.fetchall()
                
                users_to_visit = []
                
                for (user_id,) in active_users:
                    if can_receive_scavenger_visit(user_id):
                        users_to_visit.append(user_id)
                
                logger.info(f"Scavenger: {len(users_to_visit)} users can receive visits today")
                
                # Обрабатываем ВСЕХ пользователей, которые могут получить визит
                successful_visits = 0
                failed_visits = 0
                
                for user_id in users_to_visit:
                    try:
                        # Случайная задержка между визитами (30-120 секунд)
                        delay = random.randint(30, 120)
                        await asyncio.sleep(delay)
                        
                        success = await send_scavenger_visit(user_id)
                        if success:
                            successful_visits += 1
                        else:
                            failed_visits += 1
                        
                        # Короткая пауза между обработкой пользователей
                        await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"Error sending scavenger visit to user {user_id}: {e}")
                        failed_visits += 1
                        continue
                
                logger.info(f"Scavenger round completed: {successful_visits} successful, {failed_visits} failed")
            
            # Ждем 2 часа до следующего раунда рассылки
            logger.info("Scavenger scheduler sleeping for 2 hours")
            await asyncio.sleep(7200)  # 2 часа
            
        except Exception as e:
            logger.error(f"Error in scavenger scheduler: {e}")
            await asyncio.sleep(600)  # Ждем 10 минут при ошибке

# Команда для тестирования скупщика (только для админа)
@dp.message(Command("test_scavenger"))
async def test_scavenger_command(message: Message):
    """Тестовая команда для отправки скупщика (только для админов)"""
    if message.from_user.id not in ADMINS:  # Замените на ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        success = await send_scavenger_visit(user_id)
        
        if success:
            await message.answer("✅ Тестовый визит скупщика отправлен!")
        else:
            await message.answer("❌ Не удалось отправить визит скупщика (нет видеокарт?)")
            
    except Exception as e:
        logger.error(f"Error in test_scavenger: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

# Команда для тестирования скупщика (только для админа)
@dp.message(Command("test_scavenger"))
async def test_scavenger_command(message: Message):
    """Тестовая команда для отправки скупщика (только для админов)"""
    if message.from_user.id not in ADMINS:  # Замените на ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        success = await send_scavenger_visit(user_id)
        
        if success:
            await message.answer("✅ Тестовый визит скупщика отправлен!")
        else:
            await message.answer("❌ Не удалось отправить визит скупщика (нет видеокарт?)")
            
    except Exception as e:
        logger.error(f"Error in test_scavenger: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

# Команда для проверки статуса скупщика
@dp.message(Command("scavenger_status"))
async def scavenger_status_command(message: Message):
    """Показывает статус визитов скупщика"""
    user_id = message.from_user.id
    
    try:
        today = datetime.now().date().isoformat()
        
        cursor.execute('''
        SELECT last_visit_date, visits_today 
        FROM user_scavenger_visits 
        WHERE user_id = ?
        ''', (user_id,))
        
        result = cursor.fetchone()
        
        if result:
            last_visit_date, visits_today = result
            if last_visit_date == today:
                visits_text = f"Сегодня: {visits_today} визит(ов)"
                if visits_today >= 2:
                    next_visit = "Завтра"
                elif visits_today == 1:
                    # Проверяем возможность второго визита
                    if random.randint(1, 100) <= SCAVENGER_CHANCE_SECOND_VISIT:
                        next_visit = "Возможен сегодня (5% шанс)"
                    else:
                        next_visit = "Завтра"
                else:
                    next_visit = "Сегодня"
            else:
                visits_text = "Сегодня: 0 визитов"
                next_visit = "Сегодня"
        else:
            visits_text = "Никогда не было визитов"
            next_visit = "Сегодня"
        
        text = (
            f"🛒 <b>Статус Скупщика</b>\n\n"
            f"{visits_text}\n"
            f"Следующий возможный визит: {next_visit}\n\n"
            f"<b>Лимиты:</b>\n"
            f"• 1-2 визита в день\n"
            f"• Только в активное время (8:00-23:00)\n\n"
            f"💡 Скупщик появляется случайным образом!"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in scavenger_status: {e}")
        await message.answer("❌ Произошла ошибка при получении статуса")

# Запуск планировщика при старте бота
async def start_scavenger_scheduler():
    """Запускает планировщик визитов скупщика"""
    asyncio.create_task(scavenger_visits_scheduler())

# Добавляем константы для системы сбора
SERVER_FUND_GOALS = [
    {"min": 15000000000, "max": 30000000000, "reward_type": "booster", "reward_value": "cleaner", "duration": 1},
    {"min": 15000000000, "max": 30000000000, "reward_type": "booster", "reward_value": "investor", "duration": 2},
    {"min": 15000000000, "max": 30000000000, "reward_type": "booster", "reward_value": "cleaner", "duration": 3},
    {"min": 15000000000, "max": 30000000000, "reward_type": "card", "reward_value": "last_available", "duration": 0}
]

# Функции для работы с системой сбора
def get_current_fund() -> Optional[Dict]:
    """Получает текущий активный сбор"""
    try:
        cursor.execute('''
        SELECT id, current_goal, current_amount, reward_type, reward_value, reward_duration, start_date, status
        FROM server_fund 
        WHERE status = 'active'
        ORDER BY id DESC 
        LIMIT 1
        ''')
        result = cursor.fetchone()
        if result:
            return {
                "id": result[0],
                "goal": result[1],
                "amount": result[2],
                "reward_type": result[3],
                "reward_value": result[4],
                "duration": result[5],
                "start_date": result[6],
                "status": result[7]
            }
        return None
    except sqlite3.Error as e:
        logger.error(f"Error getting current fund: {e}")
        return None

def create_new_fund() -> bool:
    """Создает новый сбор с случайной целью"""
    try:
        # Выбираем случайную цель из доступных
        goal_info = random.choice(SERVER_FUND_GOALS)
        goal_amount = random.randint(goal_info["min"], goal_info["max"])
        
        cursor.execute('''
        INSERT INTO server_fund (current_goal, current_amount, reward_type, reward_value, reward_duration, start_date)
        VALUES (?, 0, ?, ?, ?, ?)
        ''', (goal_amount, goal_info["reward_type"], goal_info["reward_value"], goal_info["duration"], datetime.now().isoformat()))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error creating new fund: {e}")
        conn.rollback()
        return False

def contribute_to_fund(user_id: int, amount: float) -> Tuple[bool, str]:
    """Вносит взнос в сбор"""
    try:
        fund = get_current_fund()
        if not fund:
            return False, "❌ В данный момент нет активного сбора"
        
        user = get_user(user_id)
        if not user:
            return False, "❌ Пользователь не найден"
        
        if user[2] < amount:
            return False, f"❌ Недостаточно средств! Ваш баланс: ${format_number(user[2], True)}"
        
        if amount <= 0:
            return False, "❌ Сумма взноса должна быть положительной"
        
        # Списываем средства
        new_usd = user[2] - amount
        update_balance(user_id, usd=new_usd)
        
        # Обновляем сбор
        new_amount = fund["amount"] + amount
        cursor.execute('''
        UPDATE server_fund 
        SET current_amount = ? 
        WHERE id = ?
        ''', (new_amount, fund["id"]))
        
        # Записываем взнос
        cursor.execute('''
        INSERT INTO server_fund_contributions (user_id, amount, contribution_date)
        VALUES (?, ?, ?)
        ''', (user_id, amount, datetime.now().isoformat()))
        
        conn.commit()
        
        # УБРАТЬ автоматическое завершение при достижении цели
        # if new_amount >= fund["goal"]:
        #     asyncio.create_task(complete_fund(fund["id"]))
        
        return True, f"✅ Взнос ${format_number(amount, True)} принят! Собрано: {format_number(new_amount/fund['goal']*100, True)}%"
        
    except Exception as e:
        logger.error(f"Error contributing to fund: {e}")
        conn.rollback()
        return False, "❌ Произошла ошибка при внесении взноса"

async def complete_fund(fund_id: int):
    """Завершает сбор и выдает награды"""
    try:
        # Получаем информацию о сборе
        cursor.execute('''
        SELECT current_goal, current_amount, reward_type, reward_value, reward_duration
        FROM server_fund 
        WHERE id = ?
        ''', (fund_id,))
        fund_data = cursor.fetchone()
        
        if not fund_data:
            return
        
        goal, amount, reward_type, reward_value, duration = fund_data
        
        # Получаем всех участников
        cursor.execute('''
        SELECT user_id, SUM(amount) as total_contribution
        FROM server_fund_contributions 
        WHERE id IN (
            SELECT id FROM server_fund_contributions 
            WHERE contribution_date >= (SELECT start_date FROM server_fund WHERE id = ?)
        )
        GROUP BY user_id
        ''', (fund_id,))
        
        participants = cursor.fetchall()
        
        # Выдаем награды
        for user_id, contribution in participants:
            try:
                if reward_type == "card":
                    # Даем последнюю доступную видеокарту
                    user = get_user(user_id)
                    if user:
                        expansion_info = EXPANSIONS[user[5] - 1]
                        last_available_card_id = expansion_info["last_available_card"]
                        last_available_card = next((card for card in GRAPHICS_CARDS if card["id"] == last_available_card_id), None)
                        
                        if last_available_card:
                            # Проверяем свободные слоты
                            user_cards, total_cards = get_user_cards(user_id)
                            max_cards = expansion_info['max_cards']
                            
                            if total_cards < max_cards:
                                # Есть место - даем карту
                                add_user_card(user_id, last_available_card_id)
                                calculate_income(user_id)
                            else:
                                # Нет места - начисляем 70% стоимости
                                compensation = int(last_available_card['price'] * 0.7)
                                current_balance = get_user(user_id)[2]
                                new_balance = current_balance + compensation
                                update_balance(user_id, usd=new_balance)
                
                elif reward_type == "booster":
                    # Даем бустер - добавляем поле bonus со значением 0
                    booster_until = (datetime.now() + timedelta(days=duration)).isoformat()
                    cursor.execute('''
                    INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until, bonus)
                    VALUES (?, ?, ?, ?)
                    ''', (user_id, reward_value, booster_until, 0))
            
            except Exception as e:
                logger.error(f"Error giving reward to user {user_id}: {e}")
                continue
        
        # Сохраняем в историю
        cursor.execute('''
        INSERT INTO server_fund_history (goal, amount_collected, reward_type, reward_value, completion_date, participants_count)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (goal, amount, reward_type, reward_value, datetime.now().isoformat(), len(participants)))
        
        # Закрываем текущий сбор
        cursor.execute('''
        UPDATE server_fund 
        SET status = 'completed', end_date = ?
        WHERE id = ?
        ''', (datetime.now().isoformat(), fund_id))
        
        conn.commit()
        
        # Отправляем уведомление в канал
        try:
            reward_text = ""
            if reward_type == "card":
                reward_text = "последнюю доступную видеокарту"
            else:
                reward_text = f"{duration} дней бустера {reward_value}"
            
            notification = (
                "🎉 <b>Сбор завершен!</b>\n\n"
                f"💰 Собрано: ${format_number(amount, True)} / ${format_number(goal, True)}\n"
                f"👥 Участников: {len(participants)}\n"
                f"🎁 Награда: {reward_text}\n\n"
                f"Спасибо всем участникам! 🚀"
            )
            
            # Отправляем в канал (замените на ваш ID канала)
            await bot.send_message(chat_id=-1002734900704, text=notification, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
        
        # Создаем новый сбор
        create_new_fund()
        
    except Exception as e:
        logger.error(f"Error completing fund: {e}")
        conn.rollback()

def get_fund_progress() -> Dict:
    """Возвращает информацию о текущем прогрессе сбора"""
    fund = get_current_fund()
    if not fund:
        # Если нет активного сбора, создаем новый
        create_new_fund()
        fund = get_current_fund()
    
    # Получаем топ-10 участников
    cursor.execute('''
    SELECT u.user_id, u.username, SUM(sf.amount) as total_contribution
    FROM server_fund_contributions sf
    LEFT JOIN users u ON sf.user_id = u.user_id
    WHERE sf.contribution_date >= ?
    GROUP BY sf.user_id
    ORDER BY total_contribution DESC
    LIMIT 10
    ''', (fund["start_date"],))
    
    top_contributors = cursor.fetchall()
    
    return {
        "fund": fund,
        "top_contributors": top_contributors
    }

def get_user_last_available_card(user_id: int) -> Optional[Dict]:
    """Возвращает последнюю доступную видеокарту для пользователя"""
    try:
        user = get_user(user_id)
        if not user:
            return None
        
        expansion_info = EXPANSIONS[user[5] - 1]
        last_card_id = expansion_info["last_available_card"]
        
        return next((card for card in GRAPHICS_CARDS if card["id"] == last_card_id), None)
    except Exception as e:
        logger.error(f"Error getting last available card: {e}")
        return None

# Команды для системы сбора
@dp.message(F.text.regexp(r'^!сбор$'))
async def fund_info(message: Message):
    """Показывает информацию о текущем сборе"""
    progress = get_fund_progress()
    fund = progress["fund"]
    
    if not fund:
        await message.answer("❌ В данный момент нет активного сбора")
        return
    
    percentage = (fund["amount"] / fund["goal"]) * 100
    # Ограничиваем процент для полоски прогресса, но оставляем реальный для отображения
    progress_percentage = min(percentage, 100)
    progress_bar = create_progress_bar(progress_percentage, 20)
    
    # Описание награды
    reward_text = ""
    if fund["reward_type"] == "card":
        user_card = get_user_last_available_card(message.from_user.id)
        if user_card:
            reward_text = f"🎁 {user_card['name']} (+{format_number(user_card['income'])} BTC/10мин)"
        else:
            reward_text = "🎁 Последняя доступная видеокарта"
    else:
        reward_text = f"⚡ {fund['reward_value'].capitalize()} бустер ({fund['duration']} дней)"
    
    text = (
        "🚀 <b>Сбор</b>\n\n"
        f"{progress_bar}\n"
        f"📊 Прогресс: {format_number(percentage, True)}%\n"
        f"💰 Собрано: ${format_number(fund['amount'], True)} / ${format_number(fund['goal'], True)}\n\n"
        f"🎯 Цель: {reward_text}\n\n"
        f"💡 Внесите взнос: !взнос [сумма]\n"
        f"⏰ Награды будут выданы тем, кто сделал взнос, в конце недели в 18:00."
    )
    
    await message.answer(text, parse_mode='HTML')
    
def create_progress_bar(percentage: float, length: int = 20) -> str:
    """Создает текстовый прогресс-бар"""
    filled = int(percentage / 100 * length)
    empty = length - filled
    return f"[{'█' * filled}{'░' * empty}]"

@dp.message(F.text.regexp(r'^!взнос\s+(\d+)$'))
async def fund_contribute(message: Message):
    """Обработчик внесения взноса"""
    user_id = message.from_user.id
    try:
        amount = float(message.text.split()[1])
        
        success, result = contribute_to_fund(user_id, amount)
        if success:
            # Отдельное сообщение о успешном взносе
            await message.answer(f"✅ {result}")
        else:
            await message.answer(result)
            
    except ValueError:
        await message.answer("❌ Неверный формат суммы. Используйте: !взнос [сумма]")
    except Exception as e:
        logger.error(f"Error in fund contribution: {e}")
        await message.answer("❌ Произошла ошибка при обработке взноса")

@dp.message(Command("fund_complete"))
async def admin_complete_fund(message: Message):
    """Админская команда для завершения сбора (только для админов)"""
    if message.from_user.id not in ADMINS:  # Замените на ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        fund = get_current_fund()
        if not fund:
            await message.answer("❌ Нет активного сбора для завершения")
            return
        
        # Принудительно завершаем сбор
        asyncio.create_task(complete_fund(fund["id"]))
        await message.answer("✅ Сбор принудительно завершен. Награды выданы участникам.")
        
    except Exception as e:
        logger.error(f"Error in admin fund complete: {e}")
        await message.answer("❌ Произошла ошибка при завершении сбора")

@dp.message(Command("fund_reset"))
async def admin_reset_fund(message: Message):
    """Админская команда для сброса сбора (только для админов)"""
    if message.from_user.id not in ADMINS:  # Замените на ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Закрываем текущий сбор
        cursor.execute('''
        UPDATE server_fund 
        SET status = 'cancelled', end_date = ?
        WHERE status = 'active'
        ''', (datetime.now().isoformat(),))
        
        # Создаем новый сбор
        create_new_fund()
        
        conn.commit()
        await message.answer("✅ Сбор сброшен. Создан новый сбор с новой целью.")
        
    except Exception as e:
        logger.error(f"Error in fund reset: {e}")
        conn.rollback()
        await message.answer("❌ Произошла ошибка при сбросе сбора")

# Инициализация системы сбора при запуске
def initialize_fund_system():
    """Инициализирует систему сбора при запуске бота"""
    try:
        # Проверяем, есть ли активный сбор
        fund = get_current_fund()
        if not fund:
            create_new_fund()
            logger.info("Создан новый сбор на улучшение серверов")
        else:
            logger.info(f"Активный сбор найден: ${fund['amount']}/${fund['goal']}")
    except Exception as e:
        logger.error(f"Error initializing fund system: {e}")

# Добавляем инициализацию при запуске
initialize_fund_system()


BUSINESSES = [
    {
        "id": 1,
        "name": "Автомойка",
        "base_price": 500000,
        "base_income": 5000,
        "upgrade_multiplier": 5,
        "max_level": 3,
        "emoji": "🚗"
    },
    {
        "id": 11,
        "name": "Шиномонтаж",
        "base_price": 500000,
        "base_income": 5000,
        "upgrade_multiplier": 5,
        "max_level": 3,
        "emoji": "🛞"
    },
    {
        "id": 12,
        "name": "Автодетейлинг",
        "base_price": 500000,
        "base_income": 5000,
        "upgrade_multiplier": 5,
        "max_level": 3,
        "emoji": "✨"
    },
    {
        "id": 2,
        "name": "Пиццерия",
        "base_price": 5000000,
        "base_income": 65000,
        "upgrade_multiplier": 3,
        "max_level": 3,
        "emoji": "🍕"
    },
    {
        "id": 21,
        "name": "Бургерная",
        "base_price": 5000000,
        "base_income": 65000,
        "upgrade_multiplier": 3,
        "max_level": 3,
        "emoji": "🍔"
    },
    {
        "id": 22,
        "name": "Суши-бар",
        "base_price": 5000000,
        "base_income": 65000,
        "upgrade_multiplier": 3,
        "max_level": 3,
        "emoji": "🍣"
    },
    {
        "id": 3,
        "name": "IT-компания",
        "base_price": 15000000,
        "base_income": 165000,
        "upgrade_multiplier": 1.4,
        "max_level": 3,
        "emoji": "💻"
    },
    {
        "id": 31,
        "name": "Кибербезопасность",
        "base_price": 15000000,
        "base_income": 165000,
        "upgrade_multiplier": 1.4,
        "max_level": 3,
        "emoji": "🔒"
    },
    {
        "id": 32,
        "name": "Мобильная разработка",
        "base_price": 15000000,
        "base_income": 165000,
        "upgrade_multiplier": 1.4,
        "max_level": 3,
        "emoji": "📱"
    },
    {
        "id": 4,
        "name": "Строительная фирма",
        "base_price": 30000000,
        "base_income": 300000,
        "upgrade_multiplier": 1.3,
        "max_level": 3,
        "emoji": "🏗"
    },
    {
        "id": 41,
        "name": "Ремонт квартир",
        "base_price": 30000000,
        "base_income": 300000,
        "upgrade_multiplier": 1.3,
        "max_level": 3,
        "emoji": "🛠️"
    },
    {
        "id": 42,
        "name": "Дорожное строительство",
        "base_price": 30000000,
        "base_income": 300000,
        "upgrade_multiplier": 1.3,
        "max_level": 3,
        "emoji": "🛣️"
    },
    {
        "id": 5,
        "name": "Нефтяная компания",
        "base_price": 100000000,
        "base_income": 700000,
        "upgrade_multiplier": 1.5,
        "max_level": 3,
        "emoji": "⛽"
    },
    {
        "id": 51,
        "name": "Газовая корпорация",
        "base_price": 100000000,
        "base_income": 700000,
        "upgrade_multiplier": 1.5,
        "max_level": 3,
        "emoji": "🔥"
    },
    {
        "id": 52,
        "name": "Добыча угля",
        "base_price": 100000000,
        "base_income": 700000,
        "upgrade_multiplier": 1.5,
        "max_level": 3,
        "emoji": "⚫"
    }
]

MAX_BUSINESSES = 3  # Максимальное количество бизнесов

GRAPHICS_CARDS = [
    {"id": 1, "name": "NVIDA GeTeiX 550", "price": 50, "income": 0.0001},
    {"id": 2, "name": "NVIDA GeTeiX 560", "price": 150, "income": 0.0003},
    {"id": 3, "name": "NVIDA GeTeiX 570", "price": 250, "income": 0.0004},
    {"id": 4, "name": "NVIDA GeTeiX 580", "price": 500, "income": 0.0005},
    {"id": 5, "name": "NVIDA GeTeiX 650", "price": 1000, "income": 0.0006},
    {"id": 6, "name": "NVIDA GeTeiX 650 TITAN", "price": 2000, "income": 0.0007},
    {"id": 7, "name": "NVIDA GeTeiX 660", "price": 1900, "income": 0.0007},
    {"id": 8, "name": "NVIDA GeTeiX 666", "price": 6000, "income": 0.0009},
    {"id": 9, "name": "NVIDA GeTeiX 670", "price": 4500, "income": 0.0008},
    {"id": 10, "name": "NVIDA GeTeiX 680", "price": 5850, "income": 0.0009},
    {"id": 11, "name": "NVIDA GeTeiX 750", "price": 10000, "income": 0.001},
    {"id": 12, "name": "NVIDA GeTeiX 760", "price": 20000, "income": 0.0011},
    {"id": 13, "name": "NVIDA GeTeiX 750 TITAN", "price": 50000, "income": 0.0012},
    {"id": 14, "name": "NVIDA GeTeiX 770", "price": 48000, "income": 0.00125},
    {"id": 15, "name": "NVIDA GeTeiX 590", "price": 65000, "income": 0.0013},
    {"id": 16, "name": "NVIDA GeTeiX 690", "price": 78000, "income": 0.0014},
    {"id": 17, "name": "NVIDA GeTeiX 780", "price": 90000, "income": 0.0015},
    {"id": 18, "name": "NVIDA GeTeiX 790", "price": 125000, "income": 0.0017},
    {"id": 19, "name": "NVIDA GeTeiX 950", "price": 180000, "income": 0.0018},
    {"id": 20, "name": "NVIDA GeTeiX 960", "price": 200000, "income": 0.0019},
    {"id": 21, "name": "NVIDA GeTeiX 950 TITAN", "price": 400000, "income": 0.0024},
    {"id": 22, "name": "NVIDA GeTeiX 960 TITAN", "price": 900000, "income": 0.0028},
    {"id": 23, "name": "NVIDA GeTeiX 970", "price": 1850000, "income": 0.0035},
    {"id": 24, "name": "NVIDA GeTeiX 970 TITAN", "price": 3500000, "income": 0.005},
    {"id": 25, "name": "NVIDA GeTeiX 980", "price": 4000000, "income": 0.006},
    {"id": 26, "name": "NVIDA GeTeiX 1050", "price": 4750000, "income": 0.007},
    {"id": 27, "name": "NVIDA GeTeiX 980 MinerEdition", "price": 6000000, "income": 0.008},
    {"id": 28, "name": "NVIDA GeTeiX 1050 TITAN", "price": 8000000, "income": 0.009},
    {"id": 29, "name": "NVIDA GeTeiX 1060", "price": 10000000, "income": 0.01},
    {"id": 30, "name": "NVIDA GeTeiX 1060 TITAN", "price": 35000000, "income": 0.02},
    {"id": 31, "name": "NVIDA GeTeiX 1070", "price": 75000000, "income": 0.03},
    {"id": 32, "name": "NVIDA GeTeiX 1060 MinerEdition", "price": 155000000, "income": 0.035},
    {"id": 33, "name": "NVIDA GeTeiX 1070 TITAN", "price": 200000000, "income": 0.04},
    {"id": 34, "name": "NVIDA GeTeiX 1070 MinerEdition", "price": 235000000, "income": 0.05},
    {"id": 35, "name": "NVIDA GeTeiX 1080", "price": 300000000, "income": 0.05},
    {"id": 36, "name": "NVIDA RTX 2050", "price": 325000000, "income": 0.075},
    {"id": 37, "name": "NVIDA RTX 2060", "price": 350000000, "income": 0.1},
    {"id": 38, "name": "NVIDA RTX 2070", "price": 375000000, "income": 0.125},
    {"id": 39, "name": "NVIDA RTX 2080", "price": 400000000, "income": 0.15},
    {"id": 40, "name": "NVIDA RTX 2090", "price": 425000000, "income": 0.2},
    {"id": 41, "name": "NVIDA RTX 3050", "price": 450000000, "income": 0.225},
    {"id": 42, "name": "NVIDA RTX 3060", "price": 475000000, "income": 0.25},
    {"id": 43, "name": "NVIDA RTX 2090 SUPER DUPER", "price": 500000000, "income": 0.3},
    {"id": 44, "name": "NVIDA RTX 3070", "price": 525000000, "income": 0.325},
    {"id": 45, "name": "NVIDA RTX 3070 MinerEdition", "price": 550000000, "income": 0.35},
    {"id": 46, "name": "NVIDA RTX 3070 TitanMiner", "price": 575000000, "income": 0.375},
    {"id": 47, "name": "NVIDA RTX 3080", "price": 600000000, "income": 0.4},
    {"id": 48, "name": "NVIDA RTX 3080 TITAN", "price": 625000000, "income": 0.425},
    {"id": 49, "name": "NVIDA RTX 3080 TITAN SUPER", "price": 650000000, "income": 0.45},
    {"id": 50, "name": "NVIDA RTX 4050", "price": 675000000, "income": 0.475},
    {"id": 51, "name": "NVIDA RTX 4060", "price": 700000000, "income": 0.5},
    {"id": 52, "name": "NVIDA RTX 3090", "price": 725000000, "income": 0.525},
    {"id": 53, "name": "NVIDA RTX 4070", "price": 750000000, "income": 0.55},
    {"id": 54, "name": "NVIDA RTX 3090 PRO Edition", "price": 775000000, "income": 0.575},
    {"id": 55, "name": "NVIDA RTX 3090 PRO MAX Edition", "price": 800000000, "income": 0.6},
    {"id": 56, "name": "NVIDA RTX 3090 TITAN", "price": 825000000, "income": 0.625},
    {"id": 57, "name": "NVIDA RTX 3090 TITAN SUPER", "price": 850000000, "income": 0.65},
    {"id": 58, "name": "NVIDA RTX 4070 MinerEdition", "price": 850000000, "income": 0.675},
    {"id": 59, "name": "NVIDA RTX 3090 TITAN SUPER", "price": 875000000, "income": 0.7},
    {"id": 60, "name": "NVIDA RTX 4080", "price": 900000000, "income": 0.725},
    {"id": 61, "name": "NVIDA RTX 4080 TITAN", "price": 925000000, "income": 0.75},
    {"id": 62, "name": "NVIDA RTX 4080 TitanMiner", "price": 950000000, "income": 0.775},
    {"id": 63, "name": "NVIDA RTX 4090", "price": 975000000, "income": 0.9},
    {"id": 64, "name": "NVIDA RTX 4090 TITAN", "price": 1000000000, "income": 0.925},
    {"id": 65, "name": "NVIDA RTX 4090 TitanEdition", "price": 1250000000, "income": 0.95},
    {"id": 66, "name": "NVIDA RTX 4090 TitanMiner", "price": 1500000000, "income": 0.975},
    {"id": 67, "name": "NVIDA RTX 4090 TITAN SUPER", "price": 1750000000, "income": 1.0},
    {"id": 68, "name": "NVIDA RTX 4090 GodEdition", "price": 2000000000, "income": 1.5}
]


BADGES = {
    1: {"name": "🍀 Опять что ли?", "command": "/setbadge_1"},
    2: {"name": "☠️ Хозяин Ва‌йпуа", "command": "/setbadge_2"},
    3: {"name": "♻️ Бессмертный Тапок", "command": "/setbadge_3"},
    4: {"name": "💾 Бэкап Бога", "command": "/setbadge_4"},
    5: {"name": "🪓 Вайпернатор", "command": "/setbadge_5"},
    6: {"name": "🛡 Беглец из /dev/null", "command": "/setbadge_6"},
    7: {"name": "🌌 Дитя Забытых Богов", "command": "/setbadge_7"},
    8: {"name": "🔌 404 User Not Found", "command": "/setbadge_8"},
    9: {"name": "😐 Ты еблан? Сколько можно", "command": "/setbadge_9"},
    10: {"name": "👑 Вайп? Не, не слышал", "command": "/setbadge_10"},
    11: {"name": "🙋🏻Главный фембойчик🌈", "command": "/setbadge_11"},
    12: {"name": "👅Зам фембойчика😮‍💨", "command": "/setbadge_12"},
    13: {"name": "Гений, миллиардер, плейбой, филантроп💅", "command": "/setbadge_13"},
    14: {"name": "Ебанутый тестер 🤡", "command": "/setbadge_14"},
    15: {"name": "Спонсор", "command": "/setbadge_15"},
    16: {"name": "ᴅᴀᴠɪᴅ ᴍᴄᴄᴏʟʟ🔪", "command": "/setbadge_16"}
    
} 

WIPES_SLOTS = {
    0: 195,   # Без вайпов
    1: 215,    # 1 вайп
    2: 235,    # 2 вайпа
    3: 255,    # 3 вайпа
    4: 275,    # 4 вайпа
    5: 295,    # 5 вайпов
    6: 315,    # 6 вайпов
    7: 335,    # 7 вайпов
    8: 355,    # 8 вайпов
    9: 375,    # 9 вайпов
    10: 400    # 10 вайпов (максимум)
}
MAX_WIPES = 10
WIPE_BONUS_PERCENT = 20

# Replace the current EXPANSIONS generation code with this:

EXPANSIONS = []
MAX_LEVEL = 80  # Максимальный уровень фермы
BASE_SLOTS = 5  # Базовое количество слотов
SLOTS_PER_LEVEL = 5  # Количество добавляемых слотов за уровень

for level in range(1, MAX_LEVEL + 1):
    # Calculate max cards for this level
    max_cards = BASE_SLOTS + (level - 1) * SLOTS_PER_LEVEL
    price = int(400 * (1.1 ** (level - 1)))  # Exponential price growth
    
    # Determine available cards
    if level == 1:
        first_available_card = 1
        last_available_card = 2
    elif level <= 10:
        first_available_card = 1
        last_available_card = level
    else:
        first_available_card = level - 9
        last_available_card = level
    
    last_available_card = min(last_available_card, len(GRAPHICS_CARDS))
    
    available_cards = GRAPHICS_CARDS[first_available_card-1:last_available_card]
    
    # Рассчитываем требуемый доход (для уровней >60)
    min_income = 0
    if level > 12:  # Начинаем требовать доход с 13 уровня
        if len(available_cards) >= 4:
            min_income_card = available_cards[-4]['income']
            min_income = round(min_income_card * max_cards, 6)
        elif len(available_cards) >= 1:
            min_income = round(available_cards[0]['income'] * max_cards, 6)
    
    # Определяем required_wipe на основе WIPES_SLOTS
    required_wipe = 0
    for wipe, slots in sorted(WIPES_SLOTS.items(), reverse=True):
        if max_cards <= slots:
            required_wipe = wipe
    
    EXPANSIONS.append({
        "level": level,
        "price": price,
        "max_cards": max_cards,
        "min_income": min_income,
        "first_available_card": first_available_card,
        "last_available_card": last_available_card,
        "required_wipe": required_wipe
    })
        
def get_user(user_id: int) -> Optional[Tuple]:
    try:
        cursor.execute('''
        SELECT user_id, username, usd_balance, btc_balance, income_btc, expansion, last_income_time
        FROM users 
        WHERE user_id = ?
        ''', (user_id,))
        return cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"Error getting user: {e}")
        return None    
        
        
    
def create_user(user_id: int, username: Optional[str]) -> bool:
    """Регистрирует нового пользователя с начальным балансом"""
    try:
        cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
        if cursor.fetchone() is not None:
            return False  # Пользователь уже существует
            
        cursor.execute('''
        INSERT INTO users 
        (user_id, username, usd_balance, btc_balance, income_btc, expansion) 
        VALUES (?, ?, 10000, 0, 0, 1)
        ''', (user_id, username))
        
        # Не добавляем начальную видеокарту - ферма должна быть пустой
        # cursor.execute('''
        # INSERT INTO user_cards (user_id, card_id, count)
        # VALUES (?, ?, 1)
        # ''', (user_id, 1))
        
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Error registering user: {e}")
        conn.rollback()
        return False

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_work_stats (
    user_id INTEGER PRIMARY KEY,
    register_date TEXT DEFAULT CURRENT_TIMESTAMP,
    total_experience INTEGER DEFAULT 0,
    last_work_time TEXT,
    total_usd_earned REAL DEFAULT 0,
    total_btc_earned REAL DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()



def get_user_work_stats(user_id: int) -> Tuple[int, Optional[datetime]]:
    """Получаем опыт и время последней работы"""
    try:
        cursor.execute('SELECT total_experience, last_work_time FROM user_work_stats WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            last_work_time = datetime.fromisoformat(result[1]) if result[1] else None
            return result[0], last_work_time
        
        # Если записи нет - создаем
        create_user_extended(user_id, None)
        return 0, None
    except sqlite3.Error as e:
        logger.error(f"Error getting work stats: {e}")
        return 0, None

WORK_JOBS = [
    {"id": 1, "name": "Начинающий майнер", "reward": 50000, "min_exp": 0, "max_exp": 100},
    {"id": 2, "name": "Опытный майнер", "reward": 100000, "min_exp": 100, "max_exp": 200},
    {"id": 3, "name": "Специалист по фермам", "reward": 150000, "min_exp": 200, "max_exp": 300},
    {"id": 4, "name": "Эксперт по видеокартам", "reward": 400000, "min_exp": 300, "max_exp": 400},
    {"id": 5, "name": "Профессиональный майнер", "reward": 500000, "min_exp": 400, "max_exp": 500},
    {"id": 6, "name": "Криптоаналитик", "reward": 600000, "min_exp": 500, "max_exp": 600},
    {"id": 7, "name": "Архитектор майнинг-ферм", "reward": 700000, "min_exp": 600, "max_exp": 700},
    {"id": 8, "name": "Директор майнинг-центра", "reward": 800000, "min_exp": 700, "max_exp": 800},
    {"id": 9, "name": "Криптоинвестор", "reward": 900000, "min_exp": 800, "max_exp": 900},
    {"id": 10, "name": "Криптопредприниматель", "reward": 1000000, "min_exp": 900, "max_exp": 1000},
    {"id": 11, "name": "Криптокороль", "reward": 1100000, "min_exp": 1000, "max_exp": 1100},
    {"id": 12, "name": "Крипто-гуру", "reward": 1200000, "min_exp": 1100, "max_exp": 1200},
    {"id": 13, "name": "Мастер блокчейна", "reward": 1300000, "min_exp": 1200, "max_exp": 1300},
    {"id": 14, "name": "Крипто-миллиардер", "reward": 1400000, "min_exp": 1300, "max_exp": 1400},
    {"id": 15, "name": "Владелец криптобиржи", "reward": 1500000, "min_exp": 1400, "max_exp": 1500},
    {"id": 16, "name": "Создатель криптопротокола", "reward": 1600000, "min_exp": 1500, "max_exp": 1600},
    {"id": 17, "name": "Крипто-легенда", "reward": 1700000, "min_exp": 1600, "max_exp": 1700},
    {"id": 18, "name": "Повелитель блокчейна", "reward": 1800000, "min_exp": 1700, "max_exp": 1800},
    {"id": 19, "name": "Крипто-император", "reward": 1900000, "min_exp": 1800, "max_exp": 1900},
    {"id": 20, "name": "Бог криптомира", "reward": 2000000, "min_exp": 1900, "max_exp": 2000}
]

@dp.message(Command("work"))
async def show_works(message: Message):
    user_id = message.from_user.id
    experience, _ = get_user_work_stats(user_id)
    
    text = "📋 Доступные работы:\n"
    for job in WORK_JOBS:
        if job['min_exp'] <= experience:
            status = "✅"
            exp_text = f"Требуется: {job['min_exp']}+"
        else:
            status = "🔒"
            exp_text = f"Требуется: {job['min_exp']}+"

        text += (
            f"{status} /work_{job['id']} - {job['name']}\n"
            f"${format_number(job['reward'], True)} | {exp_text}\n\n"
        )
    
    await message.answer(text)


@dp.message(Command("give_badge"))
async def give_badge_command(message: Message):
    """Команда для выдачи административного титула пользователю (только для админа)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Получаем аргументы команды
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer(
                "❌ Неверный формат команды\n\n"
                "Формат:\n"
                "/give_badge [user_id] [номер_титула]\n\n"
                "Пример:\n"
                "/give_badge 123456789 11\n\n"
                "📋 Доступные административные титулы (11-12):\n"
                "11 - 🙋🏻Главный фембойчик🌈\n"
                "12 - 👅Зам фембойчика😮‍💨"
            )
            return
        
        # Получаем user_id и номер титула
        target_user_id = int(parts[1])
        badge_id = int(parts[2])
        
        # Проверяем, что титул с таким ID существует и это административный титул (11-12)
        if badge_id not in BADGES or badge_id <= 10:
            await message.answer(f"❌ Титул с ID {badge_id} не существует или не является административным")
            return
        
        # Проверяем, существует ли пользователь
        target_user = get_user(target_user_id)
        if not target_user:
            await message.answer("❌ Пользователь с указанным ID не найден")
            return
        
        # Выдаем административный титул пользователю
        cursor.execute('''
        INSERT OR REPLACE INTO user_admin_badges (user_id, badge_id, assigned_by, assigned_at)
        VALUES (?, ?, ?, ?)
        ''', (target_user_id, badge_id, message.from_user.id, datetime.now().isoformat()))
        conn.commit()
        
        # Получаем информацию о пользователе для красивого ответа
        try:
            target_user_info = await bot.get_chat(target_user_id)
            target_name = target_user_info.full_name
        except:
            target_name = f"ID {target_user_id}"
        
        badge_info = BADGES[badge_id]
        
        # Формируем ответ
        success_message = (
            f"✅ <b>Административный титул успешно выдан!</b>\n\n"
            f"👤 Пользователь: {target_name}\n"
            f"🆔 ID: {target_user_id}\n"
            f"🎖️ Титул: {badge_info['name']}\n"
            f"🔢 ID титула: {badge_id}\n\n"
            f"💡 Пользователь может установить титул с помощью {badge_info['command']}"
        )
        
        await message.answer(success_message, parse_mode='HTML')
        
        # Также уведомляем получателя, если это не тот же пользователь
        if target_user_id != message.from_user.id:
            try:
                user_notification = (
                    f"🎉 <b>Вам выдан административный титул!</b>\n\n"
                    f"🎖️ {badge_info['name']}\n\n"
                    f"💡 Установите его с помощью команды:\n"
                    f"{badge_info['command']}"
                )
                await bot.send_message(chat_id=target_user_id, text=user_notification, parse_mode='HTML')
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
    except ValueError:
        await message.answer(
            "❌ Неверный формат аргументов\n\n"
            "Формат:\n"
            "/give_badge [user_id] [номер_титула]\n\n"
            "Пример:\n"
            "/give_badge 123456789 11"
        )
    except Exception as e:
        logger.error(f"Error in give_badge command: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

@dp.message(Command("create_badge"))
async def create_badge_command(message: Message):
    """Команда для создания кастомного бейджа и выдачи его пользователю (только для админа)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        # Формат: /create_badge [user_id] [текст бейджа]
        text_parts = message.text.split(maxsplit=2)

        if len(text_parts) < 3:
            await message.answer(
                "❌ Неверный формат команды\n\n"
                "Формат:\n"
                "/create_badge [user_id] [текст бейджа]\n\n"
                "Пример:\n"
                "/create_badge 123456789 🍺Амбассадор пива🍺"
            )
            return

        target_user_id = int(text_parts[1])
        badge_name = text_parts[2]

        # Проверяем, существует ли пользователь
        target_user = get_user(target_user_id)
        if not target_user:
            await message.answer("❌ Пользователь с указанным ID не найден. Сначала он должен зарегистрироваться с помощью /start")
            return

        # Ищем свободный ID для нового бейджа (начиная с 100)
        cursor.execute('SELECT MAX(badge_id) FROM custom_badges')
        max_custom_id = cursor.fetchone()[0]

        if max_custom_id is None or max_custom_id < 100:
            new_badge_id = 100
        else:
            new_badge_id = max_custom_id + 1

        # Сохраняем кастомный бейдж в базу
        cursor.execute('''
        INSERT INTO custom_badges (badge_id, badge_name, created_by, created_at)
        VALUES (?, ?, ?, ?)
        ''', (new_badge_id, badge_name, message.from_user.id, datetime.now().isoformat()))

        # Выдаем бейдж пользователю
        cursor.execute('''
        INSERT OR REPLACE INTO user_admin_badges (user_id, badge_id, assigned_by, assigned_at)
        VALUES (?, ?, ?, ?)
        ''', (target_user_id, new_badge_id, message.from_user.id, datetime.now().isoformat()))

        conn.commit()

        # Получаем информацию о пользователе
        try:
            target_user_info = await bot.get_chat(target_user_id)
            target_name = target_user_info.full_name
        except:
            target_name = f"ID {target_user_id}"

        # Формируем ответ
        success_message = (
            f"✅ <b>Кастомный бейдж создан и выдан!</b>\n\n"
            f"👤 Получатель: {target_name}\n"
            f"🆔 ID: {target_user_id}\n"
            f"🎖️ Бейдж: {badge_name}\n"
            f"🔢 ID бейджа: {new_badge_id}\n\n"
            f"💡 Пользователь может установить его с помощью /setbadge_{new_badge_id}"
        )

        await message.answer(success_message, parse_mode='HTML')

        # Уведомляем получателя
        if target_user_id != message.from_user.id:
            try:
                user_notification = (
                    f"🎉 <b>Вам выдан кастомный бейдж!</b>\n\n"
                    f"🎖️ {badge_name}\n\n"
                    f"💡 Установите его с помощью команды:\n"
                    f"/setbadge_{new_badge_id}"
                )
                await bot.send_message(chat_id=target_user_id, text=user_notification, parse_mode='HTML')
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")

    except ValueError:
        await message.answer("❌ Неверный формат user_id. Используйте число")
    except Exception as e:
        logger.error(f"Error in create_badge command: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

@dp.message(Command("give_all_boost"))
async def give_all_boost_command(message: Message):
    """Выдать бустер дохода фермы всем"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Формат: /give_all_boost [дни]")
            return

        days = float(parts[1])
        if days <= 0:
            await message.answer("❌ Дни должны быть больше 0")
            return

        until = datetime.now() + timedelta(days=days)
        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()
        count = 0

        for (user_id,) in users:
            cursor.execute(
                'INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until, bonus) VALUES (?, ?, ?, ?)',
                (user_id, "cleaner", until.isoformat(), 0.25)
            )
            count += 1

        conn.commit()
        await message.answer(f"✅ Бустер дохода фермы (+25%) выдан {count} пользователям на {days} дней")
    except Exception as e:
        logger.error(f"Error in give_all_boost: {e}")
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("give_all_auto"))
async def give_all_auto_command(message: Message):
    """Выдать автоматизацию всем"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Формат: /give_all_auto [дни]")
            return

        days = float(parts[1])
        if days <= 0:
            await message.answer("❌ Дни должны быть больше 0")
            return

        until = datetime.now() + timedelta(days=days)
        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()
        count = 0

        for (user_id,) in users:
            cursor.execute(
                'INSERT OR REPLACE INTO automation_users (user_id, automation_until) VALUES (?, ?)',
                (user_id, until.isoformat())
            )
            count += 1

        conn.commit()
        await message.answer(f"✅ Автоматизация выдана {count} пользователям на {days} дней")
    except Exception as e:
        logger.error(f"Error in give_all_auto: {e}")
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("give_all_virus"))
async def give_all_virus_command(message: Message):
    """Выдать антивирус всем"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Формат: /give_all_virus [дни]")
            return

        days = float(parts[1])
        if days <= 0:
            await message.answer("❌ Дни должны быть больше 0")
            return

        until = datetime.now() + timedelta(days=days)
        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()
        count = 0

        for (user_id,) in users:
            cursor.execute(
                'INSERT OR REPLACE INTO antivirus_users (user_id, antivirus_until) VALUES (?, ?)',
                (user_id, until.isoformat())
            )
            count += 1

        conn.commit()
        await message.answer(f"✅ Антивирус выдан {count} пользователям на {days} дней")
    except Exception as e:
        logger.error(f"Error in give_all_virus: {e}")
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("give_all_biz"))
async def give_all_biz_command(message: Message):
    """Выдать бустер бизнеса всем"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ Нет прав")
        return

    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Формат: /give_all_biz [дни]")
            return

        days = float(parts[1])
        if days <= 0:
            await message.answer("❌ Дни должны быть больше 0")
            return

        until = datetime.now() + timedelta(days=days)
        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()
        count = 0

        for (user_id,) in users:
            cursor.execute(
                'INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until, bonus) VALUES (?, ?, ?, ?)',
                (user_id, "investor", until.isoformat(), 0.25)
            )
            count += 1

        conn.commit()
        await message.answer(f"✅ Бустер бизнеса (+25%) выдан {count} пользователям на {days} дней")
    except Exception as e:
        logger.error(f"Error in give_all_biz: {e}")
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("badges_list"))
async def badges_list_command(message: Message):
    """Команда для просмотра всех доступных бейджей"""
    try:
        # Разделяем бейджи на категории
        wipe_badges = {k: v for k, v in BADGES.items() if k <= 10}
        admin_badges = {k: v for k, v in BADGES.items() if k > 10}
        
        text = "🎖️ <b>Список всех бейджей</b>\n\n"
        
        # Бейджи за вайпы (1-10)
        text += "🔄 <b>Бейджи за вайпы:</b>\n"
        for badge_id, badge_info in wipe_badges.items():
            text += f"{badge_id}. {badge_info['name']}\n"
            text += f"   ▸ Получается за определенное количество вайпов\n"
            text += f"   ▸ Установить: {badge_info['command']}\n\n"
        
        # Административные бейджи (11-12)
        text += "👑 <b>Административные бейджи:</b>\n"
        for badge_id, badge_info in admin_badges.items():
            text += f"{badge_id}. {badge_info['name']}\n"
            text += f"   ▸ Выдается администратором\n"
            text += f"   ▸ Установить: {badge_info['command']}\n\n"

        # Добавляем информацию для админа
        if message.from_user.id in ADMINS:
            text += (
                "гойда\n"

            )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in badges_list command: {e}")
        await message.answer("❌ Произошла ошибка при получении списка бейджей")




@dp.message(F.text.regexp(rf'^/work_(\d+)(@{BOT_USERNAME})?$'))
async def start_work(message: Message):
    user_id = message.from_user.id
    try:
        command_text = message.text.split('@')[0]
        job_id = int(command_text.split('_')[1])
        
        success, result = complete_work(user_id, job_id)
        if success:
            # Обновляем прогресс BP - передаем точный тип задания
            update_bp_task_progress(user_id, "work")
        await message.answer(result)
    except (ValueError, IndexError):
        await message.answer("❌ Неверный формат команды. Используйте: /work_1")
        
def complete_work(user_id: int, job_id: int) -> Tuple[bool, str]:
    try:
        job = next((j for j in WORK_JOBS if j['id'] == job_id), None)
        if not job:
            return False, "Работа не найдена"
        
        experience, last_work_time = get_user_work_stats(user_id)

        # Проверка опыта (убрано ограничение max_exp)
        if experience < job['min_exp']:
            return False, (
                f"❌ У вас недостаточно опыта!\n"
                f"Требуемый опыт - {job['min_exp']}+\n"
                f"У вас - {experience} опыта"
            )
        
        # Проверка кулдауна (1 час)
        if last_work_time:
            next_work_time = last_work_time + timedelta(hours=1)
            if datetime.now() < next_work_time:
                time_left = next_work_time - datetime.now()
                return False, (
                    f"⏳ Вы уже работали недавно!\n"
                    f"Следующая работа возможна через: "
                    f"{time_left.seconds//3600}ч {(time_left.seconds%3600)//60}м"
                )
        
        # Начисление награды
        reward = job['reward']
        user = get_user(user_id)
        new_usd = user[2] + reward
        
        # Обновляем данные
        cursor.execute('UPDATE users SET usd_balance = ? WHERE user_id = ?', (new_usd, user_id))
        cursor.execute('''
        UPDATE user_work_stats 
        SET total_experience = total_experience + 1,
            last_work_time = ?,
            total_usd_earned = total_usd_earned + ?
        WHERE user_id = ?
        ''', (datetime.now().isoformat(), reward, user_id))
        
        conn.commit()
        
        # Проверяем получение ивент валюты (5% шанс)
        event_currency_received = try_give_5percent(user_id, 1)
        
        # Формируем сообщение
        result_text = (
            f"✅ Вы успешно поработали: {job['name']}\n"
            f"💵 Заработано: ${format_number(reward, True)}\n"
            f"💰 Баланс: ${format_number(new_usd, True)}\n"
            f"🌟 Опыт: {experience + 1} (+1)"
        )
        
        # Добавляем информацию о полученной ивент валюте, если она была выдана
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            result_text += f"\n\n🎉 +1 🍁 (Баланс: {current_balance} 🍁)\n/top_ivent"
        
        return True, result_text
        
    except Exception as e:
        logger.error(f"Error in complete_work: {e}")
        conn.rollback()
        return False, "⚠ Ошибка при выполнении работы"
        
# Добавляем в начало файла (после других импортов)
import time

# Добавляем новую функцию для отправки уведомлений о новых пользователях
async def notify_new_user(user_id: int, username: Optional[str]):
    """Отправляет уведомление о новом пользователе в указанный чат"""
    notification_chat_id = -1002734900704  # ID чата для уведомлений
    try:
        user_info = await bot.get_chat(user_id)
        name = user_info.full_name if user_info.full_name else "Без имени"
        username_part = f"(@{username})" if username else ""
        
        message = (
            "🆕 Новый пользователь:\n"
            f"👤 Имя: {name} {username_part}\n"
            f"🆔 ID: {user_id}\n"
            f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )
        
        await bot.send_message(chat_id=notification_chat_id, text=message)
    except Exception as e:
        logger.error(f"Error sending new user notification: {e}")

def buy_lottery_tickets(user_id: int, count: int) -> Tuple[bool, str]:
    try:
        if count <= 0:
            return False, "Количество билетов должно быть положительным числом"
            
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
            
        # Проверяем лимиты
        max_tickets = 1500 if is_premium(user_id) else 1000
        cursor.execute('SELECT ticket_count FROM lottery_tickets WHERE user_id = ?', (user_id,))
        current_tickets = cursor.fetchone()
        current_count = current_tickets[0] if current_tickets else 0
        
        if current_count + count > max_tickets:
            return False, (
                f"❌ Превышен лимит билетов!\n"
                f"Максимум можно купить: {max_tickets} ({'PREMIUM' if is_premium(user_id) else 'без PREMIUM'})\n"
                f"Уже куплено: {current_count}\n"
                f"Попытка купить ещё: {count}"
            )
            
        # Проверяем баланс
        total_price = 50000 * count
        if user[2] < total_price:
            return False, (
                f"❌ Недостаточно средств!\n"
                f"Цена за {count} билетов: ${format_number(total_price, True)}\n"
                f"Ваш баланс: ${format_number(user[2], True)}"
            )
            
        # Покупаем билеты
        new_usd = user[2] - total_price
        update_balance(user_id, usd=new_usd)
        
        if current_tickets:
            cursor.execute('UPDATE lottery_tickets SET ticket_count = ticket_count + ? WHERE user_id = ?', 
                         (count, user_id))
        else:
            cursor.execute('INSERT INTO lottery_tickets (user_id, ticket_count) VALUES (?, ?)', 
                         (user_id, count))
        
        conn.commit()
        
        # Получаем общее количество билетов
        cursor.execute('SELECT SUM(ticket_count) FROM lottery_tickets')
        total_tickets = cursor.fetchone()[0] or 0
        
        return True, (
            f"🎫 Ты успешно приобрёл билет(ы) в лотерее #{total_tickets}\n\n"
            f"Всего купленно билетов: x{count}\n"
            f"Всего у тебя: {current_count + count}\n"
            f"Общее количество билетов: {total_tickets}\n\n"
            f"💰 Новый баланс: ${format_number(new_usd, True)}"
        )
    except Exception as e:
        logger.error(f"Error buying lottery tickets: {e}")
        conn.rollback()
        return False, "Произошла ошибка при покупке билетов"

def get_total_tickets() -> int:
    try:
        cursor.execute('SELECT SUM(ticket_count) FROM lottery_tickets')
        result = cursor.fetchone()[0]
        return result if result else 0
    except Exception as e:
        logger.error(f"Error getting total tickets: {e}")
        return 0

def get_user_tickets(user_id: int) -> int:
    try:
        cursor.execute('SELECT ticket_count FROM lottery_tickets WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"Error getting user tickets: {e}")
        return 0


async def draw_lottery_winners(count: int = 5) -> List[Dict]:
    try:
        # Получаем всех участников с их билетами
        cursor.execute('''
        SELECT user_id, ticket_count 
        FROM lottery_tickets 
        WHERE ticket_count > 0
        ''')
        participants = cursor.fetchall()
        
        if not participants:
            return []
            
        # Подсчитываем общее количество билетов
        total_tickets = sum(ticket_count for _, ticket_count in participants)
        
        # Базовый расчет призового фонда
        prize_pool = int((total_tickets * 50000) / 5)
        
        # Создаем список, где каждый билет - это шанс
        tickets_pool = []
        ticket_owners = {}
        ticket_counter = 1
        
        for user_id, ticket_count in participants:
            for _ in range(ticket_count):
                tickets_pool.append(user_id)
                ticket_owners[ticket_counter] = user_id
                ticket_counter += 1
            
        # Выбираем победителей
        winners = []
        if len(tickets_pool) < count:
            # Если билетов меньше, чем победителей, выбираем всех
            unique_winners = set(tickets_pool)
            for i, winner_id in enumerate(sorted(unique_winners), 1):
                # Призы от большего к меньшему с разницей ~10-15 млн
                prize = prize_pool - random.randint(5_000_000, 15_000_000) * (i-1)
                prize = make_uneven(prize)  # Убрано self
                winners.append({
                    "user_id": winner_id,
                    "prize": prize,
                    "ticket_number": next(k for k, v in ticket_owners.items() if v == winner_id)
                })
        else:
            # Выбираем случайные билеты (без повторов)
            selected_ticket_numbers = random.sample(range(1, len(tickets_pool)+1), count)
            for i, ticket_number in enumerate(sorted(selected_ticket_numbers), 1):
                winner_id = ticket_owners[ticket_number]
                # Призы распределяются от большего к меньшему
                prize = prize_pool - random.randint(10_000_000, 15_000_000) * (i-1)
                prize = make_uneven(prize)  # Убрано self
                winners.append({
                    "user_id": winner_id,
                    "prize": prize,
                    "ticket_number": ticket_number
                })
        
        # Сортируем победителей по убыванию приза (1 место -> 5 место)
        winners.sort(key=lambda x: -x['prize'])
        
        # Записываем победителей в базу и выдаем призы
        now = datetime.now().isoformat()
        for winner in winners:
            # Выдаем приз
            cursor.execute('SELECT usd_balance FROM users WHERE user_id = ?', (winner['user_id'],))
            current_balance = cursor.fetchone()[0]
            new_balance = current_balance + winner['prize']
            update_balance(winner['user_id'], usd=new_balance)
            
            # Записываем в историю
            cursor.execute('''
            INSERT INTO lottery_winners (user_id, prize_amount, draw_date)
            VALUES (?, ?, ?)
            ''', (winner['user_id'], winner['prize'], now))
        
        # Очищаем билеты
        cursor.execute('UPDATE lottery_tickets SET ticket_count = 0')
        conn.commit()
        
        return winners
    except Exception as e:
        logger.error(f"Error drawing lottery winners: {e}")
        conn.rollback()
        return []

def make_uneven(amount: int) -> int:  # Убрано self
    """Делает сумму более естественной, добавляя/убавляя случайные тысячи"""
    variation = random.randint(-250_000, 250_000)
    return max(1_000_000, amount + variation)  # Не меньше 1 млн
        

        
@dp.message(Command("auction"))
async def auction_list(message: Message):
    try:
        # Получаем количество карт каждого типа на аукционе
        cursor.execute('''
        SELECT card_id, COUNT(*) as count, MIN(price) as min_price
        FROM auction_cards
        GROUP BY card_id
        ORDER BY card_id DESC
        ''')
        lots = cursor.fetchall()
        
        if not lots:
            await message.answer("🔨 Это аукцион видеокарт, где ты можешь купить нужную для себя видеокарту по низкой цене. Желаю удачи словить то, что ты ищешь! 🍀 \n\nДоступные предложения:\n ")
            return
            
        text = "🔨 Это аукцион видеокарт, где ты можешь купить нужную для себя видеокарту по низкой цене. Желаю удачи словить то, что ты ищешь! 🍀\n"
        text += f"Доступно карт: {sum(count for _, count, _ in lots)} шт.\n"
        text += "Доступные предложения:\n\n"
        
        for card_id, count, min_price in lots:
            card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
            if card:
                text += f"x{count} {card['name']} - от {format_number(min_price, True)} $ (/auc_info_{card_id})\n"
        
        await message.answer(text)
    except Exception as e:
        logger.error(f"Ошибка при загрузке аукциона: {e}")
        await message.answer("❌ Произошла ошибка при загрузке аукциона")

@dp.message(F.text.regexp(r'^/auc_info_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def auction_card_info(message: Message):
    try:
        # Извлекаем ID карты из команды (игнорируя @username если есть)
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        card_id = int(command_text.split('_')[2])
        
        card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        
        if not card:
            await message.answer("❌ Видеокарта не найдена")
            return
            
        # Получаем все лоты для этого типа карт, сортированные по цене
        cursor.execute('''
        SELECT id, price 
        FROM auction_cards 
        WHERE card_id = ?
        ORDER BY price ASC
        LIMIT 10
        ''', (card_id,))
        lots = cursor.fetchall()
        
        if not lots:
            await message.answer(f"❌ Нет доступных предложений для {card['name']}")
            return
            
        text = f"Предложения по карте {card['name']}:\n\n"
        
        for lot_id, price in lots:
            text += f"#{lot_id}. {card['name']} - {format_number(price, True)} $ (/auc_buy_{lot_id})\n"
            
        await message.answer(text)
    except Exception as e:
        logger.error(f"Ошибка при загрузке информации о карте: {e}")
        await message.answer("❌ Произошла ошибка при загрузке информации")

@dp.message(F.text.regexp(r'^/auc_buy_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def buy_from_auction(message: Message):
    user_id = message.from_user.id
    try:
        # Извлекаем ID лота из команды (игнорируя @username если есть)
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        lot_id = int(command_text.split('_')[2])
        
        # Получаем информацию о лоте
        cursor.execute('''
        SELECT card_id, price 
        FROM auction_cards 
        WHERE id = ?
        ''', (lot_id,))
        lot = cursor.fetchone()
        
        if not lot:
            await message.answer("❌ Предложение не найдено или уже куплено")
            return
            
        card_id, price = lot
        
        # Проверяем доступность карты для текущего уровня фермы
        user = get_user(user_id)
        if not user:
            await message.answer("❌ Покупатель не найден")
            return
            
        current_expansion = user[5]
        expansion_info = EXPANSIONS[current_expansion - 1]
        
        if not (expansion_info['first_available_card'] <= card_id <= expansion_info['last_available_card']):
            await message.answer(
                f"❌ Эта видеокарта недоступна для вашего уровня фермы!\n"
                f"Доступные карты: {expansion_info['first_available_card']}-{expansion_info['last_available_card']}\n"
                f"Улучшайте ферму (/capacity) чтобы открыть новые карты."
            )
            return
            
        card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        
        if not card:
            await message.answer("❌ Видеокарта не найдена")
            return
            
        # Проверяем баланс покупателя
        buyer = get_user(user_id)
        if not buyer:
            await message.answer("❌ Покупатель не найден")
            return
            
        if buyer[2] < price:
            await message.answer(
                f"❌ Недостаточно средств для покупки!\n"
                f"Цена: ${format_number(price, True)}\n"
                f"Ваш баланс: ${format_number(buyer[2], True)}"
            )
            return
            
        # Проверяем свободные слоты
        user_cards, total_cards = get_user_cards(user_id)
        max_cards = expansion_info['max_cards']
        
        if total_cards >= max_cards:
            await message.answer(
                f"❌ Не хватает места для новой карты!\n"
                f"Использовано слотов: {total_cards}/{max_cards}\n"
                f"Освободите место или улучшите ферму (/capacity)"
            )
            return
            
        # Совершаем покупку
        with conn:
            # Удаляем из аукциона
            cursor.execute('DELETE FROM auction_cards WHERE id = ?', (lot_id,))
            
            # Списываем деньги у покупателя
            new_buyer_usd = buyer[2] - price
            update_balance(user_id, usd=new_buyer_usd)
            
            # Добавляем карту покупателю
            add_user_card(user_id, card_id)
            calculate_income(user_id)
            
        await message.answer(
            f"✅ Вы успешно купили {card['name']} за ${format_number(price, True)}!\n"
            f"Новый баланс: ${format_number(new_buyer_usd, True)}\n"
            f"Доход: +{format_number(card['income'])} BTC/10мин"
        )
            
    except Exception as e:
        logger.error(f"Ошибка при покупке с аукциона: {e}")
        await message.answer("❌ Произошла ошибка при покупке")
        
        
@dp.message(Command("lottery"))
async def lottery_info(message: Message):
    user_id = message.from_user.id
    total_tickets = get_total_tickets()
    user_tickets = get_user_tickets(user_id)
    
    text = (
        "🎫 <b>ЛОТЕРЕЯ!</b> Покупайте билеты за 50.000 $ чтобы получить шанс выиграть крупный приз!\n\n"
        f"💰 Всего куплено билетов: <b>{total_tickets}</b>\n"
        f"🎟 Ты купил: <b>x{user_tickets}</b> билетов\n\n"
        "🏆 Итоги лотереи публикуются каждое воскресенье в новостном канале @CryptoMiner_News\n\n"
        f"💎 Максимум можно купить: <b>{'1500 (PREMIUM)' if is_premium(user_id) else '1000'}</b> билетов\n\n"
        "<b>Приобрести билет:</b>\n"
        "/lottery_confirm [количество]"
    )
    
    await message.answer(text, parse_mode='HTML')

@dp.message(F.text.regexp(rf'^/lottery_confirm(?:\@{re.escape(BOT_USERNAME)})?\s+(\d+)$'))
async def lottery_confirm(message: Message):
    user_id = message.from_user.id
    try:
        # Извлекаем количество билетов (по умолчанию 1)
        amount = int(message.text.split()[-1])
        if amount <= 0:
            await message.answer("❌ Количество билетов должно быть положительным числом")
            return
            
        success, result = buy_lottery_tickets(user_id, amount)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
    except ValueError:
        await message.answer("❌ Неверный формат. Используйте: /lottery_confirm [количество]")

@dp.message(Command("draw_lottery"))
async def draw_lottery(message: Message):
    # Проверяем, что команду вызывает владелец
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Получаем общее количество билетов
        total_tickets = get_total_tickets()
        if total_tickets == 0:
            await message.answer("❌ Нет участников лотереи")
            return
        
        # Розыгрыш 5 победителей
        winners = await draw_lottery_winners(5)
        
        if not winners:
            await message.answer("❌ Не удалось определить победителей")
            return
            
        # Формируем отчет для канала
        report = [
            "🔥 <b>Результаты ЛОТЕРЕИ этой недели:</b>\n",
            f"Всего было приобретено билетов: {total_tickets} шт\n\n"
        ]
        
        for i, winner in enumerate(winners, 1):
            try:
                user = await bot.get_chat(winner['user_id'])
                name = user.full_name or f"ID {winner['user_id']}"
                
                # Генерируем случайный номер билета для наглядности
                ticket_number = random.randint(1, total_tickets)
                
                report.append(
                    f"{name} с билетом #{ticket_number} получает {format_number(winner['prize'], True)} $\n"
                )
            except Exception as e:
                logger.error(f"Error getting user info: {e}")
                report.append(
                    f"ID {winner['user_id']} с билетом #{random.randint(1, total_tickets)} получает {format_number(winner['prize'], True)} $\n"
                )
        
        report.append("\n🏆 Поздравляем победителей! 🏆")
        
        # Отправляем в канал (замените CHANNEL_ID на реальный ID вашего канала)
        CHANNEL_ID = -1002780167646  # Замените на реальный ID канала
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text="".join(report),
            parse_mode='HTML'
        )
        
        await message.answer("✅ Результаты лотереи успешно опубликованы в канале")
    except Exception as e:
        logger.error(f"Error in lottery draw: {e}")
        await message.answer(f"❌ Ошибка при розыгрыше лотереи: {str(e)}")   


DEPOSIT_TYPES = {
    1: {"name": "1 месяц", "duration_days": 30, "weekly_rate": 0.10, "min_amount": 100000},
    2: {"name": "2 месяца", "duration_days": 60, "weekly_rate": 0.13, "min_amount": 500000},
    3: {"name": "3 месяца", "duration_days": 90, "weekly_rate": 0.15, "min_amount": 1000000},
    4: {"name": "6 месяцев", "duration_days": 180, "weekly_rate": 0.20, "min_amount": 5000000}
}

LOAN_INTEREST_RATE = 0.30  # 30% в неделю
LOAN_LIMITS = {
    (15, 19): 40000000,
    (20, 29): 200000000,
    (30, 39): 500000000,
    (40, 49): 1500000000,
    (50, 59): 3000000000,
    (60, 80): 10000000000
}

# Функции для работы с банком
def get_user_loan_limit(user_id: int) -> float:
    """Получает лимит кредита для пользователя на основе уровня фермы"""
    user = get_user(user_id)
    if not user:
        return 0
    
    expansion_level = user[5]  # Уровень фермы
    
    for (min_level, max_level), limit in LOAN_LIMITS.items():
        if min_level <= expansion_level <= max_level:
            return limit
    
    return 0

def get_user_active_loan(user_id: int) -> Optional[Tuple]:
    """Получает активный кредит пользователя"""
    try:
        cursor.execute('''
        SELECT id, amount, interest_rate, start_date, next_payment_date 
        FROM bank_loans 
        WHERE user_id = ? AND status = 'active'
        ''', (user_id,))
        return cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"Error getting user loan: {e}")
        return None

def get_user_active_deposits(user_id: int) -> List[Tuple]:
    """Получает активные вклады пользователя"""
    try:
        cursor.execute('''
        SELECT id, deposit_type, amount, interest_rate, start_date, end_date 
        FROM bank_deposits 
        WHERE user_id = ? AND status = 'active'
        ''', (user_id,))
        return cursor.fetchall()
    except sqlite3.Error as e:
        logger.error(f"Error getting user deposits: {e}")
        return []

def create_deposit(user_id: int, deposit_type: int, amount: float) -> Tuple[bool, str]:
    """Создает вклад для пользователя"""
    try:
        if deposit_type not in DEPOSIT_TYPES:
            return False, "Неверный тип вклада"
        
        deposit_info = DEPOSIT_TYPES[deposit_type]
        
        # Проверяем минимальную сумму
        if amount < deposit_info['min_amount']:
            return False, f"Минимальная сумма для этого вклада: ${format_number(deposit_info['min_amount'], True)}"
        
        # Проверяем баланс пользователя
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        if user[2] < amount:
            return False, f"Недостаточно средств. Ваш баланс: ${format_number(user[2], True)}"
        
        # Списываем средства
        new_usd = user[2] - amount
        update_balance(user_id, usd=new_usd)
        
        # Создаем вклад
        start_date = datetime.now()
        end_date = start_date + timedelta(days=deposit_info['duration_days'])
        
        cursor.execute('''
        INSERT INTO bank_deposits (user_id, deposit_type, amount, interest_rate, start_date, end_date)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, deposit_type, amount, deposit_info['weekly_rate'], 
              start_date.isoformat(), end_date.isoformat()))
        
        # Записываем транзакцию
        cursor.execute('''
        INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
        VALUES (?, 'deposit_create', ?, ?, ?)
        ''', (user_id, -amount, f"Создание вклада {deposit_info['name']}", start_date.isoformat()))
        
        conn.commit()
        
        return True, (
            f"✅ Вклад успешно создан!\n"
            f"Тип: {deposit_info['name']}\n"
            f"Сумма: ${format_number(amount, True)}\n"
            f"Процентная ставка: {deposit_info['weekly_rate']*100}% в неделю\n"
            f"Срок до: {end_date.strftime('%d.%m.%Y')}\n"
            f"Новый баланс: ${format_number(new_usd, True)}"
        )
        
    except Exception as e:
        logger.error(f"Error creating deposit: {e}")
        conn.rollback()
        return False, "Произошла ошибка при создании вклада"

def create_loan(user_id: int, amount: float) -> Tuple[bool, str]:
    """Выдает кредит пользователю"""
    try:
        # Проверяем лимит
        loan_limit = get_user_loan_limit(user_id)
        if amount > loan_limit:
            return False, f"Превышен лимит кредита. Ваш лимит: ${format_number(loan_limit, True)}"
        
        # Проверяем, есть ли активный кредит
        existing_loan = get_user_active_loan(user_id)
        if existing_loan:
            return False, "У вас уже есть активный кредит"
        
        # Получаем текущую ставку (может зависеть от цены BTC)
        btc_price = get_btc_price()
        interest_rate = LOAN_INTEREST_RATE
        
        # Начисляем средства
        user = get_user(user_id)
        new_usd = user[2] + amount
        update_balance(user_id, usd=new_usd)
        
        # Создаем кредит
        start_date = datetime.now()
        next_payment_date = start_date + timedelta(days=7)  # Первый платеж через неделю
        
        cursor.execute('''
        INSERT INTO bank_loans (user_id, amount, interest_rate, start_date, next_payment_date)
        VALUES (?, ?, ?, ?, ?)
        ''', (user_id, amount, interest_rate, start_date.isoformat(), next_payment_date.isoformat()))
        
        # Записываем транзакцию
        cursor.execute('''
        INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
        VALUES (?, 'loan_create', ?, ?, ?)
        ''', (user_id, amount, f"Получение кредита под {interest_rate*100}%", start_date.isoformat()))
        
        conn.commit()
        
        return True, (
            f"✅ Кредит успешно получен!\n"
            f"Сумма: ${format_number(amount, True)}\n"
            f"Процентная ставка: {interest_rate*100}% в неделю\n"
            f"Первый платеж: {next_payment_date.strftime('%d.%m.%Y')}\n"
            f"Новый баланс: ${format_number(new_usd, True)}"
        )
        
    except Exception as e:
        logger.error(f"Error creating loan: {e}")
        conn.rollback()
        return False, "Произошла ошибка при получении кредита"

def process_loan_payments():
    """Обрабатывает еженедельные платежи по кредитам"""
    try:
        now = datetime.now()
        cursor.execute('''
        SELECT id, user_id, amount, interest_rate 
        FROM bank_loans 
        WHERE status = 'active' AND next_payment_date <= ?
        ''', (now.isoformat(),))
        
        loans = cursor.fetchall()
        
        for loan_id, user_id, amount, interest_rate in loans:
            user = get_user(user_id)
            if not user:
                continue
            
            # Рассчитываем платеж
            payment = amount * interest_rate
            
            if user[2] >= payment:
                # Списание платежа
                new_usd = user[2] - payment
                update_balance(user_id, usd=new_usd)
                
                # Обновляем дату следующего платежа
                next_payment = now + timedelta(days=7)
                cursor.execute('''
                UPDATE bank_loans 
                SET next_payment_date = ?
                WHERE id = ?
                ''', (next_payment.isoformat(), loan_id))
                
                # Записываем транзакцию
                cursor.execute('''
                INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                VALUES (?, 'loan_payment', ?, ?, ?)
                ''', (user_id, -payment, f"Платеж по кредиту", now.isoformat()))
                
            else:
                # Пользователь не может оплатить - закрываем кредит с долгом
                cursor.execute('''
                UPDATE bank_loans 
                SET status = 'default'
                WHERE id = ?
                ''', (loan_id,))
                
                # Записываем транзакцию
                cursor.execute('''
                INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                VALUES (?, 'loan_default', ?, ?, ?)
                ''', (user_id, 0, f"Дефолт по кредиту", now.isoformat()))
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error processing loan payments: {e}")
        conn.rollback()

def process_deposit_interests():
    """Начисляет проценты по вкладам"""
    try:
        now = datetime.now()
        cursor.execute('''
        SELECT id, user_id, deposit_type, amount, interest_rate 
        FROM bank_deposits 
        WHERE status = 'active'
        ''', (now.isoformat(),))
        
        deposits = cursor.fetchall()
        
        for deposit_id, user_id, deposit_type, amount, interest_rate in deposits:
            # Начисляем проценты
            interest = amount * interest_rate
            
            user = get_user(user_id)
            if user:
                new_usd = user[2] + interest
                update_balance(user_id, usd=new_usd)
                
                # Записываем транзакцию
                cursor.execute('''
                INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                VALUES (?, 'deposit_interest', ?, ?, ?)
                ''', (user_id, interest, f"Проценты по вкладу", now.isoformat()))
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error processing deposit interests: {e}")
        conn.rollback()

def check_deposit_maturity():
    """Проверяет зрелость вкладов и возвращает средства"""
    try:
        now = datetime.now()
        cursor.execute('''
        SELECT id, user_id, amount 
        FROM bank_deposits 
        WHERE status = 'active' AND end_date <= ?
        ''', (now.isoformat(),))
        
        deposits = cursor.fetchall()
        
        for deposit_id, user_id, amount in deposits:
            user = get_user(user_id)
            if user:
                # Возвращаем основную сумму
                new_usd = user[2] + amount
                update_balance(user_id, usd=new_usd)
                
                # Закрываем вклад
                cursor.execute('''
                UPDATE bank_deposits 
                SET status = 'completed'
                WHERE id = ?
                ''', (deposit_id,))
                
                # Записываем транзакцию
                cursor.execute('''
                INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                VALUES (?, 'deposit_complete', ?, ?, ?)
                ''', (user_id, amount, f"Возврат вклада", now.isoformat()))
        
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error checking deposit maturity: {e}")
        conn.rollback()

@dp.callback_query(F.data.startswith("bank_"))
async def bank_handlers(callback: CallbackQuery):
    """Общий обработчик для всех банковских callback'ов"""
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[-1])

        if callback.from_user.id != user_id:
            return
        
        user = get_user(user_id)
        if not user:
            return
        
        # Перенаправляем на соответствующий обработчик
        if callback.data.startswith("bank_deposits_"):
            await bank_deposits_handler(callback)
        elif callback.data.startswith("bank_loans_"):
            await bank_loans_handler(callback)
        elif callback.data.startswith("bank_transactions_"):
            await bank_transactions_handler(callback)
        elif callback.data.startswith("bank_back_"):
            await bank_back_handler(callback)
        elif callback.data.startswith("bank_close_"):
            await bank_close_handler(callback)
            
    except Exception as e:
        logger.error(f"Error in bank handler: {e}")

async def process_all_deposits_and_loans():
    """Обрабатывает все активные вклады и кредиты с уведомлениями"""
    try:
        now = datetime.now()
        processed_deposits = 0
        processed_loans = 0
        notifications = []  # Список для хранения уведомлений
        
        # Обработка вкладов
        cursor.execute('''
        SELECT id, user_id, deposit_type, amount, interest_rate, start_date, end_date 
        FROM bank_deposits 
        WHERE status = 'active'
        ''')
        
        deposits = cursor.fetchall()
        
        for deposit_id, user_id, deposit_type, amount, interest_rate, start_date, end_date in deposits:
            try:
                end_date_obj = datetime.fromisoformat(end_date)
                
                # Если срок вклада истек
                if now >= end_date_obj:
                    # Возвращаем основную сумму + проценты
                    total_return = amount + (amount * interest_rate * 4)  # 4 недели в месяце
                    
                    user = get_user(user_id)
                    if user:
                        new_usd = user[2] + total_return
                        update_balance(user_id, usd=new_usd)
                        
                        # Закрываем вклад
                        cursor.execute('''
                        UPDATE bank_deposits 
                        SET status = 'completed'
                        WHERE id = ?
                        ''', (deposit_id,))
                        
                        # Записываем транзакцию
                        cursor.execute('''
                        INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                        VALUES (?, 'deposit_complete', ?, ?, ?)
                        ''', (user_id, total_return, f"Завершение вклада + проценты", now.isoformat()))
                        
                        processed_deposits += 1
                        
                        # Добавляем уведомление для пользователя
                        deposit_info = DEPOSIT_TYPES.get(deposit_type, {"name": "Неизвестный вклад"})
                        notifications.append({
                            "user_id": user_id,
                            "type": "deposit_completed",
                            "message": (
                                f"💰 <b>Вклад завершен!</b>\n\n"
                                f"Тип вклада: {deposit_info['name']}\n"
                                f"Сумма возврата: ${format_number(total_return, True)}\n"
                                f"Новый баланс: ${format_number(new_usd, True)}\n\n"
                                f"⏰ Дата: {now.strftime('%d.%m.%Y %H:%M')}"
                            ),
                            "success": True
                        })
                        
                        logger.info(f"Deposit completed for user {user_id}: ${total_return}")
                        
            except Exception as e:
                logger.error(f"Error processing deposit {deposit_id}: {e}")
                # Добавляем уведомление об ошибке
                notifications.append({
                    "user_id": user_id,
                    "type": "deposit_error",
                    "message": f"❌ Ошибка при обработке вклада: {str(e)}",
                    "success": False,
                    "error": str(e)
                })
                continue
        
        # Обработка кредитов - ТОЛЬКО если срок платежа настал
        cursor.execute('''
        SELECT id, user_id, amount, interest_rate, start_date, next_payment_date 
        FROM bank_loans 
        WHERE status = 'active' AND next_payment_date <= ?
        ''', (now.isoformat(),))
        
        loans = cursor.fetchall()
        
        for loan_id, user_id, loan_amount, interest_rate, start_date, next_payment_date in loans:
            try:
                user = get_user(user_id)
                if not user:
                    continue
                
                weekly_payment = loan_amount * interest_rate
                
                # Проверяем, хватает ли денег для полного погашения
                if user[2] >= loan_amount:
                    # Полное погашение
                    new_usd = user[2] - loan_amount
                    update_balance(user_id, usd=new_usd)
                    
                    # Закрываем кредит
                    cursor.execute('''
                    UPDATE bank_loans 
                    SET status = 'repaid', amount = 0
                    WHERE id = ?
                    ''', (loan_id,))
                    
                    # Записываем транзакцию
                    cursor.execute('''
                    INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                    VALUES (?, 'loan_full_repayment', ?, ?, ?)
                    ''', (user_id, -loan_amount, f"Полное погашение кредита", now.isoformat()))
                    
                    # Добавляем уведомление
                    notifications.append({
                        "user_id": user_id,
                        "type": "loan_full_repayment",
                        "message": (
                            f"🎉 <b>Кредит полностью погашен!</b>\n\n"
                            f"Сумма погашения: ${format_number(loan_amount, True)}\n"
                            f"Новый баланс: ${format_number(new_usd, True)}\n\n"
                            f"⏰ Дата: {now.strftime('%d.%m.%Y %H:%M')}"
                        ),
                        "success": True
                    })
                    
                    logger.info(f"Loan fully repaid by user {user_id}: ${loan_amount}")
                    
                elif user[2] >= weekly_payment:
                    # Еженедельный платеж
                    new_usd = user[2] - weekly_payment
                    update_balance(user_id, usd=new_usd)
                    
                    # Обновляем дату следующего платежа
                    next_payment = now + timedelta(days=7)
                    cursor.execute('''
                    UPDATE bank_loans 
                    SET next_payment_date = ?
                    WHERE id = ?
                    ''', (next_payment.isoformat(), loan_id))
                    
                    # Записываем транзакцию
                    cursor.execute('''
                    INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                    VALUES (?, 'loan_payment', ?, ?, ?)
                    ''', (user_id, -weekly_payment, f"Еженедельный платеж по кредиту", now.isoformat()))
                    
                    # Добавляем уведомление
                    notifications.append({
                        "user_id": user_id,
                        "type": "loan_payment",
                        "message": (
                            f"📋 <b>Еженедельный платеж по кредиту</b>\n\n"
                            f"Сумма платежа: ${format_number(weekly_payment, True)}\n"
                            f"Остаток по кредиту: ${format_number(loan_amount, True)}\n"
                            f"Новый баланс: ${format_number(new_usd, True)}\n"
                            f"Следующий платеж: {next_payment.strftime('%d.%m.%Y')}\n\n"
                            f"⏰ Дата: {now.strftime('%d.%m.%Y %H:%M')}"
                        ),
                        "success": True
                    })
                    
                    logger.info(f"Loan payment by user {user_id}: ${weekly_payment}")
                    
                else:
                    # Не хватает денег - списываем все что есть и уходим в минус
                    available_funds = user[2]
                    remaining_debt = weekly_payment - available_funds
                    
                    # Списываем все доступные средства
                    new_usd = user[2] - available_funds
                    update_balance(user_id, usd=new_usd)
                    
                    # Обновляем сумму кредита (добавляем оставшийся долг)
                    new_loan_amount = loan_amount + remaining_debt
                    next_payment = now + timedelta(days=7)
                    cursor.execute('''
                    UPDATE bank_loans 
                    SET amount = ?, next_payment_date = ?
                    WHERE id = ?
                    ''', (new_loan_amount, next_payment.isoformat(), loan_id))
                    
                    # Записываем транзакцию
                    cursor.execute('''
                    INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
                    VALUES (?, 'loan_partial_payment', ?, ?, ?)
                    ''', (user_id, -available_funds, f"Частичный платеж по кредиту (не хватило ${format_number(remaining_debt, True)})", now.isoformat()))
                    
                    # Добавляем уведомление
                    notifications.append({
                        "user_id": user_id,
                        "type": "loan_partial_payment",
                        "message": (
                            f"⚠️ <b>ЧАСТИЧНЫЙ ПЛАТЕЖ ПО КРЕДИТУ!</b>\n\n"
                            f"Было списано: ${format_number(available_funds, True)}\n"
                            f"Не хватило: ${format_number(remaining_debt, True)}\n"
                            f"Долг увеличен до: ${format_number(new_loan_amount, True)}\n"
                            f"Текущий баланс: ${format_number(new_usd, True)}\n"
                            f"Следующий платеж: {next_payment.strftime('%d.%m.%Y')}\n\n"
                            f"💡 <b>Ваш баланс ушел в минус!</b> Рекомендуем пополнить счет.\n\n"
                            f"⏰ Дата: {now.strftime('%d.%m.%Y %H:%M')}"
                        ),
                        "success": True
                    })
                    
                    logger.warning(f"Partial loan payment by user {user_id}: paid ${available_funds}, missing ${remaining_debt}")
                
                processed_loans += 1
                
            except Exception as e:
                logger.error(f"Error processing loan {loan_id}: {e}")
                # Добавляем уведомление об ошибке
                notifications.append({
                    "user_id": user_id,
                    "type": "loan_error",
                    "message": f"❌ Ошибка при обработке кредита: {str(e)}",
                    "success": False,
                    "error": str(e)
                })
                continue
        
        conn.commit()
        
        # Отправляем уведомления пользователям
        await send_user_notifications(notifications)
        
        return processed_deposits, processed_loans, notifications
        
    except Exception as e:
        logger.error(f"Error in process_all_deposits_and_loans: {e}")
        conn.rollback()
        return 0, 0, []

async def send_user_notifications(notifications):
    """Отправляет уведомления пользователям и админу об ошибках"""
    error_notifications = []
    
    for notification in notifications:
        try:
            user_id = notification["user_id"]
            
            if notification["success"]:
                # Отправляем успешное уведомление пользователю
                await bot.send_message(
                    chat_id=user_id,
                    text=notification["message"],
                    parse_mode='HTML'
                )
                await asyncio.sleep(0.1)  # Небольшая задержка между сообщениями
            else:
                # Для ошибок собираем информацию для админа
                error_notifications.append(notification)
                
        except Exception as e:
            logger.error(f"Error sending notification to user {notification['user_id']}: {e}")
            # Если не удалось отправить пользователю, добавляем в ошибки
            error_notifications.append({
                **notification,
                "send_error": str(e)
            })
    
    # Отправляем ошибки админу
    if error_notifications:
        error_text = "🚨 <b>ОШИБКИ ПРИ ОБРАБОТКЕ БАНКОВСКИХ ОПЕРАЦИЙ</b>\n\n"
        
        for i, error in enumerate(error_notifications, 1):
            try:
                user_info = await bot.get_chat(error["user_id"])
                user_name = user_info.full_name
                user_link = f'<a href="tg://user?id={error["user_id"]}">{user_name}</a>'
            except:
                user_link = f'ID {error["user_id"]}'
            
            error_text += (
                f"{i}. 👤 {user_link}\n"
                f"   📝 Тип: {error['type']}\n"
                f"   ❌ Ошибка: {error['error']}\n"
            )
            
            if "send_error" in error:
                error_text += f"   📤 Ошибка отправки: {error['send_error']}\n"
            
            error_text += "   ────────────────────\n"
        
        try:
            # Отправляем всем админам
            for admin_id in ADMINS:
                await bot.send_message(
                    chat_id=admin_id,
                    text=error_text,
                    parse_mode='HTML'
                )
        except Exception as e:
            logger.error(f"Error sending error report to admin: {e}")

@dp.message(Command("process_bank_operations"))
async def process_bank_operations_command(message: Message):
    """Команда для ручной обработки банковских операций (только для админа)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        processing_msg = await message.answer("🔄 Начинаю обработку банковских операций...")
        
        deposits_processed, loans_processed, notifications = await process_all_deposits_and_loans()
        
        # Статистика по уведомлениям
        success_notifications = sum(1 for n in notifications if n["success"])
        error_notifications = sum(1 for n in notifications if not n["success"])
        
        result_text = (
            "✅ <b>Обработка банковских операций завершена!</b>\n\n"
            f"💰 Обработано вкладов: {deposits_processed}\n"
            f"💳 Обработано кредитов: {loans_processed}\n"
            f"📨 Уведомлений отправлено: {success_notifications}\n"
            f"❌ Ошибок: {error_notifications}\n\n"
            f"⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )
        
        await processing_msg.edit_text(result_text, parse_mode='HTML')
        
        # Отправляем детальный отчет админу в ЛС
        if notifications:
            detail_text = "📊 <b>Детальный отчет по операциям</b>\n\n"
            
            deposit_count = sum(1 for n in notifications if n["type"] == "deposit_completed")
            full_repay_count = sum(1 for n in notifications if n["type"] == "loan_full_repayment")
            payment_count = sum(1 for n in notifications if n["type"] == "loan_payment")
            partial_count = sum(1 for n in notifications if n["type"] == "loan_partial_payment")
            
            detail_text += (
                f"💰 Завершено вкладов: {deposit_count}\n"
                f"🎉 Полных погашений: {full_repay_count}\n"
                f"📋 Обычных платежей: {payment_count}\n"
                f"⚠️ Частичных платежей: {partial_count}\n"
                f"❌ Ошибок: {error_notifications}\n\n"
                f"Общее количество операций: {len(notifications)}"
            )
            
            await bot.send_message(
                chat_id=message.from_user.id,
                text=detail_text,
                parse_mode='HTML'
            )
        
    except Exception as e:
        logger.error(f"Error in process_bank_operations_command: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

@dp.message(Command("bank_schedule"))
async def bank_schedule_command(message: Message):
    """Команда для просмотра расписания банковских операций"""
    try:
        now = datetime.now(pytz.timezone('Europe/Moscow'))
        target_time = now.replace(hour=18, minute=30, second=0, microsecond=0)
        
        # Если текущее время уже после 18:30, показываем время на завтра
        if now >= target_time:
            target_time += timedelta(days=1)
        
        time_until = target_time - now
        hours = time_until.seconds // 3600
        minutes = (time_until.seconds % 3600) // 60
        seconds = time_until.seconds % 60
        
        # Статистика активных операций
        cursor.execute('SELECT COUNT(*) FROM bank_deposits WHERE status = "active"')
        active_deposits = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM bank_loans WHERE status = "active"')
        active_loans = cursor.fetchone()[0]
        
        # Кредиты, у которых срок платежа уже наступил
        cursor.execute('SELECT COUNT(*) FROM bank_loans WHERE status = "active" AND next_payment_date <= ?', (now.isoformat(),))
        overdue_loans = cursor.fetchone()[0]
        
        text = (
            "⏰ <b>Расписание банковских операций</b>\n\n"
            f"🕒 <b>Следующая автоматическая обработка:</b>\n"
            f"▸ Дата: {target_time.strftime('%d.%m.%Y')}\n"
            f"▸ Время: {target_time.strftime('%H:%M')} (МСК)\n"
            f"▸ Через: {hours:02d}:{minutes:02d}:{seconds:02d}\n\n"
            f"📊 <b>Статистика активных операций:</b>\n"
            f"▸ Активных вкладов: {active_deposits}\n"
            f"▸ Активных кредитов: {active_loans}\n"
            f"▸ Просроченных платежей: {overdue_loans}\n\n"
            f"⚙️ <b>Ручная обработка:</b>\n"
            f"▸ Команда: /process_bank_operations\n"
            f"▸ Только для администратора\n\n"
            f"🔄 <b>Последняя обработка:</b>\n"
            f"▸ {now.strftime('%d.%m.%Y %H:%M:%S')}"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in bank_schedule_command: {e}")
        await message.answer("❌ Произошла ошибка при получении расписания")

async def daily_bank_processing():
    """Ежедневная обработка банковских операций в 18:30 по Москве с уведомлением админа"""
    while True:
        try:
            now = datetime.now(pytz.timezone('Europe/Moscow'))
            
            # Проверяем, сейчас ли 18:30
            if now.hour == 18 and now.minute == 30:
                logger.info("🔄 Starting automatic daily bank processing...")
                
                # Отправляем уведомление о начале обработки
                try:
                    for admin_id in ADMINS:
                        await bot.send_message(
                            chat_id=admin_id,
                            text=f"🔄 <b>Начинаю автоматическую обработку банковских операций</b>\n\n"
                                 f"⏰ Время: {now.strftime('%d.%m.%Y %H:%M:%S')}",
                            parse_mode='HTML'
                        )
                except Exception as e:
                    logger.error(f"Error sending start notification to admin: {e}")
                
                # Выполняем обработку
                deposits_processed, loans_processed, notifications = await process_all_deposits_and_loans()
                
                # Статистика по уведомлениям
                success_notifications = sum(1 for n in notifications if n["success"])
                error_notifications = sum(1 for n in notifications if not n["success"])
                
                # Отправляем отчет админу
                report_text = (
                    "✅ <b>АВТОМАТИЧЕСКАЯ ОБРАБОТКА ЗАВЕРШЕНА</b>\n\n"
                    f"💰 Обработано вкладов: {deposits_processed}\n"
                    f"💳 Обработано кредитов: {loans_processed}\n"
                    f"📨 Уведомлений отправлено: {success_notifications}\n"
                    f"❌ Ошибок: {error_notifications}\n\n"
                    f"⏰ Время завершения: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                    f"🕒 Следующая обработка: завтра в 18:30"
                )

                try:
                    for admin_id in ADMINS:
                        await bot.send_message(
                            chat_id=admin_id,
                            text=report_text,
                            parse_mode='HTML'
                        )
                except Exception as e:
                    logger.error(f"Error sending completion notification to admin: {e}")
                
                logger.info(f"✅ Daily bank processing completed: {deposits_processed} deposits, {loans_processed} loans")
                
                # Ждем 24 часа до следующей проверки
                await asyncio.sleep(86400)  # 24 часа
            else:
                # Ждем 1 минуту до следующей проверки
                await asyncio.sleep(60)
                
        except Exception as e:
            logger.error(f"Error in daily_bank_processing: {e}")
            
            # Отправляем ошибку админу
            try:
                await bot.send_message(
                    chat_id=admin_id,
                    text=f"❌ <b>ОШИБКА В АВТОМАТИЧЕСКОЙ ОБРАБОТКЕ</b>\n\n"
                         f"Ошибка: {str(e)}\n"
                         f"Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}",
                    parse_mode='HTML'
                )
            except Exception as notify_error:
                logger.error(f"Error sending error notification: {notify_error}")
            
            await asyncio.sleep(300)  # Ждем 5 минут при ошибке

async def start_daily_bank_processing():
    """Запускает фоновую задачу ежедневной обработки банковских операций"""
    asyncio.create_task(daily_bank_processing())
# Обновляем главную команду банка
@dp.message(Command("bank"))
async def bank_command(message: Message):
    """Главное меню банка"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью /start")
        return
    
    # Остальной код банка без изменений...
    loan = get_user_active_loan(user_id)
    deposits = get_user_active_deposits(user_id)
    loan_limit = get_user_loan_limit(user_id)
    
    text = "🏦 <b>Банк CryptoMiner</b>\n\n"
    
    # Информация о кредите
    if loan:
        loan_id, loan_amount, interest_rate, start_date, next_payment = loan
        next_payment_date = datetime.fromisoformat(next_payment)
        text += (
            f"📊 <b>Активный кредит:</b>\n"
            f"Сумма: ${format_number(loan_amount, True)}\n"
            f"Ставка: {interest_rate*100}% в неделю\n"
            f"Следующий платеж: {next_payment_date.strftime('%d.%m.%Y')}\n\n"
        )
    else:
        text += f"📊 <b>Лимит кредита:</b> ${format_number(loan_limit, True)}\n\n"
    
    # Информация о вкладах
    if deposits:
        text += "💰 <b>Активные вклады:</b>\n"
        total_deposits = 0
        for deposit in deposits:
            deposit_id, deposit_type, amount, interest_rate, start_date, end_date = deposit
            deposit_info = DEPOSIT_TYPES.get(deposit_type, {"name": "Неизвестный"})
            end_date_obj = datetime.fromisoformat(end_date)
            text += (
                f"▸ {deposit_info['name']}: ${format_number(amount, True)} "
                f"({interest_rate*100}%) до {end_date_obj.strftime('%d.%m.%Y')}\n"
            )
            total_deposits += amount
        text += f"<b>Итого вкладов:</b> ${format_number(total_deposits, True)}\n\n"
    
    # Клавиатура с действиями
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="💰 Вклады", callback_data=f"bank_deposits_{user_id}"),
        InlineKeyboardButton(text="💳 Кредиты", callback_data=f"bank_loans_{user_id}")
    )
    builder.row(
        InlineKeyboardButton(text="📊 Мои операции", callback_data=f"bank_transactions_{user_id}"),
        InlineKeyboardButton(text="❌ Закрыть", callback_data=f"bank_close_{user_id}")
    )
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')
@dp.callback_query(F.data.startswith("bank_deposits_"))
async def bank_deposits_handler(callback: CallbackQuery):
    """Меню вкладов"""
    user_id = int(callback.data.split('_')[2])
    
    if callback.from_user.id != user_id:
        await callback.answer("Это не ваш банк!", show_alert=True)
        return
    
    text = "💰 <b>Виды вкладов:</b>\n\n"
    
    for dep_type, info in DEPOSIT_TYPES.items():
        text += (
            f"{dep_type}. {info['name']}\n"
            f"   ▸ Ставка: {info['weekly_rate']*100}% в неделю\n"
            f"   ▸ Мин. сумма: ${format_number(info['min_amount'], True)}\n"
            f"   ▸ Срок: {info['duration_days']} дней\n"
            f"   ▸ Открыть: /deposit_{dep_type} [сумма]\n\n"
        )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=f"bank_back_{user_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()


def repay_loan(user_id: int, amount: float = None) -> Tuple[bool, str]:
    """Досрочное погашение кредита (полное или частичное)"""
    try:
        loan = get_user_active_loan(user_id)
        if not loan:
            return False, "У вас нет активного кредита"
        
        loan_id, loan_amount, interest_rate, start_date, next_payment = loan
        user = get_user(user_id)
        
        if not user:
            return False, "Пользователь не найден"
        
        # Если amount не указан - полное погашение
        if amount is None:
            amount = loan_amount
        
        # Проверяем, что сумма погашения не превышает остаток
        if amount > loan_amount:
            return False, f"Сумма погашения не может превышать остаток по кредиту (${format_number(loan_amount, True)})"
        
        # Проверяем баланс
        if user[2] < amount:
            return False, f"Недостаточно средств. Ваш баланс: ${format_number(user[2], True)}"
        
        # Списание средств
        new_usd = user[2] - amount
        update_balance(user_id, usd=new_usd)
        
        # Обновляем или закрываем кредит
        remaining_amount = loan_amount - amount
        
        if remaining_amount <= 0:
            # Полное погашение
            cursor.execute('''
            UPDATE bank_loans 
            SET status = 'repaid', amount = 0
            WHERE id = ?
            ''', (loan_id,))
            status_message = "полностью погашен"
        else:
            # Частичное погашение
            cursor.execute('''
            UPDATE bank_loans 
            SET amount = ?
            WHERE id = ?
            ''', (remaining_amount, loan_id))
            status_message = f"частично погашен, остаток: ${format_number(remaining_amount, True)}"
        
        # Записываем транзакцию
        cursor.execute('''
        INSERT INTO bank_transactions (user_id, transaction_type, amount, description, timestamp)
        VALUES (?, 'loan_repayment', ?, ?, ?)
        ''', (user_id, -amount, f"Досрочное погашение кредита", datetime.now().isoformat()))
        
        conn.commit()
        
        return True, (
            f"✅ Кредит {status_message}!\n"
            f"Погашено: ${format_number(amount, True)}\n"
            f"Новый баланс: ${format_number(new_usd, True)}"
        )
        
    except Exception as e:
        logger.error(f"Error repaying loan: {e}")
        conn.rollback()
        return False, "Произошла ошибка при погашении кредита"

# Обновляем обработчик кредитов с кнопкой погашения
@dp.callback_query(F.data.startswith("bank_loans_"))
async def bank_loans_handler(callback: CallbackQuery):
    """Меню кредитов"""
    user_id = int(callback.data.split('_')[2])
    
    if callback.from_user.id != user_id:
        await callback.answer("Это не ваш банк!", show_alert=True)
        return
    
    user = get_user(user_id)
    if not user:
        await callback.answer("Сначала зарегистрируйтесь с помощью /start", show_alert=True)
        return
    
    loan_limit = get_user_loan_limit(user_id)
    loan = get_user_active_loan(user_id)
    
    text = "💳 <b>Кредиты</b>\n\n"
    text += f"📊 Ваш кредитный лимит: ${format_number(loan_limit, True)}\n"
    
    builder = InlineKeyboardBuilder()
    
    if loan:
        loan_id, loan_amount, interest_rate, start_date, next_payment = loan
        next_payment_date = datetime.fromisoformat(next_payment)
        text += (
            f"\n📋 <b>Активный кредит:</b>\n"
            f"Сумма: ${format_number(loan_amount, True)}\n"
            f"Ставка: {interest_rate*100}% в неделю\n"
            f"Следующий платеж: {next_payment_date.strftime('%d.%m.%Y')}\n"
            f"Еженедельный платеж: ${format_number(loan_amount * interest_rate, True)}\n\n"
            f"💡 Вы можете досрочно погасить кредит:\n"
            f"Полностью: /repay_loan\n"
            f"Частично: /repay_loan [сумма]"
        )
        
        # Добавляем кнопку полного погашения
        builder.row(
            InlineKeyboardButton(
                text="💰 Погасить полностью", 
                callback_data=f"loan_repay_full_{user_id}"
            )
        )
    else:
        text += (
            f"\n💡 Вы можете взять кредит под {LOAN_INTEREST_RATE*100}% в неделю\n"
            f"Команда: /loan [сумма]\n"
            f"Пример: /loan 1000000"
        )
    
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=f"bank_back_{user_id}")
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()

# Обработчик кнопки полного погашения
@dp.callback_query(F.data.startswith("loan_repay_full_"))
async def loan_repay_full_handler(callback: CallbackQuery):
    """Обработка полного погашения кредита"""
    user_id = int(callback.data.split('_')[3])
    
    if callback.from_user.id != user_id:
        await callback.answer("Это не ваш кредит!", show_alert=True)
        return
    
    user = get_user(user_id)
    if not user:
        await callback.answer("Сначала зарегистрируйтесь с помощью /start", show_alert=True)
        return
    
    success, result = repay_loan(user_id)
    await callback.message.edit_text(
        f"✅ {result}" if success else f"❌ {result}",
        reply_markup=None
    )
    await callback.answer()

# Команда для досрочного погашения кредита
@dp.message(F.text.regexp(rf'^/repay_loan(?:\@{re.escape(BOT_USERNAME)})?(?:\s+(\d+))?$'))
async def repay_loan_handler(message: Message):
    """Досрочное погашение кредита"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью /start")
        return
    
    try:
        # Проверяем, есть ли сумма для частичного погашения
        command_text = message.text.split('@')[0]
        parts = command_text.split()
        
        if len(parts) > 1:
            amount = float(parts[1])
            success, result = repay_loan(user_id, amount)
        else:
            success, result = repay_loan(user_id)
        
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
        
    except ValueError:
        await message.answer(
            "❌ Неверный формат суммы\n"
            "Используйте: /repay_loan [сумма]\n"
            "Пример: /repay_loan 500000"
        )


@dp.callback_query(F.data.startswith("bank_transactions_"))
async def bank_transactions_handler(callback: CallbackQuery):
    """История операций"""
    user_id = int(callback.data.split('_')[2])
    
    if callback.from_user.id != user_id:
        await callback.answer("Это не ваш банк!", show_alert=True)
        return
    
    try:
        cursor.execute('''
        SELECT transaction_type, amount, description, timestamp 
        FROM bank_transactions 
        WHERE user_id = ? 
        ORDER BY timestamp DESC 
        LIMIT 10
        ''', (user_id,))
        
        transactions = cursor.fetchall()
        
        text = "📊 <b>Последние операции:</b>\n\n"
        
        if not transactions:
            text += "У вас еще нет операций в банке\n"
        else:
            for trans_type, amount, description, timestamp in transactions:
                date = datetime.fromisoformat(timestamp).strftime('%d.%m.%Y')
                sign = "+" if amount > 0 else ""
                text += f"{date} - {description}: {sign}${format_number(abs(amount), True)}\n"
        
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=f"bank_back_{user_id}")
        )
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error getting transactions: {e}")
        await callback.answer("Ошибка при получении операций", show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("bank_back_"))
async def bank_back_handler(callback: CallbackQuery):
    """Возврат в главное меню банка"""
    user_id = int(callback.data.split('_')[2])
    
    if callback.from_user.id != user_id:
        await callback.answer("Это не ваш банк!", show_alert=True)
        return
    
    user = get_user(user_id)
    if not user:
        await callback.answer("Сначала зарегистрируйтесь с помощью /start", show_alert=True)
        return
    
    # Если пользователь зарегистрирован, редактируем сообщение как обычно
    try:
        loan = get_user_active_loan(user_id)
        deposits = get_user_active_deposits(user_id)
        loan_limit = get_user_loan_limit(user_id)
        
        text = "🏦 <b>Банк CryptoMiner</b>\n\n"
        
        if loan:
            loan_id, loan_amount, interest_rate, start_date, next_payment = loan
            next_payment_date = datetime.fromisoformat(next_payment)
            text += (
                f"📊 <b>Активный кредит:</b>\n"
                f"Сумма: ${format_number(loan_amount, True)}\n"
                f"Ставка: {interest_rate*100}% в неделю\n"
                f"Следующий платеж: {next_payment_date.strftime('%d.%m.%Y')}\n\n"
            )
        else:
            text += f"📊 <b>Лимит кредита:</b> ${format_number(loan_limit, True)}\n\n"
        
        if deposits:
            text += "💰 <b>Активные вклады:</b>\n"
            total_deposits = 0
            for deposit in deposits:
                deposit_id, deposit_type, amount, interest_rate, start_date, end_date = deposit
                deposit_info = DEPOSIT_TYPES.get(deposit_type, {"name": "Неизвестный"})
                end_date_obj = datetime.fromisoformat(end_date)
                text += (
                    f"▸ {deposit_info['name']}: ${format_number(amount, True)} "
                    f"({interest_rate*100}%) до {end_date_obj.strftime('%d.%m.%Y')}\n"
                )
                total_deposits += amount
            text += f"<b>Итого вкладов:</b> ${format_number(total_deposits, True)}\n\n"
        
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="💰 Вклады", callback_data=f"bank_deposits_{user_id}"),
            InlineKeyboardButton(text="💳 Кредиты", callback_data=f"bank_loans_{user_id}")
        )
        builder.row(
            InlineKeyboardButton(text="📊 Мои операции", callback_data=f"bank_transactions_{user_id}"),
            InlineKeyboardButton(text="❌ Закрыть", callback_data=f"bank_close_{user_id}")
        )
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in bank back handler: {e}")
        await callback.answer("Произошла ошибка при возврате в меню", show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data.startswith("bank_close_"))
async def bank_close_handler(callback: CallbackQuery):
    """Закрытие меню банка"""
    user_id = int(callback.data.split('_')[2])
    
    if callback.from_user.id != user_id:
        await callback.answer("Это не ваш банк!", show_alert=True)
        return
    
    await callback.message.delete()
    await callback.answer()

@dp.message(F.text.regexp(rf'^/loan(?:\@{re.escape(BOT_USERNAME)})?\s+(\d+)$'))
async def create_loan_handler(message: Message):
    """Получение кредита"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью /start")
        return
    
    try:
        # Извлекаем сумму кредита (игнорируя @username если есть)
        command_text = message.text.split('@')[0]
        amount = float(command_text.split()[1])
        
        success, result = create_loan(user_id, amount)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
        
    except (ValueError, IndexError):
        await message.answer(
            "❌ Неверный формат команды\n"
            "Используйте: /loan [сумма]\n"
            "Пример: /loan 1000000"
        )

# Обновляем команду создания вклада с учетом юзернейма
@dp.message(F.text.regexp(rf'^/deposit_(\d+)(?:\@{re.escape(BOT_USERNAME)})?\s+(\d+)$'))
async def create_deposit_handler(message: Message):
    """Создание вклада"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью /start")
        return
    
    try:
        # Извлекаем тип вклада и сумму (игнорируя @username если есть)
        command_text = message.text.split('@')[0]
        parts = command_text.split()
        deposit_type = int(parts[0].split('_')[1])
        amount = float(parts[1])
        
        success, result = create_deposit(user_id, deposit_type, amount)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
        
    except (ValueError, IndexError):
        await message.answer(
            "❌ Неверный формат команды\n"
            "Используйте: /deposit_1 [сумма]\n"
            "Где 1 - тип вклада (1-4)"
        )


# Добавляем команду /bank в меню
@dp.message(F.text == "🏦 Банк")
async def bank_button_handler(message: Message):
    await bank_command(message)



def create_user_extended(user_id: int, username: Optional[str]):
    """Создает пользователя с расширенной статистикой"""
    try:
        # Проверяем, существует ли уже пользователь
        cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
        user_exists = cursor.fetchone() is not None
        
        # Создаем основную запись (если не существует)
        cursor.execute('INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)', (user_id, username))
        
        # Создаем расширенную статистику (если не существует)
        cursor.execute('''
        INSERT OR IGNORE INTO user_work_stats 
        (user_id, register_date, total_experience, last_work_time, total_usd_earned, total_btc_earned)
        VALUES (?, ?, 0, NULL, 0, 0)
        ''', (user_id, datetime.now().isoformat()))
        
        conn.commit()
        
        # Отправляем уведомление только если пользователь новый
        if not user_exists:
            asyncio.create_task(notify_new_user(user_id, username))
    except sqlite3.Error as e:
        logger.error(f"Error creating extended user stats: {e}")
        conn.rollback()
        

                
@dp.message(Command("botstats"))
async def bot_stats(message: Message):
    """Команда для просмотра расширенной статистики бота и управления"""
    # Проверяем доступ только для конкретного чата
    if message.chat.id != -1002734900704:
        return
    
    try:
        start_time = time.time()
        
        # Получаем расширенную статистику
        cursor.execute('SELECT COUNT(*) FROM users')
        total_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE last_income_time IS NOT NULL')
        active_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(usd_balance) FROM users')
        total_usd = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT SUM(btc_balance) FROM users')
        total_btc = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM premium_users WHERE premium_until > datetime("now")')
        premium_users = cursor.fetchone()[0]
        
        # Рассчитываем время работы бота
        uptime = datetime.now() - BOT_START_TIME
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Рассчитываем пинг
        ping = round((time.time() - start_time) * 1000)
        
        # Формируем текст с полезной статистикой
        text = (
            "🤖 <b>Расширенная статистика бота</b>\n\n"
            f"👥 <b>Пользователи:</b>\n"
            f"▸ Всего: {total_users}\n"
            f"▸ Активных: {active_users}\n"
            f"▸ Premium: {premium_users}\n\n"
            
            f"💰 <b>Экономика:</b>\n"
            f"▸ Всего USD: ${format_number(total_usd, True)}\n"
            f"▸ Всего BTC: {format_number(total_btc)}\n\n"
            
            f"⚙️ <b>Система:</b>\n"
            f"▸ Время работы: {days}д {hours}ч {minutes}м\n"
            f"▸ Пинг: {ping}мс\n"
            f"▸ Версия: 2.1.3\n"
            f"▸ Последний рестарт: {BOT_START_TIME.strftime('%d.%m.%Y %H:%M')}"
        )
        
        # Создаем инлайн-кнопки для управления
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🔄 Полный рестарт", callback_data="bot_full_restart"),
            InlineKeyboardButton(text="🛑 Остановить", callback_data="bot_shutdown")
        )
        builder.row(
            InlineKeyboardButton(text="🧹 Очистка кэша", callback_data="bot_clear_cache"),
            InlineKeyboardButton(text="📊 Детали", callback_data="bot_more_stats")
        )
        
        await message.answer(text, parse_mode='HTML', reply_markup=builder.as_markup())
        
    except Exception as e:
        logger.error(f"Error in botstats command: {e}")
        await message.answer(f"❌ Ошибка при получении статистики: {str(e)}")

@dp.callback_query(F.data.startswith("bot_"))
async def handle_bot_management(callback: CallbackQuery):
    await callback.answer()
    try:
        # Проверяем доступ только для конкретного чата
        if callback.message.chat.id != -1002734900704:
            return
            
        action = callback.data.split("_")[1]
        
        if action == "full_restart":
            # Полный рестарт бота
            await callback.message.edit_text("🔄 Инициирую полный рестарт...")
            
            # Закрываем соединения
            await bot.session.close()
            conn.close()
            
            # Перезапускаем процесс
            os.execl(sys.executable, sys.executable, *sys.argv)
            
        elif action == "shutdown":
            # Подтверждение остановки
            builder = InlineKeyboardBuilder()
            builder.row(
                InlineKeyboardButton(text="✅ Да, остановить", callback_data="bot_confirm_shutdown"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="bot_cancel")
            )
            
            await callback.message.edit_text(
                "🛑 <b>Подтверждение остановки бота</b>\n\n"
                "Вы уверены, что хотите остановить бота?\n"
                "Это приведет к прекращению обработки всех команд.",
                parse_mode='HTML',
                reply_markup=builder.as_markup()
            )
            
        elif action == "confirm_shutdown":
            # Реальная остановка бота
            await callback.message.edit_text("🛑 Останавливаю бота...")
            
            # Закрываем соединения
            await bot.session.close()
            conn.close()
            
            # Выходим
            os._exit(0)
            
        elif action == "clear_cache":
            # Очистка кэша
            active_games.clear()  # Очищаем активные игры
            await callback.message.edit_text("🧹 Кэш успешно очищен!")
            
        elif action == "more_stats":
            # Дополнительная статистика
            cursor.execute('SELECT COUNT(*) FROM user_cards')
            total_cards = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM user_businesses')
            total_businesses = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM lottery_tickets WHERE ticket_count > 0')
            lottery_participants = cursor.fetchone()[0]
            
            text = (
                "📊 <b>Дополнительная статистика</b>\n\n"
                f"🖥️ Видеокарт у игроков: {total_cards}\n"
                f"🏢 Бизнесов у игроков: {total_businesses}\n"
                f"🎟 Участников лотереи: {lottery_participants}\n\n"
                f"🔄 Последнее обновление: {datetime.now().strftime('%H:%M:%S')}"
            )
            
            await callback.message.edit_text(text, parse_mode='HTML')
                
        elif action == "cancel":
            # Возврат к основной статистике
            await bot_stats(callback.message)
            
        await callback.answer()
            
    except Exception as e:
        logger.error(f"Error in bot management: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.message(Command("restore_user"))
async def restore_user_command(message: Message):
    # Проверяем, что команда вызвана в нужном чате
    if message.chat.id != -1002734900704:
        return  # Игнорируем команду в других чатах

    try:
        # Проверяем формат команды
        if len(message.text.split()) < 2:
            await message.answer(
                "ℹ️ Формат команды:\n"
                "/restore_user [user_id]\n"
                "[usd_balance] [btc_balance] [expansion_level]\n"
                "[wipe_count] [premium_until] (или 0 если нет)\n"
                "[card1_id:count,card2_id:count,...]\n\n"
                "Пример:\n"
                "/restore_user 123456789\n"
                "1000000 5.2 25\n"
                "3 2024-12-31\n"
                "5:2,10:1,15:3"
            )
            return

        # Разбираем аргументы
        args = message.text.split('\n')
        if len(args) < 4:
            raise ValueError("Недостаточно аргументов")

        # Первая строка - user_id
        user_id = int(args[0].split()[1])

        # Вторая строка - балансы и уровень фермы
        balance_args = args[1].split()
        if len(balance_args) != 3:
            raise ValueError("Неверный формат балансов и уровня")
        
        usd_balance = float(balance_args[0])
        btc_balance = float(balance_args[1])
        expansion_level = int(balance_args[2])

        # Третья строка - вайпы и премиум
        wipe_premium_args = args[2].split()
        if len(wipe_premium_args) != 2:
            raise ValueError("Неверный формат вайпов и премиума")
        
        wipe_count = int(wipe_premium_args[0])
        premium_until = wipe_premium_args[1] if wipe_premium_args[1] != "0" else None

        # Четвертая строка - видеокарты
        cards_str = args[3].strip()
        cards_data = []
        if cards_str:
            for card_pair in cards_str.split(','):
                card_id, count = map(int, card_pair.split(':'))
                cards_data.append((card_id, count))

        # Начинаем транзакцию
        with conn:
            # 1. Обновляем основную информацию пользователя
            cursor.execute(
                "INSERT OR REPLACE INTO users "
                "(user_id, usd_balance, btc_balance, expansion, income_btc) "
                "VALUES (?, ?, ?, ?, 0)",
                (user_id, usd_balance, btc_balance, expansion_level)
            )

            # 2. Обновляем вайпы
            cursor.execute(
                "INSERT OR REPLACE INTO user_wipes "
                "(user_id, wipe_count, total_wipe_bonus) "
                "VALUES (?, ?, ?)",
                (user_id, wipe_count, wipe_count * WIPE_BONUS_PERCENT)
            )

            # 3. Обновляем премиум
            if premium_until:
                cursor.execute(
                    "INSERT OR REPLACE INTO premium_users "
                    "(user_id, premium_until) "
                    "VALUES (?, ?)",
                    (user_id, premium_until)
                )
            else:
                cursor.execute("DELETE FROM premium_users WHERE user_id = ?", (user_id,))

            # 4. Обновляем видеокарты
            cursor.execute("DELETE FROM user_cards WHERE user_id = ?", (user_id,))
            for card_id, count in cards_data:
                cursor.execute(
                    "INSERT INTO user_cards (user_id, card_id, count) "
                    "VALUES (?, ?, ?)",
                    (user_id, card_id, count)
                )

            # 5. Пересчитываем доход
            calculate_income(user_id)

        await message.answer(
            f"✅ Данные пользователя {user_id} успешно восстановлены!\n\n"
            f"💰 Баланс: ${format_number(usd_balance, True)} / {format_number(btc_balance)} BTC\n"
            f"📦 Уровень фермы: {expansion_level}\n"
            f"🔄 Вайпы: {wipe_count} (+{wipe_count * WIPE_BONUS_PERCENT}% к доходу)\n"
            f"👑 Премиум до: {premium_until if premium_until else 'нет'}\n"
            f"🎮 Видеокарты: {len(cards_data)} видов, всего {sum(c[1] for c in cards_data)} шт."
        )

    except Exception as e:
        logger.error(f"Error in restore_user: {e}")
        await message.answer(
            "❌ Ошибка при восстановлении пользователя. Проверьте формат:\n\n"
            "ℹ️ Формат команды:\n"
            "/restore_user [user_id]\n"
            "[usd_balance] [btc_balance] [expansion_level]\n"
            "[wipe_count] [premium_until] (или 0 если нет)\n"
            "[card1_id:count,card2_id:count,...]\n\n"
            "Пример:\n"
            "/restore_user 123456789\n"
            "1000000 5.2 25\n"
            "3 2024-12-31\n"
            "5:2,10:1,15:3"
        )
        
# Обновленная команда /stats
@dp.message(Command("stats"))
async def stats(message: Message):
    user_id = message.from_user.id
    create_user(user_id, message.from_user.username)
    create_user_extended(user_id, message.from_user.username)
    
    try:
        # Получаем данные из всех таблиц
        cursor.execute('''
        SELECT u.usd_balance, u.btc_balance, 
               w.register_date, w.total_usd_earned, w.total_btc_earned, w.total_experience,
               uw.total_wipe_bonus
        FROM users u
        LEFT JOIN user_work_stats w ON u.user_id = w.user_id
        LEFT JOIN user_wipes uw ON u.user_id = uw.user_id
        WHERE u.user_id = ?
        ''', (user_id,))
        
        data = cursor.fetchone()
        if not data:
            return await message.answer("❌ Ошибка загрузки статистики")
        
        # Расчет времени игры
        reg_date = datetime.fromisoformat(data[2]) if data[2] else datetime.now()
        play_time = datetime.now() - reg_date
        years = play_time.days // 365
        months = (play_time.days % 365) // 30
        days = play_time.days % 30
        
        # Форматируем время игры в зависимости от продолжительности
        if years > 0:
            play_time_str = f"{years}г {months}м"
        else:
            play_time_str = f"{months}м {days}д"
        
        # Бонус от вайпов
        wipe_bonus = data[6] if data[6] else 0
        
        # Получаем общую сумму заработанных USD (включая потраченные)
        total_usd_earned = data[3] or 0
        # Добавляем текущий баланс USD, если он еще не учтен
        if data[0] > 0:
            total_usd_earned += data[0]
        
        # Получаем общую сумму заработанных BTC (включая потраченные)
        total_btc_earned = data[4] or 0
        # Добавляем текущий баланс BTC, если он еще не учтен
        if data[1] > 0:
            total_btc_earned += data[1]
        
        text = (
            f"📊 Статистика игрока {message.from_user.full_name}\n\n"
            f"💰 Всего заработано:\n"
            f"   ▸ USD: ${format_number(total_usd_earned, True)}\n"
            f"⏳ Играет: {play_time_str}\n"
            f"🌟 Опыт работы: {data[5] or 0}\n"
            f"🔰 Бонус к доходу: +{int(wipe_bonus)}%"
        )
        
        await message.answer(text)
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.answer("❌ Ошибка при получении статистики")

def update_balance(user_id: int, usd: Optional[float] = None, btc: Optional[float] = None, usd_delta: Optional[float] = None, btc_delta: Optional[float] = None):
    try:
        if usd is not None:
            cursor.execute('UPDATE users SET usd_balance = ? WHERE user_id = ?', (usd, user_id))
            # Обновляем статистику заработанных USD (используем дельту если передана)
            delta = usd_delta if usd_delta is not None else usd
            if delta > 0:
                cursor.execute('''
                UPDATE user_work_stats
                SET total_usd_earned = total_usd_earned + ?
                WHERE user_id = ?
                ''', (delta, user_id))

        if btc is not None:
            cursor.execute('UPDATE users SET btc_balance = ? WHERE user_id = ?', (btc, user_id))
            # Обновляем статистику заработанных BTC (используем дельту если передана)
            delta = btc_delta if btc_delta is not None else btc
            if delta > 0:
                cursor.execute('''
                UPDATE user_work_stats
                SET total_btc_earned = total_btc_earned + ?
                WHERE user_id = ?
                ''', (delta, user_id))

        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error updating balance: {e}")
        conn.rollback()

def check_ban(user_id: int) -> tuple[bool, str]:
    """Проверка на глобальный бан. Возвращает (забанен?, причина)"""
    try:
        cursor.execute('SELECT reason FROM banned_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            return True, result[0]
        return False, ""
    except:
        return False, ""

def get_user_cards(user_id: int) -> Tuple[List[Tuple[int, int]], int]:
    try:
        cursor.execute('SELECT card_id, count FROM user_cards WHERE user_id = ?', (user_id,))
        cards = cursor.fetchall()
        total_count = sum(count for _, count in cards) if cards else 0
        return cards, total_count
    except sqlite3.Error as e:
        logger.error(f"Error getting user cards: {e}")
        return [], 0

def add_user_card(user_id: int, card_id: int):
    try:
        cursor.execute('SELECT id, count FROM user_cards WHERE user_id = ? AND card_id = ?', (user_id, card_id))
        card = cursor.fetchone()
        if card:
            cursor.execute('UPDATE user_cards SET count = count + 1 WHERE id = ?', (card[0],))
        else:
            cursor.execute('INSERT INTO user_cards (user_id, card_id, count) VALUES (?, ?, 1)', (user_id, card_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error adding user card: {e}")
        conn.rollback()
        
def calculate_upgrade_bonus(user_id: int) -> float:
    try:
        cursor.execute('SELECT wiring, ventilation, traffic, software, cooling FROM user_upgrades WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            total = sum(result)  # Каждое улучшение дает +0.5%
            return total * 0.005  # Изменено с 0.01 на 0.005 (0.5% вместо 1%)
        return 0.0
    except sqlite3.Error as e:
        logger.error(f"Error calculating upgrade bonus: {e}")
        return 0.0

        
async def check_income(user_id: int) -> float:
    try:
        user = get_user(user_id)
        if not user:
            return 0.0
        
        last_income_time = user[6]
        now = datetime.now()
        
        if last_income_time:
            last_time = datetime.fromisoformat(last_income_time)
            current_interval = now.minute // 10
            last_interval = last_time.minute // 10
            
            if now.hour == last_time.hour and current_interval == last_interval:
                return 0.0
            
            if now >= last_time + timedelta(minutes=10):
                delta = now - last_time
                full_intervals = delta.total_seconds() // 600
                # Пересчитываем доход с актуальными бустерами
                btc_income = calculate_income(user_id) * full_intervals
                new_btc = user[3] + btc_income
                income_time = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
                
                # Логируем доход для чатов (без премиума и бустеров)
                cursor.execute('SELECT chat_id FROM chat_members WHERE user_id = ?', (user_id,))
                for (chat_id,) in cursor.fetchall():
                    chat_income = calculate_base_income(user_id) * full_intervals
                    log_chat_income(chat_id, user_id, chat_income)
                
                cursor.execute('UPDATE users SET btc_balance = ?, last_income_time = ? WHERE user_id = ?', 
                             (new_btc, income_time.isoformat(), user_id))
                conn.commit()
                return btc_income
        
        income_time = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
        cursor.execute('UPDATE users SET last_income_time = ? WHERE user_id = ?', 
                      (income_time.isoformat(), user_id))
        conn.commit()
        return 0.0
    except Exception as e:
        logger.error(f"Error checking income: {e}")
        return 0.0
    
def buy_card(user_id: int, card_id: int, quantity: int = 1) -> Tuple[bool, str]:
    try:
        card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        if not card:
            return False, "Карта не найдена"
        
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        current_expansion = user[5]
        expansion_info = EXPANSIONS[current_expansion - 1]
        
        # Получаем только верхнюю границу доступных карт
        last_card = expansion_info['last_available_card']
        
        # Проверяем, что карта не превышает максимальный доступный уровень
        if card_id > last_card:
            return False, (
                f"❌ Эта видеокарта пока недоступна!\n"
                f"Максимально доступная карта: {last_card}\n"
                f"Улучшайте ферму (/capacity) чтобы открыть новые карты."
            )
        
        max_cards = expansion_info['max_cards']
        cursor.execute('SELECT SUM(count) FROM user_cards WHERE user_id = ?', (user_id,))
        total_cards = cursor.fetchone()[0] or 0
        
        if total_cards + quantity > max_cards:
            return False, (
                f"Не хватит места для {quantity} карт.\n"
                f"Доступно слотов: {max_cards - total_cards}\n"
                f"Максимум: {max_cards} (уровень {current_expansion})"
            )
        
        total_price = card['price'] * quantity
        if user[2] < total_price:
            return False, (
                f"Недостаточно средств для покупки {quantity}x {card['name']}\n"
                f"Цена: ${format_number(int(total_price), True)}\n"
                f"Ваш баланс: ${format_number(int(user[2]), True)}"
            )
        
        new_usd = user[2] - total_price
        update_balance(user_id, usd=new_usd)
        
        cursor.execute('SELECT id, count FROM user_cards WHERE user_id = ? AND card_id = ?', (user_id, card_id))
        existing_card = cursor.fetchone()
        if existing_card:
            cursor.execute('UPDATE user_cards SET count = count + ? WHERE id = ?', (quantity, existing_card[0]))
        else:
            cursor.execute('INSERT INTO user_cards (user_id, card_id, count) VALUES (?, ?, ?)', 
                         (user_id, card_id, quantity))
        
        conn.commit()
        calculate_income(user_id)
        
        return True, (
            f"Поздравляем с покупкой x{quantity} {card['name']}!\n"
            f"Баланс: ${format_number(int(new_usd), True)}\n"
            f"Доход: +{format_number(card['income'] * quantity)} BTC/10мин"
        )
    except Exception as e:
        logger.error(f"Error buying card: {e}")
        conn.rollback()
        return False, "Произошла ошибка при покупке карты"
        
@dp.message(Command("games"))
async def help_games(message: Message):
    text = """
🎮 <b>ДОСТУПНЫЕ ИГРЫ</b> 🎮

💎 <b>Игры на ставки</b> (выигрыш 2-3x):
┣ 🪙 <b>Монетка</b> (орёл/решка)
┃   <code>!монетка [орёл/решка] [ставка]</code>
┃   Пример: <code>!монетка решка 500</code>
┃
┣ 🎲 <b>Кости</b> (угадай число)
┃   <code>!кубик [ставка] [1-6]</code>
┃   Пример: <code>!кубик 200 4</code>
┃
┣ 🎰 <b>Слоты</b> (упрощённые)
┃   <code>!слоты [ставка]</code>
┃   Пример: <code>!слоты 300</code>
┃
┗ 💣 <b>Минное поле</b> (классика)
    <code>!мины [ставка]</code>
    Пример: <code>!мины 500</code>
    Максимальая ставка 10.000.000$

⚙️ <b>Другие игры</b>:
┗ 🛠️ <b>Апгрейд видеокарт</b>
    <code>!апгрейд</code> - рискованное улучшение

💰 <b>Минимальная ставка</b>: 1$
💰 <b>Максимальная ставка</b>: 50.000.000$
"""
    # Отправляем баннер
    banner_path = os.path.join(BANNER_DIR, 'games.jpg')
    try:
        from aiogram.types import FSInputFile
        photo = FSInputFile(banner_path)
        await message.answer_photo(
            photo=photo,
            caption=text,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error sending games banner: {e}")
        await message.answer(text, parse_mode='HTML')

def validate_bet(bet: int) -> bool:
    return 1 <= bet <= 50000000

def validate_betmin(bet: int) -> bool:
    return 1 <= bet <= 10000000


import random
import time
from aiogram import F, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Состояния для FSM
class MinefieldStates(StatesGroup):
    waiting_for_bet = State()
    waiting_for_size = State()
    waiting_for_cell = State()

# Данные о минных полях
MINE_FIELD_SIZES = {
    "3x3": {"size": 3, "bombs": 2, "multiplier": 1.5},
    "5x5": {"size": 5, "bombs": 5, "multiplier": 2.0},
    "7x7": {"size": 7, "bombs": 10, "multiplier": 3.0}
}

# Эмодзи для отображения поля
EMPTY_CELL = "◻️"
BOMB = "💣"
TREASURE = "💰"
FLAG = "🚩"
BLUE_SQUARE = "🟦"

# Хранилище текущих игр
active_games = {}

@dp.message(F.text.regexp(r'^!м(?:ины)?\s+(\d+)$'))
async def mines_game(message: Message):
    user_id = message.from_user.id
    logger.info(f"Starting mines game for user {user_id}")
    
    user = get_user(user_id)
    if not user:
        logger.warning(f"User {user_id} not found")
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    try:
        bet = int(message.text.split()[-1])
        logger.info(f"User {user_id} bet: {bet}")
        
        if not validate_betmin(bet):
            logger.warning(f"Invalid bet amount: {bet}")
            await message.answer("Ставка должна быть от 1 до 10.000.000!")
            return
            
        if user[2] < bet:
            logger.warning(f"Insufficient balance: {user[2]} < {bet}")
            await message.answer(f"Недостаточно средств! Ваш баланс: ${format_number(user[2], True)}")
            return
        
        # Списываем ставку сразу
        update_balance(user_id, usd=user[2] - bet)
        logger.info(f"Bet {bet} deducted from user {user_id}")
        
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="3x3 (2 💣) x1.5", callback_data=f"mines_start_{user_id}_{bet}_3")],
                [InlineKeyboardButton(text="5x5 (5 💣) x2.0", callback_data=f"mines_start_{user_id}_{bet}_5")],
                [InlineKeyboardButton(text="7x7 (10 💣) x3.0", callback_data=f"mines_start_{user_id}_{bet}_7")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"mines_cancel_{user_id}_{bet}")]
            ]
        )
        
        await message.answer(
            "💣 <b>Игра 'Минное поле'</b>\n\n"
            f"Ваша ставка: ${format_number(bet, True)}\n"
            "Выберите размер поля:",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        logger.info(f"Field selection menu sent to user {user_id}")
        
    except ValueError as e:
        logger.error(f"ValueError in mines_game: {e}")
        await message.answer("❌ Неверный формат ставки. Используйте: !м [ставка]")
    except Exception as e:
        logger.error(f"Unexpected error in mines_game: {e}")
        await message.answer("❌ Произошла ошибка при запуске игры")

@dp.callback_query(F.data.startswith("mines_start_"))
async def mines_start(call: CallbackQuery):
    await call.answer()
    try:
        parts = call.data.split('_')
        user_id = int(parts[2])
        bet = int(parts[3])
        size = int(parts[4])

        if call.from_user.id != user_id:
            return
            
        game_params = {
            3: {"bombs": 2, "multiplier": 1.5},
            5: {"bombs": 5, "multiplier": 2.0},
            7: {"bombs": 10, "multiplier": 3.0}
        }
        
        bombs = game_params[size]["bombs"]
        multiplier = game_params[size]["multiplier"]
        
        bomb_positions = set()
        while len(bomb_positions) < bombs:
            x, y = random.randint(0, size-1), random.randint(0, size-1)
            bomb_positions.add((x, y))
        
        treasure_positions = set()
        while len(treasure_positions) < bombs * 2:
            x, y = random.randint(0, size-1), random.randint(0, size-1)
            if (x, y) not in bomb_positions:
                treasure_positions.add((x, y))
        
        game_id = f"{user_id}_{int(time.time())}"
        
        active_games[game_id] = {
            "user_id": user_id,
            "bet": bet,
            "current_bank": bet,
            "multiplier": 1.0,
            "size": size,
            "bombs": bombs,
            "bomb_positions": bomb_positions,
            "treasure_positions": treasure_positions,
            "revealed_cells": set(),
            "field": [[EMPTY_CELL for _ in range(size)] for _ in range(size)],
            "base_multiplier": multiplier
        }
        
        keyboard = create_mines_field(size, game_id)
        
        await call.message.edit_text(
            f"🎮 Игра началась! Поле {size}x{size}, {bombs} бомб\n"
            f"Текущий множитель: x1.0\n"
            f"Банк: ${format_number(bet, True)}",
            reply_markup=keyboard
        )
        await call.answer()
        
    except Exception as e:
        logger.error(f"Error starting mines game: {e}")
        await call.answer("Произошла ошибка", show_alert=True)

def create_mines_field(size: int, game_id: str) -> InlineKeyboardMarkup:
    keyboard = []
    for i in range(size):
        row = []
        for j in range(size):
            row.append(InlineKeyboardButton(
                text=EMPTY_CELL, 
                callback_data=f"mines_cell_{game_id}_{i}_{j}"
            ))
        keyboard.append(row)
    
    keyboard.append([
        InlineKeyboardButton(
            text="💵 Забрать деньги", 
            callback_data=f"mines_cashout_{game_id}"
        ),
        InlineKeyboardButton(
            text="❌ Выход", 
            callback_data=f"mines_cancel_{game_id}"
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@dp.callback_query(F.data.startswith("mines_cell_"))
async def mines_select_cell(call: CallbackQuery):
    await call.answer()
    try:
        parts = call.data.split('_')
        game_id = f"{parts[2]}_{parts[3]}"
        x = int(parts[4])
        y = int(parts[5])

        if game_id not in active_games:
            return
            
        game = active_games[game_id]
        
        if call.from_user.id != int(game["user_id"]):
            await call.answer("Это не ваша игра!", show_alert=True)
            return
            
        if (x, y) in game["revealed_cells"]:
            await call.answer("Ячейка уже открыта", show_alert=True)
            return
            
        game["revealed_cells"].add((x, y))
        
        # Обновляем прогресс BP
        if len(game["revealed_cells"]) == 1:  # Первое открытие ячейки
            update_bp_task_progress(game["user_id"], "mines_play")
        update_bp_task_progress(game["user_id"], "mines_cells")
        
        if (x, y) in game["bomb_positions"]:
            await end_mines_game(call.message, game_id, False)
            await call.answer()
            return
            
        elif (x, y) in game["treasure_positions"]:
            treasure_multiplier = round(random.uniform(0.2, 0.5), 1)
            game["multiplier"] += treasure_multiplier
            game["current_bank"] = int(game["bet"] * game["multiplier"])
            game["field"][x][y] = TREASURE
            
            # Обновляем прогресс BP
            update_bp_task_progress(game["user_id"], "mines_treasure")
            
            remaining_bombs = len(game["bomb_positions"] - game["revealed_cells"])
            if remaining_bombs == 0:
                await end_mines_game(call.message, game_id, True)
            else:
                keyboard = update_mines_field(call.message.reply_markup, x, y, TREASURE, game_id)
                text = (f"💰 Вы нашли сокровище! +{treasure_multiplier}x\n"
                       f"Текущий множитель: x{game['multiplier']:.1f}\n"
                       f"Банк: ${format_number(game['current_bank'], True)}\n"
                       f"Осталось бомб: {remaining_bombs}")
                await call.message.edit_text(text, reply_markup=keyboard)
            await call.answer()
                
        else:
            game["field"][x][y] = BLUE_SQUARE
            remaining_bombs = len(game["bomb_positions"] - game["revealed_cells"])
            
            keyboard = update_mines_field(call.message.reply_markup, x, y, BLUE_SQUARE, game_id)
            text = (f"🟦 Пустая ячейка. Продолжаем!\n"
                   f"Текущий множитель: x{game['multiplier']:.1f}\n"
                   f"Банк: ${format_number(game['current_bank'], True)}\n"
                   f"Осталось бомб: {remaining_bombs}")
            await call.message.edit_text(text, reply_markup=keyboard)
            await call.answer()
            
    except Exception as e:
        logger.error(f"Error processing cell selection: {e}")


@dp.callback_query(F.data.startswith("mines_cashout_"))
async def mines_cashout(call: CallbackQuery):
    await call.answer()
    try:
        game_id = call.data.split('_', 2)[2]

        if game_id not in active_games:
            return
            
        game = active_games[game_id]
        
        if call.from_user.id != int(game["user_id"]):
            await call.answer("Это не ваша игра!", show_alert=True)
            return
            
        win_amount = int(game["bet"] * game["multiplier"])
        update_balance(game["user_id"], usd=get_user(game["user_id"])[2] + win_amount)
        
        text = (f"💰 Вы забрали деньги!\n"
               f"Финальный множитель: x{game['multiplier']:.1f}\n"
               f"Выигрыш: ${format_number(win_amount, True)}")
        
        await call.message.edit_text(text)
        del active_games[game_id]
        await call.answer()
        
    except Exception as e:
        logger.error(f"Error in mines cashout: {e}")

@dp.callback_query(F.data.startswith("mines_cancel_"))
async def mines_cancel(call: CallbackQuery):
    await call.answer()
    try:
        parts = call.data.split('_')
        game_id = f"{parts[2]}_{parts[3]}"

        if game_id not in active_games:
            return
            
        game = active_games[game_id]
        
        if call.from_user.id != int(game["user_id"]):
            await call.answer("Это не ваша игра!", show_alert=True)
            return
            
        # Возвращаем только ставку (без начислений)
        update_balance(game["user_id"], usd=get_user(game["user_id"])[2] + game["bet"])
        
        await call.message.edit_text("❌ Игра отменена. Ставка возвращена.")
        del active_games[game_id]
        await call.answer()
        
    except Exception as e:
        logger.error(f"Error canceling mines game: {e}")
        await call.answer("Произошла ошибка", show_alert=True)

def update_mines_field(keyboard: InlineKeyboardMarkup, x: int, y: int, new_text: str, game_id: str) -> InlineKeyboardMarkup:
    new_keyboard = []
    for row in keyboard.inline_keyboard[:-1]:
        new_row = []
        for button in row:
            if f"mines_cell_{game_id}_{x}_{y}" in button.callback_data:
                new_row.append(InlineKeyboardButton(
                    text=new_text, 
                    callback_data=button.callback_data
                ))
            else:
                new_row.append(button)
        new_keyboard.append(new_row)
    
    new_keyboard.append(keyboard.inline_keyboard[-1])
    return InlineKeyboardMarkup(inline_keyboard=new_keyboard)

async def end_mines_game(message: Message, game_id: str, win: bool):
    try:
        if game_id not in active_games:
            logger.error(f"Game not found when ending: {game_id}")
            await message.answer("❌ Игра не найдена или уже завершена")
            return
            
        game = active_games[game_id]
        user_id = game["user_id"]
        
        if win:
            win_amount = int(game["bet"] * game["multiplier"])
            update_balance(user_id, usd=get_user(user_id)[2] + win_amount)
            
            text = (
                f"🎉 Вы победили!\n"
                f"Финальный множитель: x{game['multiplier']:.1f}\n"
                f"Выигрыш: ${format_number(win_amount, True)}"
            )
        else:
            text = (
                f"💣 БУМ! Вы проиграли\n"
                f"Ваша ставка: ${format_number(game['bet'], True)}\n"
                f"Максимальный множитель: x{game['multiplier']:.1f}"
            )
        
        await message.edit_text(text)
        del active_games[game_id]
            
    except Exception as e:
        logger.error(f"Error ending mines game: {e}")
        await message.answer("❌ Произошла ошибка при завершении игры")                
                                                
@dp.message(F.text.regexp(r'^!монетка (ор[её]л|решка) (\d+)$'))
async def coin_flip(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    try:
        parts = message.text.split()
        if len(parts) < 3:
            await message.answer("Неверный формат команды. Используйте: !монетка [орёл/решка] [ставка]")
            return
            
        choice = parts[1].lower().replace("орел", "орёл")
        bet = int(parts[2])
        
        if not validate_bet(bet):
            await message.answer("Ставка должна быть от 1 до 50.000.000!")
            return
            
        if user[2] < bet:
            await message.answer(f"Недостаточно средств! Ваш баланс: ${format_number(user[2], True)}")
            return
        
        # Сначала списываем ставку
        update_balance(user_id, usd=user[2] - bet)
        
        # Генерируем результат с шансом выигрыша 45%
        win = random.random() <= 0.45
        result = choice if win else "орёл" if choice == "решка" else "решка"
        
        if win:
            # При выигрыше зачисляем ставку x2
            new_balance = get_user(user_id)[2] + (bet * 2)
            update_balance(user_id, usd=new_balance)
            win_multiplier = 2
            
            # Проверяем получение ивент валюты (1% шанс при выигрыше)
            event_currency_received = try_give_1percent(user_id, 1)
        
        # Обновляем прогресс BP
        update_bp_task_progress(user_id, "coin_flip")
        if win:
            if choice == "орёл":
                update_bp_task_progress(user_id, "coin_win_heads")
            else:
                update_bp_task_progress(user_id, "coin_win_tails")
        
        current_balance = get_user(user_id)[2]
        text = (
            f"🪙 Монетка подброшена: <b>{result.capitalize()}</b>\n"
            f"Ваш выбор: <b>{choice.capitalize()}</b>\n\n"
        )
        
        if win:
            text += (
                f"🎉 Вы выиграли ${format_number(bet * win_multiplier, True)} (x{win_multiplier})!\n"
                f"💰 Ваш баланс: ${format_number(current_balance, True)}"
            )
            # Добавляем информацию о полученной ивент валюте, если она была выдана
            if event_currency_received:
                current_event_balance = get_event_currency(user_id)
                text += f"\n\n🎉 +1 🎃 (Баланс: {current_event_balance} 🎃)\n/top_ivent"
        else:
            text += (
                f"😢 Вы проиграли ${format_number(bet, True)}\n"
                f"💰 Ваш баланс: ${format_number(current_balance, True)}"
            )
            
        await message.answer(text, parse_mode='HTML')
    except ValueError:
        await message.answer("Неверный формат ставки. Используйте целое число от 1 до 50.000.000")
        
@dp.message(F.text.regexp(r'^!кубик (\d+) (\d+)$'))
async def dice_game(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    try:
        bet = int(message.text.split()[1])
        number = int(message.text.split()[2])
        
        if not validate_bet(bet):
            await message.answer("Ставка должна быть от 1 до 50.000.000!")
            return
            
        if number < 1 or number > 6:
            await message.answer("Число должно быть от 1 до 6!")
            return
            
        if user[2] < bet:
            await message.answer(f"Недостаточно средств! Ваш баланс: ${format_number(user[2], True)}")
            return
        
        # Сначала списываем ставку
        update_balance(user_id, usd=user[2] - bet)
            
        dice = random.randint(1, 6)
        # Определяем победу по совпадению чисел, а не случайному шансу
        win = (dice == number)
        
        if win:
            multiplier = 5
            new_balance = get_user(user_id)[2] + (bet * multiplier)
            update_balance(user_id, usd=new_balance)
            
            # Проверяем получение ивент валюты (1% шанс при выигрыше)
            event_currency_received = try_give_1percent(user_id, 1)
            
            # Обновляем прогресс BP
            update_bp_task_progress(user_id, f"dice_win_{number}")
            update_bp_task_progress(user_id, "dice_win_any")
        
        # Всегда обновляем прогресс игры
        update_bp_task_progress(user_id, "dice_play")
        
        current_balance = get_user(user_id)[2]
        text = (
            f"🎲 Выпало: <b>{dice}</b>\n"
            f"Ваше число: <b>{number}</b>\n\n"
        )
        
        if win:
            text += (
                f"🎉 Вы выиграли ${format_number(bet * multiplier, True)} (x{multiplier})!\n"
                f"💰 Ваш баланс: ${format_number(current_balance, True)}"
            )
            # Добавляем информацию о полученной ивент валюте, если она была выдана
            if event_currency_received:
                current_event_balance = get_event_currency(user_id)
                text += f"\n\n🎉 +1 🎃 (Баланс: {current_event_balance} 🎃)\n/top_ivent"
        else:
            text += (
                f"😢 Вы проиграли ${format_number(bet, True)}\n"
                f"💰 Ваш баланс: ${format_number(current_balance, True)}"
            )
            
        await message.answer(text, parse_mode='HTML')
    except ValueError:
        await message.answer("Неверный формат ставки. Используйте целое число от 1 до 50.000.000")
        
@dp.message(F.text.regexp(r'^!слоты (\d+)$'))
async def slots_game(message: Message):
    try:
        user_id = message.from_user.id
        user = get_user(user_id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
            return
        
        bet = int(message.text.split()[1])
        
        if not validate_bet(bet):
            await message.answer("Ставка должна быть от 1 до 50.000.000!")
            return
            
        if user[2] < bet:
            await message.answer(f"Недостаточно средств! Ваш баланс: ${format_number(user[2], True)}")
            return
        
        # Списываем ставку
        update_balance(user_id, usd=user[2] - bet)
            
        # Обновляем прогресс BP
        update_bp_task_progress(user_id, "slots_play")
            
        # Символы и их вероятности (7️⃣ реже других)
        symbols = ["🍒"]*15 + ["🍋"]*15 + ["🍊"]*15 + ["🍇"]*15 + ["🔔"]*10 + ["7️⃣"]*5
        slots = [random.choice(symbols) for _ in range(3)]
        
        # Фиксированные множители
        win_data = {
            "777": {"multiplier": 10, "message": "777 ДЖЕКПОТ!", "bp_task": "slots_jackpot"},
            "3bells": {"multiplier": 5, "message": "3 КОЛОКОЛА", "bp_task": "slots_3x"},
            "3fruits": {"multiplier": 3, "message": "3 ОДИНАКОВЫХ ФРУКТА", "bp_task": "slots_3x"},
            "2any": {"multiplier": 2, "message": "2 ОДИНАКОВЫХ СИМВОЛА", "bp_task": "slots_2x"}
        }
        
        # Определяем выигрышную комбинацию
        win_type = None
        if all(s == "7️⃣" for s in slots):
            win_type = "777"
        elif all(s == "🔔" for s in slots):
            win_type = "3bells"
        elif slots[0] == slots[1] == slots[2]:
            win_type = "3fruits"
        elif len(set(slots)) < 3:  # Если есть хотя бы 2 одинаковых
            win_type = "2any"
        
        # Обработка выигрыша
        if win_type:
            win_info = win_data[win_type]
            win_amount = bet * win_info["multiplier"]
            update_balance(user_id, usd=get_user(user_id)[2] + win_amount)
            
            # Проверяем получение ивент валюты (1% шанс при выигрыше)
            event_currency_received = try_give_1percent(user_id, 1)
            
            # Обновляем прогресс BP
            update_bp_task_progress(user_id, "slots_win")
            update_bp_task_progress(user_id, win_info['bp_task'])
            
            text = (
                f"🎰 [ {slots[0]} | {slots[1]} | {slots[2]} ]\n\n"
                f"🎉 {win_info['message']}\n"
                f"Вы выиграли ${format_number(win_amount, True)} (x{win_info['multiplier']})!\n"
                f"💰 Ваш баланс: ${format_number(get_user(user_id)[2], True)}"
            )
            # Добавляем информацию о полученной ивент валюте, если она была выдана
            if event_currency_received:
                current_event_balance = get_event_currency(user_id)
                text += f"\n\n🎉 +1 🎃 (Баланс: {current_event_balance} 🎃)\n/top_ivent"
        else:
            text = (
                f"🎰 [ {slots[0]} | {slots[1]} | {slots[2]} ]\n\n"
                f"😢 Ничего не совпало\n"
                f"Вы проиграли ${format_number(bet, True)}\n"
                f"💰 Ваш баланс: ${format_number(get_user(user_id)[2], True)}"
            )
            
        await message.answer(text, parse_mode='HTML')

    except ValueError:
        await message.answer("Неверный формат ставки. Используйте целое число от 1 до 50.000.000")
    except Exception as e:
        logger.error(f"Ошибка в slots_game: {str(e)}")
        await message.answer("Произошла ошибка при обработке команды")

def try_give_1percent(user_id: int, amount: int = 1) -> bool:
    """1% шанс выдачи валюты"""
    return try_give_event_currency(user_id, 0.01, amount)
def sell_all_btc(user_id: int) -> Tuple[bool, str]:
    try:
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        if user[3] <= 0:
            return False, "Нет BTC для продажи"
        
        btc_price = get_btc_price()
        usd_amount = user[3] * btc_price
        
        new_usd = user[2] + usd_amount
        update_balance(user_id, usd=new_usd, btc=0)
        
        return True, f"Продано {format_number(user[3])} BTC за ${format_number(usd_amount)}"
    except Exception as e:
        logger.error(f"Error selling BTC: {e}")
        return False, "Произошла ошибка при продаже BTC"

def sell_card(user_id: int, card_id: int, quantity: int = 1) -> Tuple[bool, str]:
    try:
        cursor.execute('SELECT id, count FROM user_cards WHERE user_id = ? AND card_id = ?', (user_id, card_id))
        card = cursor.fetchone()
        
        if not card or card[1] <= 0:
            return False, "У вас нет такой видеокарты"
        
        if quantity > card[1]:
            quantity = card[1]
            if quantity == 0:
                return False, "У вас нет таких видеокарт"
        
        card_info = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        if not card_info:
            return False, "Карта не найдена"
        
        # Проверяем, сколько уже таких карт на аукционе
        cursor.execute('SELECT COUNT(*) FROM auction_cards WHERE card_id = ?', (card_id,))
        current_on_auction = cursor.fetchone()[0] or 0
        
        # Определяем сколько карт можно добавить на аукцион
        max_can_add = max(0, 20 - current_on_auction)
        to_auction = min(quantity, max_can_add)
        to_sell_directly = quantity - to_auction
        
        total_reward = 0
        
        with conn:
            # Продаем карты напрямую (если есть)
            if to_sell_directly > 0:
                direct_reward = int(card_info['price'] * 0.7 * to_sell_directly)
                total_reward += direct_reward
            
            # Добавляем карты на аукцион (если есть место)
            if to_auction > 0:
                auction_reward = int(card_info['price'] * 0.7 * to_auction)
                total_reward += auction_reward
                
                for _ in range(to_auction):
                    discount = random.uniform(0.2, 0.3)
                    auction_price = int(card_info['price'] * (1 - discount))
                    cursor.execute('''
                    INSERT INTO auction_cards (card_id, price, timestamp)
                    VALUES (?, ?, ?)
                    ''', (card_id, auction_price, datetime.now().isoformat()))
            
            # Обновляем баланс пользователя
            if total_reward > 0:
                cursor.execute('SELECT usd_balance FROM users WHERE user_id = ?', (user_id,))
                current_balance = cursor.fetchone()[0]
                new_balance = current_balance + total_reward
                update_balance(user_id, usd=new_balance)
            
            # Удаляем карты у пользователя
            if card[1] > quantity:
                cursor.execute('UPDATE user_cards SET count = count - ? WHERE id = ?', (quantity, card[0]))
            else:
                cursor.execute('DELETE FROM user_cards WHERE id = ?', (card[0],))
            
            # Обновляем доход
            calculate_income(user_id)
            
        # Формируем простое сообщение
        return True, f" Ты продал x{quantity} {card_info['name']} и получил {format_number(total_reward, True)} $"
    except Exception as e:
        logger.error(f"Ошибка при продаже карты: {e}")
        conn.rollback()
        return False, "Произошла ошибка при продаже карты"

def get_current_week() -> str:
    today = datetime.now().date()
    return (today - timedelta(days=today.weekday())).isoformat()

def add_chat(chat_id: int, title: str):
    try:
        cursor.execute('INSERT OR IGNORE INTO chat_stats (chat_id, title) VALUES (?, ?)', (chat_id, title))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error adding chat: {e}")


@dp.message(Command("join_chat"))
async def join_chat_handler(message: Message):
    if message.chat.type == 'private':
        await message.answer("Эта команда работает только в групповых чатах!")
        return
    
    if message.chat.id == -1002752285369:
        await message.answer("❌ Использование этой команды запрещено в данном чате!")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    chat_title = message.chat.title
    
    # Проверяем, состоит ли пользователь в каких-либо чатах
    cursor.execute('SELECT COUNT(*) FROM chat_members WHERE user_id = ?', (user_id,))
    if cursor.fetchone()[0] > 0:
        cursor.execute('''
        SELECT cs.title 
        FROM chat_members cm
        JOIN chat_stats cs ON cm.chat_id = cs.chat_id
        WHERE cm.user_id = ?
        ''', (user_id,))
        existing_chat = cursor.fetchone()
        chat_name = existing_chat[0] if existing_chat else "неизвестный чат"
        await message.answer(f"❌ Вы уже состоите в чате {chat_name}. Сначала выйдите из него с помощью /decline_chat")
        return
    
    join_chat(user_id, chat_id, chat_title)
    await message.answer(f"🎉 Теперь ты представляешь чат {chat_title} в битве чатов!") 

def join_chat(user_id: int, chat_id: int, chat_title: str):
    try:
        add_chat(chat_id, chat_title)
        
        # Покидаем предыдущие чаты
        cursor.execute('DELETE FROM chat_members WHERE user_id = ?', (user_id,))
        
        cursor.execute('INSERT OR REPLACE INTO chat_members (user_id, chat_id) VALUES (?, ?)', (user_id, chat_id))
        
        cursor.execute('''
        UPDATE chat_stats 
        SET members_count = (SELECT COUNT(*) FROM chat_members WHERE chat_id = ?),
            title = ?,
            last_updated = ?
        WHERE chat_id = ?
        ''', (chat_id, chat_title, datetime.now().isoformat(), chat_id))
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error joining chat: {e}")
        conn.rollback()

@dp.message(Command("decline_chat"))
async def decline_chat_handler(message: Message):
    """Обработчик команды для выхода из чата"""
    user_id = message.from_user.id
    
    # Получаем chat_id, к которому привязан пользователь
    cursor.execute('SELECT chat_id FROM chat_members WHERE user_id = ?', (user_id,))
    chat_data = cursor.fetchone()
    
    if not chat_data:
        await message.answer("❌ Вы не состоите ни в одном чате")
        return
    
    chat_id = chat_data[0]
    leave_chat(user_id, chat_id)
    await message.answer("✅ Вы больше не представляете этот чат")

def leave_chat(user_id: int, chat_id: int):
    """Функция для выхода пользователя из чата"""
    try:
        # 1. Получаем вклад пользователя в доход чата
        cursor.execute('''
        SELECT SUM(btc_income) 
        FROM chat_income_log 
        WHERE user_id = ? AND chat_id = ?
        ''', (user_id, chat_id))
        user_earned = cursor.fetchone()
        
        if user_earned and user_earned[0]:
            user_contribution = user_earned[0]
            
            # 2. Уменьшаем общий доход чата на вклад пользователя
            cursor.execute('''
            UPDATE chat_stats 
            SET weekly_btc_earned = weekly_btc_earned - ?,
                members_count = members_count - 1,
                last_updated = ?
            WHERE chat_id = ?
            ''', (user_contribution, datetime.now().isoformat(), chat_id))
            
            # 3. Удаляем запись о заработке пользователя в этом чате
            cursor.execute('''
            DELETE FROM chat_income_log 
            WHERE user_id = ? AND chat_id = ?
            ''', (user_id, chat_id))
        
        # 4. Удаляем пользователя из списка участников чата
        cursor.execute('''
        DELETE FROM chat_members 
        WHERE user_id = ? AND chat_id = ?
        ''', (user_id, chat_id))
        
        # 5. Если в чате не осталось участников, удаляем его статистику
        cursor.execute('SELECT COUNT(*) FROM chat_members WHERE chat_id = ?', (chat_id,))
        if cursor.fetchone()[0] == 0:
            cursor.execute('DELETE FROM chat_stats WHERE chat_id = ?', (chat_id,))
        
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error leaving chat: {e}")
        conn.rollback()
        
def get_user_chats(user_id: int) -> List[int]:
    try:
        cursor.execute('SELECT chat_id FROM chat_members WHERE user_id = ?', (user_id,))
        return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Error getting user chats: {e}")
        return []

def get_chat_info(chat_id: int) -> Optional[Tuple]:
    try:
        cursor.execute('SELECT * FROM chat_stats WHERE chat_id = ?', (chat_id,))
        return cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"Error getting chat info: {e}")
        return None

def get_chat_members(chat_id: int) -> List[int]:
    try:
        cursor.execute('SELECT user_id FROM chat_members WHERE chat_id = ?', (chat_id,))
        return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Error getting chat members: {e}")
        return []

def get_top_chats(limit: int = 15) -> List[Dict]:
    try:
        cursor.execute('''
        SELECT chat_id, title, weekly_btc_earned, members_count 
        FROM chat_stats 
        WHERE members_count > 0
        ORDER BY weekly_btc_earned DESC 
        LIMIT ?
        ''', (limit,))
        
        return [{
            'chat_id': row[0],
            'title': row[1],
            'weekly_btc_earned': row[2],
            'members_count': row[3]
        } for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Error getting top chats: {e}")
        return []

def get_chat_rank(chat_id: int) -> int:
    try:
        cursor.execute('''
        SELECT rank FROM (
            SELECT chat_id, RANK() OVER (ORDER BY weekly_btc_earned DESC) as rank
            FROM chat_stats
            WHERE members_count > 0
        ) WHERE chat_id = ?
        ''', (chat_id,))
        
        result = cursor.fetchone()
        return result[0] if result else 0
    except sqlite3.Error as e:
        logger.error(f"Error getting chat rank: {e}")
        return 0

def reset_weekly_stats():
    try:
        cursor.execute('UPDATE chat_stats SET weekly_btc_earned = 0')
        cursor.execute('DELETE FROM chat_income_log')
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error resetting weekly stats: {e}")
        conn.rollback()

async def distribute_premium_rewards():
    try:
        current_week = get_current_week()
        top_chats = get_top_chats(10)

        # Список для формирования текста публикации
        chat_report = []

        for i, chat in enumerate(top_chats, 1):
            members = get_chat_members(chat['chat_id'])
            if not members:
                continue

            if len(members) <= 10:
                winners = members
            else:
                # Сортируем по доходу (8 лучших)
                members_with_income = []
                for user_id in members:
                    user = get_user(user_id)
                    if user:
                        members_with_income.append((user_id, user[4]))  # income_btc

                members_with_income.sort(key=lambda x: x[1], reverse=True)
                top_members = [x[0] for x in members_with_income[:8]]
                other_members = [x[0] for x in members_with_income[8:]]
                random_winners = random.sample(other_members, min(2, len(other_members)))
                winners = top_members + random_winners

            # Даем премиум на 24 часа
            premium_until = (datetime.now() + timedelta(days=1)).isoformat()
            for user_id in winners:
                cursor.execute('''
                INSERT OR REPLACE INTO premium_users (user_id, premium_until)
                VALUES (?, ?)
                ''', (user_id, premium_until))

            # Добавляем в отчёт
            chat_report.append(f"{i}. {chat['title']} - {len(winners)} победителей")

        conn.commit()

        # Формируем текст для публикации
        if chat_report:
            publication_text = "🏆 Топ чатов этой недели:\n\n" + "\n".join(chat_report)

            # Отправляем админам текст для публикации
            for admin_id in ADMINS:
                try:
                    await bot.send_message(
                        admin_id,
                        "🏆 <b>АВТОМАТИЧЕСКИЙ ТОП ЧАТОВ</b>\n\n"
                        "Премиум выдан!\n"
                        "Скопируй и опубликуй этот текст:\n\n"
                        "━━━━━━━━━━━━━━━\n\n" +
                        publication_text,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Error sending chat top to admin {admin_id}: {e}")

    except Exception as e:
        logger.error(f"Error distributing premium rewards: {e}")
        conn.rollback()

def is_premium(user_id: int) -> bool:
    try:
        cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if not result:
            return False
        
        premium_until = datetime.fromisoformat(result[0])
        return datetime.now() < premium_until
    except sqlite3.Error as e:
        logger.error(f"Error checking premium status: {e}")
        return False
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton        

# Добавляем таблицы для реферальной системы
cursor.execute('''
CREATE TABLE IF NOT EXISTS referrals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    referrer_id INTEGER,
    referred_id INTEGER,
    registered_at TEXT,
    completed_captcha BOOLEAN DEFAULT FALSE,
    reward_claimed BOOLEAN DEFAULT FALSE,
    FOREIGN KEY(referrer_id) REFERENCES users(user_id),
    FOREIGN KEY(referred_id) REFERENCES users(user_id),
    UNIQUE(referred_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_referral_stats (
    user_id INTEGER PRIMARY KEY,
    total_referrals INTEGER DEFAULT 0,
    active_referrals INTEGER DEFAULT 0,
    total_rewards_claimed INTEGER DEFAULT 0,
    last_reward_claim TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS referral_captchas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    number1 INTEGER,
    number2 INTEGER,
    operator TEXT,
    correct_answer INTEGER,
    wrong_answer1 INTEGER,
    wrong_answer2 INTEGER,
    message_id INTEGER,
    created_at TEXT,
    expires_at TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()

@dp.message(Command("revir"))
async def remove_all_viruses_command(message: Message):
    """Команда для снятия всех активных вирусов (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Получаем всех пользователей с активными вирусами
        cursor.execute('''
        SELECT DISTINCT user_id 
        FROM user_viruses 
        WHERE status = 'active'
        ''')
        
        users_with_viruses = cursor.fetchall()
        
        if not users_with_viruses:
            await message.answer("✅ На данный момент нет пользователей с активными вирусами")
            return
        
        total_removed = 0
        failed_notifications = 0
        
        status_msg = await message.answer(
            f"🦠 <b>НАЧАЛО УДАЛЕНИЯ ВИРУСОВ</b>\n\n"
            f"👥 Пользователей с вирусами: {len(users_with_viruses)}\n"
            f"⏳ Начинаю обработку...",
            parse_mode='HTML'
        )
        
        # Обрабатываем всех пользователей с вирусами
        for i, (user_id,) in enumerate(users_with_viruses, 1):
            try:
                # Меняем статус вируса на 'removed_by_admin'
                cursor.execute('''
                UPDATE user_viruses 
                SET status = 'removed_by_admin' 
                WHERE user_id = ? AND status = 'active'
                ''', (user_id,))
                
                # Пересчитываем доход пользователя
                calculate_income(user_id)
                total_removed += 1
                
                # Отправляем уведомление пользователю
                try:
                    notification_text = (
                        "✅ <b>ВИРУС УДАЛЕН!</b>\n\n"
                        "Администратор удалил вирус с вашей фермы.\n"
                        "Ваш доход восстановлен до нормального уровня!\n\n"
                        "💡 Будьте осторожны - вирусы могут появиться снова."
                    )
                    await bot.send_message(
                        chat_id=user_id,
                        text=notification_text,
                        parse_mode='HTML'
                    )
                    # Небольшая задержка между сообщениями
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error sending notification to user {user_id}: {e}")
                    failed_notifications += 1
                    continue
                
                # Обновляем статус каждые 10 пользователей
                if i % 10 == 0:
                    progress = (i / len(users_with_viruses)) * 100
                    await status_msg.edit_text(
                        f"🦠 <b>УДАЛЕНИЕ ВИРУСОВ</b>\n\n"
                        f"📊 Прогресс: {i}/{len(users_with_viruses)} ({progress:.1f}%)\n"
                        f"✅ Удалено вирусов: {total_removed}\n"
                        f"❌ Ошибок уведомлений: {failed_notifications}",
                        parse_mode='HTML'
                    )
                
            except Exception as e:
                logger.error(f"Error processing user {user_id}: {e}")
                continue
        
        # Фиксируем изменения в базе данных
        conn.commit()
        
        # Финальный отчет
        result_text = (
            f"🎯 <b>УДАЛЕНИЕ ВИРУСОВ ЗАВЕРШЕНО!</b>\n\n"
            f"👥 Обработано пользователей: {len(users_with_viruses)}\n"
            f"✅ Удалено вирусов: {total_removed}\n"
            f"📨 Уведомлений отправлено: {total_removed - failed_notifications}\n"
            f"❌ Ошибок отправки: {failed_notifications}\n\n"
            f"⏰ Время завершения: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )
        
        await status_msg.edit_text(result_text, parse_mode='HTML')
        
        # Логируем результат
        logger.info(f"🦠 VIRUS REMOVAL COMPLETED: {total_removed}/{len(users_with_viruses)} viruses removed")
        
    except Exception as e:
        logger.error(f"Error in remove_all_viruses command: {e}")
        await message.answer(f"❌ Произошла ошибка при удалении вирусов: {str(e)}")

# Функции для работы с реферальной системой
def get_user_referral_stats(user_id: int) -> Dict:
    """Получает статистику рефералов пользователя"""
    try:
        cursor.execute('''
        SELECT total_referrals, active_referrals, total_rewards_claimed, last_reward_claim
        FROM user_referral_stats 
        WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        
        if result:
            return {
                "total_referrals": result[0],
                "active_referrals": result[1],
                "total_rewards_claimed": result[2],
                "last_reward_claim": result[3]
            }
        else:
            # Создаем запись если нет
            cursor.execute('''
            INSERT INTO user_referral_stats (user_id) VALUES (?)
            ''', (user_id,))
            conn.commit()
            return {
                "total_referrals": 0,
                "active_referrals": 0,
                "total_rewards_claimed": 0,
                "last_reward_claim": None
            }
    except Exception as e:
        logger.error(f"Error getting referral stats: {e}")
        return {
            "total_referrals": 0,
            "active_referrals": 0,
            "total_rewards_claimed": 0,
            "last_reward_claim": None
        }

def get_referral_link(user_id: int) -> str:
    """Генерирует реферальную ссылку"""
    return f"https://t.me/{BOT_USERNAME}?start={user_id}"

def add_referral(referrer_id: int, referred_id: int) -> bool:
    """Добавляет реферала"""
    try:
        # Проверяем, не является ли пользователь сам себе рефералом
        if referrer_id == referred_id:
            return False
            
        # Проверяем, не зарегистрирован ли уже этот пользователь как реферал
        cursor.execute('SELECT 1 FROM referrals WHERE referred_id = ?', (referred_id,))
        if cursor.fetchone():
            return False
            
        # Добавляем реферала
        cursor.execute('''
        INSERT INTO referrals (referrer_id, referred_id, registered_at)
        VALUES (?, ?, ?)
        ''', (referrer_id, referred_id, datetime.now().isoformat()))
        
        # Обновляем статистику реферера
        cursor.execute('''
        INSERT OR REPLACE INTO user_referral_stats 
        (user_id, total_referrals, active_referrals)
        VALUES (?, 
                COALESCE((SELECT total_referrals FROM user_referral_stats WHERE user_id = ?), 0) + 1,
                COALESCE((SELECT active_referrals FROM user_referral_stats WHERE user_id = ?), 0) + 1
        )
        ''', (referrer_id, referrer_id, referrer_id))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding referral: {e}")
        conn.rollback()
        return False

def mark_referral_completed(referred_id: int) -> bool:
    """Отмечает, что реферал прошел капчу"""
    try:
        cursor.execute('''
        UPDATE referrals 
        SET completed_captcha = TRUE 
        WHERE referred_id = ?
        ''', (referred_id,))
        
        # Получаем ID реферера
        cursor.execute('SELECT referrer_id FROM referrals WHERE referred_id = ?', (referred_id,))
        result = cursor.fetchone()
        
        if result:
            referrer_id = result[0]
            # Выдаем награду рефереру
            return give_referral_reward(referrer_id, referred_id)
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error marking referral completed: {e}")
        conn.rollback()
        return False

def give_referral_reward(referrer_id: int, referred_id: int) -> bool:
    """Выдает награду за реферала"""
    try:
        # Проверяем, не была ли уже выдана награда
        cursor.execute('''
        SELECT reward_claimed FROM referrals 
        WHERE referrer_id = ? AND referred_id = ?
        ''', (referrer_id, referred_id))
        result = cursor.fetchone()
        
        if result and result[0]:
            return False  # Награда уже выдана
            
        # Отмечаем награду как выданную
        cursor.execute('''
        UPDATE referrals 
        SET reward_claimed = TRUE 
        WHERE referrer_id = ? AND referred_id = ?
        ''', (referrer_id, referred_id))
        
        # Обновляем статистику наград
        cursor.execute('''
        UPDATE user_referral_stats 
        SET total_rewards_claimed = total_rewards_claimed + 1,
            last_reward_claim = ?
        WHERE user_id = ?
        ''', (datetime.now().isoformat(), referrer_id))
        
        conn.commit()
        
        # Выдаем награду - 3 часа дохода фермы
        user = get_user(referrer_id)
        if user:
            farm_income = calculate_income(referrer_id) * 6 * 3  # 6 интервалов по 10 минут в часе * 3 часа
            new_btc = user[3] + farm_income
            update_balance(referrer_id, btc=new_btc, btc_delta=farm_income)

            # Отправляем уведомление рефереру
            asyncio.create_task(send_referral_reward_notification(referrer_id, referred_id, farm_income))
        
        return True
    except Exception as e:
        logger.error(f"Error giving referral reward: {e}")
        conn.rollback()
        return False

async def send_referral_reward_notification(referrer_id: int, referred_id: int, reward: float):
    """Отправляет уведомление о получении награды за реферала"""
    try:
        # Получаем информацию о реферале
        referred_user = await bot.get_chat(referred_id)
        referred_name = referred_user.full_name
        
        message = (
            "🎉 <b>Новая награда за реферала!</b>\n\n"
            f"👤 Ваш друг {referred_name} прошел капчу и активировал бота!\n"
            f"💰 Вы получаете +3 часа дохода своей фермы:\n"
            f"   <b>{format_number(reward)} BTC</b>\n\n"
            f"💎 Продолжайте приглашать друзей для получения большего дохода!"
        )
        
        await bot.send_message(chat_id=referrer_id, text=message, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error sending referral reward notification: {e}")

def get_user_referrals(user_id: int) -> List[Tuple]:
    """Получает список рефералов пользователя"""
    try:
        cursor.execute('''
        SELECT r.referred_id, r.registered_at, r.completed_captcha, r.reward_claimed,
               u.username
        FROM referrals r
        LEFT JOIN users u ON r.referred_id = u.user_id
        WHERE r.referrer_id = ?
        ORDER BY r.registered_at DESC
        ''', (user_id,))
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting user referrals: {e}")
        return []

# Функции для работы с капчей
def generate_captcha() -> Tuple[int, int, str, int]:
    """Генерирует капчу с двумя числами от 5 до 15 и оператором + или -"""
    number1 = random.randint(5, 15)
    number2 = random.randint(5, 15)
    operator = random.choice(['+', '-'])
    
    if operator == '+':
        correct_answer = number1 + number2
    else:
        correct_answer = number1 - number2
    
    return number1, number2, operator, correct_answer

def generate_wrong_answers(correct_answer: int) -> Tuple[int, int]:
    """Генерирует два неправильных ответа"""
    wrong1 = correct_answer + random.randint(1, 5)
    wrong2 = correct_answer - random.randint(1, 5)
    
    # Убеждаемся, что неправильные ответы не совпадают с правильным и друг с другом
    while wrong1 == correct_answer or wrong1 == wrong2 or wrong1 < 0:
        wrong1 = correct_answer + random.randint(1, 5)
    
    while wrong2 == correct_answer or wrong2 == wrong1 or wrong2 < 0:
        wrong2 = correct_answer - random.randint(1, 5)
    
    return wrong1, wrong2

async def create_referral_captcha(user_id: int) -> Optional[int]:
    """Создает капчу для реферала и возвращает message_id"""
    try:
        # Генерируем капчу
        number1, number2, operator, correct_answer = generate_captcha()
        wrong1, wrong2 = generate_wrong_answers(correct_answer)
        
        # Создаем список ответов и перемешиваем
        answers = [correct_answer, wrong1, wrong2]
        random.shuffle(answers)
        
        # Создаем сообщение с капчей
        captcha_text = (
            "🔐 <b>Проверка безопасности</b>\n\n"
            "Чтобы активировать бота и получить награду для пригласившего вас друга, "
            "решите простой пример:\n\n"
            f"<code>{number1} {operator} {number2} = ?</code>\n\n"
            "⏰ У вас есть <b>5 минут</b> чтобы ответить!"
        )
        
        # Создаем инлайн-кнопки с ответами
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=str(answers[0]), callback_data=f"captcha_{user_id}_{answers[0]}"),
                    InlineKeyboardButton(text=str(answers[1]), callback_data=f"captcha_{user_id}_{answers[1]}"),
                    InlineKeyboardButton(text=str(answers[2]), callback_data=f"captcha_{user_id}_{answers[2]}")
                ]
            ]
        )
        
        # Отправляем сообщение с капчей
        message = await bot.send_message(
            chat_id=user_id,
            text=captcha_text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        # Сохраняем капчу в базу
        expires_at = datetime.now() + timedelta(minutes=5)
        cursor.execute('''
        INSERT INTO referral_captchas 
        (user_id, number1, number2, operator, correct_answer, wrong_answer1, wrong_answer2, message_id, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user_id, number1, number2, operator, correct_answer, wrong1, wrong2, message.message_id, 
              datetime.now().isoformat(), expires_at.isoformat()))
        
        conn.commit()
        
        # Запускаем таймер удаления сообщения
        asyncio.create_task(delete_expired_captcha(user_id, message.message_id, expires_at))
        
        return message.message_id
        
    except Exception as e:
        logger.error(f"Error creating referral captcha: {e}")
        return None

async def delete_expired_captcha(user_id: int, message_id: int, expires_at: datetime):
    """Удаляет капчу после истечения времени"""
    try:
        now = datetime.now()
        wait_seconds = (expires_at - now).total_seconds()
        
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        
        # Проверяем, не была ли капча уже пройдена
        cursor.execute('SELECT completed_captcha FROM referrals WHERE referred_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result and not result[0]:
            # Капча не пройдена - удаляем данные реферала
            await handle_failed_captcha(user_id)
            
            # Пытаемся удалить сообщение
            try:
                await bot.delete_message(chat_id=user_id, message_id=message_id)
            except:
                pass  # Сообщение уже удалено или недоступно
                
    except Exception as e:
        logger.error(f"Error in delete_expired_captcha: {e}")

async def handle_failed_captcha(user_id: int):
    """Обрабатывает неудачную попытку прохождения капчи"""
    try:
        # Удаляем данные реферала
        cursor.execute('DELETE FROM referrals WHERE referred_id = ?', (user_id,))
        
        # Обновляем статистику реферера
        cursor.execute('SELECT referrer_id FROM referrals WHERE referred_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            referrer_id = result[0]
            cursor.execute('''
            UPDATE user_referral_stats 
            SET active_referrals = active_referrals - 1 
            WHERE user_id = ?
            ''', (referrer_id,))
        
        # Удаляем капчу
        cursor.execute('DELETE FROM referral_captchas WHERE user_id = ?', (user_id,))
        
        conn.commit()
        
        # Отправляем сообщение пользователю
        try:
            await bot.send_message(
                chat_id=user_id,
                text="❌ Время для решения капчи истекло. Регистрация отменена.\n\n"
                     "Если вы хотите зарегистрироваться, используйте команду /start без реферальной ссылки."
            )
        except:
            pass
            
    except Exception as e:
        logger.error(f"Error handling failed captcha: {e}")
        conn.rollback()

async def handle_captcha_answer(user_id: int, answer: int) -> bool:
    """Обрабатывает ответ на капчу"""
    try:
        # Получаем данные капчи
        cursor.execute('''
        SELECT correct_answer, message_id 
        FROM referral_captchas 
        WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        
        if not result:
            return False
            
        correct_answer, message_id = result
        
        if answer == correct_answer:
            # Капча пройдена успешно
            success = mark_referral_completed(user_id)
            
            if success:
                # Удаляем капчу из базы
                cursor.execute('DELETE FROM referral_captchas WHERE user_id = ?', (user_id,))
                conn.commit()
                
                # Удаляем сообщение с капчей
                try:
                    await bot.delete_message(chat_id=user_id, message_id=message_id)
                except:
                    pass
                
                # Отправляем сообщение об успехе
                await bot.send_message(
                    chat_id=user_id,
                    text="✅ <b>Капча успешно пройдена!</b>\n\n"
                         "Теперь вы полностью зарегистрированы в боте.\n"
                         "Ваш друг получит награду за вашу регистрацию!\n\n"
                         "Начните играть с команды /farm",
                    parse_mode='HTML'
                )
                return True
            else:
                return False
        else:
            # Неправильный ответ
            await handle_failed_captcha(user_id)
            
            # Отправляем сообщение об ошибке
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text="❌ <b>Неправильный ответ!</b>\n\n"
                         "Регистрация по реферальной ссылке отменена.\n\n"
                         "Если вы хотите зарегистрироваться, используйте команду /start без реферальной ссылки.",
                    parse_mode='HTML'
                )
            except:
                pass
            return False
            
    except Exception as e:
        logger.error(f"Error handling captcha answer: {e}")
        return False

# Команда /ref
@dp.message(Command("ref"))
async def referral_command(message: Message):
    """Показывает реферальную систему"""
    user_id = message.from_user.id
    
    # Получаем статистику
    stats = get_user_referral_stats(user_id)
    referral_link = get_referral_link(user_id)
    
    text = (
        "👑 <b>Реферальная Система</b>\n\n"
        "Привет! Добро пожаловать в нашу партнёрскую программу.\n"
        "Приглашай друзей и получай щедрое вознаграждение!\n\n"
        
        "✨ <b>Твоя Награда:</b>\n"
        "За каждого приглашенного пользователя, который пройдет капчу, ты получишь:\n\n"
        "💰 <b>+3 Часа Заработка со Своей Фермы!</b> 🕓\n\n"
        
        "🔗 <b>Твоя Реферальная Ссылка:</b>\n\n"
        f"<code>{referral_link}</code>\n\n"
        "Нажми, чтобы скопировать 👆 и поделись ею в социальных сетях, чатах или с друзьями!\n\n"
        
        "📊 <b>Твоя Статистика:</b>\n\n"
        f"👥 Всего Рефералов: <b>{stats['total_referrals']}</b>\n"
        f"✅ Активных: <b>{stats['active_referrals']}</b>\n"
        f"🎁 Получено наград: <b>{stats['total_rewards_claimed']}</b>\n"
    )
    
    # Добавляем информацию о последней награде
    if stats['last_reward_claim']:
        last_claim = datetime.fromisoformat(stats['last_reward_claim'])
        text += f"⏰ Последняя награда: <b>{last_claim.strftime('%d.%m.%Y %H:%M')}</b>"
    
    # Создаем инлайн-кнопки
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="📋 Мои рефералы", 
            callback_data=f"ref_list_{user_id}"
        )
    )
    builder.row(
        InlineKeyboardButton(
            text="🔄 Обновить", 
            callback_data=f"ref_refresh_{user_id}"
        )
    )
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')

# Обработчик для кнопки "Мои рефералы"
@dp.callback_query(F.data.startswith("ref_list_"))
async def show_referral_list(callback: CallbackQuery):
    """Показывает список рефералов пользователя"""
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    referrals = get_user_referrals(user_id)
    
    if not referrals:
        text = "📋 <b>Мои рефералы</b>\n\nУ вас пока нет рефералов.\nПригласите друзей по своей реферальной ссылке!"
    else:
        text = "📋 <b>Мои рефералы</b>\n\n"
        
        for i, (ref_id, reg_date, completed, claimed, username) in enumerate(referrals, 1):
            try:
                user_chat = await bot.get_chat(ref_id)
                display_name = user_chat.full_name
            except:
                display_name = username or f"ID {ref_id}"
            
            status = "✅ Прошел капчу" if completed else "⏳ Ожидает капчу"
            reward = "🎁 Награда получена" if claimed else "💎 Награда ожидает"
            
            reg_date_formatted = datetime.fromisoformat(reg_date).strftime('%d.%m.%Y')
            
            text += (
                f"{i}. <b>{display_name}</b>\n"
                f"   📅 {reg_date_formatted} | {status}\n"
                f"   {reward}\n\n"
            )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="🔙 Назад", 
            callback_data=f"ref_back_{user_id}"
        )
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
    await callback.answer()

# Обработчик для кнопки "Обновить"
@dp.callback_query(F.data.startswith("ref_refresh_"))
async def refresh_referral(callback: CallbackQuery):
    """Обновляет реферальную страницу"""
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    # Просто вызываем команду /ref заново
    await referral_command(callback.message)

# Обработчик для кнопки "Назад"
@dp.callback_query(F.data.startswith("ref_back_"))
async def back_to_referral_main(callback: CallbackQuery):
    """Возвращает к главной странице реферальной системы"""
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    await referral_command(callback.message)

# Обработчик ответов на капчу
@dp.callback_query(F.data.startswith("captcha_"))
async def handle_captcha_callback(callback: CallbackQuery):
    """Обрабатывает ответ на капчу"""
    await callback.answer()
    try:
        parts = callback.data.split('_')
        user_id = int(parts[1])
        answer = int(parts[2])

        if callback.from_user.id != user_id:
            return
        
        # Обрабатываем ответ
        success = await handle_captcha_answer(user_id, answer)
        
        if success:
            await callback.answer("✅ Капча пройдена успешно!", show_alert=True)
        else:
            await callback.answer("❌ Неправильный ответ!", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error handling captcha callback: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)
@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Проверяем, есть ли реферальный параметр в команде
    referral_id = None
    if len(message.text.split()) > 1:
        referral_param = message.text.split()[1]
        try:
            referral_id = int(referral_param)
        except ValueError:
            # Если это не число, игнорируем
            pass
    
    # Проверяем, зарегистрирован ли пользователь уже
    existing_user = get_user(user_id)
    
    # Создаем пользователя если его нет
    user_created = False
    if not existing_user:
        user_created = create_user(user_id, username)
        create_user_extended(user_id, username)
        
        # Если пользователь новый и есть реферальный ID, добавляем реферала и создаем капчу
        if user_created and referral_id and referral_id != user_id:
            # Проверяем, существует ли реферер
            referrer = get_user(referral_id)
            if referrer:
                add_referral(referral_id, user_id)
                # Создаем капчу для нового пользователя
                await create_referral_captcha(user_id)
                
                # Отправляем приветственное сообщение с информацией о капче
                welcome_text = (
                    "👋 Добро пожаловать в <b>CryptoMiner</b>!\n\n"
                    "🤖 Я - бот для майнинга криптовалюты. "
                    "Собирайте видеокарты, улучшайте ферму и зарабатывайте BTC!\n\n"
                    "🔐 <b>Вам нужно пройти проверку безопасности</b>\n"
                    "Решите простой пример в следующем сообщении, чтобы активировать бота "
                    "и получить награду для пригласившего вас друга.\n\n"
                    "⏰ У вас есть <b>5 минут</b> чтобы решить пример!"
                )
                
                if message.chat.type == 'private':
                    # Создаем клавиатуру с кнопками только для личных сообщений
                    keyboard = ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="🖥️ Ферма"), KeyboardButton(text="🛒 Магазин")],
                            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🎒 Инвентарь")],
                            [KeyboardButton(text="🎮 Игры"), KeyboardButton(text="🏢 Бизнес")]
                        ],
                        resize_keyboard=True
                    )
                    await message.answer(welcome_text, reply_markup=keyboard, parse_mode='HTML')
                else:
                    await message.answer(welcome_text, parse_mode='HTML')
                return
    
    # Если пользователь уже зарегистрирован или нет реферальной ссылки, отправляем обычное приветствие
    if message.chat.type == 'private':
        # Создаем клавиатуру с кнопками только для личных сообщений
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="🖥️ Ферма"), KeyboardButton(text="🛒 Магазин")],
                [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🎒 Инвентарь")],
                [KeyboardButton(text="🎮 Игры"), KeyboardButton(text="🏢 Бизнес")]
            ],
            resize_keyboard=True
        )
        
        text = (
            "🚀 <b>Добро пожаловать в симулятор майнера!</b>\n\n"
            "📊 Здесь ты можешь прочувствовать все прелести (а иногда даже в кавычках) жизни майнером!\n\n"
            "🔥 Покупай новые видеокарты, майни валюту, войди в топ лучших майнеров и не забывай следить за курсом валюты!\n\n"
            "📲 <b>Используй кнопки ниже для навигации:</b>\n"
            "▸ 🖥️ <b>Ферма</b> - твои видеокарты и доход\n"
            "▸ 🛒 <b>Магазин</b> - покупка карт и улучшений\n"
            "▸ 👤 <b>Профиль</b> - статистика и баланс\n"
            "▸ 🎒 <b>Инвентарь</b> - твои предметы и управление ими\n"
            "▸ 🎮 <b>Игры</b> - азартные развлечения\n"
            "▸ 🏢 <b>Бизнес</b> - пассивный доход\n\n"
        )
        
        # Добавляем информацию о реферальной системе для новых пользователей
        if user_created:
            text += (
                "💎 <b>Хочешь получить бонус?</b>\n"
                "Пригласи друзей по своей реферальной ссылке и получай награды!\n"
                "Используй команду /ref для получения ссылки."
            )
        else:
            text += "💡 Начни с покупки видеокарт в магазине!"

        # Отправляем баннер
        banner_path = os.path.join(BANNER_DIR, 'hello.png')
        try:
            from aiogram.types import FSInputFile
            photo = FSInputFile(banner_path)
            await message.answer_photo(
                photo=photo,
                caption=text,
                reply_markup=keyboard,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error sending start banner: {e}")
            await message.answer(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        # Для групповых чатов отправляем текст без кнопок
        text = (
            "🚀 <b>Добро пожаловать в симулятор майнера!</b>\n\n"
            "📊 Здесь ты можешь прочувствовать все прелести жизни майнером!\n\n"
            "🔥 Покупай новые видеокарты, майни валюту, войди в топ лучших майнеров!\n\n"
        )
        
        # Добавляем информацию о реферальной системе для новых пользователей
        if user_created:
            text += (
                "💎 <b>Хочешь получить бонус?</b>\n"
                "Пригласи друзей по реферальной ссылке!\n"
                "Напиши мне в личные сообщения команду /ref"
            )
        else:
            text += "💡 Начни с покупки видеокарт в магазине! Для управления ботом используй команды в личных сообщениях."
        
        await message.answer(text, parse_mode='HTML')

# Фоновая задача для очистки просроченных капч
async def clean_expired_captchas():
    """Очищает просроченные капчи из базы данных"""
    while True:
        try:
            now = datetime.now()
            cursor.execute('SELECT user_id, message_id FROM referral_captchas WHERE expires_at < ?', (now.isoformat(),))
            expired_captchas = cursor.fetchall()
            
            for user_id, message_id in expired_captchas:
                await handle_failed_captcha(user_id)
            
            await asyncio.sleep(300)  # Проверяем каждые 5 минут
            
        except Exception as e:
            logger.error(f"Error cleaning expired captchas: {e}")
            await asyncio.sleep(300)

# Запускаем фоновую задачу при старте бота
async def start_captcha_cleaner():
    """Запускает очистку просроченных капч"""
    asyncio.create_task(clean_expired_captchas())

# Обработчики для кнопок
@dp.message(F.text == "🖥️ Ферма")
async def farm_button(message: Message):
    await user_farm(message)

@dp.message(F.text == "🎒 Инвентарь")
async def inventory_button(message: Message):
    await inventory_command(message)

@dp.message(F.text == "🛒 Магазин")
async def shop_button(message: Message):
    await shop(message)

@dp.message(F.text == "👤 Профиль")
async def profile_button(message: Message):
    await profile(message)
    
def format_number_short(number: float, is_usd: bool = False) -> str:
    """
    Сокращает большие числа для лучшей читаемости с русскими сокращениями
    """
    if number == 0:
        return "0"
    
    # Для BTC оставляем больше знаков после запятой
    if not is_usd:
        if number < 0.001:
            return f"{number:.6f}"
        elif number < 1:
            return f"{number:.4f}"
        elif number < 1000:
            return f"{number:.3f}"
    
    abs_number = abs(number)
    sign = "-" if number < 0 else ""
    
    if abs_number < 1000:
        if is_usd:
            return f"{sign}{abs_number:,.0f}".replace(',', ' ')
        return f"{sign}{abs_number:.3f}"
    
    elif abs_number < 1_000_000:  # Тысячи
        formatted = f"{abs_number/1000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} тыс."
    
    elif abs_number < 1_000_000_000:  # Миллионы
        formatted = f"{abs_number/1_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} млн"
    
    elif abs_number < 1_000_000_000_000:  # Миллиарды
        formatted = f"{abs_number/1_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} млрд"
    
    elif abs_number < 1_000_000_000_000_000:  # Триллионы
        formatted = f"{abs_number/1_000_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} трлн"
    
    elif abs_number < 1_000_000_000_000_000_000:  # Квадриллионы
        formatted = f"{abs_number/1_000_000_000_000_000:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} квадрлн"
    
    elif abs_number < 1e18:  # Квинтиллионы
        formatted = f"{abs_number/1e15:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} квинтлн"
    
    elif abs_number < 1e21:  # Секстиллионы
        formatted = f"{abs_number/1e18:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} секстилн"
    
    elif abs_number < 1e24:  # Септиллионы
        formatted = f"{abs_number/1e21:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} септилн"
    
    elif abs_number < 1e27:  # Октиллионы
        formatted = f"{abs_number/1e24:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} октилн"
    
    elif abs_number < 1e30:  # Нониллионы
        formatted = f"{abs_number/1e27:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} нонилн"
    
    else:  # Дециллионы и больше
        formatted = f"{abs_number/1e30:.2f}".rstrip('0').rstrip('.')
        return f"{sign}{formatted} децилн"
@dp.message(Command("me"))
async def profile(message: Message):
    user_id = message.from_user.id
    create_user(user_id, message.from_user.username)
    
    btc_income = await check_income(user_id)
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Произошла ошибка при загрузке профиля")
        return
    
    # Получаем титул пользователя
    cursor.execute('SELECT badge_id FROM user_badges WHERE user_id = ?', (user_id,))
    badge_data = cursor.fetchone()
    badge_name = ""
    if badge_data and badge_data[0] > 0:
        badge_id = badge_data[0]
        if badge_id in BADGES:
            badge_name = f"{BADGES[badge_id]['name']}\n"
        else:
            # Проверяем кастомный бейдж
            cursor.execute('SELECT badge_name FROM custom_badges WHERE badge_id = ?', (badge_id,))
            custom_badge = cursor.fetchone()
            if custom_badge:
                badge_name = f"{custom_badge[0]}\n"
    
    # Получаем премиум статус
    premium_status = ""
    cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
    premium_data = cursor.fetchone()
    if premium_data and premium_data[0]:
        premium_until = datetime.fromisoformat(premium_data[0])
        now = datetime.now()
        if now < premium_until:
            delta = premium_until - now
            days = delta.days
            hours = delta.seconds // 3600
            minutes = (delta.seconds % 3600) // 60
            
            premium_status = f"✨PREMIUM ⏱️ {days}д {hours}ч {minutes}м\n"
    
    user_cards, total_cards = get_user_cards(user_id)
    total_income = calculate_income(user_id)
    btc_price = get_btc_price()
    
    # Получаем информацию о чате
    cursor.execute('SELECT chat_id FROM chat_members WHERE user_id = ?', (user_id,))
    chat_id_row = cursor.fetchone()
    chat_info = None
    if chat_id_row:
        chat_id = chat_id_row[0]
        chat_info = get_chat_info(chat_id)
    
    # Рассчитываем доход для чата (без премиум бонуса)
    chat_income = 0.0
    cards, _ = get_user_cards(user_id)
    for card_id, count in cards:
        card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        if card:
            chat_income += card['income'] * count
    
    # Добавляем бонус от улучшений
    bonus = calculate_upgrade_bonus(user_id)
    chat_income *= (1 + bonus)
    
    # Добавляем бонус от вайпов
    cursor.execute('SELECT total_wipe_bonus FROM user_wipes WHERE user_id = ?', (user_id,))
    wipe_bonus = cursor.fetchone()
    if wipe_bonus and wipe_bonus[0]:
        chat_income *= (1 + wipe_bonus[0] / 100)
    
    # Используем те же данные о слотах, что и в /farm
    max_slots_farm = EXPANSIONS[user[5]-1]['max_cards']
    
    # Проверяем активный вирус
    virus_status = ""
    virus_time = get_virus_time_remaining(user_id)
    if virus_time:
        hours, minutes = virus_time
        virus_status = f"🦠 Вирус: -30% Дохода\n⏳ Осталось: {hours}ч {minutes}м\n——————————\n"
    
    # Формируем текст профиля с сокращенными числами
    text = f"👑 ᴘʀᴏғɪʟᴇ | {message.from_user.full_name}\n"
    if badge_name:
        text += f"{badge_name}"
    if premium_status:
        text += f"{premium_status}"
    
    text += f"——————————\n"
    
    text += f"🧑‍🌾 Ферма:\n"
    text += f"⚙️ Доход: {format_number_short(total_income)} BTC/10 мин.\n"
    # Используем те же данные о слотах, что и в /farm
    text += f"📦 Слоты: {total_cards}/{max_slots_farm}\n\n"
    
    text += f"🗣 Доход чата:\n"
    if chat_info:
        text += f"💬 Чат: {chat_info[1]}\n"
        text += f"💸 Доход: {format_number_short(chat_income)} BTC/10 мин.\n"
        text += f"Отменить привязку: /decline_chat\n"
    else:
        text += f"💬 Чат: Отсутствует\n"
        text += f"💸 Доход: {format_number_short(chat_income)} BTC/10 мин.\n"
        text += f"Привязать: /join_chat\n"
    
    text += f"——————————\n"
    
    text += f"💰Баланс:\n"
    text += f"🌐 BTC: {format_number_short(user[3])}\n"
    text += f"💵 $: {format_number_short(user[2], is_usd=True)}\n"
    text += f"📊 Курс BTC: {format_number_short(btc_price, is_usd=True)}$/ 1 BTC\n"
    text += f"Купить: /btc_buy [сумма]\n"
    text += f"Продать: /btc_sell [сумма]\n"
    text += f"——————————\n"
    
    # Добавляем статус вируса только если он есть
    if virus_status:
        text += virus_status
    
    text += f"🗂 Полезные команды:\n"
    text += f"/stats | /business | /bonus | /boosts | /nalog | /ebonus"
    
    # Создаем кнопки (только продажа BTC)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text="💰 Продать все BTC",
        callback_data=f"sell_all_btc_{user_id}")
    )
    
    # Отправляем с баннером
    banner_path = os.path.join(BANNER_DIR, 'profile.png')
    try:
        from aiogram.types import FSInputFile
        photo = FSInputFile(banner_path)
        await message.answer_photo(
            photo=photo,
            caption=text,
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )
    except Exception as e:
        # Если не удалось отправить фото, отправляем просто текст
        logger.error(f"Error sending profile banner: {e}")
        await message.answer(
            text,
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
            disable_web_page_preview=True
        )

@dp.message(F.text == "🏆 Топ")
async def top_button(message: Message):
    await top_users(message)

@dp.message(F.text == "🎮 Игры")
async def games_button(message: Message):
    await help_games(message)

@dp.message(F.text == "🏢 Бизнес")
async def business_button(message: Message):
    await business_command(message)


from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

@dp.message(Command("shop"))
async def shop(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    # Получаем информацию о вайпах
    cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
    wipe_data = cursor.fetchone()
    wipe_count = wipe_data[0] if wipe_data else 0
    
    current_expansion = user[5]
    max_slots = WIPES_SLOTS.get(wipe_count, 195)
    
    # Получаем информацию о предметах
    equipped_items, total_items = get_user_items_count(user_id)
    
    # Получаем баланс ивент валюты
    event_balance = get_event_currency(user_id)
    
    # Создаем клавиатуру с ДОБАВЛЕННОЙ кнопкой ивента
    buttons = [
        [InlineKeyboardButton(text="🎮 Видеокарты", callback_data=f"shop_cards_{user_id}")],
        [InlineKeyboardButton(text="📦 Расширения", callback_data=f"shop_capacity_{user_id}")],
        [InlineKeyboardButton(text="🔧 Улучшения", callback_data=f"shop_upgrades_{user_id}")],
        [InlineKeyboardButton(text="🎁 Предметы", callback_data=f"shop_items_{user_id}")] 
#         [InlineKeyboardButton(text="🎃 Ивент", callback_data=f"shop_event_{user_id}")]  # НОВАЯ КНОПКА
    ]
    
    # Добавляем кнопку бизнесов только если уровень фермы >= 12
    if current_expansion >= 12:
        buttons.append([InlineKeyboardButton(text="🏢 Бизнесы", callback_data=f"shop_business_{user_id}_1")])
    
    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    text = (
        f"🛒 <b>Магазин</b> | Ур. {current_expansion}\n"
        f"📦 Слоты: {EXPANSIONS[current_expansion-1]['max_cards']}/{max_slots}\n"
        f"🔢 Вайпы: {wipe_count}/{MAX_WIPES}\n"
        f"🎁 Предметы: {equipped_items}/{MAX_EQUIPPED_ITEMS} надето, {total_items}/{MAX_TOTAL_ITEMS} всего\n"
#         f"🎃 Ивент валюта: {event_balance}\n\n"
        "Выберите категорию:"
    )

    # Отправляем баннер с кешированием file_id для быстрой загрузки
    global SHOP_BANNER_FILE_ID
    try:
        if SHOP_BANNER_FILE_ID:
            # Используем кешированный file_id
            sent_msg = await message.answer_photo(
                photo=SHOP_BANNER_FILE_ID,
                caption=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        else:
            # Загружаем баннер с диска первый раз и кешируем file_id
            from aiogram.types import FSInputFile
            banner_path = os.path.join(BANNER_DIR, 'shops.png')
            photo = FSInputFile(banner_path)
            sent_msg = await message.answer_photo(
                photo=photo,
                caption=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
            # Сохраняем file_id для последующих использований
            SHOP_BANNER_FILE_ID = sent_msg.photo[-1].file_id
    except Exception as e:
        logger.error(f"Error sending shop banner: {e}")
        await message.answer(text, reply_markup=markup, parse_mode='HTML')

# @dp.callback_query(F.data.startswith("shop_event_"))
async def shop_event_handler(callback: CallbackQuery):
    """Обработчик для раздела ивента в магазине"""
    try:
        user_id = int(callback.data.split('_')[2])
        
        if callback.from_user.id != user_id:
            return
        
        event_balance = get_event_currency(user_id)
        
        text = (
            f"🎃 <b>Меню ивента</b>\n\n"
            f"Ваш баланс - {event_balance} 🎃\n\n"
            f"<code>/case_1</code> - Цена: 5🎃\n\n"
            f"Топ игроков по заработку ивент валюты - /top_ivent"
        )
        
        # Создаем клавиатуру
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🎁 Открыть кейс", callback_data=f"event_open_case_{user_id}")
        )
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=f"shop_back_{user_id}")
        )
        
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in shop event handler: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)
        
@dp.message(Command("finish_event"))
async def finish_event_command(message: Message):
    """Команда для завершения ивента (только для админа)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # 1. Получаем топ-10 победителей
        top_players = get_event_top(10)
        
        if not top_players:
            await message.answer("❌ Нет участников ивента")
            return
        
        # 2. Создаем красивое оформление для победителей
        winners_text = "👋 <b>Дорогие игроки</b>\n\n"
        winners_text += "<b>Осенний ивент завершён!</b>\n\n"
        winners_text += "<i>Благодарим каждого, кто проявлял участие и активность!</i>\n\n"
        winners_text += "🏆 <b>А вот и награды:</b>\n\n"
        
        medals = ["🥇", "🥈", "🥉"]
        
        # Топ-3 победителя с медалями
        for i, (user_id, username, earned) in enumerate(top_players[:3], 1):
            try:
                user_chat = await bot.get_chat(user_id)
                display_name = user_chat.full_name
                mention = f'<a href="tg://user?id={user_id}">{display_name}</a>'
            except Exception:
                display_name = username if username else f"ID {user_id}"
                mention = display_name
            
            winners_text += f"{medals[i-1]} <b>{mention}</b>\n"
            winners_text += f"   🎃 {earned} коинов\n\n"
        
        winners_text += "🎁 <b>Награды:</b>\n"
        winners_text += "🥇 - Уникальный префикс и 2 недели премиума!\n"
        winners_text += "🥈 - Уникальный префикс и 1 неделя премиума!\n"
        winners_text += "🥉 - Уникальный префикс и 5 дней премиума!\n\n"
        
        # Игроки на 4-10 местах
        if len(top_players) > 3:
            winners_text += "<b>Игроки на 4-10 месте:</b>\n"
            for i, (user_id, username, earned) in enumerate(top_players[3:10], 4):
                try:
                    user_chat = await bot.get_chat(user_id)
                    display_name = user_chat.full_name
                    mention = f'<a href="tg://user?id={user_id}">{display_name}</a>'
                except Exception:
                    display_name = username if username else f"ID {user_id}"
                    mention = display_name
                
                winners_text += f"{i}. {mention} - {earned} 🎃\n"
            
            winners_text += "\n🎁 <b>Получают 3 дня премиума</b> 😍\n\n"
        
        winners_text += "📢 <b>Важно:</b>\n"
        winners_text += "Игроки, которые заняли 1, 2 и 3 место, просьба написать менеджеру ( @Mngr_Nexoria ) для выдачи префикса\n\n"
        winners_text += "❤️ <i>Поздравляем всех победителей!</i> ❤️"
        
        # 3. Отправляем в канал
        CHANNEL_ID = -1002734900704  # ID канала
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=winners_text,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
        
        # 4. Выдаем награды победителям
        for i, (user_id, username, earned) in enumerate(top_players, 1):
            try:
                if i == 1:
                    # 1 место: 2 недели премиума
                    premium_until = (datetime.now() + timedelta(days=14)).isoformat()
                    cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)', 
                                 (user_id, premium_until))
                
                elif i == 2:
                    # 2 место: 1 неделя премиума
                    premium_until = (datetime.now() + timedelta(days=7)).isoformat()
                    cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)',
                                 (user_id, premium_until))
                
                elif i == 3:
                    # 3 место: 5 дней премиума
                    premium_until = (datetime.now() + timedelta(days=5)).isoformat()
                    cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)',
                                 (user_id, premium_until))
                
                elif i <= 10:
                    # 4-10 места: 3 дня премиума
                    premium_until = (datetime.now() + timedelta(days=3)).isoformat()
                    cursor.execute('INSERT OR REPLACE INTO premium_users (user_id, premium_until) VALUES (?, ?)',
                                 (user_id, premium_until))
                
                # Записываем в историю победителей
                now = datetime.now().isoformat()
                cursor.execute('INSERT INTO event_top_winners (user_id, place, created_at) VALUES (?, ?, ?)',
                             (user_id, i, now))
                
            except Exception as e:
                logger.error(f"Error giving reward to user {user_id} (place {i}): {e}")
                continue
        
        # 5. Очищаем все данные ивента
        cursor.execute('DELETE FROM event_currency')
        cursor.execute('DELETE FROM event_rewards_log')
        conn.commit()
        
        # 6. Отправляем подтверждение админу
        await message.answer(
            "✅ Ивент успешно завершен!\n\n"
            f"📊 Победители объявлены в канале\n"
            f"🎁 Награды выданы (премиум добавлен)\n"
            f"👥 Всего участников в топе: {len(top_players)}\n"
            f"🗑️ Данные ивента очищены\n\n"
            f"⚠️ Не забудьте написать @Mngr_Nexoria для выдачи префиксов топ-3!"
        )
        
        logger.info(f"Event finished by admin {message.from_user.id}. Winners count: {len(top_players)}")
        
    except Exception as e:
        logger.error(f"Error finishing event: {e}")
        await message.answer(f"❌ Ошибка при завершении ивента: {str(e)}")


@dp.callback_query(F.data.startswith("event_open_case_"))
async def event_open_case_handler(callback: CallbackQuery):
    """Обработчик открытия кейса через кнопку"""
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[3])

        if callback.from_user.id != user_id:
            return

        success, result = open_event_case(user_id)
        await callback.message.edit_text(result)

    except Exception as e:
        logger.error(f"Error in event open case handler: {e}")

@dp.callback_query(F.data.startswith("event_show_top_"))
async def event_show_top_handler(callback: CallbackQuery):
    """Показать топ через кнопку"""
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[3])

        if callback.from_user.id != user_id:
            return

        await top_ivent_command(callback.message)

    except Exception as e:
        logger.error(f"Error in event show top handler: {e}")
        
        
@dp.callback_query(F.data.startswith("shop_items_"))
async def shop_items_handler(callback: CallbackQuery):
    """Обработчик для раздела предметов в магазине"""
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[2])

        if callback.from_user.id != user_id:
            return
        
        # Создаем клавиатуру для выбора категории предметов
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🟢 Новичок", callback_data=f"items_novice_{user_id}"),
            InlineKeyboardButton(text="🔴 Бывалый", callback_data=f"items_experienced_{user_id}")
        )
        builder.row(
            InlineKeyboardButton(text="⚫️ Старпёр", callback_data=f"items_veteran_{user_id}"),
            InlineKeyboardButton(text="💼 Кейсы", callback_data=f"items_cases_{user_id}")
        )
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=f"shop_back_{user_id}")
        )
        
        # Получаем информацию о предметах пользователя
        equipped_items, total_items = get_user_items_count(user_id)
        
        text = (
            "🎁 <b>Магазин предметов</b>\n\n"
            f"📊 Ваши предметы: {equipped_items}/{MAX_EQUIPPED_ITEMS} надето, {total_items}/{MAX_TOTAL_ITEMS} всего\n\n"
            "Выберите категорию:"
        )

        await callback.message.edit_caption(caption=text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in shop items handler: {e}")

@dp.callback_query(F.data.startswith("items_novice_"))
async def items_novice_handler(callback: CallbackQuery):
    """Показывает предметы категории Новичок"""
    await callback.answer()
    await show_items_category(callback, "novice")

@dp.callback_query(F.data.startswith("items_experienced_"))
async def items_experienced_handler(callback: CallbackQuery):
    """Показывает предметы категории Бывалый"""
    await callback.answer()
    await show_items_category(callback, "experienced")

@dp.callback_query(F.data.startswith("items_veteran_"))
async def items_veteran_handler(callback: CallbackQuery):
    """Показывает предметы категории Старпёр"""
    await callback.answer()
    await show_items_category(callback, "veteran")

async def show_items_category(callback: CallbackQuery, category: str):
    """Показывает предметы выбранной категории"""
    try:
        user_id = int(callback.data.split('_')[2])
        
        if callback.from_user.id != user_id:
            return
        
        # Фильтруем предметы по категории
        category_items = [item for item in ITEMS if item["category"] == category]
        
        if not category_items:
            await callback.answer("В этой категории пока нет предметов", show_alert=True)
            return
        
        # Создаем текст с предметами
        category_names = {
            "novice": "🟢 Новичок",
            "experienced": "🔴 Бывалый", 
            "veteran": "⚫️ Старпёр"
        }
        
        text = f"🏪 <b>Магазин предметов: {category_names[category]}</b>\n\n"
        
        for item in category_items:
            text += f"<b>{item['name']}</b>\n"
            text += f"💵 Цена: ${format_number(item['price'], True)}\n"
            text += f"🎯 Бонус: +{item['farm_bonus']*100:.0f}% к доходу фермы и +{item['business_bonus']*100:.0f}% к доходу бизнесов\n"
            
            # Генерируем команду для покупки
            command_map = {
                1: "terp", 2: "term", 3: "cor", 4: "hdd", 5: "cyl",
                6: "oppam", 7: "bp", 8: "ssd", 9: "vent", 10: "wifi", 
                11: "matpl", 12: "videokar", 13: "ventl", 14: "nvme", 15: "sobpk"
            }
            command = command_map.get(item["id"], f"item_{item['id']}")
            text += f"🛒 Купить - /buy_{command}\n\n"
        
        # Кнопка назад
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=f"shop_items_{user_id}")
        )

        await callback.message.edit_caption(caption=text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in items category handler: {e}")

@dp.callback_query(F.data.startswith("items_cases_"))
async def items_cases_handler(callback: CallbackQuery):
    """Показывает доступные кейсы"""
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[2])
        
        if callback.from_user.id != user_id:
            return
        
        text = "💼 <b>Кейсы</b>\n\n"
        
        text += "💼 Кейс \"Новичок\" - /case_buy\n"
        text += f"💵 Цена: ${format_number(CASE_PRICES['novice'], True)}\n\n"
        
        text += "💼 Кейс \"Бывалый\" - /case_buy1\n" 
        text += f"💵 Цена: ${format_number(CASE_PRICES['experienced'], True)}\n\n"
        
        text += "💼 Кейс \"Старпёр\" - /case_buy2\n"
        text += f"💵 Цена: ${format_number(CASE_PRICES['veteran'], True)}\n\n"
        
        text += "📦 Название кейса указывает на то, какие предметы в нём есть.\n"
        text += "🔍 С предметами можно ознакомиться в магазине предметов\n\n"
        text += "<b>🎰 Шансы выпадения:</b>\n"
        text += "• Обычные предметы - 70%\n"
        text += "• Необычные предметы - 20-26%\n" 
        text += "• Редкие предметы - 1-4%\n"
        
        # Кнопка назад
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="🔙 Назад", callback_data=f"shop_items_{user_id}")
        )

        await callback.message.edit_caption(caption=text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in items cases handler: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)
# Команды покупки предметов
@dp.message(F.text.regexp(r'^/buy_(terp|term|cor|hdd|cyl|oppam|bp|ssd|vent|wifi|matpl|videokar|ventl|nvme|sobpk)$'))
async def buy_item_command(message: Message):
    """Обработчик покупки предметов"""
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    try:
        item_type = message.text.split('_')[1]
        
        success, result = buy_item(user_id, item_type)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
        
    except Exception as e:
        logger.error(f"Error in buy item command: {e}")
        await message.answer("❌ Произошла ошибка при покупке предмета")

# Команды открытия кейсов
@dp.message(Command("case_buy"))
async def case_buy_novice(message: Message):
    """Открывает кейс новичка"""
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
        
    success, result = open_case(user_id, "novice")
    await message.answer(f"✅ {result}" if success else f"❌ {result}")

@dp.message(Command("case_buy1"))
async def case_buy_experienced(message: Message):
    """Открывает кейс бывалого"""
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
        
    success, result = open_case(user_id, "experienced")
    await message.answer(f"✅ {result}" if success else f"❌ {result}")

@dp.message(Command("case_buy2"))
async def case_buy_veteran(message: Message):
    """Открывает кейс старпёра"""
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
        
    success, result = open_case(user_id, "veteran")
    await message.answer(f"✅ {result}" if success else f"❌ {result}")
    
@dp.callback_query(F.data.startswith("shop_"))
async def shop_callback_handler(callback: CallbackQuery):
    await callback.answer()  # Сразу отвечаем Telegram
    try:
        parts = callback.data.split("_")
        if len(parts) < 3:
            await callback.message.answer("Неверный формат callback")
            return

        action = parts[1]
        callback_user_id = int(parts[2])
        
        if callback.from_user.id != callback_user_id:
            await callback.message.answer("Это не ваш магазин!")
            return
            
        if action == "cards":
            await shop_cards_handler(callback, callback_user_id)
        elif action == "capacity":
            await shop_capacity_handler(callback, callback_user_id)
        elif action == "business":
            # Handle business with page number
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
            await shop_business_handler(callback, callback_user_id, page)
        elif action == "upgrades":
            await upgrades_menu_handler(callback, callback_user_id)
        elif action == "back":
            await shop_back_handler(callback)
        else:
            await callback.message.answer("Неизвестный раздел магазина")
    except Exception as e:
        logger.error(f"Error in shop callback: {e}")
        await callback.message.answer("Произошла ошибка")
        
def buy_upgrade(user_id: int, upgrade_type: str) -> Tuple[bool, str]:
    try:
        # Цены за уровни
        PRICES = [10000, 100000, 3000000, 10000000, 25000000]
        
        cursor.execute(f'SELECT {upgrade_type} FROM user_upgrades WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if not result:
            # Создаем запись, если ее нет
            cursor.execute('INSERT INTO user_upgrades (user_id) VALUES (?)', (user_id,))
            current_level = 0
        else:
            current_level = result[0]
        
        if current_level >= 5:
            return False, f"Вы уже достигли максимального уровня для этого улучшения!"
        
        price = PRICES[current_level]
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
            
        if user[2] < price:
            return False, (
                f"Недостаточно средств для улучшения!\n"
                f"Цена: ${format_number(price, True)}\n"
                f"Ваш баланс: ${format_number(user[2], True)}"
            )
        
        # Обновляем уровень улучшения
        cursor.execute(
            f'UPDATE user_upgrades SET {upgrade_type} = ? WHERE user_id = ?',
            (current_level + 1, user_id)
        )
        
        # Списываем деньги
        new_usd = user[2] - price
        update_balance(user_id, usd=new_usd)
        
        # Пересчитываем доход
        calculate_income(user_id)
        
        return True, (
            f"Улучшение успешно куплено!\n"
            f"Новый уровень: {current_level + 1}/5 (+{current_level + 1}% к доходу)\n"
            f"Баланс: ${format_number(new_usd, True)}"
        )
    except Exception as e:
        logger.error(f"Error buying upgrade: {e}")
        conn.rollback()
        return False, "Произошла ошибка при покупке улучшения"        
        
        
        
async def shop_cards_handler(callback: CallbackQuery, user_id: int):
    user = get_user(user_id)
    if not user:
        await callback.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return

    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="🔙 Назад",
        callback_data=f"shop_back_{user_id}"
    ))
    
    user_cards, total_cards = get_user_cards(user_id)
    current_expansion = user[5]
    expansion_info = EXPANSIONS[current_expansion - 1]
    max_cards = expansion_info['max_cards']
    
    # Получаем корректные границы доступных карт
    first_card = expansion_info['first_available_card']
    last_card = expansion_info['last_available_card']
    
    # Исправляем случай, когда first_card > last_card
    if first_card > last_card:
        first_card, last_card = last_card, first_card
    
    # Ограничиваем last_card максимальным ID карты
    last_card = min(last_card, len(GRAPHICS_CARDS))
    
    user_card_ids = {card_id for card_id, _ in user_cards}
    
    text = "🛒 <b>Магазин видеокарт</b>\n\n"
    text += f"📦 Слоты: {total_cards}/{max_cards} | Ур. фермы: {current_expansion}\n"
    text += f"💵 Баланс: ${format_number(user[2], True)}\n"
    text += f"🔓 Доступно карт: {first_card}-{last_card}\n\n"
    text += "<b>Доступные видеокарты:</b>\n\n"

    available_cards = []
    for card in GRAPHICS_CARDS:
        if first_card <= card['id'] <= last_card:
            available_cards.append(card)
    
    if not available_cards:
        text += "❌ Нет доступных видеокарт для этого уровня фермы\n"
    else:
        for card in available_cards:
            status = "✅" if card['id'] in user_card_ids else "🔹"
            text += (
                f"{status} /buy_{card['id']} - {card['name']}\n"
                f"💵 ${format_number(card['price'], True)} | 💰 {format_number(card['income'])} BTC/10мин\n\n"
            )

    if last_card < len(GRAPHICS_CARDS):
        text += f"\n🔒 Улучшайте ферму для новых карт"

    # Проверяем длину текста (лимит Telegram для caption - 1024 символа)
    if len(text) > 1000:
        # Обрезаем текст, если он слишком длинный
        text = text[:950] + "\n\n⚠️ Список слишком большой. Используйте команды для покупки."

    try:
        await callback.message.edit_caption(caption=text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
    except Exception as e:
        logger.error(f"Error editing shop cards message: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)
         
async def shop_capacity_handler(callback: CallbackQuery, user_id: int):
    try:
        user = get_user(user_id)
        if not user:
            await callback.answer("Сначала зарегистрируйтесь с помощью команды /start")
            return
        
        # Создаем кнопку "Назад"
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(
            text="🔙 Назад",
            callback_data=f"shop_back_{user_id}"
        ))
        
        try:
            # Получаем информацию о вайпах
            cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
            wipe_data = cursor.fetchone()
            wipe_count = wipe_data[0] if wipe_data else 0
            
            current_expansion = user[5]
            current_max_slots = WIPES_SLOTS.get(wipe_count, 195)
            
            # Рассчитываем БАЗОВЫЙ доход (без бустеров, только улучшения + вайпы)
            base_income = calculate_base_income(user_id)
            
            # Добавляем только премиум бонус (как в capacity_buy)
            premium_bonus = 0.35 if is_premium(user_id) else 0
            total_income_for_check = base_income * (1 + premium_bonus)
            
            text = f"📦 <b>Улучшение фермы</b>\n\n"
            text += f"Текущий уровень: {current_expansion}\n"
            text += f"Текущие слоты: {EXPANSIONS[current_expansion-1]['max_cards']}/{current_max_slots}\n"
            
            if current_expansion < len(EXPANSIONS):
                next_expansion = EXPANSIONS[current_expansion]
                text += f"Следующий уровень: {next_expansion['max_cards']} слотов\n"
                text += f"Стоимость: ${format_number(next_expansion['price'], True)}\n"
                
                if next_expansion['max_cards'] > 60:
                    text += f"Требуемый доход: {format_number(next_expansion['min_income'])} BTC/10мин\n"
            
            text += f"\nВаш доход для проверки: {format_number(total_income_for_check)} BTC/10мин\n"
            text += f"(базовый + улучшения + вайпы + премиум)\n"
            
            if EXPANSIONS[current_expansion-1]['max_cards'] >= current_max_slots:
                text += "\nℹ️ Вы достигли текущего лимита слотов.\n"
                text += "Выполните вайп (/wipe), чтобы увеличить лимит.\n"
            else:
                text += "Купить - /capacity_buy"

            await callback.message.edit_caption(caption=text, reply_markup=builder.as_markup(), parse_mode='HTML')
            
        except sqlite3.Error as e:
            await callback.answer("❌ Произошла ошибка при получении данных. Попробуйте позже.")
            logging.error(f"Database error in shop_capacity_handler: {e}")
            
    except Exception as e:
        await callback.answer("❌ Произошла неизвестная ошибка. Попробуйте позже.")
        logging.error(f"Unexpected error in shop_capacity_handler: {e}")
        
async def upgrades_menu_handler(callback: CallbackQuery, user_id: int):
    user = get_user(user_id)
    if not user:
        await callback.answer("Сначала зарегистрируйтесь с помощью команды /start", show_alert=True)
        return
    
    # Создаем кнопку "Назад"
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="🔙 Назад",
        callback_data=f"shop_back_{user_id}"
    ))
    
    # Получаем текущие уровни улучшений
    cursor.execute('SELECT wiring, ventilation, traffic, software, cooling FROM user_upgrades WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    
    if not result:
        wiring = ventilation = traffic = software = cooling = 0
    else:
        wiring, ventilation, traffic, software, cooling = result
    
    # Цены за уровни
    PRICES = [10000, 100000, 3000000, 10000000, 25000000]
    
    text = "🔧 <b>Улучшения фермы</b>\n\n"
    text += "Каждое улучшение дает +0.5% к доходу вашей фермы!\n\n"  # Изменено с 1% на 0.5%
    
    text += f"🔌 <b>Проводка</b>: +{wiring * 0.5}%\n"  # Изменено отображение бонуса
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[wiring] if wiring < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_wiring\n\n" if wiring < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"🌀 <b>Вентиляция</b>: +{ventilation * 0.5}%\n"  # Изменено отображение бонуса
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[ventilation] if ventilation < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_ventilation\n\n" if ventilation < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"📶 <b>Трафик</b>: +{traffic * 0.5}%\n"  # Изменено отображение бонуса
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[traffic] if traffic < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_traffic\n\n" if traffic < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"💾 <b>Программное обеспечение</b>: +{software * 0.5}%\n"  # Изменено отображение бонуса
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[software] if software < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_software\n\n" if software < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"❄️ <b>Охлаждение</b>: +{cooling * 0.5}%\n"  # Изменено отображение бонуса
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[cooling] if cooling < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_cooling\n\n" if cooling < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    total_bonus = (wiring + ventilation + traffic + software + cooling) * 0.5  # Изменено вычисление общего бонуса
    text += f"🔰 <b>Общий бонус к доходу</b>: +{total_bonus}%\n"
    
    try:
        await callback.message.edit_caption(
            caption=text,
            reply_markup=builder.as_markup(),
            parse_mode='HTML'
        )
        await callback.answer()
    except Exception as e:
        logger.error(f"Error editing upgrades menu: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)

        
async def shop_business_handler(callback: CallbackQuery, user_id: int, page: int = 1):
    user = get_user(user_id)
    if not user:
        await callback.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    # Объявление кнопок для навигации
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="🔙 Назад",
        callback_data=f"shop_back_{user_id}"
    ))
    
    # Настройка параметров пагинации
    items_per_page = 3  # Количество бизнесов на странице
    total_pages = (len(BUSINESSES) + items_per_page - 1) // items_per_page  # Общее количество страниц
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    paginated_businesses = BUSINESSES[start_idx:end_idx]
    
    text = "🏢 <b>Магазин бизнесов</b>\n\n"
    text += f"Страница {page} из {total_pages}\n\n"
    
    # Добавление информации о бизнесах
    for business in paginated_businesses:
        text += (
            f"{business['emoji']} <b>{business['name']}</b>\n"
            f"▸ Цена: ${format_number(business['base_price'], True)}\n"
            f"▸ Базовый доход: ${format_number(business['base_income'], True)}/час\n"
            f"▸ Макс. уровень: {business['max_level']}\n"
            f"▸ Купить: /buybiz_{business['id']}\n\n"
        )
    
    # Добавление кнопок навигации по страницам
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"shop_business_{user_id}_{page-1}"
        ))
    if page < total_pages:
        row.append(InlineKeyboardButton(
            text="Вперёд ➡️",
            callback_data=f"shop_business_{user_id}_{page+1}"
        ))
    builder.row(*row)
    
    text += f"\n💵 Ваш баланс: ${format_number(user[2], True)}"
    text += f"\n\nℹ️ У вас может быть максимум {MAX_BUSINESSES} бизнесов"
    
    try:
        await callback.message.edit_caption(caption=text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
    except Exception as e:
        logger.error(f"Error editing shop business message: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("shop_back_"))
async def shop_back_handler(callback: CallbackQuery):
    await callback.answer()
    try:
        # Извлекаем user_id из callback_data
        user_id = int(callback.data.split('_')[2])

        if callback.from_user.id != user_id:
            return
            
        # Возвращаемся в главное меню магазина
        user = get_user(user_id)
        if not user:
            await callback.answer("Сначала зарегистрируйтесь с помощью команды /start")
            return
        
        # Получаем информацию о вайпах
        cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
        wipe_data = cursor.fetchone()
        wipe_count = wipe_data[0] if wipe_data else 0
        
        current_expansion = user[5]
        max_slots = WIPES_SLOTS.get(wipe_count, 195)

        # Получаем информацию о предметах
        equipped_items, total_items = get_user_items_count(user_id)

        # Создаем клавиатуру
        buttons = [
            [InlineKeyboardButton(text="🎮 Видеокарты", callback_data=f"shop_cards_{user_id}")],
            [InlineKeyboardButton(text="📦 Расширения", callback_data=f"shop_capacity_{user_id}")],
            [InlineKeyboardButton(text="🔧 Улучшения", callback_data=f"shop_upgrades_{user_id}")],
            [InlineKeyboardButton(text="🎁 Предметы", callback_data=f"shop_items_{user_id}")]
        ]

        # Добавляем кнопку бизнесов только если уровень фермы >= 12
        if current_expansion >= 12:
            buttons.append([InlineKeyboardButton(text="🏢 Бизнесы", callback_data=f"shop_business_{user_id}_1")])

        markup = InlineKeyboardMarkup(inline_keyboard=buttons)

        text = (
            f"🛒 <b>Магазин</b> | Ур. {current_expansion}\n"
            f"📦 Слоты: {EXPANSIONS[current_expansion-1]['max_cards']}/{max_slots}\n"
            f"🔢 Вайпы: {wipe_count}/{MAX_WIPES}\n"
            f"🎁 Предметы: {equipped_items}/{MAX_EQUIPPED_ITEMS} надето, {total_items}/{MAX_TOTAL_ITEMS} всего\n"
            "Выберите категорию:"
        )

        # Редактируем сообщение в зависимости от типа
        logger.info(f"shop_back_handler: photo={bool(callback.message.photo)}, text={bool(callback.message.text)}, caption={bool(callback.message.caption)}")

        if callback.message.photo:
            # Сообщение с фото - редактируем caption
            await callback.message.edit_caption(caption=text, reply_markup=markup, parse_mode='HTML')
        elif callback.message.text:
            # Обычное текстовое сообщение - редактируем текст
            await callback.message.edit_text(text=text, reply_markup=markup, parse_mode='HTML')
        else:
            # Не поняли тип сообщения, просто логируем
            logger.error(f"Unknown message type in shop_back_handler")

        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in shop back handler: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)
    
@dp.message(Command("upgrades"))
async def upgrades_menu(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    # Получаем текущие уровни улучшений
    cursor.execute('SELECT wiring, ventilation, traffic, software, cooling FROM user_upgrades WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    
    if not result:
        wiring = ventilation = traffic = software = cooling = 0
    else:
        wiring, ventilation, traffic, software, cooling = result
    
    # Цены за уровни
    PRICES = [10000, 100000, 3000000, 10000000, 25000000]
    
    text = "🔧 <b>Улучшения фермы</b>\n\n"
    text += "Каждое улучшение дает +1% к доходу вашей фермы!\n\n"
    
    text += f"🔌 <b>Проводка</b>: +{wiring}%\n"
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[wiring] if wiring < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_wiring\n\n" if wiring < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"🌀 <b>Вентиляция</b>: +{ventilation}%\n"
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[ventilation] if ventilation < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_ventilation\n\n" if ventilation < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"📶 <b>Трафик</b>: +{traffic}%\n"
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[traffic] if traffic < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_traffic\n\n" if traffic < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"💾 <b>Программное обеспечение</b>: +{software}%\n"
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[software] if software < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_software\n\n" if software < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    text += f"❄️ <b>Охлаждение</b>: +{cooling}%\n"
    text += f"💵 Стоимость улучшения - ${format_number(PRICES[cooling] if cooling < 5 else 0, True)}\n"
    text += f"🆙 Улучшить - /up_cooling\n\n" if cooling < 5 else "✅ Максимальный уровень достигнут!\n\n"
    
    total_bonus = wiring + ventilation + traffic + software + cooling
    text += f"🔰 <b>Общий бонус к доходу</b>: +{total_bonus}%\n"
    
    await message.answer(text, parse_mode='HTML')
    
@dp.message(Command("up_wiring"))
async def upgrade_wiring(message: Message):
    user_id = message.from_user.id
    success, result = buy_upgrade(user_id, "wiring")
    
    if success:
        # Проверяем получение ивент валюты (100% шанс) только при успешной покупке
        event_currency_received = try_give_100percent(user_id, 1)
        
        response_text = f"✅ {result}"
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    else:
        response_text = f"❌ {result}"
    
    await message.answer(response_text)

@dp.message(Command("up_ventilation"))
async def upgrade_ventilation(message: Message):
    user_id = message.from_user.id
    success, result = buy_upgrade(user_id, "ventilation")
    
    if success:
        # Проверяем получение ивент валюты (100% шанс) только при успешной покупке
        event_currency_received = try_give_100percent(user_id, 1)
        
        response_text = f"✅ {result}"
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    else:
        response_text = f"❌ {result}"
    
    await message.answer(response_text)

@dp.message(Command("up_traffic"))
async def upgrade_traffic(message: Message):
    user_id = message.from_user.id
    success, result = buy_upgrade(user_id, "traffic")
    
    if success:
        # Проверяем получение ивент валюты (100% шанс) только при успешной покупке
        event_currency_received = try_give_100percent(user_id, 1)
        
        response_text = f"✅ {result}"
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    else:
        response_text = f"❌ {result}"
    
    await message.answer(response_text)

@dp.message(Command("up_software"))
async def upgrade_software(message: Message):
    user_id = message.from_user.id
    success, result = buy_upgrade(user_id, "software")
    
    if success:
        # Проверяем получение ивент валюты (100% шанс) только при успешной покупке
        event_currency_received = try_give_100percent(user_id, 1)
        
        response_text = f"✅ {result}"
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    else:
        response_text = f"❌ {result}"
    
    await message.answer(response_text)

@dp.message(Command("up_cooling"))
async def upgrade_cooling(message: Message):
    user_id = message.from_user.id
    success, result = buy_upgrade(user_id, "cooling")
    
    if success:
        # Проверяем получение ивент валюты (100% шанс) только при успешной покупке
        event_currency_received = try_give_100percent(user_id, 1)
        
        response_text = f"✅ {result}"
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    else:
        response_text = f"❌ {result}"
    
    await message.answer(response_text)    
    
# Добавьте эти константы в раздел с другими константами (после API_TOKEN)
CHANNEL_ID = -1002780167646  # ID канала
CHAT_ID = -1002752285369     # ID чата

# Добавьте эту таблицу в раздел CREATE TABLE (после других CREATE TABLE)
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_social_bonus (
    user_id INTEGER PRIMARY KEY,
    channel_subscribed BOOLEAN DEFAULT FALSE,
    chat_subscribed BOOLEAN DEFAULT FALSE,
    bio_checked BOOLEAN DEFAULT FALSE,
    last_check TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()


# Функция для проверки подписки пользователя
async def check_user_subscription(user_id: int, target_id: int) -> bool:
    """Проверяет, подписан ли пользователь на канал/чат"""
    try:
        member = await bot.get_chat_member(chat_id=target_id, user_id=user_id)
        # Более надежная проверка статуса
        return member.status in ['member', 'administrator', 'creator', 'restricted']
    except Exception as e:
        logger.error(f"Error checking subscription for user {user_id} on {target_id}: {e}")
        return False

# Функция для проверки био пользователя
async def check_user_bio(user_id: int) -> bool:
    """Проверяет, есть ли тег бота в био пользователя"""
    try:
        user = await bot.get_chat(user_id)
        bio = user.bio or ""
        
        # Ищем тег в разных вариантах написания
        target_username = "CryptoMiner_sBot"
        variations = [
            target_username,
            target_username.lower(),
            target_username.upper(),
            f"@{target_username}",
            f"@{target_username.lower()}",
            f"@{target_username.upper()}"
        ]
        
        # Проверяем все варианты
        for variation in variations:
            if variation in bio:
                logger.info(f"Bio tag found for user {user_id}: {variation}")
                return True
        
        logger.info(f"Bio tag NOT found for user {user_id}. Bio: {bio}")
        return False
        
    except Exception as e:
        logger.error(f"Error checking bio for user {user_id}: {e}")
        return False

# Функция для обновления всех бонусов
async def update_all_bonuses(user_id: int):
    """Обновляет статус подписок, био и пересчитывает доход"""
    try:
        # Проверяем подписки с логированием
        channel_subscribed = await check_user_subscription(user_id, CHANNEL_ID)
        chat_subscribed = await check_user_subscription(user_id, CHAT_ID)
        bio_checked = await check_user_bio(user_id)
        
        logger.info(f"Bonus check for user {user_id}: channel={channel_subscribed}, chat={chat_subscribed}, bio={bio_checked}")
        
        # Получаем текущий статус
        cursor.execute('''
        SELECT channel_subscribed, chat_subscribed, bio_checked 
        FROM user_social_bonus 
        WHERE user_id = ?
        ''', (user_id,))
        current_status = cursor.fetchone()
        
        # Логируем изменения
        if current_status:
            logger.info(f"Previous status: channel={current_status[0]}, chat={current_status[1]}, bio={current_status[2]}")
        
        # Если статус изменился, обновляем и пересчитываем доход
        if (not current_status or 
            current_status[0] != channel_subscribed or 
            current_status[1] != chat_subscribed or
            current_status[2] != bio_checked):
            
            logger.info(f"Status changed for user {user_id}, updating database...")
            
            cursor.execute('''
            INSERT OR REPLACE INTO user_social_bonus 
            (user_id, channel_subscribed, chat_subscribed, bio_checked, last_check)
            VALUES (?, ?, ?, ?, ?)
            ''', (user_id, channel_subscribed, chat_subscribed, bio_checked, datetime.now().isoformat()))
            conn.commit()
            
            # Пересчитываем доход
            calculate_income(user_id)
            logger.info(f"Income recalculated for user {user_id}")
            
        return channel_subscribed, chat_subscribed, bio_checked
        
    except Exception as e:
        logger.error(f"Error updating bonuses for user {user_id}: {e}")
        return False, False, False

# Функция для получения текущего бонуса
def get_social_bonus(user_id: int) -> float:
    """Возвращает текущий бонус от подписок и био в процентах"""
    try:
        cursor.execute('''
        SELECT channel_subscribed, chat_subscribed, bio_checked 
        FROM user_social_bonus 
        WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        
        if not result:
            return 0.0
            
        channel_bonus = 0.05 if result[0] else 0.0    # +5% за канал
        chat_bonus = 0.05 if result[1] else 0.0       # +5% за чат
        bio_bonus = 0.05 if result[2] else 0.0        # +5% за био
        
        return channel_bonus + chat_bonus + bio_bonus
        
    except Exception as e:
        logger.error(f"Error getting social bonus: {e}")
        return 0.0

# =============================================================================
# КОМАНДЫ И ОБРАБОТЧИКИ
# =============================================================================

@dp.message(Command("bonus"))
async def bonus_command(message: Message):
    """Показывает статус подписок и бонусы"""
    user_id = message.from_user.id
    
    # Создаем клавиатуру
    builder = InlineKeyboardBuilder()
    
    # Кнопки для проверки
    builder.row(
        InlineKeyboardButton(
            text="📢 Проверить канал",
            callback_data=f"check_channel_{user_id}"
        ),
        InlineKeyboardButton(
            text="💬 Проверить чат", 
            callback_data=f"check_chat_{user_id}"
        )
    )
    
    builder.row(
        InlineKeyboardButton(
            text="👤 Проверить био",
            callback_data=f"check_bio_{user_id}"
        )
    )
    
    builder.row(
        InlineKeyboardButton(
            text="🔄 Обновить всё",
            callback_data=f"refresh_bonus_{user_id}"
        )
    )
    
    # Получаем текущий статус с принудительной проверкой
    channel_sub, chat_sub, bio_checked = await update_all_bonuses(user_id)
    total_bonus = get_social_bonus(user_id) * 100
    
    # Добавляем отладочную информацию (только для админа)
    debug_info = ""
    if message.from_user.id in ADMINS:  # Только для админа
        debug_info = f"\n🔧 [DEBUG] UserID: {user_id}, Channel: {channel_sub}, Chat: {chat_sub}, Bio: {bio_checked}"
    
    # Формируем красивый текст
    text = (
        "🌟 <b>Бонусная система</b>\n\n"
        
        "📊 <b>Статус бонусов:</b>\n"
        f"   📢 Канал: {'✅ Подписан (+5%)' if channel_sub else '❌ Не подписан'}\n"
        f"   💬 Чат: {'✅ Подписан (+5%)' if chat_sub else '❌ Не подписан'}\n"
        f"   👤 Био: {'✅ Тег добавлен (+5%)' if bio_checked else '❌ Тег не добавлен'}\n\n"
        
        "💰 <b>Ваш общий бонус:</b>\n"
        f"   🎯 +{total_bonus:.1f}% к заработку\n\n"
        
        "📈 <b>Как увеличить доход:</b>\n"
        "   • Подпишись на <a href='https://t.me/CryptoMiner_News'>канал</a> (+5%)\n"
        "   • Присоединяйся к <a href='https://t.me/+hu4plD6ELAU1ZGNi'>чату</a> (+5%)\n"
        "   • Добавь в био тег <code>@CryptoMiner_sBot</code> (+5%)\n\n"
        
        "💡 <b>Инструкция по добавлению тега в био:</b>\n"
        "1. Зайди в 'Настройки' Telegram\n"
        "2. Выбери 'Изменить профиль'\n"
        "3. В поле 'Био' добавь <code>@CryptoMiner_sBot</code>\n"
        "4. Сделай био открытым для всех\n"
        "5. Нажми 'Проверить био' здесь\n\n"
        
        f"{debug_info}"
        
        "🔍 Нажми кнопки ниже чтобы проверить статус"
    )
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML', disable_web_page_preview=True)

# Обработчики callback'ов
@dp.callback_query(F.data.startswith("check_channel_"))
async def check_channel_handler(callback: CallbackQuery):
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    # Принудительно проверяем подписку
    is_subscribed = await check_user_subscription(user_id, CHANNEL_ID)
    
    # Немедленно обновляем статус в базе
    cursor.execute('''
    INSERT OR REPLACE INTO user_social_bonus 
    (user_id, channel_subscribed, chat_subscribed, bio_checked, last_check)
    VALUES (?, ?, COALESCE((SELECT chat_subscribed FROM user_social_bonus WHERE user_id = ?), FALSE), 
            COALESCE((SELECT bio_checked FROM user_social_bonus WHERE user_id = ?), FALSE), ?)
    ''', (user_id, is_subscribed, user_id, user_id, datetime.now().isoformat()))
    conn.commit()
    
    # Пересчитываем доход
    calculate_income(user_id)
    
    if is_subscribed:
        await callback.answer("✅ Вы подписаны на канал! +5% к доходу", show_alert=True)
    else:
        await callback.answer("❌ Вы не подписаны на канал!\n\nПодпишись: @CryptoMiner_News", show_alert=True)

@dp.callback_query(F.data.startswith("check_chat_"))
async def check_chat_handler(callback: CallbackQuery):
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    # Принудительно проверяем подписку
    is_subscribed = await check_user_subscription(user_id, CHAT_ID)
    
    # Немедленно обновляем статус в базе
    cursor.execute('''
    INSERT OR REPLACE INTO user_social_bonus 
    (user_id, channel_subscribed, chat_subscribed, bio_checked, last_check)
    VALUES (?, COALESCE((SELECT channel_subscribed FROM user_social_bonus WHERE user_id = ?), FALSE), 
            ?, COALESCE((SELECT bio_checked FROM user_social_bonus WHERE user_id = ?), FALSE), ?)
    ''', (user_id, user_id, is_subscribed, user_id, datetime.now().isoformat()))
    conn.commit()
    
    # Пересчитываем доход
    calculate_income(user_id)
    
    if is_subscribed:
        await callback.answer("✅ Вы подписаны на чат! +5% к доходу", show_alert=True)
    else:
        await callback.answer("❌ Вы не подписаны на чат!\n\nПрисоединяйся: https://t.me/+hu4plD6ELAU1ZGNi", show_alert=True)

@dp.callback_query(F.data.startswith("check_bio_"))
async def check_bio_handler(callback: CallbackQuery):
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    # Принудительно проверяем био
    has_bio_tag = await check_user_bio(user_id)
    
    # Немедленно обновляем статус в базе
    cursor.execute('''
    INSERT OR REPLACE INTO user_social_bonus 
    (user_id, channel_subscribed, chat_subscribed, bio_checked, last_check)
    VALUES (?, COALESCE((SELECT channel_subscribed FROM user_social_bonus WHERE user_id = ?), FALSE), 
            COALESCE((SELECT chat_subscribed FROM user_social_bonus WHERE user_id = ?), FALSE), ?, ?)
    ''', (user_id, user_id, user_id, has_bio_tag, datetime.now().isoformat()))
    conn.commit()
    
    # Пересчитываем доход
    calculate_income(user_id)
    
    if has_bio_tag:
        await callback.answer("✅ Тег найден в био! +5% к доходу", show_alert=True)
    else:
        await callback.answer(
            "❌ Тег не найден в био!\n\n"
            "Добавь в био: @CryptoMiner_sBot\n"
            "И сделай био открытым для всех!",
            show_alert=True
        )

@dp.callback_query(F.data.startswith("refresh_bonus_"))
async def refresh_bonus_handler(callback: CallbackQuery):
    await callback.answer()
    user_id = int(callback.data.split('_')[2])

    if callback.from_user.id != user_id:
        return
    
    # Обновляем статус
    channel_sub, chat_sub, bio_checked = await update_all_bonuses(user_id)
    total_bonus = get_social_bonus(user_id) * 100
    
    # Обновляем сообщение
    text = (
        "🌟 <b>Бонусная система</b>\n\n"
        
        "📊 <b>Статус бонусов:</b>\n"
        f"   📢 Канал: {'✅ Подписан (+5%)' if channel_sub else '❌ Не подписан'}\n"
        f"   💬 Чат: {'✅ Подписан (+5%)' if chat_sub else '❌ Не подписан'}\n"
        f"   👤 Био: {'✅ Тег добавлен (+5%)' if bio_checked else '❌ Тег не добавлен'}\n\n"
        
        "💰 <b>Ваш общий бонус:</b>\n"
        f"   🎯 +{total_bonus:.1f}% к заработку\n\n"
        
        "📈 <b>Как увеличить доход:</b>\n"
        "   • Подпишись на <a href='https://t.me/CryptoMiner_News'>канал</a> (+5%)\n"
        "   • Присоединяйся к <a href='https://t.me/+hu4plD6ELAU1ZGNi'>чату</a> (+5%)\n"
        "   • Добавь в био тег <code>@CryptoMiner_sBot</code> (+5%)\n\n"
        
        "💡 <b>Инструкция по добавлению тега в био:</b>\n"
        "1. Зайди в 'Настройки' Telegram\n"
        "2. Выбери 'Изменить профиль'\n"
        "3. В поле 'Био' добавь <code>@CryptoMiner_sBot</code>\n"
        "4. Сделай био открытым для всех\n"
        "5. Нажми 'Проверить био' здесь\n\n"
        
        "✅ <b>Статус обновлен!</b>"
    )
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text="📢 Проверить канал",
            callback_data=f"check_channel_{user_id}"
        ),
        InlineKeyboardButton(
            text="💬 Проверить чат", 
            callback_data=f"check_chat_{user_id}"
        )
    )
    
    builder.row(
        InlineKeyboardButton(
            text="👤 Проверить био",
            callback_data=f"check_bio_{user_id}"
        )
    )
    
    builder.row(
        InlineKeyboardButton(
            text="🔄 Обновить всё",
            callback_data=f"refresh_bonus_{user_id}"
        )
    )
    
    try:
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML', disable_web_page_preview=True)
    except:
        pass
    
    await callback.answer("✅ Статус обновлен!")

# =============================================================================
# ФОНОВАЯ ЗАДАЧА ДЛЯ ПРОВЕРКИ
# =============================================================================

async def check_all_social_bonuses():
    """Проверяет подписки пользователей, которым ранее был выдан бонус"""
    while True:
        try:
            logger.info("Starting social bonus check...")
            
            # Получаем только пользователей, у которых есть запись о бонусах
            cursor.execute('''
            SELECT user_id FROM user_social_bonus 
            WHERE channel_subscribed = TRUE OR chat_subscribed = TRUE OR bio_checked = TRUE
            ''')
            users_with_bonus = cursor.fetchall()
            
            # Проверяем только первых 10 пользователей (чтобы не перегружать API)
            users_to_check = users_with_bonus[:10]
            
            for (user_id,) in users_to_check:
                try:
                    await update_all_bonuses(user_id)
                    await asyncio.sleep(0.1)  # Небольшая задержка между запросами
                except Exception as e:
                    logger.error(f"Error checking bonuses for user {user_id}: {e}")
                    continue
                    
            logger.info(f"Social bonus check completed for {len(users_to_check)} users")
            
        except Exception as e:
            logger.error(f"Error in social bonus check: {e}")
        
        # Ждем 10 минут до следующей проверки
        await asyncio.sleep(600)

# Задача для автоматической проверки бонусов
async def start_social_bonus_checker():
    """Запускает фоновую задачу проверки бонусов"""
    asyncio.create_task(check_all_social_bonuses())

    
                        
# Добавить после других CREATE TABLE
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    item_id INTEGER,
    is_equipped BOOLEAN DEFAULT FALSE,
    created_at TEXT,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY,
    name TEXT,
    category TEXT,
    price INTEGER,
    farm_bonus REAL,
    business_bonus REAL,
    rarity TEXT
)
''')
conn.commit()

# Предметы
ITEMS = [
    # Новичок
    {"id": 1, "name": "Термопрокладки", "category": "novice", "price": 10000000, "farm_bonus": 0.01, "business_bonus": 0.01, "rarity": "common"},
    {"id": 2, "name": "Термопаста", "category": "novice", "price": 15000000, "farm_bonus": 0.02, "business_bonus": 0.02, "rarity": "common"},
    {"id": 3, "name": "Корпус", "category": "novice", "price": 25000000, "farm_bonus": 0.03, "business_bonus": 0.03, "rarity": "uncommon"},
    {"id": 4, "name": "HDD диск", "category": "novice", "price": 40000000, "farm_bonus": 0.04, "business_bonus": 0.04, "rarity": "uncommon"},
    {"id": 5, "name": "Кулер", "category": "novice", "price": 50000000, "farm_bonus": 0.05, "business_bonus": 0.05, "rarity": "rare"},
    
    # Бывалый
    {"id": 6, "name": "Оперативная память", "category": "experienced", "price": 175000000, "farm_bonus": 0.06, "business_bonus": 0.06, "rarity": "common"},
    {"id": 7, "name": "Блок питания", "category": "experienced", "price": 200000000, "farm_bonus": 0.07, "business_bonus": 0.07, "rarity": "common"},
    {"id": 8, "name": "SSD диск", "category": "experienced", "price": 225000000, "farm_bonus": 0.08, "business_bonus": 0.08, "rarity": "uncommon"},
    {"id": 9, "name": "Вентилятор 80мм", "category": "experienced", "price": 250000000, "farm_bonus": 0.09, "business_bonus": 0.09, "rarity": "uncommon"},
    {"id": 10, "name": "WiFi модуль", "category": "experienced", "price": 300000000, "farm_bonus": 0.10, "business_bonus": 0.10, "rarity": "rare"},
    
    # Старпёр
    {"id": 11, "name": "Материнская плата", "category": "veteran", "price": 1200000000, "farm_bonus": 0.11, "business_bonus": 0.11, "rarity": "common"},
    {"id": 12, "name": "Видеокарта", "category": "veteran", "price": 1400000000, "farm_bonus": 0.12, "business_bonus": 0.12, "rarity": "common"},
    {"id": 13, "name": "Вентилятор 120мм", "category": "veteran", "price": 1600000000, "farm_bonus": 0.13, "business_bonus": 0.13, "rarity": "uncommon"},
    {"id": 14, "name": "NVME M.2 диск", "category": "veteran", "price": 1800000000, "farm_bonus": 0.14, "business_bonus": 0.14, "rarity": "uncommon"},
    {"id": 15, "name": "Собранный ПК", "category": "veteran", "price": 3000000000, "farm_bonus": 0.15, "business_bonus": 0.15, "rarity": "rare"}
]

# Шансы для кейсов
CASE_DROP_RATES = {
    "novice": [
        {"item_id": 1, "chance": 70},
        {"item_id": 2, "chance": 20},
        {"item_id": 3, "chance": 6},
        {"item_id": 4, "chance": 3},
        {"item_id": 5, "chance": 1}
    ],
    "experienced": [
        {"item_id": 6, "chance": 70},
        {"item_id": 7, "chance": 20},
        {"item_id": 8, "chance": 6},
        {"item_id": 9, "chance": 3},
        {"item_id": 10, "chance": 1}
    ],
    "veteran": [
        {"item_id": 11, "chance": 70},
        {"item_id": 12, "chance": 20},
        {"item_id": 13, "chance": 6},
        {"item_id": 14, "chance": 3},
        {"item_id": 15, "chance": 1}
    ]
}

CASE_PRICES = {
    "novice": 30000000,
    "experienced": 200000000,
    "veteran": 2000000000
}

# Лимиты предметов
MAX_EQUIPPED_ITEMS = 3
MAX_TOTAL_ITEMS = 10

def get_user_items(user_id: int) -> List[Tuple]:
    """Получает все предметы пользователя"""
    try:
        cursor.execute('''
        SELECT ui.id, ui.item_id, ui.is_equipped, i.name, i.farm_bonus, i.business_bonus
        FROM user_items ui
        JOIN items i ON ui.item_id = i.id
        WHERE ui.user_id = ?
        ORDER BY ui.is_equipped DESC, ui.id DESC
        ''', (user_id,))
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error getting user items: {e}")
        return []

def get_user_items_count(user_id: int) -> Tuple[int, int]:
    """Получает количество надетых и общих предметов пользователя"""
    try:
        cursor.execute('SELECT COUNT(*) FROM user_items WHERE user_id = ? AND is_equipped = TRUE', (user_id,))
        equipped_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_items WHERE user_id = ?', (user_id,))
        total_count = cursor.fetchone()[0]
        
        return equipped_count, total_count
    except Exception as e:
        logger.error(f"Error getting user items count: {e}")
        return 0, 0

def add_user_item(user_id: int, item_id: int) -> bool:
    """Добавляет предмет пользователю"""
    try:
        if get_total_items_count(user_id) >= MAX_TOTAL_ITEMS:
            return False
        
        cursor.execute('''
        INSERT INTO user_items (user_id, item_id, created_at)
        VALUES (?, ?, ?)
        ''', (user_id, item_id, datetime.now().isoformat()))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding user item: {e}")
        conn.rollback()
        return False

def get_total_items_count(user_id: int) -> int:
    """Получает общее количество предметов"""
    try:
        cursor.execute('SELECT COUNT(*) FROM user_items WHERE user_id = ?', (user_id,))
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting total items count: {e}")
        return 0

def get_equipped_items_count(user_id: int) -> int:
    """Получает количество надетых предметов"""
    try:
        cursor.execute('SELECT COUNT(*) FROM user_items WHERE user_id = ? AND is_equipped = TRUE', (user_id,))
        return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"Error getting equipped items count: {e}")
        return 0

def equip_item(user_id: int, item_id: int) -> bool:
    """Надевает предмет"""
    try:
        # Проверяем лимит надетых предметов
        if get_equipped_items_count(user_id) >= MAX_EQUIPPED_ITEMS:
            return False
        
        cursor.execute('''
        UPDATE user_items 
        SET is_equipped = TRUE 
        WHERE id = ? AND user_id = ?
        ''', (item_id, user_id))
        conn.commit()
        
        # Пересчитываем доход
        calculate_income(user_id)
        return True
    except Exception as e:
        logger.error(f"Error equipping item: {e}")
        conn.rollback()
        return False

def unequip_item(user_id: int, item_id: int) -> bool:
    """Снимает предмет"""
    try:
        cursor.execute('''
        UPDATE user_items 
        SET is_equipped = FALSE 
        WHERE id = ? AND user_id = ?
        ''', (item_id, user_id))
        conn.commit()
        
        # Пересчитываем доход
        calculate_income(user_id)
        return True
    except Exception as e:
        logger.error(f"Error unequipping item: {e}")
        conn.rollback()
        return False

def sell_item(user_id: int, item_id: int) -> Tuple[bool, str]:
    """Продает предмет"""
    try:
        # Получаем информацию о предмете
        cursor.execute('''
        SELECT ui.id, i.name, i.price
        FROM user_items ui
        JOIN items i ON ui.item_id = i.id
        WHERE ui.id = ? AND ui.user_id = ?
        ''', (item_id, user_id))
        
        item_data = cursor.fetchone()
        if not item_data:
            return False, "Предмет не найден"
        
        item_name = item_data[1]
        sell_price = int(item_data[2] * 0.3)  # 30% от цены
        
        # Удаляем предмет
        cursor.execute('DELETE FROM user_items WHERE id = ? AND user_id = ?', (item_id, user_id))
        
        # Начисляем деньги
        user = get_user(user_id)
        new_usd = user[2] + sell_price
        update_balance(user_id, usd=new_usd)
        
        conn.commit()
        
        # Пересчитываем доход
        calculate_income(user_id)
        
        return True, f"Вы продали {item_name} за ${format_number(sell_price, True)}"
    except Exception as e:
        logger.error(f"Error selling item: {e}")
        conn.rollback()
        return False, "Ошибка при продаже предмета"

def calculate_items_bonus(user_id: int) -> Tuple[float, float]:
    """Рассчитывает общий бонус от надетых предметов"""
    try:
        cursor.execute('''
        SELECT SUM(i.farm_bonus), SUM(i.business_bonus)
        FROM user_items ui
        JOIN items i ON ui.item_id = i.id
        WHERE ui.user_id = ? AND ui.is_equipped = TRUE
        ''', (user_id,))
        
        result = cursor.fetchone()
        farm_bonus = result[0] if result[0] else 0.0
        business_bonus = result[1] if result[1] else 0.0
        
        return farm_bonus, business_bonus
    except Exception as e:
        logger.error(f"Error calculating items bonus: {e}")
        return 0.0, 0.0

def buy_item(user_id: int, item_type: str) -> Tuple[bool, str]:
    """Покупает предмет"""
    try:
        # Находим предмет по команде
        command_to_id = {
            "terp": 1, "term": 2, "cor": 3, "hdd": 4, "cyl": 5,
            "oppam": 6, "bp": 7, "ssd": 8, "vent": 9, "wifi": 10,
            "matpl": 11, "videokar": 12, "ventl": 13, "nvme": 14, "sobpk": 15
        }
        
        if item_type not in command_to_id:
            return False, "Предмет не найден"
        
        item_id = command_to_id[item_type]
        item = next((i for i in ITEMS if i["id"] == item_id), None)
        
        if not item:
            return False, "Предмет не найден"
        
        # Проверяем лимит предметов
        if get_total_items_count(user_id) >= MAX_TOTAL_ITEMS:
            return False, f"Достигнут лимит предметов ({MAX_TOTAL_ITEMS}). Продайте некоторые предметы."
        
        # Проверяем баланс
        user = get_user(user_id)
        if user[2] < item["price"]:
            return False, f"Недостаточно средств! Цена: ${format_number(item['price'], True)}"
        
        # Списываем деньги
        new_usd = user[2] - item["price"]
        update_balance(user_id, usd=new_usd)
        
        # Добавляем предмет
        if not add_user_item(user_id, item["id"]):
            # Возвращаем деньги если не удалось добавить предмет
            update_balance(user_id, usd=user[2])
            return False, "Не удалось добавить предмет (достигнут лимит)"
        
        return True, f"Вы купили {item['name']} за ${format_number(item['price'], True)}"
    except Exception as e:
        logger.error(f"Error buying item: {e}")
        return False, "Ошибка при покупке предмета"

def open_case(user_id: int, case_type: str) -> Tuple[bool, str]:
    """Открывает кейс и выдает случайный предмет"""
    try:
        # Проверяем существование кейса
        if case_type not in CASE_PRICES:
            return False, "Такого кейса не существует"
        
        price = CASE_PRICES[case_type]
        
        # Проверяем баланс
        user = get_user(user_id)
        if user[2] < price:
            return False, f"Недостаточно средств! Цена: ${format_number(price, True)}"
        
        # Проверяем лимит предметов
        if get_total_items_count(user_id) >= MAX_TOTAL_ITEMS:
            return False, f"Достигнут лимит предметов ({MAX_TOTAL_ITEMS}). Продайте некоторые предметы."
        
        # Списываем деньги
        new_usd = user[2] - price
        update_balance(user_id, usd=new_usd)
        
        # Выбираем случайный предмет по шансам
        drop_rates = CASE_DROP_RATES[case_type]
        random_value = random.random() * 100
        cumulative_chance = 0
        
        selected_item = None
        for drop in drop_rates:
            cumulative_chance += drop["chance"]
            if random_value <= cumulative_chance:
                selected_item = next((i for i in ITEMS if i["id"] == drop["item_id"]), None)
                break
        
        if not selected_item:
            selected_item = next((i for i in ITEMS if i["id"] == drop_rates[0]["item_id"]), None)
        
        # Добавляем предмет
        if not add_user_item(user_id, selected_item["id"]):
            # Возвращаем деньги если не удалось добавить предмет
            update_balance(user_id, usd=user[2])
            return False, "Не удалось добавить предмет (достигнут лимит)"
        
        return True, f"Поздравляем! Вы успешно выбили предмет {selected_item['name']}\nБонусы выбитого предмета можно посмотреть в - /inventory"
    except Exception as e:
        logger.error(f"Error opening case: {e}")
        return False, "Ошибка при открытии кейса"

def initialize_items():
    """Добавляет предметы в базу данных если их нет"""
    try:
        for item in ITEMS:
            cursor.execute('''
            INSERT OR IGNORE INTO items (id, name, category, price, farm_bonus, business_bonus, rarity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (item["id"], item["name"], item["category"], item["price"], 
                  item["farm_bonus"], item["business_bonus"], item["rarity"]))
        conn.commit()
        logger.info("Items initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing items: {e}")
        
        
@dp.message(Command("inventory"))
async def inventory_command(message: Message):
    """Показывает инвентарь пользователя"""
    user_id = message.from_user.id
    
    user_items = get_user_items(user_id)
    
    if not user_items:
        await message.answer("🧰 Ваш инвентарь пуст!\n\nПосетите магазин (/shop) чтобы приобрести предметы.")
        return
    
    text = "🧰 Ваш инвентарь:\n\n"
    
    # Надетые предметы
    equipped_items = [item for item in user_items if item[2]]  # is_equipped
    unequipped_items = [item for item in user_items if not item[2]]
    
    if equipped_items:
        text += "<b>Надетые предметы:</b>\n"
        for item in equipped_items:
            item_id, item_db_id, is_equipped, name, farm_bonus, business_bonus = item
            text += f"🔹 {name}\n"
            text += f"   Бонус: +{farm_bonus*100:.0f}% к доходу фермы, +{business_bonus*100:.0f}% к доходу бизнесов\n"
            text += f"   Снять предмет - /snim_{item_id}\n"
            text += f"   Продать предмет - /pred_sell_{item_id}\n\n"
    
    if unequipped_items:
        text += "<b>Не надетые предметы:</b>\n"
        for item in unequipped_items:
            item_id, item_db_id, is_equipped, name, farm_bonus, business_bonus = item
            text += f"🔸 {name}\n"
            text += f"   Бонус: +{farm_bonus*100:.0f}% к доходу фермы, +{business_bonus*100:.0f}% к доходу бизнесов\n"
            text += f"   Надеть предмет - /nad_{item_id}\n"
            text += f"   Продать предмет - /pred_sell_{item_id}\n\n"
    
    # Статистика
    equipped_count = len(equipped_items)
    total_count = len(user_items)
    farm_bonus_total, business_bonus_total = calculate_items_bonus(user_id)
    
    text += f"<b>Статистика:</b>\n"
    text += f"🔸 Надето: {equipped_count}/{MAX_EQUIPPED_ITEMS}\n"
    text += f"🔸 Всего предметов: {total_count}/{MAX_TOTAL_ITEMS}\n"
    text += f"🔸 Общий бонус: +{farm_bonus_total*100:.0f}% к ферме, +{business_bonus_total*100:.0f}% к бизнесам\n"

    # Отправляем с баннером
    banner_path = os.path.join(BANNER_DIR, 'invent.png')
    try:
        from aiogram.types import FSInputFile
        photo = FSInputFile(banner_path)
        await message.answer_photo(
            photo=photo,
            caption=text,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error sending inventory banner: {e}")
        await message.answer(text, parse_mode='HTML')

@dp.message(F.text.regexp(r'^/snim_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def unequip_item_command(message: Message):
    """Команда для снятия предмета"""
    user_id = message.from_user.id
    try:
        # Извлекаем ID предмета (игнорируя @username если есть)
        command_text = message.text.split('@')[0]
        item_id = int(command_text.split('_')[1])
        
        # Проверяем, есть ли предмет у пользователя
        cursor.execute('''
        SELECT ui.id, i.name 
        FROM user_items ui
        JOIN items i ON ui.item_id = i.id
        WHERE ui.id = ? AND ui.user_id = ? AND ui.is_equipped = TRUE
        ''', (item_id, user_id))
        item_data = cursor.fetchone()
        
        if not item_data:
            await message.answer("❌ Предмет не найден или уже снят!")
            return
        
        success = unequip_item(user_id, item_id)
        if success:
            await message.answer(f"😔 Вы успешно сняли {item_data[1]}!")
        else:
            await message.answer("❌ Не удалось снять предмет.")
    except ValueError:
        await message.answer("❌ Неверный формат команды. Используйте: /snim_[id]")

@dp.message(F.text.regexp(r'^/nad_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def equip_item_command(message: Message):
    """Команда для надевания предмета"""
    user_id = message.from_user.id
    try:
        # Извлекаем ID предмета (игнорируя @username если есть)
        command_text = message.text.split('@')[0]
        item_id = int(command_text.split('_')[1])
        
        # Проверяем, есть ли предмет у пользователя
        cursor.execute('''
        SELECT ui.id, i.name 
        FROM user_items ui
        JOIN items i ON ui.item_id = i.id
        WHERE ui.id = ? AND ui.user_id = ? AND ui.is_equipped = FALSE
        ''', (item_id, user_id))
        item_data = cursor.fetchone()
        
        if not item_data:
            await message.answer("❌ Предмет не найден или уже надет!")
            return
        
        success = equip_item(user_id, item_id)
        if success:
            await message.answer(f"🔥 Вы успешно надели {item_data[1]}!")
        else:
            await message.answer("❌ Не удалось надеть предмет. Возможно, достигнут лимит надетых предметов (3).")
    except ValueError:
        await message.answer("❌ Неверный формат команды. Используйте: /nad_[id]")

@dp.message(F.text.regexp(r'^/pred_sell_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def sell_item_command(message: Message):
    """Команда для продажи предмета"""
    user_id = message.from_user.id
    try:
        # Извлекаем ID предмета (игнорируя @username если есть)
        command_text = message.text.split('@')[0]
        item_id = int(command_text.split('_')[2])  # pred_sell_[id]
        
        # Проверяем, есть ли предмет у пользователя
        cursor.execute('''
        SELECT ui.id, i.name, i.price 
        FROM user_items ui
        JOIN items i ON ui.item_id = i.id
        WHERE ui.id = ? AND ui.user_id = ?
        ''', (item_id, user_id))
        item_data = cursor.fetchone()
        
        if not item_data:
            await message.answer("❌ Предмет не найден!")
            return
        
        success, result = sell_item(user_id, item_id)
        if success:
            await message.answer(f"✅ {result}")
        else:
            await message.answer(f"❌ {result}")
    except (ValueError, IndexError):
        await message.answer("❌ Неверный формат команды. Используйте: /pred_sell_[id]")
        
def user_has_item(user_id: int, item_db_id: int) -> bool:
    """Проверяет, есть ли у пользователя предмет с указанным ID в user_items"""
    try:
        cursor.execute('SELECT id FROM user_items WHERE id = ? AND user_id = ?', (item_db_id, user_id))
        return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking user item: {e}")
        return False        

# Добавляем таблицу для ежедневных бонусов
cursor.execute('''
CREATE TABLE IF NOT EXISTS daily_bonuses (
    user_id INTEGER PRIMARY KEY,
    last_bonus_time TEXT,
    streak_count INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()

# Константы для ежедневного бонуса
DAILY_BONUS_COOLDOWN = 24 * 3600  # 24 часа в секундах
PREMIUM_BONUS_COOLDOWN = 12 * 3600  # 12 часов для премиум пользователей

# Функции для работы с ежедневным бонусом
def get_daily_bonus_info(user_id: int) -> Optional[Dict]:
    """Получает информацию о ежедневном бонусе пользователя"""
    try:
        cursor.execute('''
        SELECT last_bonus_time, streak_count 
        FROM daily_bonuses 
        WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        if result:
            return {
                "last_bonus_time": result[0],
                "streak_count": result[1]
            }
        return None
    except Exception as e:
        logger.error(f"Error getting daily bonus info: {e}")
        return None

def update_daily_bonus(user_id: int):
    """Обновляет время последнего бонуса и увеличивает счетчик серии"""
    try:
        now = datetime.now().isoformat()
        bonus_info = get_daily_bonus_info(user_id)
        
        if bonus_info:
            cursor.execute('''
            UPDATE daily_bonuses 
            SET last_bonus_time = ?, streak_count = streak_count + 1
            WHERE user_id = ?
            ''', (now, user_id))
        else:
            cursor.execute('''
            INSERT INTO daily_bonuses (user_id, last_bonus_time, streak_count)
            VALUES (?, ?, 1)
            ''', (user_id, now))
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error updating daily bonus: {e}")
        conn.rollback()

def can_claim_daily_bonus(user_id: int) -> Tuple[bool, Optional[timedelta]]:
    """Проверяет, может ли пользователь получить ежедневный бонус"""
    try:
        bonus_info = get_daily_bonus_info(user_id)
        
        if not bonus_info or not bonus_info["last_bonus_time"]:
            return True, None
        
        last_bonus_time = datetime.fromisoformat(bonus_info["last_bonus_time"])
        now = datetime.now()
        
        # Определяем время ожидания в зависимости от премиум статуса
        cooldown_seconds = PREMIUM_BONUS_COOLDOWN if is_premium(user_id) else DAILY_BONUS_COOLDOWN
        next_bonus_time = last_bonus_time + timedelta(seconds=cooldown_seconds)
        
        if now >= next_bonus_time:
            return True, None
        else:
            time_left = next_bonus_time - now
            return False, time_left
            
    except Exception as e:
        logger.error(f"Error checking daily bonus: {e}")
        return False, None

def generate_daily_bonus(user_id: int) -> Tuple[str, str]:
    """Генерирует случайный ежедневный бонус"""
    try:
        user = get_user(user_id)
        if not user:
            return "error", "Пользователь не найден"
        
        # Проверяем, можно ли получить бонус
        can_claim, time_left = can_claim_daily_bonus(user_id)
        if not can_claim:
            return "error", "Бонус еще не доступен"
        
        # Шансы выпадения различных бонусов
        bonus_type = random.choices(
            ['btc', 'card', 'premium', 'cleaner', 'investor'],
            weights=[30, 5, 1, 1, 1]  # 5% на сброс, по 1% на бустеры
        )[0]
        
        if bonus_type == 'btc':
            # BTC бонус: x1-x12 от текущего дохода
            multiplier = random.randint(1, 12)
            base_income = calculate_income(user_id)
            btc_amount = base_income * multiplier
            new_btc = user[3] + btc_amount
            update_balance(user_id, btc=new_btc, btc_delta=btc_amount)
            
            return 'btc', f"{format_number(btc_amount)} BTC (x{multiplier} от дохода)"
            
            
        elif bonus_type == 'card':
            # Видеокарта: последняя доступная из магазина
            expansion_info = EXPANSIONS[user[5] - 1]
            last_card_id = expansion_info["last_available_card"]
            last_card = next((card for card in GRAPHICS_CARDS if card["id"] == last_card_id), None)
            
            if not last_card:
                # Если карта не найдена, даем компенсацию
                compensation = 1000000  # 1M USD
                new_usd = user[2] + compensation
                update_balance(user_id, usd=new_usd)
                return 'usd', f"${format_number(compensation, True)} (компенсация за видеокарту)"
            
            # Проверяем свободные слоты
            user_cards, total_cards = get_user_cards(user_id)
            max_cards = expansion_info['max_cards']
            
            if total_cards < max_cards:
                # Есть место - даем карту
                add_user_card(user_id, last_card_id)
                calculate_income(user_id)
                return 'card', f"{last_card['name']}"
            else:
                # Нет места - начисляем полную стоимость
                compensation = last_card['price']
                new_usd = user[2] + compensation
                update_balance(user_id, usd=new_usd)
                return 'usd', f"${format_number(compensation, True)} за видеокарту {last_card['name']}"
            
        elif bonus_type == 'premium':
            # Премиум статус на 1-24 часа
            hours = random.randint(1, 24)
            premium_until = (datetime.now() + timedelta(hours=hours)).isoformat()
            
            cursor.execute('''
            INSERT OR REPLACE INTO premium_users (user_id, premium_until)
            VALUES (?, ?)
            ''', (user_id, premium_until))
            conn.commit()
            
            return 'premium', f"Премиум статус на {hours} часов"
            
        elif bonus_type == 'cleaner':
            # Бустер "Уборщик" на 1-24 часа
            hours = random.randint(1, 24)
            booster_until = (datetime.now() + timedelta(hours=hours)).isoformat()
            
            cursor.execute('''
            INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until, bonus)
            VALUES (?, 'cleaner', ?, 0.25)
            ''', (user_id, booster_until))
            conn.commit()
            
            return 'cleaner', f"бустер 'Уборщик' на {hours} часов"
            
        elif bonus_type == 'investor':
            # Бустер "Инвестор" на 1-24 часа
            hours = random.randint(1, 24)
            booster_until = (datetime.now() + timedelta(hours=hours)).isoformat()
            
            cursor.execute('''
            INSERT OR REPLACE INTO user_boosters (user_id, booster_type, until, bonus)
            VALUES (?, 'investor', ?, 0.15)
            ''', (user_id, booster_until))
            conn.commit()
            
            return 'investor', f"бустер 'Инвестор' на {hours} часов"
            
        conn.commit()
        return 'error', "Неизвестный тип бонуса"
        
    except Exception as e:
        logger.error(f"Error generating daily bonus: {e}")
        conn.rollback()
        return 'error', f"Ошибка: {str(e)}"

# Команда для ежедневного бонуса
@dp.message(Command("ebonus"))
async def daily_bonus_info(message: Message):
    """Показывает информацию о ежедневном бонусе"""
    user_id = message.from_user.id
    
    # Проверяем, зарегистрирован ли пользователь
    user = get_user(user_id)
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    can_claim, time_left = can_claim_daily_bonus(user_id)
    bonus_info = get_daily_bonus_info(user_id)
    streak_count = bonus_info["streak_count"] if bonus_info else 0
    
    text = "🎁 <b>Ежедневный бонус</b>\n\n"
    
    if can_claim:
        text += "✅ <b>Бонус доступен!</b>\n"
        text += f"📊 Серия: {streak_count} дней\n\n"
        text += "Нажмите кнопку ниже чтобы получить бонус!"
    else:
        hours = time_left.seconds // 3600
        minutes = (time_left.seconds % 3600) // 60
        seconds = time_left.seconds % 60
        
        text += "⏳ <b>Бонус еще не доступен</b>\n"
        text += f"📊 Серия: {streak_count} дней\n"
        text += f"⏰ Следующий бонус через: {hours:02d}:{minutes:02d}:{seconds:02d}\n\n"
    
    text += "\n<b>Возможные награды:</b>\n"
    text += "• 1-12x от дохода в BTC\n"
    text += "• Последняя доступная видеокарта\n"
    text += "• Мгновенное обновление бонуса (5%)\n"
    text += "• Премиум статус 1-24ч (1%)\n"
    text += "• Бустер 'Уборщик' 1-24ч (1%)\n"
    text += "• Бустер 'Инвестор' 1-24ч (1%)\n\n"
    
    if is_premium(user_id):
        text += "👑 <b>PREMIUM:</b> Бонус доступен каждые 12 часов!\n"
    else:
        text += "💡 <b>Обычный:</b> Бонус доступен каждые 24 часа\n"
    
    # Создаем кнопку для получения бонуса (только если доступен)
    builder = InlineKeyboardBuilder()
    if can_claim:
        builder.row(
            InlineKeyboardButton(
                text="🎁 Получить Ежедневный Бонус",
                callback_data=f"claim_daily_{user_id}"
            )
        )
    
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')

# Обработчик кнопки получения бонуса
@dp.callback_query(F.data.startswith("claim_daily_"))
async def claim_daily_bonus(callback: CallbackQuery):
    """Обрабатывает получение ежедневного бонуса"""
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[2])

        if callback.from_user.id != user_id:
            return

        # Проверяем, зарегистрирован ли пользователь
        user = get_user(user_id)
        if not user:
            return
        
        # Проверяем, можно ли получить бонус
        can_claim, time_left = can_claim_daily_bonus(user_id)
        if not can_claim:
            await callback.answer("❌ Бонус еще не доступен!", show_alert=True)
            return
        
        # Генерируем бонус
        bonus_type, bonus_description = generate_daily_bonus(user_id)
        
        if bonus_type == 'error':
            await callback.answer(f"❌ {bonus_description}", show_alert=True)
            return
        
        # Обновляем время бонуса
        update_daily_bonus(user_id)
        
        # Проверяем получение ивент валюты (5% шанс)
        event_currency_received = try_give_5percent(user_id, 1)
        
        # Формируем понятное сообщение о награде
        bonus_titles = {
            'btc': "💰 Биткоины",
            'card': "💳 Карта",
            'premium': "👑 Премиум",
            'cleaner': "🧹 Чистильщик", 
            'investor': "📈 Инвестор"
        }
        
        bonus_title = bonus_titles.get(bonus_type, "🎁 Бонус")
        
        # Создаем основное сообщение
        reward_message = f"🎉 <b>Вы получили бонус!</b>\n\n"
        reward_message += f"<b>{bonus_title}:</b> {bonus_description}\n"
        
        # Добавляем информацию о серии
        bonus_info = get_daily_bonus_info(user_id)
        streak_count = bonus_info["streak_count"] if bonus_info else 1
        
        reward_message += f"\n📊 <b>Текущая серия:</b> {streak_count} дней"
        
        # Добавляем информацию о времени до следующего бонуса
        if is_premium(user_id):
            reward_message += "\n⏰ <b>Следующий бонус через:</b> 12 часов"
        else:
            reward_message += "\n⏰ <b>Следующий бонус через:</b> 24 часа"
        
        # Добавляем информацию о полученной ивент валюте, если она была выдана
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            reward_message += f"\n\n🎉 <b>Дополнительная награда!</b>"
            reward_message += f"\n🎃 +1 ивент валюта"
            reward_message += f"\n💰 <b>Текущий баланс:</b> {current_balance} 🎃"
            reward_message += f"\n\n🏆 Смотрите рейтинг: /top_ivent"
        
        # Отправляем понятное сообщение с наградой
        await callback.message.edit_text(reward_message, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error claiming daily bonus: {e}")
        await callback.answer("❌ Произошла ошибка при получении бонуса", show_alert=True)

# Добавляем таблицу для отслеживания налогов
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_taxes (
    user_id INTEGER PRIMARY KEY,
    business_tax_debt REAL DEFAULT 0,
    farm_tax_debt REAL DEFAULT 0,
    last_tax_check TEXT,
    farm_penalty_applied BOOLEAN DEFAULT FALSE,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()

@dp.message(Command("bio_stats"))
async def bio_stats_command(message: Message):
    """Показывает статистику пользователей с тегом бота в био (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Получаем всех пользователей с тегом в био
        cursor.execute('''
        SELECT usb.user_id, u.username 
        FROM user_social_bonus usb
        LEFT JOIN users u ON usb.user_id = u.user_id
        WHERE usb.bio_checked = TRUE
        ORDER BY usb.last_check DESC
        ''')
        
        users_with_bio = cursor.fetchall()
        
        if not users_with_bio:
            await message.answer("📊 <b>Статистика по био</b>\n\n"
                               "❌ Никто еще не добавил тег бота в био")
            return
        
        text = f"📊 <b>Статистика по био</b>\n\n"
        text += f"👥 Всего пользователей с тегом: <b>{len(users_with_bio)}</b>\n\n"
        
        # Ограничиваем вывод до 50 пользователей чтобы не превысить лимит сообщения
        display_users = users_with_bio[:50]
        
        for i, (user_id, username) in enumerate(display_users, 1):
            try:
                # Получаем информацию о пользователе
                user_info = await bot.get_chat(user_id)
                user_name = user_info.full_name
                
                # Форматируем имя пользователя
                username_display = f"(@{username})" if username else ""
                
                text += f"{i}. <a href='tg://user?id={user_id}'>{user_name}</a> {username_display}\n"
                text += f"   🆔 ID: <code>{user_id}</code>\n"
                
                # Добавляем разделитель между пользователями
                if i < len(display_users):
                    text += "   ────────────────────\n"
                    
            except Exception as e:
                logger.error(f"Error getting user info for {user_id}: {e}")
                text += f"{i}. ID: <code>{user_id}</code> (ошибка получения данных)\n"
                text += "   ────────────────────\n"
                continue
        
        # Если пользователей больше 50, показываем информацию об этом
        if len(users_with_bio) > 50:
            text += f"\n... и еще {len(users_with_bio) - 50} пользователей"
        
        # Добавляем общую статистику
        cursor.execute('SELECT COUNT(*) FROM user_social_bonus')
        total_with_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_social_bonus WHERE channel_subscribed = TRUE')
        channel_subscribers = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_social_bonus WHERE chat_subscribed = TRUE')
        chat_subscribers = cursor.fetchone()[0]
        
        text += f"\n\n📈 <b>Общая статистика:</b>\n"
        text += f"👤 Всего в системе бонусов: {total_with_records}\n"
        text += f"📢 Подписаны на канал: {channel_subscribers}\n"
        text += f"💬 Подписаны на чат: {chat_subscribers}\n"
        text += f"👤 Добавили тег в био: {len(users_with_bio)}\n"
        
        # Процент пользователей с тегом в био
        if total_with_records > 0:
            bio_percentage = (len(users_with_bio) / total_with_records) * 100
            text += f"📊 Процент с тегом: {bio_percentage:.1f}%"
        
        await message.answer(text, parse_mode='HTML', disable_web_page_preview=True)
        
    except Exception as e:
        logger.error(f"Error in bio_stats command: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

# Константы для налогов
BUSINESS_TAX_RATE = 0.10  # 10% налог на бизнесы
FARM_TAX_RATE = 0.15      # 15% налог на ферму
MAX_BUSINESS_TAX = 200000000  # 100M максимальный налог бизнесов
MAX_FARM_TAX = 500000000      # 500M максимальный налог фермы
FARM_PENALTY_RATE = 0.30      # 30% штраф к доходу фермы
TAX_CHECK_INTERVAL = 1800     # 30 минут в секундах

def get_user_tax_info(user_id: int) -> Optional[Dict]:
    """Получает информацию о налогах пользователя"""
    try:
        cursor.execute('''
        SELECT business_tax_debt, farm_tax_debt, last_tax_check, farm_penalty_applied
        FROM user_taxes 
        WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        if result:
            return {
                "business_tax_debt": result[0],
                "farm_tax_debt": result[1],
                "last_tax_check": result[2],
                "farm_penalty_applied": bool(result[3])
            }
        return None
    except Exception as e:
        logger.error(f"Error getting user tax info: {e}")
        return None

def update_tax_info(user_id: int, business_tax: float = None, farm_tax: float = None, farm_penalty: bool = None):
    """Обновляет информацию о налогах пользователя"""
    try:
        tax_info = get_user_tax_info(user_id)
        now = datetime.now().isoformat()
        
        if tax_info:
            new_business_tax = business_tax if business_tax is not None else tax_info["business_tax_debt"]
            new_farm_tax = farm_tax if farm_tax is not None else tax_info["farm_tax_debt"]
            new_farm_penalty = farm_penalty if farm_penalty is not None else tax_info["farm_penalty_applied"]
            
            cursor.execute('''
            UPDATE user_taxes 
            SET business_tax_debt = ?, farm_tax_debt = ?, last_tax_check = ?, farm_penalty_applied = ?
            WHERE user_id = ?
            ''', (new_business_tax, new_farm_tax, now, new_farm_penalty, user_id))
        else:
            new_business_tax = business_tax or 0
            new_farm_tax = farm_tax or 0
            new_farm_penalty = farm_penalty or False
            
            cursor.execute('''
            INSERT INTO user_taxes (user_id, business_tax_debt, farm_tax_debt, last_tax_check, farm_penalty_applied)
            VALUES (?, ?, ?, ?, ?)
            ''', (user_id, new_business_tax, new_farm_tax, now, new_farm_penalty))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error updating tax info: {e}")
        conn.rollback()
        return False

def calculate_business_tax(user_id: int) -> float:
    """Рассчитывает налог на бизнесы (10% от дохода каждого бизнеса)"""
    try:
        # Получаем бизнесы пользователя и рассчитываем налог
        cursor.execute('''
        SELECT ub.business_id, ub.level 
        FROM user_businesses ub 
        WHERE ub.user_id = ?
        ''', (user_id,))
        
        user_businesses = cursor.fetchall()
        
        if not user_businesses:
            return 0
        
        total_tax = 0
        
        for business_id, level in user_businesses:
            # Находим бизнес в списке BUSINESSES
            business = next((b for b in BUSINESSES if b['id'] == business_id), None)
            if business:
                # Рассчитываем доход бизнеса и 10% налог
                business_income = business['base_income'] * level
                business_tax = business_income * BUSINESS_TAX_RATE
                total_tax += business_tax
        
        return total_tax
        
    except Exception as e:
        logger.error(f"Error calculating business tax: {e}")
        return 0

def calculate_farm_tax(user_id: int) -> float:
    """Рассчитывает налог на ферму (15% от дохода фермы в денежном эквиваленте)"""
    try:
        # Получаем базовый доход фермы (без бонусов)
        base_farm_income = calculate_base_income(user_id)
        
        # Получаем текущую цену BTC
        btc_price = get_btc_price()
        
        # Переводим доход фермы в денежный эквивалент и берем 15%
        farm_tax = base_farm_income * btc_price * FARM_TAX_RATE
        return farm_tax
    except Exception as e:
        logger.error(f"Error calculating farm tax: {e}")
        return 0

def apply_farm_penalty(user_id: int):
    """Применяет штраф к доходу фермы (30% снижение)"""
    try:
        cursor.execute('''
        UPDATE user_taxes 
        SET farm_penalty_applied = TRUE
        WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        
        # Пересчитываем доход с учетом штрафа
        calculate_income(user_id)
        return True
    except Exception as e:
        logger.error(f"Error applying farm penalty: {e}")
        conn.rollback()
        return False

def remove_farm_penalty(user_id: int):
    """Убирает штраф с дохода фермы"""
    try:
        cursor.execute('''
        UPDATE user_taxes 
        SET farm_penalty_applied = FALSE
        WHERE user_id = ?
        ''', (user_id,))
        conn.commit()
        
        # Пересчитываем доход без штрафа
        calculate_income(user_id)
        return True
    except Exception as e:
        logger.error(f"Error removing farm penalty: {e}")
        conn.rollback()
        return False

def calculate_income_with_penalty(user_id: int) -> float:
    """Рассчитывает доход фермы с учетом всех бонусов и штрафов"""
    # ДХ - Доход фермы (без каких-либо бонусов)
    base_income = calculate_base_income(user_id)

    # Начинаем с базового дохода
    current_income = base_income

    # П - Премиум бонус (+35%)
    if is_premium(user_id):
        current_income = current_income * 1.35

    # УБ - Уборщик фермы (бустеры)
    cursor.execute('''
    SELECT booster_type, bonus FROM user_boosters
    WHERE user_id = ? AND until > ?
    ''', (user_id, datetime.now().isoformat()))

    boosters = cursor.fetchall()
    boosters_total_bonus = 0.0
    for booster_type, bonus in boosters:
        boosters_total_bonus += bonus

    # Применяем бонусы бустеров
    current_income = current_income * (1 + boosters_total_bonus)

    # ПКЧБ - Подписка на канал, чат, тег в био
    subscription_bonus = get_social_bonus(user_id)
    current_income = current_income * (1 + subscription_bonus)

    # ПТ - Предметы
    farm_bonus, business_bonus = calculate_items_bonus(user_id)
    final_income_before_penalties = current_income * (1 + farm_bonus)

    # Применяем штрафы (они вычитаются)
    virus_penalty = calculate_virus_penalty(user_id)
    income_after_virus = final_income_before_penalties * (1 - virus_penalty)

    # Применяем налоговый штраф (30% снижение дохода)
    tax_info = get_user_tax_info(user_id)
    if tax_info and tax_info["farm_penalty_applied"]:
        final_income = income_after_virus * (1 - FARM_PENALTY_RATE)
    else:
        final_income = income_after_virus

    return final_income
    
@dp.message(Command("recalculate_all_income"))
async def recalculate_all_income_command(message: Message):
    """Пересчитать доход для всех пользователей (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Получаем всех пользователей
        cursor.execute('SELECT user_id FROM users')
        all_users = cursor.fetchall()
        
        total_users = len(all_users)
        processed = 0
        errors = 0
        
        status_msg = await message.answer(
            f"🔄 <b>Начинаю пересчет дохода для всех пользователей</b>\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"⏳ Обработано: 0/{total_users}\n"
            f"❌ Ошибок: 0",
            parse_mode='HTML'
        )
        
        for i, (user_id,) in enumerate(all_users, 1):
            try:
                # Пересчитываем доход для пользователя
                new_income = calculate_income(user_id)
                
                # Обновляем доход в базе
                cursor.execute(
                    'UPDATE users SET income_btc = ? WHERE user_id = ?',
                    (new_income, user_id)
                )
                
                processed += 1
                
                # Обновляем статус каждые 50 пользователей
                if i % 50 == 0:
                    progress = (i / total_users) * 100
                    await status_msg.edit_text(
                        f"🔄 <b>Пересчет дохода</b>\n\n"
                        f"👥 Всего пользователей: {total_users}\n"
                        f"📊 Прогресс: {i}/{total_users} ({progress:.1f}%)\n"
                        f"✅ Обработано: {processed}\n"
                        f"❌ Ошибок: {errors}",
                        parse_mode='HTML'
                    )
                
                # Небольшая задержка чтобы не перегружать базу
                await asyncio.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error recalculating income for user {user_id}: {e}")
                errors += 1
                continue
        
        # Финальный отчет
        conn.commit()
        
        result_text = (
            f"✅ <b>Пересчет дохода завершен!</b>\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"✅ Успешно обработано: {processed}\n"
            f"❌ Ошибок: {errors}\n"
            f"📊 Эффективность: {(processed/total_users*100):.1f}%\n\n"
            f"⏰ Время завершения: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )
        
        await status_msg.edit_text(result_text, parse_mode='HTML')
        
        # Логируем результат
        logger.info(f"Income recalculated for {processed}/{total_users} users")
        
    except Exception as e:
        logger.error(f"Error in recalculate_all_income command: {e}")
        await message.answer(f"❌ Произошла ошибка при пересчете дохода: {str(e)}")    

@dp.message(Command("income_info"))
async def show_income_calculation(message: Message):
    """Показывает подробный расчет дохода пользователя (только для админа)"""

    if message.from_user.id not in ADMINS:
        await message.answer("❌ Эта команда только для администраторов")
        return
    
    # Проверяем, что команда отправлена в ответ на сообщение
    if not message.reply_to_message:
        await message.answer("❌ Используйте команду в ответ на сообщение пользователя")
        return
    
    target_user_id = message.reply_to_message.from_user.id
    target_username = message.reply_to_message.from_user.username or "Без username"
    target_full_name = message.reply_to_message.from_user.full_name
    
    try:
        # Получаем базовые данные
        base_income = calculate_base_income(target_user_id)
        upgrade_bonus = calculate_upgrade_bonus(target_user_id)
        
        # Получаем бонус от вайпов
        cursor.execute('SELECT total_wipe_bonus FROM user_wipes WHERE user_id = ?', (target_user_id,))
        wipe_bonus_data = cursor.fetchone()
        wipe_bonus = wipe_bonus_data[0] / 100 if wipe_bonus_data and wipe_bonus_data[0] else 0
        
        # Проверяем премиум
        premium_active = is_premium(target_user_id)
        
        # Получаем бустеры
        cursor.execute('''
        SELECT booster_type, bonus FROM user_boosters 
        WHERE user_id = ? AND until > ?
        ''', (target_user_id, datetime.now().isoformat()))
        boosters = cursor.fetchall()
        boosters_total_bonus = sum(bonus for _, bonus in boosters)
        
        # Получаем бонус подписок
        subscription_bonus = get_social_bonus(target_user_id)
        
        # Получаем бонус предметов
        farm_bonus, business_bonus = calculate_items_bonus(target_user_id)
        
        # Получаем штрафы
        virus_penalty = calculate_virus_penalty(target_user_id)
        tax_info = get_user_tax_info(target_user_id)
        tax_penalty = FARM_PENALTY_RATE if tax_info and tax_info.get("farm_penalty_applied") else 0
        
        # Рассчитываем доход пошагово
        step1 = base_income * (1 + upgrade_bonus)
        step2 = step1 * (1 + wipe_bonus)
        step3 = step2 * (1.35 if premium_active else 1)
        step4 = step3 * (1 + boosters_total_bonus)
        step5 = step4 * (1 + subscription_bonus)
        step6 = step5 * (1 + farm_bonus)
        step7 = step6 * (1 - virus_penalty)
        final_income = step7 * (1 - tax_penalty)
        
        # Формируем подробный отчет
        text = f"📊 ДЕТАЛЬНЫЙ РАСЧЕТ ДОХОДА\n"
        text += f"👤 Пользователь: {target_full_name} (@{target_username})\n"
        text += f"🆔 ID: {target_user_id}\n"
        text += f"➖➖➖➖➖➖➖➖➖➖\n\n"
        
        text += f"🧮 ШАГИ РАСЧЕТА:\n\n"
        
        text += f"1️⃣ БАЗОВЫЙ ДОХОД (ДХ):\n"
        text += f"   💰 {format_number_short(base_income)} BTC\n"
        text += f"   📊 (доход от всех видеокарт)\n\n"
        
        text += f"2️⃣ УЛУЧШЕНИЯ (У): +{upgrade_bonus*100:.1f}%\n"
        text += f"   🧮 {format_number_short(base_income)} × (1 + {upgrade_bonus:.3f})\n"
        text += f"   💰 Результат: {format_number_short(step1)} BTC\n\n"
        
        text += f"3️⃣ ВАЙПЫ (В): +{wipe_bonus*100:.1f}%\n"
        text += f"   🧮 {format_number_short(step1)} × (1 + {wipe_bonus:.3f})\n"
        text += f"   💰 Результат: {format_number_short(step2)} BTC\n\n"
        
        text += f"4️⃣ ПРЕМИУМ (П): {'+35%' if premium_active else 'нет'}\n"
        text += f"   🧮 {format_number_short(step2)} × {1.35 if premium_active else 1}\n"
        text += f"   💰 Результат: {format_number_short(step3)} BTC\n\n"
        
        text += f"5️⃣ БУСТЕРЫ (УБ): +{boosters_total_bonus*100:.1f}%\n"
        text += f"   🎯 Активные бустеры: {len(boosters)} шт.\n"
        for booster_type, bonus in boosters:
            text += f"   • {booster_type}: +{bonus*100:.1f}%\n"
        text += f"   🧮 {format_number_short(step3)} × (1 + {boosters_total_bonus:.3f})\n"
        text += f"   💰 Результат: {format_number_short(step4)} BTC\n\n"
        
        text += f"6️⃣ ПОДПИСКИ (ПКЧБ): +{subscription_bonus*100:.1f}%\n"
        text += f"   🧮 {format_number_short(step4)} × (1 + {subscription_bonus:.3f})\n"
        text += f"   💰 Результат: {format_number_short(step5)} BTC\n\n"
        
        text += f"7️⃣ ПРЕДМЕТЫ (ПТ): +{farm_bonus*100:.1f}%\n"
        text += f"   🧮 {format_number_short(step5)} × (1 + {farm_bonus:.3f})\n"
        text += f"   💰 Результат: {format_number_short(step6)} BTC\n\n"
        
        if virus_penalty > 0:
            text += f"8️⃣ ВИРУС: -{virus_penalty*100:.1f}%\n"
            text += f"   🧮 {format_number_short(step6)} × (1 - {virus_penalty:.3f})\n"
            text += f"   💰 Результат: {format_number_short(step7)} BTC\n\n"
        else:
            step7 = step6
            text += f"8️⃣ ВИРУС: нет\n"
            text += f"   💰 Результат: {format_number_short(step7)} BTC\n\n"
        
        if tax_penalty > 0:
            text += f"9️⃣ НАЛОГ: -{tax_penalty*100:.1f}%\n"
            text += f"   🧮 {format_number_short(step7)} × (1 - {tax_penalty:.3f})\n"
            text += f"   💰 Итоговый доход: {format_number_short(final_income)} BTC\n\n"
        else:
            final_income = step7
            text += f"9️⃣ НАЛОГ: нет\n"
            text += f"   💰 Итоговый доход: {format_number_short(final_income)} BTC\n\n"
        
        text += f"🎯 ИТОГОВАЯ ФОРМУЛА:\n"
        text += f"ДХ{upgrade_bonus:+.1%} × В{wipe_bonus:+.1%} × П{'×1.35' if premium_active else ''} × УБ{boosters_total_bonus:+.1%} × ПКЧБ{subscription_bonus:+.1%} × ПТ{farm_bonus:+.1%}"
        if virus_penalty > 0:
            text += f" × Вирус{virus_penalty:+.1%}"
        if tax_penalty > 0:
            text += f" × Налог{tax_penalty:+.1%}"
        
        await message.answer(text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при расчете дохода: {str(e)}")

# Обновляем функцию calculate_income чтобы учитывать налоговый штраф
def calculate_income(user_id: int) -> float:
    """Пересчитывает и возвращает доход пользователя с учетом всех модификаторов"""
    income = calculate_income_with_penalty(user_id)
    
    # Обновляем доход в базе данных
    cursor.execute('UPDATE users SET income_btc = ? WHERE user_id = ?', (income, user_id))
    conn.commit()
    
    return income

async def tax_checker():
    """Фоновая задача для начисления налогов каждые 30 минут"""
    while True:
        try:
            now = datetime.now()
            current_minute = now.minute
            
            # Проверяем, что текущее время кратно 30 минутам (0 или 30 минут)
            if current_minute % 30 != 0:
                # Вычисляем время до следующего кратного 30 минутам
                minutes_to_wait = 30 - (current_minute % 30)
                seconds_to_wait = (minutes_to_wait * 60) - now.second
                
                logger.info(f"Next tax check in {minutes_to_wait} minutes at {(now + timedelta(minutes=minutes_to_wait)).strftime('%H:%M')}")
                await asyncio.sleep(seconds_to_wait)
                continue
            
            logger.info(f"🕒 Tax check started at {now.strftime('%H:%M:%S')}")
            
            # Получаем всех активных пользователей
            cursor.execute('SELECT user_id FROM users WHERE last_income_time IS NOT NULL')
            active_users = cursor.fetchall()
            
            logger.info(f"Tax check: processing {len(active_users)} active users")
            
            tax_count = 0
            
            for (user_id,) in active_users:
                try:
                    # Начисляем налоги каждые 30 минут
                    tax_info = get_user_tax_info(user_id)
                    
                    # Рассчитываем новые налоги
                    business_tax = calculate_business_tax(user_id)
                    farm_tax = calculate_farm_tax(user_id)
                    
                    # Если налоги равны 0, пропускаем пользователя
                    if business_tax == 0 and farm_tax == 0:
                        continue
                    
                    # Обновляем общую сумму налогов
                    new_business_tax = (tax_info["business_tax_debt"] if tax_info else 0) + business_tax
                    new_farm_tax = (tax_info["farm_tax_debt"] if tax_info else 0) + farm_tax
                    
                    # Проверяем максимальные лимиты и применяем штрафы
                    penalty_applied = tax_info["farm_penalty_applied"] if tax_info else False
                    
                    if new_business_tax >= MAX_BUSINESS_TAX:
                        # Аннулируем бизнесы
                        cursor.execute('DELETE FROM user_businesses WHERE user_id = ?', (user_id,))
                        new_business_tax = 0
                        logger.info(f"Businesses annulled for user {user_id} due to tax debt")
                    
                    if new_farm_tax >= MAX_FARM_TAX and not penalty_applied:
                        # Применяем штраф к ферме
                        apply_farm_penalty(user_id)
                        penalty_applied = True
                        logger.info(f"Farm penalty applied for user {user_id} due to tax debt")
                    
                    # Обновляем информацию о налогах
                    update_tax_info(user_id, new_business_tax, new_farm_tax, penalty_applied)
                    
                    tax_count += 1
                    
                    await asyncio.sleep(0.01)  # Небольшая задержка между пользователями
                    
                except Exception as e:
                    logger.error(f"Error processing taxes for user {user_id}: {e}")
                    continue
            
            logger.info(f"✅ Tax check completed: taxes added for {tax_count} users at {datetime.now().strftime('%H:%M:%S')}")
            
        except Exception as e:
            logger.error(f"Error in tax checker: {e}")
        
        # Ждем 60 секунд до следующей проверки (чтобы не запускать несколько раз в одну минуту)
        await asyncio.sleep(60)
        
# Команда для меню налогов
@dp.message(Command("nalog"))
async def tax_menu(message: Message):
    """Меню налогов"""
    user_id = message.from_user.id
    
    tax_info = get_user_tax_info(user_id)
    if not tax_info:
        tax_info = {
            "business_tax_debt": 0,
            "farm_tax_debt": 0,
            "farm_penalty_applied": False
        }
    
    text = (
        "💰 <b>Меню налогов</b>\n\n"
        f"🏢 <b>Налог бизнесов:</b> ${format_number(tax_info['business_tax_debt'], True)} / ${format_number(MAX_BUSINESS_TAX, True)}\n"
        f"🖥️ <b>Налог фермы:</b> ${format_number(tax_info['farm_tax_debt'], True)} / ${format_number(MAX_FARM_TAX, True)}\n\n"
        
        "⚠️ <b>Последствия неуплаты:</b>\n"
        f"• Бизнесы: аннулирование при ${format_number(MAX_BUSINESS_TAX, True)}\n"
        f"• Ферма: -30% к доходу при ${format_number(MAX_FARM_TAX, True)}\n\n"
        
        "💡 <b>Команды для уплаты:</b>\n"
        "/biz_nal - Уплатить налог бизнесов\n"
        "/ferm_nal - Уплатить налог фермы\n"
        "/pay_taxes - Уплатить все налоги\n\n"
        
        "⏰ Налоги начисляются каждые 30 минут"
    )
    
    await message.answer(text, parse_mode='HTML')

@dp.message(Command("wipe_confirm"))
async def wipe_confirm(message: Message):
    user_id = message.from_user.id
    try:
        user = get_user(user_id)
        if not user:
            await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
            return
        
        try:
            cursor.execute('SELECT wipe_count, total_wipe_bonus FROM user_wipes WHERE user_id = ?', (user_id,))
            wipe_data = cursor.fetchone()
            
            if wipe_data is None:
                wipe_count = 0
                total_wipe_bonus = 0
            else:
                wipe_count = wipe_data[0]
                total_wipe_bonus = wipe_data[1]
            
            if wipe_count >= MAX_WIPES:
                await message.answer("❌ Ты достиг максимального количества вайпов (10). Дальнейшие вайпы недоступны.")
                return
            
            current_expansion = user[5]
            current_slots = EXPANSIONS[current_expansion-1]['max_cards']
            required_slots = WIPES_SLOTS.get(wipe_count, 195)
            
            if current_slots < required_slots:
                await message.answer(
                    f"❌ Для вайпа нужно иметь минимум {required_slots} слотов!\n"
                    f"Сейчас у тебя {current_slots} слотов.\n"
                    f"Улучшай ферму (/capacity) чтобы достичь нужного количества."
                )
                return
            
            new_wipe_count = wipe_count + 1
            new_total_bonus = total_wipe_bonus + WIPE_BONUS_PERCENT
            new_max_slots = WIPES_SLOTS.get(new_wipe_count, 400)
            
            # Получаем и сохраняем часть опыта
            cursor.execute('SELECT total_experience FROM user_work_stats WHERE user_id = ?', (user_id,))
            exp_result = cursor.fetchone()
            current_exp = exp_result[0] if exp_result else 0
            save_percent = random.randint(0, 100)
            saved_exp = int(current_exp * (save_percent / 100))
            
            # Очистка данных о вкладах и кредитах
            cursor.execute('''
            SELECT COUNT(*) 
            FROM bank_deposits 
            WHERE user_id = ? AND status = 'active'
            ''', (user_id,))
            deposit_count_result = cursor.fetchone()
            deposit_count = deposit_count_result[0] if deposit_count_result else 0
            
            cursor.execute('''
            DELETE FROM bank_deposits 
            WHERE user_id = ? AND status = 'active'
            ''', (user_id,))
            
            cursor.execute('''
            SELECT COUNT(*) 
            FROM bank_loans 
            WHERE user_id = ? AND status = 'active'
            ''', (user_id,))
            loan_count_result = cursor.fetchone()
            loan_count = loan_count_result[0] if loan_count_result else 0
            
            cursor.execute('''
            DELETE FROM bank_loans 
            WHERE user_id = ? AND status = 'active'
            ''', (user_id,))
            
            # Очистка всех предметов пользователя
            cursor.execute('''
            SELECT COUNT(*) 
            FROM user_items 
            WHERE user_id = ?
            ''', (user_id,))
            items_count_result = cursor.fetchone()
            items_count = items_count_result[0] if items_count_result else 0
            
            cursor.execute('DELETE FROM user_items WHERE user_id = ?', (user_id,))
            
            # Обновляем данные пользователя (основной сброс)
            cursor.execute('DELETE FROM user_cards WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM user_upgrades WHERE user_id = ?', (user_id,))
            cursor.execute('DELETE FROM user_businesses WHERE user_id = ?', (user_id,))
            cursor.execute('UPDATE user_work_stats SET total_experience = ? WHERE user_id = ?', 
                          (saved_exp, user_id))
            
            # ИСПРАВЛЕНО: Очистка налоговых задолженностей (убираем farm_penalty)
            cursor.execute('''
            UPDATE user_taxes 
            SET business_tax_debt = 0, farm_tax_debt = 0
            WHERE user_id = ?
            ''', (user_id,))
            
            # Сбрасываем баланс пользователя
            cursor.execute('''
            UPDATE users 
            SET expansion = 1, 
                income_btc = 0,
                usd_balance = 10000,
                btc_balance = 0,
                last_income_time = NULL
            WHERE user_id = ?
            ''', (user_id,))
            
            # Добавляем стандартные бизнесы
            standard_businesses = [1, 11, 12]
            for business_id in standard_businesses:
                cursor.execute('''
                INSERT INTO user_businesses (user_id, business_id, level, last_income_time)
                VALUES (?, ?, 1, ?)
                ''', (user_id, business_id, datetime.now().isoformat()))
            
            # Обновляем счетчик вайпов
            cursor.execute('''
            INSERT OR REPLACE INTO user_wipes (user_id, wipe_count, total_wipe_bonus)
            VALUES (?, ?, ?)
            ''', (user_id, new_wipe_count, new_total_bonus))
            
            # Обновляем бейдж
            badge_id = min(new_wipe_count, len(BADGES))
            if badge_id > 0:
                cursor.execute('''
                INSERT OR REPLACE INTO user_badges (user_id, badge_id)
                VALUES (?, ?)
                ''', (user_id, badge_id))
            
            conn.commit()
            
            # Формируем ответ
            wipe_message = f"""
✨ <b>Система Обновлена! Вайп Успешно Завершён!</b> ✨

🔄 <b>Новый цикл начат!</b> (Вайп {new_wipe_count}/10)

✅ <b>Что нового и сохранено:</b>
├ Слоты: Максимальное число увеличено до {new_max_slots}
├ Стартовый бонус: +{WIPE_BONUS_PERCENT}% к доходу фермы
├ Общий бонус составляет: {int(new_total_bonus)}%
├ Бизнесы: Добавлено 3 стандартных стартовых бизнеса
├ Налоги: Все налоговые задолженности очищены
└ Опыт: {save_percent}% опыта ({saved_exp} exp) сохранено

⚠️ <b>Важная финансовая информация:</b>
Удалено {deposit_count} вклад(а). 
Средства, привязанные к удалённым вкладам, не подлежат возврату.
"""
            
            try_give_100percent(user_id, 25)
            
            await message.answer(wipe_message, parse_mode='HTML')
            
        except sqlite3.Error as e:
            conn.rollback()
            await message.answer("❌ Произошла ошибка при обновлении данных в базе. Попробуйте позже.")
            logging.error(f"Database error during wipe: {e}")
            
    except Exception as e:
        await message.answer("❌ Произошла неизвестная ошибка. Попробуйте позже.")
        logging.error(f"Unexpected error in wipe_confirm: {e}")


        
@dp.message(Command("pay_taxes"))
async def pay_all_taxes(message: Message):
    """Уплата всех налогов сразу"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    tax_info = get_user_tax_info(user_id)
    if not tax_info:
        await message.answer("✅ У вас нет налоговых задолженностей!")
        return
    
    business_tax = tax_info.get("business_tax_debt", 0)
    farm_tax = tax_info.get("farm_tax_debt", 0)
    
    total_tax = business_tax + farm_tax
    
    if total_tax <= 0:
        await message.answer("✅ У вас нет задолженностей по налогам!")
        return
    
    if user[2] < total_tax:
        await message.answer(
            f"❌ Недостаточно средств для уплаты всех налогов!\n"
            f"💵 Общая сумма налогов: ${format_number(total_tax, True)}\n"
            f"💰 Ваш баланс: ${format_number(user[2], True)}\n\n"
            f"📊 Детализация:\n"
            f"🏢 Налог бизнесов: ${format_number(business_tax, True)}\n"
            f"🚜 Налог фермы: ${format_number(farm_tax, True)}"
        )
        return
    
    # Списываем общую сумму налогов
    new_usd = user[2] - total_tax
    update_balance(user_id, usd=new_usd)
    
    # Обнуляем все задолженности
    update_tax_info(user_id, business_tax=0, farm_tax=0, farm_penalty=False)
    remove_farm_penalty(user_id)
    
    # Проверяем получение ивент валюты (5% шанс)
    event_currency_received = try_give_5percent(user_id, 1)
    
    response_text = "🔥 Вы успешно уплатили все налоги!"
    
    # Добавляем информацию о полученной ивент валюте, если она была выдана
    if event_currency_received:
        current_balance = get_event_currency(user_id)
        response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    
    await message.answer(response_text)        

@dp.message(Command("biz_nal"))
async def pay_business_tax(message: Message):
    """Уплата налога бизнесов"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    tax_info = get_user_tax_info(user_id)
    if not tax_info or tax_info["business_tax_debt"] <= 0:
        await message.answer("✅ У вас нет задолженности по налогу бизнесов!")
        return
    
    tax_amount = tax_info["business_tax_debt"]
    
    if user[2] < tax_amount:
        await message.answer(
            f"❌ Недостаточно средств для уплаты налога!\n"
            f"💵 Налог: ${format_number(tax_amount, True)}\n"
            f"💰 Ваш баланс: ${format_number(user[2], True)}"
        )
        return
    
    # Списываем налог
    new_usd = user[2] - tax_amount
    update_balance(user_id, usd=new_usd)
    
    # Обнуляем задолженность
    update_tax_info(user_id, business_tax=0)
    
    # Проверяем получение ивент валюты (5% шанс)
    event_currency_received = try_give_5percent(user_id, 1)
    
    response_text = "🔥 Вы успешно уплатили налог бизнесов!"
    
    # Добавляем информацию о полученной ивент валюте, если она была выдана
    if event_currency_received:
        current_balance = get_event_currency(user_id)
        response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    
    await message.answer(response_text)

@dp.message(Command("ferm_nal"))
async def pay_farm_tax(message: Message):
    """Уплата налога фермы"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("❌ Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    tax_info = get_user_tax_info(user_id)
    if not tax_info or tax_info["farm_tax_debt"] <= 0:
        await message.answer("✅ У вас нет задолженности по налогу фермы!")
        return
    
    tax_amount = tax_info["farm_tax_debt"]
    
    if user[2] < tax_amount:
        await message.answer(
            f"❌ Недостаточно средств для уплаты налога!\n"
            f"💵 Налог: ${format_number(tax_amount, True)}\n"
            f"💰 Ваш баланс: ${format_number(user[2], True)}"
        )
        return
    
    # Списываем налог
    new_usd = user[2] - tax_amount
    update_balance(user_id, usd=new_usd)
    
    # Обнуляем задолженность и убираем штраф если был
    remove_farm_penalty(user_id)
    update_tax_info(user_id, farm_tax=0, farm_penalty=False)
    
    # Проверяем получение ивент валюты (5% шанс)
    event_currency_received = try_give_5percent(user_ifd, 1)
    
    response_text = "🔥 Вы успешно уплатили налог фермы!"
    
    # Добавляем информацию о полученной ивент валюте, если она была выдана
    if event_currency_received:
        current_balance = get_event_currency(user_id)
        response_text += f"\n\n🎉 +1 🎃 (Баланс: {current_balance} 🎃)\n/top_ivent"
    
    await message.answer(response_text)
    
    
# Запуск налоговой проверки при старте бота
async def start_tax_checker():
    """Запускает фоновую задачу проверки налогов"""
    asyncio.create_task(tax_checker())

# Добавляем инициализацию при запуске
async def initialize_tax_system():
    """Инициализирует систему налогов при запуске бота"""
    try:
        await start_tax_checker()
        logger.info("Tax system initialized")
    except Exception as e:
        logger.error(f"Error initializing tax system: {e}")



# Команды для проверки и отладки налоговой системы
@dp.message(Command("tax_info"))
async def tax_info_debug(message: Message):
    """Подробная информация о налогах пользователя (для отладки)"""
    user_id = message.from_user.id
    
    tax_info = get_user_tax_info(user_id)
    user = get_user(user_id)
    
    if not tax_info:
        tax_info = {
            "business_tax_debt": 0,
            "farm_tax_debt": 0,
            "last_tax_check": "никогда",
            "farm_penalty_applied": False
        }
    
    # Рассчитываем текущие налоги
    current_business_tax = calculate_business_tax(user_id)
    current_farm_tax = calculate_farm_tax(user_id)
    
    # Получаем информацию о бизнесах
    cursor.execute('SELECT COUNT(*) FROM user_businesses WHERE user_id = ?', (user_id,))
    business_count = cursor.fetchone()[0] or 0
    
    # Получаем базовый доход
    base_income = calculate_base_income(user_id)
    btc_price = get_btc_price()
    
    text = (
        "🔍 <b>Детальная информация о налогах</b>\n\n"
        
        "📊 <b>Текущие задолженности:</b>\n"
        f"🏢 Бизнесы: ${format_number(tax_info['business_tax_debt'], True)}\n"
        f"🖥️ Ферма: ${format_number(tax_info['farm_tax_debt'], True)}\n\n"
        
        "💰 <b>Расчет текущих налогов:</b>\n"
        f"🏢 Бизнесы (10%): ${format_number(current_business_tax, True)}\n"
        f"🖥️ Ферма (15%): ${format_number(current_farm_tax, True)}\n\n"
        
        "📈 <b>Статистика:</b>\n"
        f"• Бизнесов: {business_count}\n"
        f"• Базовый доход фермы: {format_number(base_income)} BTC\n"
        f"• Курс BTC: ${format_number(btc_price)}\n"
        f"• Баланс: ${format_number(user[2], True) if user else 0}\n\n"
        
        "⚠️ <b>Статус штрафов:</b>\n"
        f"• Штраф фермы: {'✅ АКТИВЕН (-30%)' if tax_info['farm_penalty_applied'] else '❌ не активен'}\n"
        f"• Последняя проверка: {tax_info['last_tax_check']}\n\n"
        
        "💡 <b>Лимиты:</b>\n"
        f"• Макс. налог бизнесов: ${format_number(MAX_BUSINESS_TAX, True)}\n"
        f"• Макс. налог фермы: ${format_number(MAX_FARM_TAX, True)}"
    )
    
    await message.answer(text, parse_mode='HTML')

@dp.message(Command("tax_calc"))
async def tax_calculate_test(message: Message):
    """Тестовый расчет налогов"""
    user_id = message.from_user.id
    
    business_tax = calculate_business_tax(user_id)
    farm_tax = calculate_farm_tax(user_id)
    
    base_income = calculate_base_income(user_id)
    btc_price = get_btc_price()
    
    text = (
        "🧮 <b>Тестовый расчет налогов</b>\n\n"
        
        "📊 <b>Исходные данные:</b>\n"
        f"• Базовый доход фермы: {format_number(base_income)} BTC\n"
        f"• Курс BTC: ${format_number(btc_price)}\n"
        f"• Доход фермы в $: ${format_number(base_income * btc_price, True)}\n\n"
        
        "💰 <b>Расчет налогов за 30 минут:</b>\n"
        f"🏢 Налог бизнесов (10%): ${format_number(business_tax, True)}\n"
        f"🖥️ Налог фермы (15%): ${format_number(farm_tax, True)}\n\n"
        
        "💡 <b>Формулы:</b>\n"
        "• Бизнесы: SUM(базовый_доход_бизнеса × уровень) × 10%\n"
        "• Ферма: базовый_доход_фермы(BTC) × курс_BTC($) × 15%"
    )
    
    await message.answer(text, parse_mode='HTML')

@dp.message(Command("tax_reset"))
async def tax_reset_debug(message: Message):
    """Сброс налогов (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        update_tax_info(user_id, 0, 0, False)
        remove_farm_penalty(user_id)
        
        await message.answer("✅ Налоги сброшены! Штрафы убраны.")
        
    except Exception as e:
        logger.error(f"Error resetting taxes: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("tax_force"))
async def tax_force_check(message: Message):
    """Принудительная проверка налогов (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        
        # Рассчитываем новые налоги
        business_tax = calculate_business_tax(user_id)
        farm_tax = calculate_farm_tax(user_id)
        
        tax_info = get_user_tax_info(user_id)
        
        # Обновляем общую сумму налогов
        new_business_tax = (tax_info["business_tax_debt"] if tax_info else 0) + business_tax
        new_farm_tax = (tax_info["farm_tax_debt"] if tax_info else 0) + farm_tax
        
        # Проверяем максимальные лимиты
        penalty_applied = False
        
        if new_business_tax >= MAX_BUSINESS_TAX:
            cursor.execute('DELETE FROM user_businesses WHERE user_id = ?', (user_id,))
            new_business_tax = 0
        
        if new_farm_tax >= MAX_FARM_TAX:
            if not tax_info or not tax_info["farm_penalty_applied"]:
                apply_farm_penalty(user_id)
                penalty_applied = True
        
        # Обновляем информацию о налогах
        update_tax_info(user_id, new_business_tax, new_farm_tax, penalty_applied)
        
        text = (
            "🔧 <b>Принудительная проверка налогов</b>\n\n"
            f"🏢 Добавлено налога бизнесов: ${format_number(business_tax, True)}\n"
            f"🖥️ Добавлено налога фермы: ${format_number(farm_tax, True)}\n\n"
            f"💰 Итого бизнесы: ${format_number(new_business_tax, True)}\n"
            f"💰 Итого ферма: ${format_number(new_farm_tax, True)}\n"
            f"⚠️ Штраф применен: {'✅ ДА' if penalty_applied else '❌ НЕТ'}"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in force tax check: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("tax_penalty_test"))
async def tax_penalty_test(message: Message):
    """Тест применения штрафа (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        
        # Применяем штраф
        apply_farm_penalty(user_id)
        update_tax_info(user_id, farm_penalty=True)
        
        # Получаем доход до и после штрафа
        income_before = calculate_base_income(user_id)
        income_after = calculate_income(user_id)
        
        text = (
            "⚡ <b>Тест штрафа фермы</b>\n\n"
            f"📊 Доход до штрафа: {format_number(income_before)} BTC\n"
            f"📊 Доход после штрафа: {format_number(income_after)} BTC\n"
            f"📉 Снижение: {format_number((income_before - income_after) / income_before * 100, True)}%\n\n"
            "✅ Штраф применен!"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in penalty test: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("tax_remove_penalty"))
async def tax_remove_penalty_test(message: Message):
    """Тест снятия штрафа (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user_id = message.from_user.id
        
        # Убираем штраф
        remove_farm_penalty(user_id)
        update_tax_info(user_id, farm_penalty=False)
        
        income_after = calculate_income(user_id)
        
        text = (
            "🔧 <b>Тест снятия штрафа</b>\n\n"
            f"📊 Текущий доход: {format_number(income_after)} BTC\n"
            "✅ Штраф снят!"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error removing penalty: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("tax_set_debt"))
async def tax_set_debt_test(message: Message):
    """Установить тестовую задолженность (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        parts = message.text.split()
        if len(parts) != 3:
            await message.answer("❌ Используйте: /tax_set_debt [business_debt] [farm_debt]")
            return
        
        business_debt = float(parts[1])
        farm_debt = float(parts[2])
        
        user_id = message.from_user.id
        update_tax_info(user_id, business_debt, farm_debt)
        
        text = (
            "🎯 <b>Установлена тестовая задолженность</b>\n\n"
            f"🏢 Бизнесы: ${format_number(business_debt, True)}\n"
            f"🖥️ Ферма: ${format_number(farm_debt, True)}\n\n"
            "💡 Проверьте командами /nalog и /tax_info"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error setting tax debt: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("tax_status"))
async def tax_system_status(message: Message):
    """Статус налоговой системы (только для админа)"""
    if message.from_user.id not in ADMINS:  # Ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Статистика по пользователям с налогами
        cursor.execute('SELECT COUNT(*) FROM user_taxes')
        total_users_with_taxes = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM user_taxes WHERE farm_penalty_applied = TRUE')
        users_with_penalty = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM user_taxes WHERE business_tax_debt >= ?', (MAX_BUSINESS_TAX,))
        users_business_max = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT COUNT(*) FROM user_taxes WHERE farm_tax_debt >= ?', (MAX_FARM_TAX,))
        users_farm_max = cursor.fetchone()[0] or 0
        
        cursor.execute('SELECT SUM(business_tax_debt), SUM(farm_tax_debt) FROM user_taxes')
        total_taxes = cursor.fetchone()
        total_business_tax = total_taxes[0] or 0
        total_farm_tax = total_taxes[1] or 0
        
        text = (
            "📊 <b>Статус налоговой системы</b>\n\n"
            
            "👥 <b>Статистика пользователей:</b>\n"
            f"• Всего с налогами: {total_users_with_taxes}\n"
            f"• Со штрафом фермы: {users_with_penalty}\n"
            f"• Макс. налог бизнесов: {users_business_max}\n"
            f"• Макс. налог фермы: {users_farm_max}\n\n"
            
            "💰 <b>Общие суммы налогов:</b>\n"
            f"• Бизнесы: ${format_number(total_business_tax, True)}\n"
            f"• Фермы: ${format_number(total_farm_tax, True)}\n"
            f"• Всего: ${format_number(total_business_tax + total_farm_tax, True)}\n\n"
            
            "⚙️ <b>Настройки системы:</b>\n"
            f"• Интервал проверки: 30 минут\n"
            f"• Налог бизнесов: {BUSINESS_TAX_RATE*100}%\n"
            f"• Налог фермы: {FARM_TAX_RATE*100}%\n"
            f"• Макс. бизнесы: ${format_number(MAX_BUSINESS_TAX, True)}\n"
            f"• Макс. ферма: ${format_number(MAX_FARM_TAX, True)}\n"
            f"• Штраф фермы: {FARM_PENALTY_RATE*100}%"
        )
        
        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error getting tax system status: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")



# Добавляем после других CREATE TABLE
cursor.execute('''
CREATE TABLE IF NOT EXISTS user_admin_badges (
    user_id INTEGER,
    badge_id INTEGER,
    assigned_at TEXT DEFAULT CURRENT_TIMESTAMP,
    assigned_by INTEGER,
    PRIMARY KEY (user_id, badge_id),
    FOREIGN KEY(user_id) REFERENCES users(user_id)
)
''')
conn.commit()

# Обработчик для кнопки ежедневного бонуса из профиля
@dp.callback_query(F.data.startswith("daily_bonus_"))
async def daily_bonus_from_profile(callback: CallbackQuery):
    """Обрабатывает нажатие на кнопку бонуса из профиля"""
    await callback.answer()
    try:
        # Правильное извлечение user_id из callback_data
        # Формат: "daily_bonus_123456789"
        parts = callback.data.split('_')
        user_id = int(parts[2])  # Теперь правильно - третий элемент

        logger.info(f"Daily bonus button pressed by user {user_id}, callback data: {callback.data}")

        if callback.from_user.id != user_id:
            return
        
        # Проверяем, зарегистрирован ли пользователь
        user = get_user(user_id)
        if not user:
            logger.warning(f"User {user_id} not found in database")
            return
        
        logger.info(f"User {user_id} found, showing daily bonus info")
        
        # Создаем новое сообщение с информацией о бонусе
        can_claim, time_left = can_claim_daily_bonus(user_id)
        bonus_info = get_daily_bonus_info(user_id)
        streak_count = bonus_info["streak_count"] if bonus_info else 0
        
        text = "🎁 <b>Ежедневный бонус</b>\n\n"
        
        if can_claim:
            text += "✅ <b>Бонус доступен!</b>\n"
            text += f"📊 Серия: {streak_count} дней\n\n"
            text += "Нажмите кнопку ниже чтобы получить бонус!"
        else:
            hours = time_left.seconds // 3600
            minutes = (time_left.seconds % 3600) // 60
            seconds = time_left.seconds % 60
            
            text += "⏳ <b>Бонус еще не доступен</b>\n"
            text += f"📊 Серия: {streak_count} дней\n"
            text += f"⏰ Следующий бонус через: {hours:02d}:{minutes:02d}:{seconds:02d}\n\n"
        
        text += "\n<b>Возможные награды:</b>\n"
        text += "• 1-12x от дохода в BTC\n"
        text += "• 1-24x от дохода бизнесов в $\n" 
        text += "• Последняя доступная видеокарта\n"
        text += "• Мгновенное обновление бонуса (5%)\n"
        text += "• Премиум статус 1-24ч (1%)\n"
        text += "• Бустер 'Уборщик' 1-24ч (1%)\n"
        text += "• Бустер 'Инвестор' 1-24ч (1%)\n\n"
        
        if is_premium(user_id):
            text += "👑 <b>PREMIUM:</b> Бонус доступен каждые 12 часов!\n"
        else:
            text += "💡 <b>Обычный:</b> Бонус доступен каждые 24 часа\n"
        
        # Создаем кнопку для получения бонуса (только если доступен)
        builder = InlineKeyboardBuilder()
        if can_claim:
            builder.row(
                InlineKeyboardButton(
                    text="🎁 Получить Ежедневный Бонус",
                    callback_data=f"claim_daily_{user_id}"
                )
            )
        
        
        # Редактируем текущее сообщение
        await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode='HTML')
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in daily bonus from profile: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)


        
@dp.message(Command("boosts"))
async def show_boosters(message: Message):
    """Показывает все активные бустеры пользователя в заданном формате"""
    try:
        user_id = message.from_user.id
        response = "🌟 Ваши активные бонусы\n\n"
        has_boosters = False

        # Проверка премиума
        if is_premium(user_id):
            cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
            premium_until = datetime.fromisoformat(cursor.fetchone()[0])
            remaining = premium_until - datetime.now()
            response += (
                "👑 PREMIUM статус\n"
                f"▪ Бонус: +35% к доходу фермы, +15% к бизнесам\n"
                f"▪ Осталось: {remaining.days}д {remaining.seconds//3600}ч {remaining.seconds%3600//60}м\n\n"
            )
            has_boosters = True

        # Проверка уборщиков
        cursor.execute('''
        SELECT until FROM user_boosters 
        WHERE user_id = ? AND booster_type = 'cleaner' AND until > ?
        ORDER BY until
        ''', (user_id, datetime.now().isoformat()))
        
        cleaners = cursor.fetchall()
        if cleaners:
            response += "🧹 Уборщик фермы\n"
            for until, in cleaners:
                end_time = datetime.fromisoformat(until)
                remaining = end_time - datetime.now()
                response += (
                    f"▪ Бонус: +25%\n"
                    f"▪ Осталось: {remaining.days}д {remaining.seconds//3600}ч {remaining.seconds%3600//60}м\n"
                )
            response += "\n"
            has_boosters = True

        # Проверка инвесторов
        cursor.execute('''
        SELECT until FROM user_boosters 
        WHERE user_id = ? AND booster_type = 'investor' AND until > ?
        ORDER BY until
        ''', (user_id, datetime.now().isoformat()))
        
        investors = cursor.fetchall()
        if investors:
            response += "🏢 Инвесторы бизнесов\n"
            for until, in investors:
                end_time = datetime.fromisoformat(until)
                remaining = end_time - datetime.now()
                response += (
                    f"▪ Бонус: +15%\n"
                    f"▪ Осталось: {remaining.days}д {remaining.seconds//3600}ч {remaining.seconds%3600//60}м\n"
                )
            response += "\n"
            has_boosters = True

        if not has_boosters:
            response = "ℹ️ У вас нет активных бонусов\n\n"

        response += "💡 Приобрести дополнительные бонусы можно через /donate"
        await message.answer(response)

    except Exception as e:
        logger.error(f"Error in show_boosters: {e}")
        await message.answer("⚠️ Произошла ошибка при загрузке бонусов")
        
@dp.message(Command("wipe"))
async def wipe_info(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    cursor.execute('SELECT wipe_count, total_wipe_bonus FROM user_wipes WHERE user_id = ?', (user_id,))
    wipe_data = cursor.fetchone()
    wipe_count = wipe_data[0] if wipe_data else 0
    total_bonus = wipe_data[1] if wipe_data else 0
    
    current_expansion = user[5]
    current_exp_data = EXPANSIONS[current_expansion-1] if current_expansion <= len(EXPANSIONS) else EXPANSIONS[-1]
    
    text = "🔄 <b>Информация о вайпе</b>\n\n"
    
    if wipe_count >= MAX_WIPES:
        text += "❌ Достигнут лимит вайпов (10)"
    else:
        current_max_slots = WIPES_SLOTS.get(wipe_count, 195)
        next_max_slots = WIPES_SLOTS.get(wipe_count + 1, 400)
        
        # Проверяем, достиг ли пользователь текущего максимума слотов
        if current_exp_data['max_cards'] >= current_max_slots:
            text += (
                f"✅ Тебе доступен вайп!\n\n"
                f"После вайпа:\n"
                f"• Бонус: +{WIPE_BONUS_PERCENT}% к доходу (всего +{int(total_bonus)} + {WIPE_BONUS_PERCENT}%)\n"
                f"• Макс. слоты увеличатся с {current_max_slots} до {next_max_slots}\n"
                f"• Доступны новые уровни расширения\n\n"
                f"Подтвердить: /wipe_confirm"
            )
        else:
            text += (
                f"Для вайпа нужно:\n"
                f"• Достичь {current_max_slots} слотов (сейчас {current_exp_data['max_cards']})\n\n"
                f"После вайпа:\n"
                f"• +{WIPE_BONUS_PERCENT}% к доходу\n"
                f"• Макс. слоты увеличатся до {next_max_slots}\n\n"
                f"Текущий бонус: +{int(total_bonus)}%"
            )
    
    await message.answer(text, parse_mode='HTML')


        
@dp.message(Command("badge_unset"))
async def badge_unset(message: Message):
    """Снимает текущий установленный титул"""
    user_id = message.from_user.id
    
    # Получаем текущий титул
    cursor.execute('SELECT badge_id FROM user_badges WHERE user_id = ?', (user_id,))
    current_badge = cursor.fetchone()
    
    if not current_badge or current_badge[0] == 0:
        await message.answer("❌ У вас не установлен никакой титул")
        return
    
    current_badge_id = current_badge[0]

    # Пытаемся найти бейдж в BADGES или в кастомных
    if current_badge_id in BADGES:
        badge_name = BADGES[current_badge_id]['name']
    else:
        cursor.execute('SELECT badge_name FROM custom_badges WHERE badge_id = ?', (current_badge_id,))
        custom_badge = cursor.fetchone()
        badge_name = custom_badge[0] if custom_badge else f'Титул {current_badge_id}'

    # Снимаем титул (устанавливаем badge_id = 0)
    cursor.execute('''
    INSERT OR REPLACE INTO user_badges (user_id, badge_id)
    VALUES (?, 0)
    ''', (user_id,))
    conn.commit()
    
    # Определяем тип титула для сообщения
    if current_badge_id <= 10:
        badge_type = "Ваш"
    else:
        badge_type = "Ваш"
    
    await message.answer(f"✅ {badge_type} титул '{badge_name}' снят")
                        
@dp.message(Command("badges"))
async def badges_list(message: Message):
    user_id = message.from_user.id
    
    # Получаем текущий титул пользователя
    cursor.execute('SELECT badge_id FROM user_badges WHERE user_id = ?', (user_id,))
    current_badge = cursor.fetchone()
    current_badge_id = current_badge[0] if current_badge else 0
    
    text = "🏆 <b>Доступные титулы:</b>\n\n"
    
    has_badges = False
    
    # Проверяем количество вайпов пользователя
    cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
    wipe_data = cursor.fetchone()
    user_wipe_count = wipe_data[0] if wipe_data else 0
    
    # Сначала показываем титулы за вайпы (1-10)
    text += "🔄 <b>Титулы за вайпы:</b>\n"
    for badge_id in range(1, 11):
        if badge_id in BADGES:
            badge = BADGES[badge_id]
            has_badge = (user_wipe_count >= badge_id)
            
            if has_badge:
                has_badges = True
                status = "✅" if current_badge_id == badge_id else "🔹"
                text += f"{status} {badge['name']}\n"
                text += f"Установить: {badge['command']}\n\n"
    
    # Затем показываем специальные титулы (11-12)
    text += "👑 <b>Специальные титулы:</b>\n"
    for badge_id in range(11, 13):
        if badge_id in BADGES:
            badge = BADGES[badge_id]
            # Проверяем наличие в таблице административных титулов
            cursor.execute('SELECT 1 FROM user_admin_badges WHERE user_id = ? AND badge_id = ?', (user_id, badge_id))
            has_badge = cursor.fetchone() is not None
            
            if has_badge:
                has_badges = True
                status = "✅" if current_badge_id == badge_id else "🔹"
                text += f"{status} {badge['name']}\n"
                text += f"Установить: {badge['command']}\n\n"

    # Показываем кастомные титулы
    cursor.execute('''
    SELECT cb.badge_id, cb.badge_name
    FROM custom_badges cb
    JOIN user_admin_badges uab ON cb.badge_id = uab.badge_id
    WHERE uab.user_id = ?
    ''', (user_id,))
    custom_badges = cursor.fetchall()

    if custom_badges:
        text += "✨ <b>Кастомные титулы:</b>\n"
        for badge_id, badge_name in custom_badges:
            has_badges = True
            status = "✅" if current_badge_id == badge_id else "🔹"
            text += f"{status} {badge_name}\n"
            text += f"Установить: /setbadge_{badge_id}\n\n"

    if not has_badges:
        text = "❌ У вас пока нет доступных титулов. Выполняйте вайпы (/wipe) чтобы получать новые титулы!"
    else:
        text += "\nСнять титул: /badge_unset"
    
    await message.answer(text, parse_mode='HTML')

@dp.message(F.text.regexp(r'^/setbadge_(\d+)(@CryptoMiner_sBot)?$'))
async def set_badge(message: Message):
    user_id = message.from_user.id
    try:
        command_text = message.text.split('@')[0]
        badge_id = int(command_text.split('_')[1])

        # Определяем, это стандартный или кастомный бейдж
        is_custom = badge_id >= 100
        badge_name = None

        if not is_custom:
            # Стандартный бейдж
            if badge_id not in BADGES:
                await message.answer("❌ Такого титула не существует")
                return
            badge_name = BADGES[badge_id]['name']
        else:
            # Кастомный бейдж
            cursor.execute('SELECT badge_name FROM custom_badges WHERE badge_id = ?', (badge_id,))
            custom_badge = cursor.fetchone()
            if not custom_badge:
                await message.answer("❌ Такого титула не существует")
                return
            badge_name = custom_badge[0]

        # Проверяем права на титул
        if badge_id <= 10:
            # Титулы за вайпы
            cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
            wipe_data = cursor.fetchone()
            user_wipe_count = wipe_data[0] if wipe_data else 0

            if user_wipe_count < badge_id:
                await message.answer(f"❌ Для этого титула нужно минимум {badge_id} вайп(ов). У вас: {user_wipe_count}")
                return
        else:
            # Административные и кастомные титулы - проверяем в таблице user_admin_badges
            cursor.execute('SELECT 1 FROM user_admin_badges WHERE user_id = ? AND badge_id = ?', (user_id, badge_id))
            if not cursor.fetchone():
                await message.answer("❌ У вас нет прав на этот титул. Административные титулы выдаются вручную.")
                return
        
        # Получаем текущий установленный титул
        cursor.execute('SELECT badge_id FROM user_badges WHERE user_id = ?', (user_id,))
        current_badge = cursor.fetchone()
        current_badge_id = current_badge[0] if current_badge else 0
        
        # Если уже установлен этот же титул
        if current_badge_id == badge_id:
            await message.answer(f"❌ Титул '{badge_name}' уже установлен")
            return

        # Устанавливаем новый титул (автоматически заменяет старый)
        cursor.execute('''
        INSERT OR REPLACE INTO user_badges (user_id, badge_id)
        VALUES (?, ?)
        ''', (user_id, badge_id))
        conn.commit()

        # Сообщение о смене титула
        if current_badge_id > 0:
            # Получаем название старого бейджа
            if current_badge_id in BADGES:
                old_badge_name = BADGES[current_badge_id]['name']
            else:
                cursor.execute('SELECT badge_name FROM custom_badges WHERE badge_id = ?', (current_badge_id,))
                old_custom = cursor.fetchone()
                old_badge_name = old_custom[0] if old_custom else f'Титул {current_badge_id}'

            await message.answer(f"✅ Титул изменен:\n'{old_badge_name}' → '{badge_name}'")
        else:
            await message.answer(f"✅ Титул успешно установлен: {badge_name}")
            
    except Exception as e:
        logger.error(f"Error setting badge: {e}")
        await message.answer("❌ Произошла ошибка при установке титула")
        
@dp.callback_query(F.data.startswith("sell_all_btc_"))
async def sell_all_btc_callback(callback: CallbackQuery):
    await callback.answer()
    try:
        callback_user_id = int(callback.data.split('_')[-1])

        if callback.from_user.id != callback_user_id:
            return
        
        # Получаем текущий баланс BTC перед продажей
        user = get_user(callback_user_id)
        if not user:
            await callback.answer("❌ Пользователь не найден", show_alert=True)
            return
            
        btc_amount = user[3]
        if btc_amount <= 0:
            # Отправляем отдельное сообщение о нехватке BTC
            await callback.message.answer("❌ У вас нет BTC для продажи")
            return
            
        # Выполняем продажу
        btc_price = get_btc_price()
        usd_amount = btc_amount * btc_price
        
        # Обновляем баланс
        new_usd = user[2] + usd_amount
        update_balance(callback_user_id, usd=new_usd, btc=0)
        
        # Форматируем числа с использованием новой функции
        formatted_btc = format_number_short(btc_amount, is_usd=False)
        formatted_usd = format_number_short(usd_amount, is_usd=True)
        
        # Получаем имя пользователя и создаем ссылку на профиль
        try:
            user_info = await bot.get_chat(callback_user_id)
            username = user_info.full_name or f"ID {callback_user_id}"
            # Создаем ссылку на профиль
            profile_link = f'<a href="tg://user?id={callback_user_id}">{username}</a>'
        except Exception as e:
            logger.error(f"Error getting user info: {e}")
            profile_link = f'<a href="tg://user?id={callback_user_id}">ID {callback_user_id}</a>'
        
        # Создаем сообщение
        response = (
            f"{profile_link}, вы успешно обменяли {formatted_btc} BTC на {formatted_usd} USD\n\n"
            f"💰 Новый баланс: ${format_number_short(new_usd, is_usd=True)}"
        )
        
        # Всегда отправляем новое сообщение с результатом
        await callback.message.answer(response, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"Error in sell_all_btc callback: {e}")
        await callback.answer("❌ Произошла ошибка при продаже BTC", show_alert=True)

def get_max_expansion_level(wipe_count: int) -> int:
    """Возвращает максимальный доступный уровень для текущего количества вайпов"""
    max_slots = WIPES_SLOTS.get(wipe_count, 195)
    logger.info(f"Calculating max expansion for {wipe_count} wipes. Max slots: {max_slots}")
    
    # Находим максимальный уровень, который не превышает max_slots
    max_level = 1
    for expansion in EXPANSIONS:
        logger.info(f"Checking expansion level {expansion['level']} with {expansion['max_cards']} slots")
        if expansion['max_cards'] <= max_slots:
            max_level = expansion['level']
        else:
            break
            
    logger.info(f"Max expansion level for {wipe_count} wipes: {max_level} (max slots: {max_slots})")
    return max_level
    

def calculate_base_income(user_id: int) -> float:
    """Рассчитывает базовый доход без учета бустеров и премиума (для чатов)"""
    try:
        cards, _ = get_user_cards(user_id)
        total_income = 0.0
        for card_id, count in cards:
            card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
            if card:
                total_income += card['income'] * count
        
        # Добавляем бонус от улучшений
        bonus = calculate_upgrade_bonus(user_id)
        total_income *= (1 + bonus)
        
        # Добавляем бонус от вайпов
        cursor.execute('SELECT total_wipe_bonus FROM user_wipes WHERE user_id = ?', (user_id,))
        wipe_bonus = cursor.fetchone()
        if wipe_bonus and wipe_bonus[0]:
            total_income *= (1 + wipe_bonus[0] / 100)
        
        return total_income
    except Exception as e:
        logger.error(f"Error calculating base income: {e}")
        return 0.0

@dp.message(Command("capacity_buy"))
async def buy_capacity(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("🚫 Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    try:
        # Получаем информацию о вайпах
        cursor.execute('SELECT wipe_count, total_wipe_bonus FROM user_wipes WHERE user_id = ?', (user_id,))
        wipe_data = cursor.fetchone()
        wipe_count = wipe_data[0] if wipe_data else 0
        wipe_bonus = wipe_data[1] if wipe_data else 0
        
        current_expansion = user[5]
        
        # Проверяем, не достиг ли пользователь максимального уровня
        if current_expansion >= len(EXPANSIONS):
            await message.answer("🎉 Вы достигли максимального уровня фермы!")
            return
        
        next_expansion = EXPANSIONS[current_expansion]
        
        # Проверяем, не превышает ли следующий уровень лимит слотов для текущего количества вайпов
        max_slots_for_wipes = WIPES_SLOTS.get(wipe_count, 195)
        if next_expansion['max_cards'] > max_slots_for_wipes:
            await message.answer(
                f"❌ Вы достигли текущего лимита слотов ({max_slots_for_wipes}) для {wipe_count} вайпов.\n"
                f"Выполните вайп (/wipe), чтобы увеличить лимит до {WIPES_SLOTS.get(wipe_count + 1, 400)} слотов."
            )
            return
        
        # Проверка баланса
        if user[2] < next_expansion['price']:
            await message.answer(
                f"💸 Недостаточно средств для улучшения\n"
                f"Цена: ${format_number(next_expansion['price'], True)}\n"
                f"Ваш баланс: ${format_number(user[2], True)}"
            )
            return
        
        # Рассчитываем БАЗОВЫЙ доход (без бустеров, только улучшения + вайпы)
        base_income = calculate_base_income(user_id)
        
        # Добавляем только премиум бонус (если есть)
        premium_bonus = 0.35 if is_premium(user_id) else 0
        total_income = base_income * (1 + premium_bonus)
        
        # Проверка дохода только для уровней >60 слотов
        if next_expansion['max_cards'] > 60 and total_income < next_expansion['min_income']:
            await message.answer(
                f"⚠️ Недостаточный доход\n"
                f"Требуется: {format_number(next_expansion['min_income'])} BTC/10мин\n"
                f"Ваш доход: {format_number(total_income)} BTC/10мин\n"
                f"Без учета временных бустеров (только улучшения + вайпы + премиум)"
            )
            return
        
        # Покупка улучшения
        new_usd = user[2] - next_expansion['price']
        cursor.execute(
            'UPDATE users SET usd_balance = ?, expansion = ? WHERE user_id = ?',
            (new_usd, current_expansion + 1, user_id)
        )
        conn.commit()
        
        # Проверяем получение ивент валюты
        event_currency_received = try_give_100percent(user_id, 1)
        
        # Формируем сообщение
        response_text = (
            f"🎉 Улучшено до уровня {current_expansion + 1}!\n"
            f"Новые слоты: {next_expansion['max_cards']}\n"
            f"Баланс: ${format_number(new_usd, True)}\n"
            f"Бонусы: +{int(wipe_bonus)}% (вайпы) + {'35% (премиум)' if is_premium(user_id) else '0%'}"
        )
        
        # Добавляем информацию о полученной ивент валюте, если она была выдана
        if event_currency_received:
            current_balance = get_event_currency(user_id)
            response_text += f"\n\n🎉 +1 🍁 (Баланс: {current_balance} 🍁)\n/top_ivent"
        
        await message.answer(response_text)
        
    except Exception as e:
        logger.error(f"Error in buy_capacity: {e}", exc_info=True)
        await message.answer("❌ Произошла ошибка при покупке расширения")
         
@dp.message(Command("capacity"))
async def capacity_info(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    current_expansion = user[5]
    if current_expansion >= len(EXPANSIONS):
        await message.answer("🎉 Вы достигли максимального уровня фермы!")
        return
    
    # Получаем информацию о вайпах
    cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
    wipe_count = cursor.fetchone()
    wipe_count = wipe_count[0] if wipe_count else 0
    
    current_exp = EXPANSIONS[current_expansion-1]
    next_exp = EXPANSIONS[current_expansion]
    
    # Рассчитываем слоты с учетом вайпов
    current_max_slots = WIPES_SLOTS.get(wipe_count, 195)
    current_slots = current_exp['max_cards']
    next_slots = next_exp['max_cards']
    
    # Рассчитываем БАЗОВЫЙ доход (без бустеров, только улучшения + вайпы)
    base_income = calculate_base_income(user_id)
    
    # Добавляем только премиум бонус (как в capacity_buy)
    premium_bonus = 0.35 if is_premium(user_id) else 0
    total_income_for_check = base_income * (1 + premium_bonus)
    
    text = f"📦 Улучшение фермы\n\n"
    text += f"Текущий уровень: {current_expansion}\n"
    text += f"Слотов: {current_slots}\n"
    text += f"Следующий уровень: {next_slots} слотов\n"
    text += f"Стоимость: ${format_number(next_exp['price'], True)}\n"
    
    # Показываем требование дохода только для уровней >60
    if next_slots > 60:
        text += f"Требуемый доход: {format_number(next_exp['min_income'])} BTC/10мин\n"
    
    text += f"\nВаш доход для проверки:\n"
    text += f"- Без бустеров: {format_number(total_income_for_check)} BTC/10мин\n"
    text += f"(базовый + улучшения + вайпы + премиум)\n"
    text += f"- С бустерами: {format_number(user[4])} BTC/10мин\n"
    
    # Показываем информацию о вайпах только если текущие слоты достигли лимита
    if current_slots >= current_max_slots:
        next_wipe_slots = WIPES_SLOTS.get(wipe_count + 1, 400)
        text += f"\nℹ️ Вы достигли текущего лимита слотов ({current_max_slots}).\n"
        text += f"Выполните вайп (/wipe), чтобы увеличить лимит до {next_wipe_slots} слотов.\n"
    else:
        text += f"\nКупить - /capacity_buy"
    
    await message.answer(text)
        
@dp.message(Command("wipe_all_data"))
async def wipe_all_user_data(message: Message):
    # Проверяем, что команду вызывает владелец
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    # Проверяем, есть ли аргумент (ID пользователя)
    if len(message.text.split()) < 2:
        await message.answer("ℹ️ Использование: /wipe_all_data <user_id>")
        return

    try:
        target_user_id = int(message.text.split()[1])
    except ValueError:
        await message.answer("❌ Неверный формат ID. Укажите числовой ID пользователя")
        return

    # Удаляем все данные пользователя из всех таблиц
    try:
        # Список всех таблиц, где может быть информация о пользователе
        tables = [
            'users',
            'user_upgrades',
            'user_wipes',
            'user_cards',
            'user_work_stats',
            'chat_members',
            'premium_users',
            'user_badges'
        ]

        for table in tables:
            cursor.execute(f'DELETE FROM {table} WHERE user_id = ?', (target_user_id,))
        
        conn.commit()
        await message.answer(f"✅ Все данные пользователя {target_user_id} полностью удалены из базы данных")
        
    except Exception as e:
        logger.error(f"Error wiping user data: {e}")
        conn.rollback()
        await message.answer(f"❌ Ошибка при удалении данных пользователя {target_user_id}: {str(e)}")    
    
@dp.message(Command("farm"))
async def user_farm(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    user_cards, total_cards = get_user_cards(user_id)
    if not user_cards:
        await message.answer("У вас нет видеокарт. Купите их в магазине: /shop - видеокарты.")
        return
    
    text = "🖥️ <b>Твоя фермерная</b>\n\n"
    text += f"📦 Использовано слотов: {total_cards}/{EXPANSIONS[user[5]-1]['max_cards']}\n\n"
    text += "<b>Рабочие видеокарты:</b>\n\n"
    
    for card_id, count in user_cards:
        card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        if card:
            text += f"x{count} {card['name']}\n"
            text += f"{format_number(card['income'] * count)} BTC/10мин.\n"
            text += f"Продать - /sell_{card['id']}\n\n"
    
    total_income = calculate_income(user_id)
    text += f"Общий доход: <b>{format_number(total_income)}</b> BTC/10 мин"

    # Отправляем с баннером
    banner_path = os.path.join(BANNER_DIR, 'ferms.png')
    try:
        from aiogram.types import FSInputFile
        photo = FSInputFile(banner_path)
        await message.answer_photo(
            photo=photo,
            caption=text,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error sending farm banner: {e}")
        await message.answer(text, parse_mode='HTML')

def buy_btc(user_id: int, amount: float) -> Tuple[bool, str]:
    try:
        amount = float(amount)
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        if amount <= 0:
            return False, "Количество BTC должно быть положительным числом"
        
        btc_price = get_btc_price()
        usd_cost = amount * btc_price
        
        if user[2] < usd_cost:
            return False, (
                f"Недостаточно средств для покупки {format_number(amount)} BTC\n"
                f"Нужно: ${format_number(usd_cost, True)}\n"
                f"Ваш баланс: ${format_number(user[2], True)}"
            )
        
        new_usd = user[2] - usd_cost
        new_btc = user[3] + amount
        update_balance(user_id, usd=new_usd, btc=new_btc)
        
        return True, (
            f"Куплено {format_number(amount)} BTC за ${format_number(usd_cost, True)}\n"
            f"Новый баланс: {format_number(new_btc)} BTC, ${format_number(new_usd, True)}"
        )
    except ValueError:
        return False, "Количество BTC должно быть числом"
    except Exception as e:
        logger.error(f"Error buying BTC: {e}")
        return False, "Произошла ошибка при покупке BTC"

def sell_btc(user_id: int, amount: float) -> Tuple[bool, str]:
    try:
        amount = float(amount)
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        if amount <= 0:
            return False, "Количество BTC должно быть положительным числом"
        
        if user[3] < amount:
            return False, (
                f"Недостаточно BTC для продажи\n"
                f"Запрошено: {format_number(amount)}\n"
                f"Доступно: {format_number(user[3])}"
            )
        
        btc_price = get_btc_price()
        usd_amount = amount * btc_price
        
        new_usd = user[2] + usd_amount
        new_btc = user[3] - amount
        update_balance(user_id, usd=new_usd, btc=new_btc)
        
        return True, (
            f"Продано {format_number(amount)} BTC за ${format_number(usd_amount, True)}\n"
            f"Новый баланс: {format_number(new_btc)} BTC, ${format_number(new_usd, True)}"
        )
    except ValueError:
        return False, "Количество BTC должно быть числом"
    except Exception as e:
        logger.error(f"Error selling BTC: {e}")
        return False, "Произошла ошибка при продаже BTC"

@dp.message(F.text.regexp(rf'^/btc_buy(\@{BOT_USERNAME})?\s+(\d+\.?\d*)$'))
async def buy_btc_handler(message: Message):
    user_id = message.from_user.id
    try:
        amount_str = message.text.split()[-1]
        amount = float(amount_str)
        
        success, result = buy_btc(user_id, amount)
        if success:
            update_bp_task_progress(user_id, "buy_btc", amount)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
    except ValueError:
        await message.answer(
            "❌ Неверный формат команды.\n"
            "Используйте: /btc_buy [количество]\n"
            "Или: /btc_buy@CryptoMiner_sBot [количество]"
        )

@dp.message(F.text.regexp(rf'^/btc_sell(\@{BOT_USERNAME})?\s+(\d+\.?\d*)$'))
async def sell_btc_handler(message: Message):
    user_id = message.from_user.id
    try:
        # Извлекаем количество BTC из текста команды
        amount_str = message.text.split()[-1]
        amount = float(amount_str)
        
        success, result = sell_btc(user_id, amount)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
    except ValueError:
        await message.answer(
            "❌ Неверный формат команды.\n"
            "Используйте: /btc_sell [количество]\n"
            "Или: /btc_sell@CryptoMiner_sBot [количество]"
        )

@dp.message(F.text.regexp(rf'^/buy_(\d+)(@{BOT_USERNAME})?(?:\s+(\d+))?$'))
async def buy_card_handler(message: Message):
    user_id = message.from_user.id
    try:
        logger.info(f"Buy command received from {user_id}: {message.text}")
        
        # Извлекаем данные из команды с помощью регулярного выражения
        match = re.match(rf'^/buy_(\d+)(@{BOT_USERNAME})?(?:\s+(\d+))?$', message.text)
        if not match:
            logger.warning(f"Invalid command format: {message.text}")
            return await message.answer("❌ Неверный формат. Используйте: /buy_<номер карты> [количество]")
        
        card_id = int(match.group(1))
        quantity = int(match.group(3)) if match.group(3) else 1
        
        logger.info(f"Card ID extracted: {card_id}, quantity: {quantity}")
        
        if quantity <= 0:
            logger.error("Quantity must be positive")
            return await message.answer("❌ Количество должно быть положительным числом")
        
        # Выполняем покупку
        logger.info(f"Processing buy: user={user_id}, card={card_id}, qty={quantity}")
        success, result = buy_card(user_id, card_id, quantity)
        
        if success:
            logger.info(f"Buy successful: {result}")
            # Форматируем вывод суммы
            formatted_result = re.sub(r'\$([\d,]+)(?:\.00)?', 
                                    lambda m: f"${format_number(float(m.group(1).replace(',', '')), True)}", 
                                    result)
            await message.answer(f"✅ {formatted_result}")
        else:
            logger.warning(f"Buy failed: {result}")
            await message.answer(f"❌ {result}")
            
    except Exception as e:
        logger.error(f"Unexpected error in buy handler: {str(e)}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке команды")

@dp.message(F.text.regexp(rf'^/sell_(\d+)(@{BOT_USERNAME})?(?:\s+(\d+))?$'))
async def sell_card_handler(message: Message):
    user_id = message.from_user.id
    try:
        logger.info(f"Sell command received from {user_id}: {message.text}")
        
        # Извлекаем данные из команды с помощью регулярного выражения
        match = re.match(rf'^/sell_(\d+)(@{BOT_USERNAME})?(?:\s+(\d+))?$', message.text)
        if not match:
            logger.warning(f"Invalid command format: {message.text}")
            return await message.answer("❌ Неверный формат. Используйте: /sell_<номер карты> [количество]")
        
        card_id = int(match.group(1))
        quantity = int(match.group(3)) if match.group(3) else 1
        
        logger.info(f"Card ID extracted: {card_id}, quantity: {quantity}")
        
        if quantity <= 0:
            logger.error("Quantity must be positive")
            return await message.answer("❌ Количество должно быть положительным числом")
        
        # Выполняем продажу
        logger.info(f"Processing sell: user={user_id}, card={card_id}, qty={quantity}")
        success, result = sell_card(user_id, card_id, quantity)
        
        if success:
            logger.info(f"Sell successful: {result}")
            # Форматируем вывод суммы
            formatted_result = re.sub(r'\$([\d,]+)(?:\.00)?', 
                                    lambda m: f"${format_number(float(m.group(1).replace(',', '')), True)}", 
                                    result)
            await message.answer(f"✅ {formatted_result}")
        else:
            logger.warning(f"Sell failed: {result}")
            await message.answer(f"❌ {result}")
            
    except Exception as e:
        logger.error(f"Unexpected error in sell handler: {str(e)}", exc_info=True)
        await message.answer("❌ Произошла ошибка при обработке команды")
        
import html                      
@dp.message(Command("chatinfo"))
async def chat_info_handler(message: Message):
    if message.chat.type == 'private':
        await message.answer("💬 Эта команда работает только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, привязан ли пользователь к этому чату
    cursor.execute('SELECT 1 FROM chat_members WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
    if not cursor.fetchone():
        await message.answer("❌ <b>Доступ запрещен</b>\n\nУ вас нет доступа к статистике этого чата. Вы не являетесь его представителем.", parse_mode='HTML')
        return
    
    chat = get_chat_info(chat_id)
    
    if not chat:
        await message.answer("🤝 <b>Чат не участвует</b>\n\nЭтой чат не участвует в битве. Используй /join_chat чтобы присоединиться!", parse_mode='HTML')
        return
    
    place = get_chat_rank(chat_id)
    members = get_chat_members(chat_id)
    total_chat_income = chat[2]  # Общий заработанный доход чата
    
    # Рассчитываем общий текущий доход чата
    total_chat_current_income = 0.0
    cursor.execute('SELECT user_id FROM chat_members WHERE chat_id = ?', (chat_id,))
    chat_members = cursor.fetchall()
    
    for member in chat_members:
        member_id = member[0]
        try:
            member_income = calculate_base_income(member_id)
            total_chat_current_income += member_income
        except Exception as e:
            logger.error(f"Error calculating income for user {member_id}: {e}")
            continue
    
    # Формируем текст сообщения
    text = f"""
🏆 <b>СТАТИСТИКА ЧАТА</b>

<b>Название:</b> {html.escape(chat[1])}
<b>Рейтинг:</b> #{place}
<b>Участников:</b> {len(members)} 👥

<b>Общий доход:</b>
{total_chat_income:,.2f} BTC 💰

<b>Текущий доход / 10 мин:</b>
{total_chat_current_income:,.2f} BTC ⚡

📈 <b>Активных майнеров:</b> {sum(1 for member in chat_members if calculate_base_income(member[0]) > 0)}/{len(members)}

🔍 <b>Дополнительные команды:</b>
• /members - Статистика участников
• /top_chat - Общий рейтинг чатов
• /stats - Ваша личная статистика
"""
    
    await message.answer(text, parse_mode='HTML')
    
    
@dp.message(Command("members"))
async def members_info_handler(message: Message):
    if message.chat.type == 'private':
        await message.answer("💬 Эта команда работает только в групповых чатах!")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, привязан ли пользователь к этому чату
    cursor.execute('SELECT 1 FROM chat_members WHERE user_id = ? AND chat_id = ?', (user_id, chat_id))
    if not cursor.fetchone():
        await message.answer("❌ <b>Доступ запрещен</b>\n\nУ вас нет доступа к статистике этого чата. Вы не являетесь его представителем.", parse_mode='HTML')
        return
    
    # Получаем всех участников чата
    cursor.execute('SELECT user_id FROM chat_members WHERE chat_id = ?', (chat_id,))
    chat_members = cursor.fetchall()
    
    # Рассчитываем доход каждого участника и сортируем
    member_incomes = []
    for member in chat_members:
        member_id = member[0]
        try:
            member_income = calculate_base_income(member_id)
            member_incomes.append((member_id, member_income))
        except Exception as e:
            logger.error(f"Error calculating income for user {member_id}: {e}")
            continue
    
    member_incomes.sort(key=lambda x: x[1], reverse=True)
    
    # Получаем информацию о топ-10 участниках
    top_contributors = []
    for i, (member_id, member_income) in enumerate(member_incomes[:10]):
        try:
            member_info = await bot.get_chat_member(chat_id, member_id)
            user_name = member_info.user.full_name or f"Участник {member_id}"
            user_name = html.escape(user_name)
            
            # Получаем общий исторический вклад пользователя
            cursor.execute('''
            SELECT COALESCE(SUM(btc_income), 0) 
            FROM chat_income_log 
            WHERE user_id = ? AND chat_id = ?
            ''', (member_id, chat_id))
            total_contribution = cursor.fetchone()[0]
            
            top_contributors.append({
                'name': user_name,
                'current_income': member_income,
                'total_contribution': total_contribution
            })
            
        except Exception as e:
            logger.error(f"Error getting member info for {member_id}: {e}")
            continue
    
    # Формируем текст сообщения
    text = f"""
👥 <b>СТАТИСТИКА УЧАСТНИКОВ</b>

<b>Всего участников:</b> {len(chat_members)}
"""
    
    if top_contributors:
        text += """
🏅 <b>ТОП-10 УЧАСТНИКОВ ПО ДОХОДУ:</b>
"""
        for i, member in enumerate(top_contributors, 1):
            current_income = f"{member['current_income']:,.2f}".replace(",", " ")
            total_contribution = f"{member['total_contribution']:,.2f}".replace(",", " ")
            
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            
            # Обрезаем длинные имена
            name = member['name']
            if len(name) > 15:
                name = name[:12] + "..."
            
            text += f"\n{medal} {name}"
            text += f"\n   📊 Сейчас: {current_income} BTC / 10 мин"
            text += f"\n   💰 Всего: {total_contribution} BTC\n"
    else:
        text += "\n📊 <i>Пока нет данных об участниках</i>\n"
    
    text += f"""
📈 <b>Активных майнеров:</b> {sum(1 for _, income in member_incomes if income > 0)}/{len(chat_members)}

🔍 <b>Дополнительные команды:</b>
• /chatinfo - Общая статистика чата
• /stats - Ваша личная статистика
"""
    
    await message.answer(text, parse_mode='HTML')
                        
@dp.message(Command("top_chat"))
async def top_chat_handler(message: Message):
    user_id = message.from_user.id
    top_chats = get_top_chats(15)
    
    if not top_chats:
        await message.answer("Пока нет активных чатов в битве.")
        return
    
    # Получаем ID чата, откуда отправлена команда (если команда из группы)
    current_chat_id = message.chat.id if message.chat.type != "private" else None
    
    text = "🔥<b>Топ чатов</b>\n\n"
    
    # Убираем дубликаты чатов
    seen_chats = set()
    unique_chats = []
    for chat in top_chats:
        if chat['chat_id'] not in seen_chats:
            seen_chats.add(chat['chat_id'])
            unique_chats.append(chat)
    
    # Эмодзи для мест
    place_emojis = {
        1: "🥇",
        2: "🥈", 
        3: "🥉",
        4: "4️⃣",
        5: "5️⃣",
        6: "6️⃣",
        7: "7️⃣",
        8: "8️⃣",
        9: "9️⃣",
        10: "🔟"
    }
    
    for i, chat in enumerate(unique_chats[:10], 1):  # Только топ-10
        # Получаем эмодзи для места
        place_emoji = place_emojis.get(i, f"{i}.")
        
        # Форматируем BTC с сокращением
        btc_earned = chat['weekly_btc_earned']
        formatted_btc = format_number_short(btc_earned)
        
        # Определяем отступ для выравнивания
        if i == 10:  # Для 10 места меньший отступ
            indent = "  "
        else:
            indent = "   "
        
        # Формируем строку чата
        text += f"{place_emoji} {chat['title']}\n"
        text += f"{indent}💎{formatted_btc} BTC | {chat['members_count']} 🚩\n"
        
        # Добавляем разделитель после каждого чата, кроме последнего
        if i < 10:
            text += "➖➖➖➖➖\n"
    
    text += "\n🎁Каждое воскресенье 8 лучших игроков чата и 2 случайных участника из чатов топ-10 получают 👑 БЕСПЛАТНЫЙ PREMIUM!"
    
    await message.answer(text, parse_mode='HTML')
    
async def test_premium_distribution(message: Message):
    # Проверяем, что команду вызывает владелец
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        # Получаем топ чатов (как в реальной функции)
        top_chats = get_top_chats(10)
        
        if not top_chats:
            await message.answer("❌ Нет активных чатов для тестирования")
            return

        report = ["🔍 <b>ТЕСТОВОЕ РАСПРЕДЕЛЕНИЕ ПРЕМИУМА</b>\n\n"]
        report.append("⚠️ <b>Внимание! Это реальное распределение премиум-статусов!</b>\n\n")
        report.append(f"Найдено чатов в топе: {len(top_chats)}\n\n")

        total_winners = 0
        
        for i, chat in enumerate(top_chats, 1):
            members = get_chat_members(chat['chat_id'])
            if not members:
                report.append(f"{i}. {chat['title']} - нет участников\n")
                continue

            if len(members) <= 10:
                winners = members
                report.append(f"{i}. {chat['title']} - все {len(members)} участников получат премиум\n")
            else:
                # Сортируем по доходу (8 лучших)
                members_with_income = []
                for user_id in members:
                    user = get_user(user_id)
                    if user:
                        members_with_income.append((user_id, user[4]))  # income_btc
                
                members_with_income.sort(key=lambda x: x[1], reverse=True)
                top_members = [x[0] for x in members_with_income[:8]]
                other_members = [x[0] for x in members_with_income[8:]]
                random_winners = random.sample(other_members, min(2, len(other_members)))
                winners = top_members + random_winners
                
                report.append(
                    f"{i}. {chat['title']} - {len(winners)} победителей:\n"
                    f"   ▸ Топ-8 по доходу\n"
                    f"   ▸ 2 случайных из {len(other_members)} остальных\n"
                )

            # Собираем имена победителей для отчета и выдаем премиум
            winner_names = []
            for user_id in winners:
                try:
                    user = await bot.get_chat(user_id)
                    name = user.full_name or f"ID{user_id}"
                    
                    # Проверяем, есть ли уже премиум
                    if is_premium(user_id):
                        premium_status = " (уже премиум)"
                    else:
                        # Выдаем премиум на 24 часа
                        premium_until = (datetime.now() + timedelta(days=1)).isoformat()
                        cursor.execute('''
                        INSERT OR REPLACE INTO premium_users (user_id, premium_until)
                        VALUES (?, ?)
                        ''', (user_id, premium_until))
                        conn.commit()
                        premium_status = " (новый премиум)"
                    
                    winner_names.append(f"{name}{premium_status}")
                except Exception as e:
                    logger.error(f"Error getting user info: {e}")
                    winner_names.append(f"ID{user_id} (ошибка)")
                    continue

            total_winners += len(winners)
            report.append("   Победители: " + ", ".join(winner_names) + "\n\n")

        # Добавляем итоговую статистику
        report.append(f"\n<b>ИТОГО:</b> Выдано премиум-статусов: {total_winners}\n")
        report.append("\nСтатистика чатов сброшена, премиум выдан на 24 часа.")

        # Сбрасываем статистику чатов (как при реальном распределении)
        reset_weekly_stats()
        
        # Разбиваем сообщение на части, если оно слишком длинное
        full_report = "".join(report)
        for i in range(0, len(full_report), 4000):
            await message.answer(full_report[i:i+4000], parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error in test_premium_distribution: {e}")
        await message.answer(f"❌ Ошибка при распределении премиумов: {str(e)}")
        conn.rollback()
            
            
@dp.message(F.text.regexp(r'^!апгрейд$'))
async def upgrade_alias(message: Message):
    await upgrade_command(message)
    
async def upgrade_command(message: Message):
    user_id = message.from_user.id
    user = get_user(user_id)
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
        return
    
    user_cards, _ = get_user_cards(user_id)
    if not user_cards:
        await message.answer("У вас нет видеокарт для улучшения!")
        return
    
    text = "🛠️ <b>Апгрейдерская</b>\n\n"
    text += "<b>Рабочие видеокарты:</b>\n\n"
    
    for card_id, count in user_cards:
        card = next((c for c in GRAPHICS_CARDS if c['id'] == card_id), None)
        if card and card_id < len(GRAPHICS_CARDS):  # Проверяем, что есть следующая карта
            next_card = GRAPHICS_CARDS[card_id]  # Следующая карта в списке
            text += (
                f"x{count} {card['name']} - {format_number(card['income'] * count)} BTC/10мин.\n"
                f"Апгрейд - /upgrade_{card_id}\n\n"
            )
    
    await message.answer(text, parse_mode='HTML')                
                                        
@dp.message(F.text.regexp(r'^/upgrade_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def upgrade_card_handler(message: Message):
    user_id = message.from_user.id
    try:
        # Извлекаем ID карты из команды (игнорируя @username если есть)
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        card_id = int(command_text.split('_')[1])
        
        # Проверяем, есть ли такая карта у пользователя
        cursor.execute('SELECT count FROM user_cards WHERE user_id = ? AND card_id = ?', (user_id, card_id))
        card_data = cursor.fetchone()
        
        if not card_data or card_data[0] <= 0:
            await message.answer("❌ У вас нет такой видеокарты!")
            return
            
        # Проверяем, что есть следующая карта для апгрейда
        if card_id >= len(GRAPHICS_CARDS):
            await message.answer("❌ Эта видеокарта уже максимального уровня!")
            return
            
        current_card = GRAPHICS_CARDS[card_id-1]
        next_card = GRAPHICS_CARDS[card_id]
        
        # Создаем клавиатуру с вариантами рисков
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text="🟢 Средний риск (50% на +1 уровень)",
                callback_data=f"upgrade_{user_id}_{card_id}_1"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="🟡 Высокий риск (25% на +2 уровня)",
                callback_data=f"upgrade_{user_id}_{card_id}_2"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="🟠 Безумный риск (10% на +3 уровня)",
                callback_data=f"upgrade_{user_id}_{card_id}_3"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="🔴 Ультра риск (5% на +4 уровня)",
                callback_data=f"upgrade_{user_id}_{card_id}_4"
            )
        )
        
        text = (
            f"🛠️ {message.from_user.full_name}, Вы выбрали {current_card['name']}.\n"
            "Выберите тип улучшения:\n\n"
            "🟢 <b>Средний риск:</b>\n"
            "Шанс улучшения: 50%\n"
            "Прокачка на 1 уровень\n\n"
            "🟡 <b>Высокий риск:</b>\n"
            "Шанс улучшения: 25%\n"
            "Прокачка на 2 уровня\n\n"
            "🟠 <b>Безумный риск:</b>\n"
            "Шанс улучшения: 10%\n"
            "Прокачка на 3 уровня\n\n"
            "🔴 <b>Ультра риск:</b>\n"
            "Шанс улучшения: 5%\n"
            "Прокачка на 4 уровня"
        )
        
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in upgrade handler: {e}")
        await message.answer("❌ Произошла ошибка при обработке команды")
        
@dp.callback_query(F.data.startswith("upgrade_"))
async def process_upgrade_callback(callback: CallbackQuery):
    await callback.answer()
    try:
        parts = callback.data.split('_')
        if len(parts) != 4:
            return
            
        callback_user_id = int(parts[1])
        card_id = int(parts[2])
        levels = int(parts[3])
        
        if callback.from_user.id != callback_user_id:
            return
            
        # Проверяем, есть ли такая видеокарта у пользователя
        cursor.execute('SELECT count FROM user_cards WHERE user_id = ? AND card_id = ?', 
                      (callback_user_id, card_id))
        card_data = cursor.fetchone()
        
        if not card_data or card_data[0] <= 0:
            try:
                await callback.message.edit_text(
                    "❌ У вас больше нет этой видеокарты для апгрейда!",
                    reply_markup=None
                )
            except:
                pass
            return
            
        # Validate target card
        max_card_id = len(GRAPHICS_CARDS)
        if card_id >= max_card_id:
            try:
                await callback.message.edit_text(
                    "❌ Эта видеокарта уже максимального уровня!",
                    reply_markup=None
                )
            except:
                pass
            return
            
        target_card_id = min(card_id + levels, max_card_id)
        if target_card_id == card_id:
            try:
                await callback.message.edit_text(
                    "❌ Невозможно улучшить карту!",
                    reply_markup=None
                )
            except:
                pass
            return
            
        # Determine success chance
        chances = {1: 50, 2: 25, 3: 10, 4: 5}
        chance = chances.get(levels, 50)
        success = random.randint(1, 100) <= chance

        with conn:
            # Always remove one selected card (regardless of success)
            if card_data[0] > 1:
                cursor.execute('''
                UPDATE user_cards SET count = count - 1 
                WHERE user_id = ? AND card_id = ?
                ''', (callback_user_id, card_id))
            else:
                cursor.execute('''
                DELETE FROM user_cards 
                WHERE user_id = ? AND card_id = ?
                ''', (callback_user_id, card_id))
            
            # Обновляем прогресс BP - каждая попытка апгрейда считается
            update_bp_task_progress(callback_user_id, "upgrade")
            
            if success:
                # Check if user already has the target card
                cursor.execute('SELECT count FROM user_cards WHERE user_id = ? AND card_id = ?', 
                              (callback_user_id, target_card_id))
                target_card_data = cursor.fetchone()
                
                if target_card_data:
                    # Increment existing card count
                    cursor.execute('''
                    UPDATE user_cards SET count = count + 1 
                    WHERE user_id = ? AND card_id = ?
                    ''', (callback_user_id, target_card_id))
                else:
                    # Add new card with count = 1
                    cursor.execute('''
                    INSERT INTO user_cards (user_id, card_id, count)
                    VALUES (?, ?, 1)
                    ''', (callback_user_id, target_card_id))
                
                # Дополнительное обновление прогресса при успехе (если нужно)
                update_bp_task_progress(callback_user_id, "upgrade")
                try_give_5percent(callback_user_id, 1)  # ИСПРАВЛЕННАЯ СТРОКА
            
            # Update income
            calculate_income(callback_user_id)
            
        if success:
            current_card = GRAPHICS_CARDS[card_id-1]
            new_card = GRAPHICS_CARDS[target_card_id-1]
            await callback.message.edit_text(
                f"🎉 Успех! Ваша {current_card['name']} улучшена до {new_card['name']}!\n"
                f"Вы получили 1 {new_card['name']}\n"
                f"Новый доход: {format_number(new_card['income'])} BTC/10мин",
                reply_markup=None
            )
        else:
            current_card = GRAPHICS_CARDS[card_id-1]
            await callback.message.edit_text(
                f"💥 Неудача! Ваша {current_card['name']} сломана при попытке улучшения.",
                reply_markup=None
            )
            
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in upgrade callback: {e}")
        await callback.answer("❌ Произошла ошибка при обработке апгрейда", show_alert=True)

  
def generate_promo_code(length=16):
    """Генерирует случайный промокод"""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def create_promo_code(creator_id: int, uses: int, multiplier: float) -> str:
    """Создает новый промокод"""
    try:
        code = generate_promo_code()
        cursor.execute('''
        INSERT INTO promo_codes (code, creator_id, uses_left, max_uses, income_multiplier, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (code, creator_id, uses, uses, multiplier, datetime.now().isoformat()))
        conn.commit()
        return code
    except sqlite3.IntegrityError:
        # Если код уже существует (очень маловероятно), пробуем снова
        return create_promo_code(creator_id, uses, multiplier)
    except Exception as e:
        logger.error(f"Error creating promo code: {e}")
        return None

def activate_promo_code(user_id: int, code: str) -> Tuple[bool, str]:
    """Активирует промокод для пользователя"""
    try:
        # Проверяем, не активировал ли уже пользователь этот промокод
        cursor.execute('''
        SELECT 1 FROM promo_activations pa
        JOIN promo_codes pc ON pa.promo_id = pc.id
        WHERE pa.user_id = ? AND pc.code = ?
        ''', (user_id, code))
        if cursor.fetchone():
            return False, "Вы уже активировали этот промокод"
        
        # Получаем информацию о промокоде
        cursor.execute('''
        SELECT id, uses_left, income_multiplier, max_uses FROM promo_codes
        WHERE code = ? AND uses_left > 0
        ''', (code,))
        promo_data = cursor.fetchone()
        
        if not promo_data:
            return False, "Промокод не найден или закончились активации"
        
        promo_id, uses_left, multiplier, max_uses = promo_data
        
        # Проверяем общее количество активаций этого промокода
        cursor.execute('''
        SELECT COUNT(*) FROM promo_activations
        WHERE promo_id = ?
        ''', (promo_id,))
        total_activations = cursor.fetchone()[0]
        
        # Проверяем, не превышено ли максимальное количество активаций
        if total_activations >= max_uses:
            return False, "Достигнут лимит активаций этого промокода"
        
        # Получаем доход пользователя
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        reward = user[4] * multiplier  # user[4] - income_btc
        
        # Обновляем баланс
        new_btc = user[3] + reward
        update_balance(user_id, btc=new_btc)
        
        # Уменьшаем количество оставшихся использований
        cursor.execute('''
        UPDATE promo_codes 
        SET uses_left = uses_left - 1 
        WHERE id = ?
        ''', (promo_id,))
        
        # Записываем активацию
        cursor.execute('''
        INSERT INTO promo_activations (user_id, promo_id, activated_at)
        VALUES (?, ?, ?)
        ''', (user_id, promo_id, datetime.now().isoformat()))
        
        conn.commit()
        
        hours = int(multiplier / 6)  # Переводим множитель в часы (6 - доход за 10 минут * 6 = 1 час)
        return True, (
            f"🎉 Промокод активирован!\n"
            f"Получено: {format_number(reward)} BTC (~{hours}ч. дохода)\n"
            f"Новый баланс: {format_number(new_btc)} BTC\n"
            f"Осталось активаций: {uses_left - 1}/{max_uses}"
        )
    except Exception as e:
        logger.error(f"Error activating promo code: {e}")
        conn.rollback()
        return False, "Произошла ошибка при активации промокода"
        
        
async def generate_promo(message: Message):
    """Генерирует 2 промокода (только для владельца)"""
    if message.from_user.id not in ADMINS:  # Замените на ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        user = get_user(message.from_user.id)
        if not user:
            await message.answer("Пользователь не найден")
            return
        
        # Генерируем первый промокод (300 использований, 10-40 часов)
        hours1 = random.randint(10, 40)
        multiplier1 = hours1 * 6  # 6 - чтобы получить множитель для 10-минутного дохода
        code1 = create_promo_code(message.from_user.id, 300, multiplier1)
        
        # Генерируем второй промокод (5 использований, 40-70 часов)
        hours2 = random.randint(40, 70)
        multiplier2 = hours2 * 6
        code2 = create_promo_code(message.from_user.id, 5, multiplier2)
        
        text = (
            f"✨ Заканчиваем неделю вкусным промокодом на 300 использований, дающим {hours1}ч. заработка с фермы!\n\n"
            f"<code>/promo {code1}</code> ⚡️ Отправляйте команду боту и наслаждайтесь ⚡️\n\n"
            f"Промокод для самых быстрых на {hours2}ч. заработка с фермы! (промокод ограничен 5 использованиями)!\n\n"
            f"<code>{code2}</code>"
        )
        
        await message.answer(text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error generating promo codes: {e}")
        await message.answer("❌ Произошла ошибка при генерации промокодов")

@dp.message(Command("test_weekly_reset"))
async def test_weekly_reset_cmd(message: Message):
    """Тестовая команда для проверки недельного сброса (только для админов)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        await message.answer("🔄 Запускаю тестовый сброс чатов и сбора...")

        # Сброс статистики чатов
        reset_weekly_stats()

        # Сброс сбора
        cursor.execute('''
        UPDATE server_fund
        SET status = 'cancelled', end_date = ?
        WHERE status = 'active'
        ''', (datetime.now().isoformat(),))

        create_new_fund()
        conn.commit()

        await message.answer(
            "✅ <b>Тестовый сброс завершён!</b>\n\n"
            "✅ Топ чатов сброшен\n"
            "✅ Сбор сброшен и создан новый",
            parse_mode='HTML'
        )

    except Exception as e:
        logger.error(f"Error in test_weekly_reset: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")
        conn.rollback()

@dp.message(Command("test_auto_promo"))
async def test_auto_promo_cmd(message: Message):
    """Тестовая команда для проверки автоматической генерации промокодов (только для админов)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        await message.answer("🎁 Генерирую тестовые промокоды...")

        # Генерируем первый промокод (300 использований, 10-40 часов)
        hours1 = random.randint(10, 40)
        multiplier1 = hours1 * 6
        code1 = create_promo_code(message.from_user.id, 300, multiplier1)

        # Генерируем второй промокод (5 использований, 40-70 часов)
        hours2 = random.randint(40, 70)
        multiplier2 = hours2 * 6
        code2 = create_promo_code(message.from_user.id, 5, multiplier2)

        promo_text = (
            f"✨ Новые промокоды на этой неделе!\n\n"
            f"🔹 Промокод на {hours1}ч. заработка (300 использований):\n"
            f"/promo {code1}\n\n"
            f"🔹 Промокод для быстрых на {hours2}ч. заработка (5 использований):\n"
            f"{code2}"
        )

        await message.answer(
            "🎁 <b>Тестовые промокоды сгенерированы!</b>\n\n"
            "━━━━━━━━━━━━━━━\n\n" + promo_text,
            parse_mode='HTML'
        )

    except Exception as e:
        logger.error(f"Error in test_auto_promo: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("check_scheduler"))
async def check_scheduler_cmd(message: Message):
    """Проверка запланированных задач (только для админов)"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        jobs_info = []
        jobs_info.append("📅 <b>ЗАПЛАНИРОВАННЫЕ ЗАДАЧИ</b>\n")

        # Получаем все задачи из глобального scheduler
        # Нужно будет добавить это в main после создания scheduler
        from apscheduler.schedulers.asyncio import AsyncIOScheduler

        # Информация о воскресных задачах
        jobs_info.append("\n🔄 <b>Еженедельные задачи (воскресенье):</b>")
        jobs_info.append("18:00 - Выдача премиума топ-10 чатам")
        jobs_info.append("18:01 - Сброс топа чатов и сбора")
        jobs_info.append("18:05 - Генерация промокодов")

        jobs_info.append("\n\n✅ Задачи добавлены в scheduler")
        jobs_info.append("⏰ Timezone: Europe/Moscow")
        jobs_info.append("\n💡 Для теста используйте:")
        jobs_info.append("/test_weekly_reset")
        jobs_info.append("/test_auto_promo")

        await message.answer("\n".join(jobs_info), parse_mode='HTML')

    except Exception as e:
        logger.error(f"Error in check_scheduler: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("create_promo"))
async def create_custom_promo(message: Message):
    """Создает промокод с указанными параметрами (только для владельца)"""
    if message.from_user.id not in ADMINS:  # Замените на ваш ID
        await message.answer("❌ У вас нет прав на эту команду")
        return
    
    try:
        # Проверяем формат команды: /create_promo [использований] [часы]
        parts = message.text.split()
        if len(parts) != 3:
            await message.answer(
                "ℹ️ Формат команды:\n"
                "/create_promo [количество использований] [часы дохода]\n\n"
                "Пример:\n"
                "/create_promo 100 24 - создаст промокод на 100 использований, дающий 24 часа дохода"
            )
            return
            
        uses = int(parts[1])
        hours = int(parts[2])
        
        if uses <= 0 or hours <= 0:
            await message.answer("❌ Количество использований и часы должны быть положительными числами")
            return
            
        # Конвертируем часы в множитель (6 - чтобы получить множитель для 10-минутного дохода)
        multiplier = hours * 6
        
        # Создаем промокод
        code = create_promo_code(message.from_user.id, uses, multiplier)
        
        if not code:
            await message.answer("❌ Не удалось создать промокод")
            return
            
        await message.answer(
            f"🎉 Промокод успешно создан!\n\n"
            f"🔹 Код: <code>{code}</code>\n"
            f"🔹 Использований: {uses}\n"
            f"🔹 Награда: ~{hours}ч. дохода с фермы\n\n"
            f"Активировать: /promo {code}",
            parse_mode='HTML'
        )
        
    except ValueError:
        await message.answer("❌ Неверный формат чисел. Используйте целые числа для количества использований и часов")
    except Exception as e:
        logger.error(f"Error creating custom promo: {e}")
        await message.answer("❌ Произошла ошибка при создании промокода")
        
@dp.message(F.text.regexp(r'^/promo\s+(\w+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def use_promo(message: Message):
    """Активирует промокод"""
    try:
        # Извлекаем промокод из команды
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        code = command_text.split()[1]
        
        success, result = activate_promo_code(message.from_user.id, code)
        await message.answer(f"✅ {result}" if success else f"❌ {result}")
    except IndexError:
        await message.answer("❌ Неверный формат. Используйте: /promo [код]")
    except Exception as e:
        logger.error(f"Error in promo command: {e}")
        await message.answer("❌ Произошла ошибка при обработке промокода")                                    
        

conn = sqlite3.connect('data/miner.db', check_same_thread=False)
cursor = conn.cursor()


cursor.execute('''
CREATE TABLE IF NOT EXISTS user_bp_progress (
    user_id INTEGER PRIMARY KEY,
    current_level INTEGER DEFAULT 1,
    current_exp INTEGER DEFAULT 0,
    current_task_id INTEGER,
    task_progress INTEGER DEFAULT 0,
    last_task_date TEXT,
    completed_tasks INTEGER DEFAULT 0,
    FOREIGN KEY (current_task_id) REFERENCES BP_TASKS(id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_bp_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    task_id INTEGER,
    completion_date TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (task_id) REFERENCES BP_TASKS(id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS user_bp_rewards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    level INTEGER,
    reward_amount REAL,
    claim_date TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
)
''')

conn.commit()

# Задания боевого пропуска
BP_TASKS = [
    # Работа
    {"id": 1, "name": "Сходите на работу 3 раза", "target": 3, "type": "work"},
    {"id": 2, "name": "Сходите на работу 5 раз", "target": 5, "type": "work"},
    
    # Монетка
    {"id": 3, "name": "Сыграть в монетку 3 раза", "target": 3, "type": "coin_flip"},
    {"id": 4, "name": "Сыграть в монетку 5 раз", "target": 5, "type": "coin_flip"},
    {"id": 5, "name": "Выиграть в монетку на орле 1 раз", "target": 1, "type": "coin_win_heads"},
    {"id": 6, "name": "Выиграть в монетку на решке 1 раз", "target": 1, "type": "coin_win_tails"},
    
    # Кубики
    {"id": 7, "name": "Сыграть в кубики 3 раза", "target": 3, "type": "dice_play"},
    {"id": 8, "name": "Выиграть в кубики на любом числе", "target": 1, "type": "dice_win_any"},
    {"id": 9, "name": "Выиграть в кубики на 1", "target": 1, "type": "dice_win_1"},
    {"id": 10, "name": "Выиграть в кубики на 6", "target": 1, "type": "dice_win_6"},
    
    # Слоты
    {"id": 11, "name": "Сыграть в слоты 3 раза", "target": 3, "type": "slots_play"},
    {"id": 12, "name": "Выиграть в слотах (любая комбинация)", "target": 1, "type": "slots_win"},
    {"id": 13, "name": "Получить 2 одинаковых символа в слотах", "target": 1, "type": "slots_2x"},
    
    # Минное поле
    {"id": 14, "name": "Сыграть в минное поле 1 раз", "target": 1, "type": "mines_play"},
    {"id": 15, "name": "Открыть 5 ячеек в минном поле", "target": 5, "type": "mines_cells"},
    {"id": 16, "name": "Найти сокровище в минном поле", "target": 1, "type": "mines_treasure"},
    
    # Апгрейд
    {"id": 17, "name": "Сделать апгрейд видеокарты 1 раз", "target": 1, "type": "upgrade"},
    {"id": 18, "name": "Сделать апгрейд видеокарты 3 раза", "target": 3, "type": "upgrade"},
    
    # Комбинированные
    {"id": 19, "name": "Сыграть в любую игру 5 раз", "target": 5, "type": "any_game"},
    {"id": 20, "name": "Выиграть в любой игре 1 раз", "target": 1, "type": "any_win"}
]

# Множители наград по уровням
BP_MULTIPLIERS = {
    1: 60, 2: 66, 3: 72, 4: 78, 5: 84, 6: 90, 7: 96, 8: 102, 9: 108, 10: 114,
    11: 120, 12: 126, 13: 132, 14: 138, 15: 144, 16: 150, 17: 156, 18: 162,
    19: 168, 20: 174, 21: 180, 22: 186, 23: 192, 24: 198, 25: 204, 26: 210,
    27: 216, 28: 222, 29: 228, 30: 234, 31: 240
}

def get_days_in_current_month():
    """Возвращает количество дней в текущем месяце"""
    now = datetime.now()
    if now.month == 12:
        return 31
    next_month = datetime(now.year, now.month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day.day

def get_user_bp_level(user_id: int) -> int:
    """Получает текущий уровень боевого пропуска пользователя"""
    cursor.execute('SELECT current_level FROM user_bp_progress WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    return result[0] if result else 1

def get_user_bp_task(user_id: int) -> dict:
    """Получает текущее задание и прогресс пользователя"""
    cursor.execute('''
    SELECT current_task_id, task_progress, last_task_date, current_level, current_exp 
    FROM user_bp_progress 
    WHERE user_id = ?
    ''', (user_id,))
    result = cursor.fetchone()
    
    if not result:
        return None
    
    task_id, progress, last_date, level, exp = result
    task = next((t for t in BP_TASKS if t['id'] == task_id), None)
    
    if not task:
        return None
    
    return {
        "task": task,
        "progress": progress,
        "last_date": datetime.fromisoformat(last_date) if last_date else None,
        "level": level,
        "exp": exp
    }

def assign_random_task(user_id: int):
    """Назначает случайное задание пользователю"""
    now = datetime.now()
    
    # Получаем все доступные задания
    available_tasks = BP_TASKS.copy()
    
    # Исключаем задания, уже выполненные в этом месяце
    cursor.execute('''
    SELECT DISTINCT task_id FROM user_bp_history 
    WHERE user_id = ? AND strftime('%Y-%m', completion_date) = ?
    ''', (user_id, now.strftime('%Y-%m')))
    completed_tasks = [row[0] for row in cursor.fetchall()]
    
    if completed_tasks:
        available_tasks = [t for t in available_tasks if t['id'] not in completed_tasks]
    
    # Если все задания выполнены, выбираем из всех
    if not available_tasks:
        available_tasks = BP_TASKS
    
    # Выбираем случайное задание
    task = random.choice(available_tasks)
    
    # Обновляем или создаем запись о прогрессе
    cursor.execute('''
    INSERT OR REPLACE INTO user_bp_progress 
    (user_id, current_task_id, task_progress, last_task_date) 
    VALUES (?, ?, 0, ?)
    ''', (user_id, task['id'], now.isoformat()))
    
    conn.commit()
    return task

async def reset_daily_bp_tasks():
    """Сбрасывает ежедневные задания и назначает новые в 00:05 по Москве (без изменения уровня и опыта)"""
    moscow_tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(moscow_tz)
    logger.info(f"Reset BP tasks triggered at {now}")

    try:
        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()

        for (user_id,) in users:
            # Проверяем, есть ли запись в user_bp_progress
            cursor.execute('SELECT current_level, current_exp FROM user_bp_progress WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()

            # Получаем доступные задания
            available_tasks = BP_TASKS.copy()
            
            # Исключаем задания, выполненные в текущем месяце
            cursor.execute('''
                SELECT DISTINCT task_id FROM user_bp_history 
                WHERE user_id = ? AND strftime('%Y-%m', completion_date) = ?
            ''', (user_id, now.strftime('%Y-%m')))
            completed_tasks = [row[0] for row in cursor.fetchall()]
            
            if completed_tasks:
                available_tasks = [t for t in available_tasks if t['id'] not in completed_tasks]
            
            # Если все задания выполнены, используем все доступные
            if not available_tasks:
                available_tasks = BP_TASKS
            
            # Выбираем случайное задание
            if available_tasks:
                task = random.choice(available_tasks)
            else:
                logger.error(f"No available tasks for user {user_id}")
                continue

            if not result:
                # Создаем новую запись, если пользователь еще не в user_bp_progress
                cursor.execute('''
                    INSERT INTO user_bp_progress 
                    (user_id, current_level, current_exp, current_task_id, task_progress, last_task_date, completed_tasks)
                    VALUES (?, 1, 0, ?, 0, ?, 0)
                ''', (user_id, task['id'], now.isoformat()))
                logger.info(f"Created BP progress for user {user_id} with level 1, task_id: {task['id']}")
            else:
                # Обновляем существующую запись, сохраняя current_level и current_exp
                old_level, old_exp = result
                cursor.execute('''
                    UPDATE user_bp_progress 
                    SET 
                        current_task_id = ?,
                        task_progress = 0,
                        last_task_date = ?
                    WHERE user_id = ?
                ''', (task['id'], now.isoformat(), user_id))
                logger.info(f"Reset task for user {user_id}, level remains {old_level}, exp remains {old_exp}, new task_id: {task['id']}")

        conn.commit()
        logger.info("Daily BP tasks reset and new tasks assigned successfully")

    except Exception as e:
        logger.error(f"Error resetting BP tasks: {e}", exc_info=True)
        conn.rollback()    
              
@dp.message(Command("upbp"))
async def increase_all_bp_levels(message: Message):
    """Команда для повышения уровня БП всем пользователям (только для владельца)"""
    # Проверяем права доступа
    if message.from_user.id not in ADMINS:
        return

    try:
        # Получаем всех пользователей с BP
        cursor.execute('SELECT user_id, current_level FROM user_bp_progress')
        users = cursor.fetchall()
        
        if not users:
            await message.answer("❌ В системе BP нет пользователей")
            return
            
        total_updated = 0
        max_level_reached = 0
        
        for user_id, current_level in users:
            new_level = current_level + 1
            
            # Проверяем максимальный уровень
            if new_level > 31:
                max_level_reached += 1
                continue
                
            # Повышаем уровень
            cursor.execute('''
            UPDATE user_bp_progress 
            SET current_level = ?
            WHERE user_id = ?
            ''', (new_level, user_id))
            total_updated += 1
        
        conn.commit()
        
        report = (
            f"✅ Уровень BP повышен для {total_updated} пользователей\n"
            f"• Уже на макс. уровне: {max_level_reached}\n"
            f"• Всего обработано: {len(users)} пользователей"
        )
        
        await message.answer(report)
        
    except Exception as e:
        logger.error(f"Error increasing BP levels: {e}")
        conn.rollback()
        await message.answer(f"❌ Ошибка при массовом повышении уровней: {str(e)}")                      

async def claim_bp_reward(user_id: int) -> Tuple[bool, str]:
    """Выдает награду за выполнение задания BP"""
    try:
        now = datetime.now()
        cursor.execute('BEGIN TRANSACTION')

        # Получаем текущий прогресс из user_bp_progress
        cursor.execute('''
        SELECT current_task_id, task_progress, current_level, current_exp 
        FROM user_bp_progress 
        WHERE user_id = ?
        ''', (user_id,))
        task_data = cursor.fetchone()

        if not task_data:
            cursor.execute('ROLLBACK')
            return False, "Прогресс не найден"

        task_id, progress, level, exp = task_data
        task = next((t for t in BP_TASKS if t['id'] == task_id), None)

        if not task or progress < task['target']:
            cursor.execute('ROLLBACK')
            return False, "Задание не выполнено"

        # Проверяем, не получал ли уже награду сегодня
        cursor.execute('''
        SELECT 1 FROM user_bp_rewards 
        WHERE user_id = ? AND date(claim_date) = date(?)
        ''', (user_id, now.isoformat()))
        if cursor.fetchone():
            cursor.execute('ROLLBACK')
            return False, "Вы уже получили награду сегодня"

        # Получаем доход пользователя
        user = get_user(user_id)
        if not user or user[4] <= 0:  # user[4] - income_btc
            cursor.execute('ROLLBACK')
            return False, "Недостаточный доход для награды"

        reward = user[4] * BP_MULTIPLIERS.get(level, 60)

        # Обновляем баланс
        update_balance(user_id, btc=user[3] + reward)

        # Записываем награду в user_bp_rewards
        cursor.execute('''
        INSERT INTO user_bp_rewards (user_id, level, reward_amount, claim_date)
        VALUES (?, ?, ?, ?)
        ''', (user_id, level, reward, now.isoformat()))

        # Обновляем прогресс в user_bp_progress
        cursor.execute('''
        UPDATE user_bp_progress 
        SET 
            current_level = current_level + 1,
            current_exp = 0,
            task_progress = 0,
            completed_tasks = completed_tasks + 1
        WHERE user_id = ?
        ''', (user_id,))

        # Записываем выполнение задания в user_bp_history
        cursor.execute('''
        INSERT INTO user_bp_history (user_id, task_id, completion_date)
        VALUES (?, ?, ?)
        ''', (user_id, task_id, now.isoformat()))

        cursor.execute('COMMIT')
        return True, f"Награда получена: {format_number(reward)} BTC"

    except Exception as e:
        logger.error(f"Ошибка выдачи награды BP: {str(e)}", exc_info=True)
        cursor.execute('ROLLBACK')
        return False, "Ошибка при выдаче награды"
        
def update_bp_task_progress(user_id: int, task_type: str, amount: int = 1):
    """Обновляет прогресс выполнения задания"""
    now = datetime.now()
    task_data = get_user_bp_task(user_id)
    
    if not task_data:
        return False
    
    task = task_data['task']
    
    # Проверяем соответствие типа задания
    if task['type'] == "dice_win_any" and task_type.startswith("dice_win_"):
        # Задание "Выиграть в кубики на любом числе"
        pass  # Принимаем любую победу в кубиках
    elif task['type'] == "slots_win" and task_type.startswith("slots_"):
        # Задание "Выиграть в слотах (любая комбинация)"
        pass  # Принимаем любой выигрыш в слотах
    elif task['type'] == "any_game" and task_type in ("coin_flip", "dice_play", "slots_play", "mines_play", "upgrade"):
        # Задание "Сыграть в любую игру"
        pass  # Принимаем любую игру
    elif task['type'] == "any_win" and ("win" in task_type or task_type in ("slots_2x", "slots_3x", "slots_jackpot", "mines_treasure")):
        # Задание "Выиграть в любой игре"
        pass  # Принимаем любой выигрыш
    elif task['type'] != task_type:
        return False
    
    # Проверяем, не выполнено ли задание уже
    if task_data['progress'] >= task['target']:
        return False
    
    # Обновляем прогресс
    new_progress = min(task_data['progress'] + amount, task['target'])
    
    # Проверяем, выполнено ли задание
    is_completed = new_progress >= task['target']
    
    cursor.execute('''
    UPDATE user_bp_progress 
    SET task_progress = ?, 
        last_task_date = ?
    WHERE user_id = ?
    ''', (new_progress, now.isoformat(), user_id))
    
    if is_completed:
        # Добавляем опыт и проверяем уровень
        cursor.execute('''
        UPDATE user_bp_progress 
        SET current_exp = current_exp + 1,
            completed_tasks = completed_tasks + 1
        WHERE user_id = ?
        ''', (user_id,))
        
        # Проверяем, повысился ли уровень
        cursor.execute('SELECT current_level, current_exp FROM user_bp_progress WHERE user_id = ?', (user_id,))
        level, exp = cursor.fetchone()
        days_in_month = get_days_in_current_month()
        
        if exp >= days_in_month and level < 31:
            cursor.execute('''
            UPDATE user_bp_progress 
            SET current_level = current_level + 1,
                current_exp = 0
            WHERE user_id = ?
            ''', (user_id,))
    
    conn.commit()
    return is_completed


async def check_bp_conditions(user_id: int) -> Tuple[bool, str]:
    """Проверяет условия для получения награды (используется и в /bp и в обработчике кнопки)"""
    now = datetime.now()
    
    # Проверяем наличие прогресса у пользователя в user_bp_progress
    cursor.execute('SELECT current_task_id, task_progress FROM user_bp_progress WHERE user_id = ?', (user_id,))
    progress_data = cursor.fetchone()
    if not progress_data:
        return False, "У вас нет активного задания"
    
    task_id, progress = progress_data
    task = next((t for t in BP_TASKS if t['id'] == task_id), None)
    if not task:
        return False, "Задание не найдено"
    
    # Проверяем, не получал ли уже награду сегодня
    cursor.execute('''
    SELECT 1 FROM user_bp_rewards 
    WHERE user_id = ? AND date(claim_date) = date(?)
    ''', (user_id, now.isoformat()))
    if cursor.fetchone():
        return False, "Вы уже получили награду сегодня"
    
    # Проверяем выполнение задания (только если награду еще не получали)
    if progress < task['target']:
        return False, f"Задание не выполнено ({progress}/{task['target']})"
    
    return True, "Условия выполнены"

@dp.message(Command("reset_all_bp"))
async def reset_all_bp_stats(message: Message):
    """Полный сброс статистики боевого пропуска для всех пользователей"""
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        # Получаем статистику перед сбросом
        cursor.execute('SELECT COUNT(*) FROM user_bp_progress')
        bp_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_bp_history')
        history_records = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_bp_rewards')
        reward_records = cursor.fetchone()[0]
        
        # Полностью очищаем все таблицы BP
        cursor.execute('DELETE FROM user_bp_progress')
        cursor.execute('DELETE FROM user_bp_history')
        cursor.execute('DELETE FROM user_bp_rewards')
        
        # Сбрасыем автоинкремент для таблиц с PRIMARY KEY AUTOINCREMENT
        cursor.execute('DELETE FROM sqlite_sequence WHERE name IN ("user_bp_history", "user_bp_rewards")')
        
        conn.commit()
        
        report = (
            f"🔄 <b>Полный сброс Battle Pass</b>\n\n"
            f"✅ Все таблицы BP полностью очищены!\n"
            f"• Удалено записей прогресса: {bp_users}\n"
            f"• Удалено записей истории: {history_records}\n"
            f"• Удалено записей наград: {reward_records}\n\n"
            f"<i>Все таблицы BP теперь пустые. Новые записи будут создаваться автоматически при использовании /bp</i>"
        )
        
        await message.answer(report, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"BP: Ошибка при сбросе статистики: {e}")
        conn.rollback()
        await message.answer(f"❌ Ошибка при сбросе статистики BP: {str(e)}")
@dp.message(Command("bp"))
async def bp_command(message: Message):
    user_id = message.from_user.id
    create_user(user_id, message.from_user.username)
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    now = datetime.now(moscow_tz)
    
    try:
        # Пытаемся получить данные задания
        task_data = get_user_bp_task(user_id)
        
        # Если задания нет, создаем новое
        if not task_data:
            assign_random_task(user_id)  # Используем существующую функцию
            task_data = get_user_bp_task(user_id)
            
        # Если все еще нет задания, показываем ошибку
        if not task_data:
            text = (
                f"🎮 <b>Боевой пропуск</b>\n\n"
                f"🔄 Система обновляется...\n"
                f"Попробуйте снова через несколько секунд."
            )
            await message.answer(text, parse_mode='HTML')
            return
            
        task = task_data['task']
        level = task_data.get('level', 1)
        days_in_month = get_days_in_current_month()
        
        # Проверяем прогресс задания
        cursor.execute('''
            SELECT task_progress FROM user_bp_progress WHERE user_id = ?
        ''', (user_id,))
        result = cursor.fetchone()
        
        # Если нет прогресса, устанавливаем 0
        if not result:
            progress = 0
            cursor.execute('UPDATE user_bp_progress SET task_progress = 0 WHERE user_id = ?', (user_id,))
            conn.commit()
        else:
            progress = result[0]
        
        # Проверяем, получал ли уже награду сегодня
        cursor.execute('''
            SELECT 1 FROM user_bp_rewards 
            WHERE user_id = ? AND date(claim_date) = date(?)
        ''', (user_id, now.isoformat()))
        already_claimed = cursor.fetchone() is not None
        
        # Получаем доход для расчета награды
        user = get_user(user_id)
        income = user[4] if user and user[4] > 0 else 0
        potential_reward = income * BP_MULTIPLIERS.get(level, 60) if income > 0 else 0
        
        if already_claimed:
            text = (
                f"🎮 <b>Боевой пропуск</b>\n\n"
                f"✅ Вы уже получили награду за сегодня!\n"
                f"Уровень: {level}/{days_in_month}\n"
                f"Следующее задание будет доступно завтра в 21:00."
            )
            await message.answer(text, parse_mode='HTML')
        elif progress >= task['target']:
            if not already_claimed:
                text = (
                    f"🎮 <b>Боевой пропуск</b>\n\n"
                    f"✅ Вы выполнили сегодняшнее задание!\n"
                    f"Уровень: {level}/{days_in_month}\n"
                    f"Награда: +{format_number(potential_reward)} BTC\n\n"
                    f"Нажмите кнопку ниже, чтобы получить награду."
                )
                builder = InlineKeyboardBuilder()
                builder.add(types.InlineKeyboardButton(
                    text="🎁 Получить награду",
                    callback_data=f"claim_bp_{user_id}"
                ))
                await message.answer(text, reply_markup=builder.as_markup(), parse_mode='HTML')
            else:
                text = (
                    f"🎮 <b>Боевой пропуск</b>\n\n"
                    f"⚠️ Ошибка: Задание выполнено, но награда уже получена ранее.\n"
                    f"Уровень: {level}/{days_in_month}\n"
                    f"Следующее задание будет доступно завтра в 21:00."
                )
                await message.answer(text, parse_mode='HTML')
        else:
            text = (
                f"🎮 <b>Боевой пропуск</b>\n\n"
                f"Ваш текущий уровень: {level}/{days_in_month} ✨\n"
                f"<b>Сегодняшнее задание:</b>\n"
                f"{task['name']}: {progress}/{task['target']}\n\n"
            )
            
            remaining = task['target'] - progress
            text += f"🔹 Осталось: {remaining} {get_task_unit(task['type'])}\n"
            text += f"\n🕘 Новые задания в 21:00"
            
            await message.answer(text, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"BP: Ошибка в команде /bp для {user_id}: {e}")
        # Если произошла ошибка, создаем нового пользователя в системе
        try:
            assign_random_task(user_id)  # Используем существующую функцию
            text = (
                f"🎮 <b>Боевой пропуск</b>\n\n"
                f"🔄 Система инициализирована!\n"
                f"Используйте команду /bp снова."
            )
            await message.answer(text, parse_mode='HTML')
        except Exception as inner_e:
            logger.error(f"BP: Критическая ошибка инициализации для {user_id}: {inner_e}")
            text = (
                f"🎮 <b>Боевой пропуск</b>\n\n"
                f"❌ Временная ошибка системы.\n"
                f"Попробуйте позже или обратитесь к администратору."
            )
            await message.answer(text, parse_mode='HTML')        
@dp.callback_query(F.data.startswith("claim_bp_"))
async def claim_bp_callback(callback: CallbackQuery):
    await callback.answer()
    try:
        user_id = int(callback.data.split('_')[2])
        if callback.from_user.id != user_id:
            return

        success, message = await claim_bp_reward(user_id)
        if not success:
            return

        # Получаем обновленные данные из user_bp_progress
        cursor.execute('''
        SELECT current_level FROM user_bp_progress WHERE user_id = ?
        ''', (user_id,))
        new_level = cursor.fetchone()[0]

        days_in_month = get_days_in_current_month()
        text = (
            f"🎮 <b>Боевой пропуск</b>\n\n"
            f"✅ {message}\n"
            f"Новый уровень: {new_level}/{days_in_month}\n\n"
            f"Следующее задание будет доступно завтра."
        )

        try:
            await callback.message.edit_text(text, parse_mode='HTML', reply_markup=None)
        except:
            await callback.message.answer(text, parse_mode='HTML')

        await callback.answer()

    except Exception as e:
        logger.error(f"Ошибка в обработчике BP: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)

def get_task_unit(task_type: str) -> str:
    """Возвращает единицы измерения для задачи"""
    if task_type in ('work', 'coin_flip', 'dice_win'):
        return "раз"
    elif task_type.startswith('coin_win'):
        return "побед"
    elif task_type.startswith('dice_win'):
        return "побед"
    return ""




        
                    
@dp.message(Command("reset_all_bp"))
async def reset_all_bp_stats(message: Message):
    # Проверяем, что команду вызывает владелец
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        # Обнуляем статистику BP для всех пользователей
        cursor.execute('''
        UPDATE user_bp_progress 
        SET 
            current_level = 1,
            current_exp = 0,
            last_claim_date = NULL,
            last_task_date = NULL,
            completed_tasks = 0,
            current_task_id = 0,
            task_progress = 0
        ''')
        
        # Очищаем историю выполнения заданий
        cursor.execute('DELETE FROM user_bp_history')
        
        # Очищаем историю получения наград
        cursor.execute('DELETE FROM user_bp_rewards')
        
        conn.commit()
        
        await message.answer("✅ Статистика Battle Pass успешно обнулена для всех пользователей!")
        
    except Exception as e:
        logger.error(f"Error resetting all BP stats: {e}")
        conn.rollback()
        await message.answer(f"❌ Ошибка при обнулении статистики BP: {str(e)}")        





        
                    


async def check_and_complete_fund():
    """Проверяет и завершает сбор средств если цель достигнута, иначе сбрасывает"""
    try:
        fund = get_current_fund()
        if not fund:
            return "❌ Нет активного сбора для проверки"
        
        if fund["amount"] >= fund["goal"]:
            # Цель достигнута - завершаем сбор
            await complete_fund(fund["id"])
            return "✅ Сбор завершен! Награды выданы участникам."
        else:
            # Цель не достигнута - сбрасываем сбор
            cursor.execute('''
            UPDATE server_fund 
            SET status = 'cancelled', end_date = ?
            WHERE id = ?
            ''', (datetime.now().isoformat(), fund["id"]))
            
            # Создаем новый сбор
            create_new_fund()
            
            conn.commit()
            return "❌ Цель сбора не достигнута. Сбор сброшен, создан новый."
            
    except Exception as e:
        logger.error(f"Error in check_and_complete_fund: {e}")
        conn.rollback()
        return f"❌ Произошла ошибка: {str(e)}"

# Обновляем команду endweeks с добавлением проверки сбора
@dp.message(Command("endweeks"))
async def weekly_tasks_command(message: Message):
    # Проверяем, что команду вызывает владелец
    if message.from_user.id not in ADMINS:
        return
    
    try:
        CHANNEL_ID = -1002780167646   # ID канала для отправки отчетов
        
        # 0. Проверяем и завершаем/сбрасываем сбор средств
        fund_result = await check_and_complete_fund()
        
        # 1. Генерация промокодов
        hours1 = random.randint(10, 40)
        multiplier1 = hours1 * 6
        code1 = create_promo_code(message.from_user.id, 100, multiplier1)
        
        hours2 = random.randint(40, 70)
        multiplier2 = hours2 * 6
        code2 = create_promo_code(message.from_user.id, 5, multiplier2)
        
        promo_text = (
            "✨ <b>Новые промокоды на этой неделе!</b>\n\n"
            f"🔹 Промокод на {hours1}ч. заработка (300 использований):\n"
            f"<code>/promo {code1}</code>\n\n"
            f"🔹 Промокод для быстрых на {hours2}ч. заработка (5 использований):\n"
            f"<code>{code2}</code>"
        )
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=promo_text,
            parse_mode='HTML'
        )
        
        # 2. Распределение премиум-наград за чаты
        top_chats = get_top_chats(10)
        premium_report = ["🏆 <b>Топ чатов этой недели:</b>\n\n"]
        
        for i, chat in enumerate(top_chats, 1):
            members = get_chat_members(chat['chat_id'])
            if not members:
                continue
                
            if len(members) <= 10:
                winners = members
            else:
                members_with_income = []
                for user_id in members:
                    user = get_user(user_id)
                    if user:
                        members_with_income.append((user_id, user[4]))
                
                members_with_income.sort(key=lambda x: x[1], reverse=True)
                top_members = [x[0] for x in members_with_income[:8]]
                other_members = [x[0] for x in members_with_income[8:]]
                random_winners = random.sample(other_members, min(2, len(other_members)))
                winners = top_members + random_winners
            
            premium_duration = timedelta(days=1)
            for user_id in winners:
                # Проверяем, есть ли уже премиум у пользователя
                cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
                result = cursor.fetchone()
                
                if result and result[0]:
                    # Если премиум есть - продлеваем
                    current_until = datetime.fromisoformat(result[0])
                    if current_until > datetime.now():
                        # Если премиум еще активен - добавляем к текущей дате
                        new_until = current_until + premium_duration
                    else:
                        # Если премиум истек - начинаем с текущей даты
                        new_until = datetime.now() + premium_duration
                else:
                    # Если премиума нет - устанавливаем новый
                    new_until = datetime.now() + premium_duration
                
                cursor.execute('''
                INSERT OR REPLACE INTO premium_users (user_id, premium_until)
                VALUES (?, ?)
                ''', (user_id, new_until.isoformat()))
            
            premium_report.append(
                f"{i}. {chat['title']} - {len(winners)} победителей\n"
            )
        
        # Отправляем отчет о премиум-наградах
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text="".join(premium_report),
            parse_mode='HTML'
        )
        
        # 3. Розыгрыш лотереи
        lottery_winners = await draw_lottery_winners(5)
        lottery_report = ["🎫 <b>Результаты лотереи:</b>\n\n"]
        
        for i, winner in enumerate(lottery_winners, 1):
            try:
                user = await bot.get_chat(winner['user_id'])
                name = user.full_name or f"ID {winner['user_id']}"
                lottery_report.append(
                    f"{i}. {name} - {format_number(winner['prize'], True)} $\n"
                )
            except:
                lottery_report.append(
                    f"{i}. ID {winner['user_id']} - {format_number(winner['prize'], True)} $\n"
                )
        
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text="".join(lottery_report),
            parse_mode='HTML'
        )
        
        # 4. Сброс статистики
        reset_weekly_stats()
        conn.commit()
        
        # Добавляем результат проверки сбора в ответ
        response = (
            f"✅ Все еженедельные задачи выполнены и отправлены в канал!\n\n"
            f"Результат проверки сбора:\n{fund_result}"
        )
        
        await message.answer(response)
        
    except Exception as e:
        logger.error(f"Error in weekly tasks: {e}")
        conn.rollback()
        await message.answer(f"❌ Ошибка при выполнении задач: {str(e)}")

                      
def get_user(user_id: int) -> Optional[tuple]:
    """Получает данные пользователя"""
    cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
    return cursor.fetchone()

def update_user_balance(user_id: int, usd: float = None, btc: float = None):
    """Обновляет баланс пользователg
   я (USD и BTC)"""
    try:
        if usd is not None:
            cursor.execute('UPDATE users SET usd_balance = ? WHERE user_id = ?', (usd, user_id))
        if btc is not None:
            cursor.execute('UPDATE users SET btc_balance = ? WHERE user_id = ?', (btc, user_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error updating balance: {e}")
        conn.rollback()

def get_user_businesses_count(user_id: int) -> int:
    """Получает количество бизнесов пользователя"""
    cursor.execute('SELECT COUNT(*) FROM user_businesses WHERE user_id = ?', (user_id,))
    return cursor.fetchone()[0]

def get_user_businesses(user_id: int) -> List[Dict]:
    """Получает все бизнесы пользователя"""
    cursor.execute('''
    SELECT business_id, level, last_income_time 
    FROM user_businesses 
    WHERE user_id = ?
    ''', (user_id,))
    
    user_businesses = []
    for row in cursor.fetchall():
        business_id, level, last_income_time = row
        business = next((b for b in BUSINESSES if b['id'] == business_id), None)
        if business:
            income = business['base_income'] * (business['upgrade_multiplier'] ** (level - 1))
            user_businesses.append({
                **business,
                "level": level,
                "income": income,
                "last_income_time": last_income_time
            })
    return user_businesses

def buy_business(user_id: int, business_id: int) -> Tuple[bool, str]:
    """Покупка бизнеса"""
    try:
        business = next((b for b in BUSINESSES if b['id'] == business_id), None)
        if not business:
            return False, "Бизнес не найден"
            
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
            
        # Проверяем, есть ли у пользователя уже такой бизнес
        cursor.execute('''
        SELECT COUNT(*) FROM user_businesses 
        WHERE user_id = ? AND business_id = ?
        ''', (user_id, business_id))
        if cursor.fetchone()[0] > 0:
            return False, "Вы уже купили этот бизнес. Нельзя иметь два одинаковых бизнеса"
            
        # Проверяем лимит бизнесов
        businesses_count = get_user_businesses_count(user_id)
        if businesses_count >= MAX_BUSINESSES:
            return False, f"Вы можете иметь не более {MAX_BUSINESSES} бизнесов"
            
        # Проверяем баланс
        price = business['base_price']
        if user[2] < price:
            return False, (
                f"Недостаточно средств для покупки {business['name']}\n"
                f"Цена: ${format_number(price, True)}\n"
                f"Ваш баланс: ${format_number(user[2], True)}"
            )
            
        # Покупаем бизнес
        new_usd = user[2] - price
        update_balance(user_id, usd=new_usd)
        
        cursor.execute('''
        INSERT INTO user_businesses (user_id, business_id, level, last_income_time)
        VALUES (?, ?, 1, ?)
        ''', (user_id, business_id, datetime.now().isoformat()))
        
        conn.commit()
        return True, (
            f"🎉 Поздравляем с покупкой бизнеса {business['name']}!\n"
            f"Уровень: 1\n"
            f"Доход: ${format_number(business['base_income'], True)}/час\n"
            f"Баланс: ${format_number(new_usd, True)}\n"
            f"Бизнесов: {businesses_count + 1}/{MAX_BUSINESSES}"
        )
    except Exception as e:
        logger.error(f"Error buying business: {e}")
        conn.rollback()
        return False, "Произошла ошибка при покупке бизнеса"

def upgrade_business(user_id: int, business_id: int) -> Tuple[bool, str]:
    """Улучшение бизнеса"""
    try:
        business = next((b for b in BUSINESSES if b['id'] == business_id), None)
        if not business:
            return False, "Бизнес не найден"
            
        # Получаем текущий уровень
        cursor.execute('''
        SELECT level FROM user_businesses 
        WHERE user_id = ? AND business_id = ?
        ''', (user_id, business_id))
        result = cursor.fetchone()
        
        if not result:
            return False, "У вас нет этого бизнеса"
            
        current_level = result[0]
        if current_level >= business['max_level']:
            return False, f"Бизнес {business['name']} уже максимального уровня ({business['max_level']})"
            
        # Рассчитываем цену улучшения
        upgrade_price = business['base_price'] * (current_level * 1.5)
        
        # Проверяем баланс
        user = get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
            
        if user[2] < upgrade_price:
            return False, (
                f"Недостаточно средств для улучшения {business['name']}\n"
                f"Цена улучшения: ${format_number(upgrade_price, True)}\n"
                f"Ваш баланс: ${format_number(user[2], True)}"
            )
            
        # Улучшаем бизнес
        new_usd = user[2] - upgrade_price
        update_balance(user_id, usd=new_usd)
        
        cursor.execute('''
        UPDATE user_businesses 
        SET level = level + 1 
        WHERE user_id = ? AND business_id = ?
        ''', (user_id, business_id))
        
        conn.commit()
        
        # Рассчитываем новый доход
        new_income = business['base_income'] * (business['upgrade_multiplier'] ** current_level)
        
        return True, (
            f"🎉 Бизнес {business['name']} улучшен до уровня {current_level + 1}!\n"
            f"Новый доход: ${format_number(new_income, True)}/час\n"
            f"Баланс: ${format_number(new_usd, True)}"
        )
    except Exception as e:
        logger.error(f"Error upgrading business: {e}")
        conn.rollback()
        return False, "Произошла ошибка при улучшении бизнеса"

@dp.message(Command("business"))
async def business_command(message: Message):
    """Показать бизнесы пользователя"""
    user_id = message.from_user.id
    user = get_user(user_id)
    
    if not user:
        await message.answer("Сначала зарегистрируйтесь с помощью /start")
        return
    
    businesses = get_user_businesses(user_id)
    businesses_count = len(businesses)
    
    # Получаем все бонусы для отображения
    total_bonus = 0
    bonus_details = []
    
    # Бонус от премиума
    if is_premium(user_id):
        premium_bonus = 0.15
        total_bonus += premium_bonus
        bonus_details.append(f"🌟 Премиум: +{premium_bonus*100:.0f}%")
    
    # Бонусы от инвесторов
    cursor.execute('''
    SELECT bonus FROM user_boosters 
    WHERE user_id = ? AND booster_type = 'investor' AND until > ?
    ''', (user_id, datetime.now().isoformat()))
    
    investor_bonuses = cursor.fetchall()
    total_investor_bonus = sum(bonus for (bonus,) in investor_bonuses)
    if total_investor_bonus > 0:
        total_bonus += total_investor_bonus
        investor_count = len(investor_bonuses)
        bonus_details.append(f"📈 Инвесторы ({investor_count}): +{total_investor_bonus*100:.0f}%")
    
    # Бонус от предметов
    farm_bonus, business_bonus = calculate_items_bonus(user_id)
    if business_bonus > 0:
        total_bonus += business_bonus
        bonus_details.append(f"🎒 Экипировка: +{business_bonus*100:.0f}%")
    
    # Бонус от социальных подписок
    social_bonus = get_social_bonus(user_id)
    if social_bonus > 0:
        total_bonus += social_bonus
        bonus_details.append(f"🔗 Соц. сети: +{social_bonus*100:.0f}%")
    
    # Формируем текст с бонусами
    text = f"🏢 <b>Ваши бизнесы</b> ({businesses_count}/{MAX_BUSINESSES})\n\n"
    
    # Блок с бонусами
    if bonus_details:
        text += f"📊 <b>Активные бонусы к доходу:</b>\n"
        for bonus in bonus_details:
            text += f"├ {bonus}\n"
        text += f"└ <b>Итого: +{total_bonus*100:.0f}%</b>\n\n"
    else:
        text += "📊 <b>Активные бонусы к доходу:</b>\n└ Нет активных бонусов\n\n"
    
    if businesses:
        for business in businesses:
            # Используем ту же функцию расчета, что и в системе
            current_income = calculate_business_income(user_id, business['id'])
            
            text += (
                f"{business['emoji']} <b>{business['name']}</b>\n"
                f"├ Уровень: {business['level']}/{business['max_level']}\n"
                f"├ Доход: ${format_number_short(current_income, True)}/час\n"
                f"├ Улучшить: /upbiz_{business['id']}\n"
                f"└ Продать: /sellbiz_{business['id']}\n\n"
            )
    else:
        text += "📭 У вас пока нет бизнесов.\n\n"
        text += "💡 Бизнесы доступны с 12 уровня фермы.\n"
        text += "🛒 Купить можно в магазине: /shop"

    # Отправляем с баннером
    banner_path = os.path.join(BANNER_DIR, 'bisnes.png')
    try:
        from aiogram.types import FSInputFile
        photo = FSInputFile(banner_path)
        await message.answer_photo(
            photo=photo,
            caption=text,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error sending business banner: {e}")
        await message.answer(text, parse_mode='HTML')

    
@dp.message(F.text.regexp(r'^/buybiz_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def buy_business_handler(message: Message):
    """Обработка покупки бизнеса"""
    user_id = message.from_user.id
    try:
        # Извлекаем ID бизнеса из команды (игнорируя @username если есть)
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        business_id = int(command_text.split('_')[1])
        
        success, result = buy_business(user_id, business_id)
        await message.answer(result)
    except (ValueError, IndexError):
        await message.answer("❌ Неверный формат команды. Используйте: /buybiz_<номер бизнеса>")

@dp.message(F.text.regexp(r'^/upbiz_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def upgrade_business_handler(message: Message):
    """Обработка улучшения бизнеса"""
    user_id = message.from_user.id
    try:
        # Извлекаем ID бизнеса из команды (игнорируя @username если есть)
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        business_id = int(command_text.split('_')[1])
        
        success, result = upgrade_business(user_id, business_id)
        await message.answer(result)
    except (ValueError, IndexError):
        await message.answer("❌ Неверный формат команды. Используйте: /upbiz_<номер бизнеса>")

@dp.message(F.text.regexp(r'^/sellbiz_(\d+)(@' + re.escape(BOT_USERNAME) + r')?$'))
async def sell_business_handler(message: Message):
    """Обработка продажи бизнеса"""
    user_id = message.from_user.id
    try:
        # Извлекаем ID бизнеса из команды (игнорируя @username если есть)
        command_text = message.text.split('@')[0]  # Убираем часть с юзернеймом, если она есть
        business_id = int(command_text.split('_')[1])
        
        cursor.execute('''
        DELETE FROM user_businesses 
        WHERE user_id = ? AND business_id = ?
        RETURNING level
        ''', (user_id, business_id))
        
        result = cursor.fetchone()
        if not result:
            await message.answer("❌ У вас нет этого бизнеса или бизнес не найден")
            return
            
        level = result[0]
        business = next((b for b in BUSINESSES if b['id'] == business_id), None)
        if not business:
            await message.answer("❌ Бизнес не найден в системе")
            return
            
        # Рассчитываем сумму продажи (50% от общей стоимости вложений)
        sell_price = int((business['base_price'] * (1 + 0.5 * (level - 1))) * 0.5)
        
        # Обновляем баланс
        user = get_user(user_id)
        new_usd = user[2] + sell_price
        update_balance(user_id, usd=new_usd)
        
        conn.commit()
        await message.answer(
            f"✅ Вы продали бизнес {business['name']} за ${format_number(sell_price, True)}\n"
            f"💵 Новый баланс: ${format_number(new_usd, True)}\n"
            f"🏢 Бизнесов осталось: {get_user_businesses_count(user_id)}/{MAX_BUSINESSES}"
        )
    except (ValueError, IndexError):
        await message.answer("❌ Неверный формат команды. Используйте: /sellbiz_<номер бизнеса>")
    except Exception as e:
        logger.error(f"Error selling business: {e}")
        conn.rollback()
        await message.answer("❌ Произошла ошибка при продаже бизнеса")

def _get_next_hour_mark(dt: datetime) -> datetime:
    """Возвращает следующий ровный час после указанного времени"""
    return (dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))

def _get_last_hour_mark(dt: datetime) -> datetime:
    """Возвращает последний прошедший ровный час"""
    return dt.replace(minute=0, second=0, microsecond=0)

def _calculate_full_hours(start: datetime, end: datetime) -> int:
    """Вычисляет количество полных часов между двумя временными метками"""
    start_mark = _get_last_hour_mark(start)
    end_mark = _get_last_hour_mark(end)
    return int((end_mark - start_mark).total_seconds() / 3600)

async def auto_claim_business_income(user_id: int) -> float:
    """Автоматическое начисление дохода каждый ровный час с точным расчетом"""
    try:
        user_businesses = get_user_businesses(user_id)
        if not user_businesses:
            return 0.0
            
        total_income = 0.0
        now = datetime.now()
        need_update = False
        last_income_time = None
        
        # Находим самое старое время последнего начисления среди всех бизнесов
        business_last_times = [datetime.fromisoformat(b['last_income_time']) 
                             for b in user_businesses if b['last_income_time']]
        if business_last_times:
            last_income_time = min(business_last_times)
        
        if last_income_time:
            # Рассчитываем количество полных часов с последнего начисления
            hours_passed = _calculate_full_hours(last_income_time, now)
            if hours_passed > 0:
                # Начисляем доход за каждый бизнес
                for business in user_businesses:
                    income = business['income'] * hours_passed
                    total_income += income
                need_update = True
        else:
            # Первое начисление - начисляем за 1 час
            for business in user_businesses:
                total_income += business['income']
            need_update = True
                
        if need_update:
            # Обновляем время последнего дохода для всех бизнесов
            current_hour_mark = _get_last_hour_mark(now)
            cursor.executemany('''
            UPDATE user_businesses 
            SET last_income_time = ?
            WHERE user_id = ? AND business_id = ?
            ''', [(current_hour_mark.isoformat(), user_id, b['id']) for b in user_businesses])
            
            if total_income > 0:
                # Добавляем доход
                user = get_user(user_id)
                if user:
                    new_usd = user[2] + total_income
                    update_user_balance(user_id, usd=new_usd)
                    
            conn.commit()
            
        return total_income
    except Exception as e:
        logger.error(f"Error in auto_claim_business_income: {e}")
        conn.rollback()
        return 0.0       

@dp.message(Command("top"))
async def top_users(message: Message):
    user_id = message.from_user.id
    try:
        # Получаем данные текущего пользователя
        current_user = get_user(user_id)
        if not current_user:
            await message.answer("Сначала зарегистрируйтесь с помощью команды /start")
            return
            
        # Получаем имя текущего пользователя
        try:
            user_info = await bot.get_chat(user_id)
            current_name = user_info.full_name or f"ID {user_id}"
            # Добавляем LRM (Left-to-Right Mark) для арабских имен
            current_name = "\u200E" + current_name if any("\u0600" <= c <= "\u06FF" for c in current_name) else current_name
        except:
            current_name = f"ID {user_id}"

        # Формируем текст сообщения
        text = "🏆 <b>ТОП ИГРОКОВ:</b>\n➖➖➖➖➖\n"

        # Эмодзи для мест
        place_emojis = {
            1: "🥇",
            2: "🥈", 
            3: "🥉",
            4: "4.",
            5: "5."
        }

        # Топ по USD
        text += "<b>💰Баланс $</b>\n"
        
        # Получаем топ пользователей по USD
        cursor.execute('''
        SELECT user_id, usd_balance FROM users 
        ORDER BY usd_balance DESC 
        LIMIT 5
        ''')
        top_usd = cursor.fetchall()
        
        # Находим точную позицию текущего пользователя
        cursor.execute('''
        SELECT COUNT(*) + 1 FROM users 
        WHERE usd_balance > ?
        ''', (current_user[2],))
        current_usd_position = cursor.fetchone()[0]
        
        for i, (top_user_id, value) in enumerate(top_usd, 1):
            try:
                user = await bot.get_chat(top_user_id)
                name = user.full_name or f"ID {top_user_id}"
                # Добавляем LRM для арабских имен
                name = "\u200E" + name if any("\u0600" <= c <= "\u06FF" for c in name) else name
                emoji = place_emojis.get(i, f"{i}.")
                text += f"{emoji} {name} - ${format_number_short(value, is_usd=True)}\n"
            except:
                text += f"{i}) ID {top_user_id} - ${format_number_short(value, is_usd=True)}\n"
        
        # Добавляем позицию текущего пользователя
        text += f"<b>Вы:</b> {current_usd_position}. {current_name} - ${format_number_short(current_user[2], is_usd=True)}\n"
        text += "➖➖➖➖➖\n"

        # Топ по BTC
        text += "<b>🌐 Баланс BTC:</b>\n"
        
        cursor.execute('''
        SELECT user_id, btc_balance FROM users 
        ORDER BY btc_balance DESC 
        LIMIT 5
        ''')
        top_btc = cursor.fetchall()
        
        # Находим точную позицию текущего пользователя
        cursor.execute('''
        SELECT COUNT(*) + 1 FROM users 
        WHERE btc_balance > ?
        ''', (current_user[3],))
        current_btc_position = cursor.fetchone()[0]
        
        for i, (top_user_id, value) in enumerate(top_btc, 1):
            try:
                user = await bot.get_chat(top_user_id)
                name = user.full_name or f"ID {top_user_id}"
                # Добавляем LRM для арабских имен
                name = "\u200E" + name if any("\u0600" <= c <= "\u06FF" for c in name) else name
                emoji = place_emojis.get(i, f"{i}.")
                text += f"{emoji} {name} - {format_number_short(value)} BTC\n"
            except:
                text += f"{i}) ID {top_user_id} - {format_number_short(value)} BTC\n"
        
        # Добавляем позицию текущего пользователя
        text += f"<b>Вы:</b> {current_btc_position}. {current_name} - {format_number_short(current_user[3])} BTC\n"
        text += "➖➖➖➖➖\n"

        # Топ по доходу
        text += "<b>⚙️ Доход BTC/10 мин</b>\n"
        
        cursor.execute('''
        SELECT user_id, income_btc FROM users 
        ORDER BY income_btc DESC 
        LIMIT 5
        ''')
        top_income = cursor.fetchall()
        
        # Находим точную позицию текущего пользователя
        cursor.execute('''
        SELECT COUNT(*) + 1 FROM users 
        WHERE income_btc > ?
        ''', (current_user[4],))
        current_income_position = cursor.fetchone()[0]
        
        for i, (top_user_id, value) in enumerate(top_income, 1):
            try:
                user = await bot.get_chat(top_user_id)
                name = user.full_name or f"ID {top_user_id}"
                # Добавляем LRM для арабских имен
                name = "\u200E" + name if any("\u0600" <= c <= "\u06FF" for c in name) else name
                emoji = place_emojis.get(i, f"{i}.")
                text += f"{emoji} {name} - {format_number_short(value)} BTC\n"
            except:
                text += f"{i}) ID {top_user_id} - {format_number_short(value)} BTC\n"
        
        # Добавляем позицию текущего пользователя
        text += f"<b>Вы:</b> {current_income_position}. {current_name} - {format_number_short(current_user[4])} BTC\n"
        text += "➖➖➖➖➖\n"

        # Топ по вайпам
        text += "<b>⚔️ ТОП «Вайперов»</b>\n"
        
        cursor.execute('''
        SELECT user_id, wipe_count FROM user_wipes 
        ORDER BY wipe_count DESC 
        LIMIT 5
        ''')
        top_wipes = cursor.fetchall()
        
        # Находим количество вайпов текущего пользователя
        cursor.execute('SELECT wipe_count FROM user_wipes WHERE user_id = ?', (user_id,))
        user_wipes = cursor.fetchone()
        current_wipes = user_wipes[0] if user_wipes else 0
        
        # Находим точную позицию текущего пользователя по вайпам
        cursor.execute('''
        SELECT COUNT(*) + 1 FROM user_wipes 
        WHERE wipe_count > ?
        ''', (current_wipes,))
        current_wipes_position = cursor.fetchone()[0] if current_wipes > 0 else "—"
        
        for i, (top_user_id, count) in enumerate(top_wipes, 1):
            try:
                user = await bot.get_chat(top_user_id)
                name = user.full_name or f"ID {top_user_id}"
                # Добавляем LRM для арабских имен
                name = "\u200E" + name if any("\u0600" <= c <= "\u06FF" for c in name) else name
                text += f"{i}. {name} - {count}\n"
            except:
                text += f"{i}. ID {top_user_id} - {count}\n"
        
        # Добавляем позицию текущего пользователя
        if current_wipes_position != "—":
            text += f"<b>Вы:</b> {current_wipes_position}. {current_name} - {current_wipes}\n"
        else:
            text += f"<b>Вы:</b> —\n"

        await message.answer(text, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"Error in top command: {e}")
        await message.answer("❌ Произошла ошибка при загрузке топа")
        

        
async def background_business_income():
    """Фоновая задача для начисления дохода каждый ровный час"""
    while True:
        try:
            now = datetime.now()
            # Вычисляем время до следующего ровного часа
            next_hour = _get_next_hour_mark(now)
            sleep_time = (next_hour - now).total_seconds()
            
            # Спим до следующего ровного часа
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            
            # Получаем всех пользователей с бизнесами
            cursor.execute('SELECT DISTINCT user_id FROM user_businesses')
            users_with_businesses = [row[0] for row in cursor.fetchall()]
            
            # Начисляем доход всем пользователям
            for user_id in users_with_businesses:
                try:
                    # Получаем все бизнесы пользователя
                    user_businesses = get_user_businesses(user_id)
                    
                    total_income = 0.0
                    current_hour_mark = _get_last_hour_mark(datetime.now())
                    
                    for business in user_businesses:
                        income = calculate_business_income(user_id, business['id'])
                        total_income += income
                        
                        # Обновляем время последнего дохода
                        cursor.execute('''
                        UPDATE user_businesses 
                        SET last_income_time = ?
                        WHERE user_id = ? AND business_id = ?
                        ''', (current_hour_mark.isoformat(), user_id, business['id']))
                    
                    if total_income > 0:
                        # Добавляем доход
                        user = get_user(user_id)
                        if user:
                            new_usd = user[2] + total_income
                            update_balance(user_id, usd=new_usd)
                
                except Exception as e:
                    logger.error(f"Error processing user {user_id}: {e}")
                    conn.rollback()
                    continue
            
            conn.commit()
                    
        except Exception as e:
            logger.error(f"Error in background_business_income: {e}")
            conn.rollback()
            await asyncio.sleep(60)


ALLOWED_CHAT_ID = -1002734900704

async def check_access(chat_id: int, user_id: int) -> bool:
    """Проверяет, имеет ли пользователь доступ к команде"""
    if chat_id != ALLOWED_CHAT_ID:
        return False
    
    try:
        member = await bot.get_chat_member(ALLOWED_CHAT_ID, user_id)
        return member.status in ['creator', 'administrator', 'member']
    except Exception as e:
        logger.error(f"Error checking member status: {e}")
        return False

@dp.message(Command("ad"))
async def send_ad_command(message: Message):
    # Проверяем доступ
    if not await check_access(message.chat.id, message.from_user.id):
        await message.answer("❌ Эта команда доступна только участникам определенного чата")
        return

    # Проверяем формат команды
    if len(message.text.split()) < 2:
        await message.answer(
            "ℹ️ Формат команды:\n"
            "/ad <текст> [--photo URL] [--button Текст|URL]\n\n"
            "Примеры:\n"
            "/ad Привет всем! Это тестовое сообщение\n"
            "/ad Акция! --photo https://example.com/image.jpg --button Подробнее|https://example.com"
        )
        return

    # Разбираем аргументы команды
    args = message.text.split(" --")
    main_text = args[0].replace("/ad ", "").strip()
    
    photo_url = None
    button_text = None
    button_url = None

    for arg in args[1:]:
        if arg.startswith("photo ") and len(arg.split()) > 1:
            photo_url = arg.split()[1]
        elif arg.startswith("button ") and "|" in arg:
            button_parts = arg.replace("button ", "").split("|")
            if len(button_parts) == 2:
                button_text = button_parts[0].strip()
                button_url = button_parts[1].strip()

    # Создаем клавиатуру с кнопкой, если она указана
    reply_markup = None
    if button_text and button_url:
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(
            text=button_text,
            url=button_url
        ))
        reply_markup = builder.as_markup()

    # Подтверждение перед рассылкой
    confirmation_text = "📢 Подтвердите рассылку:\n\n" + main_text
    if photo_url:
        confirmation_text += f"\n\n🖼 Фото: {photo_url}"
    if button_text and button_url:
        confirmation_text += f"\n\n🔘 Кнопка: {button_text} -> {button_url}"

    # Создаем клавиатуру подтверждения
    confirm_builder = InlineKeyboardBuilder()
    confirm_builder.add(InlineKeyboardButton(
        text="✅ Начать рассылку",
        callback_data=f"confirm_ad_{message.from_user.id}"
    ))
    confirm_builder.add(InlineKeyboardButton(
        text="❌ Отменить",
        callback_data=f"cancel_ad_{message.from_user.id}"
    ))

    if photo_url:
        try:
            await message.answer_photo(
                photo_url,
                caption=confirmation_text,
                reply_markup=confirm_builder.as_markup()
            )
        except:
            await message.answer(
                confirmation_text,
                reply_markup=confirm_builder.as_markup()
            )
    else:
        await message.answer(
            confirmation_text,
            reply_markup=confirm_builder.as_markup()
        )

@dp.callback_query(F.data.startswith("confirm_ad_"))
async def confirm_ad_callback(callback: CallbackQuery):
    await callback.answer()
    # Проверяем доступ
    if not await check_access(callback.message.chat.id, callback.from_user.id):
        return

    try:
        owner_id = int(callback.data.split("_")[2])
        if callback.from_user.id != owner_id:
            await callback.answer("Это не ваша рассылка!", show_alert=True)
            return

        original_message = callback.message
        text = original_message.caption if original_message.photo else original_message.text
        text = text.replace("📢 Подтвердите рассылку:\n\n", "")

        photo_url = None
        if original_message.photo:
            photo_file_id = original_message.photo[-1].file_id
        else:
            photo_match = re.search(r"🖼 Фото: (http[^\s]+)", text)
            if photo_match:
                photo_url = photo_match.group(1)
                text = text.replace(f"\n\n🖼 Фото: {photo_url}", "")

        button_match = re.search(r"🔘 Кнопка: ([^\|]+)\|([^\s]+)", text)
        button_text = None
        button_url = None
        if button_match:
            button_text = button_match.group(1)
            button_url = button_match.group(2)
            text = text.replace(f"\n\n🔘 Кнопка: {button_text}|{button_url}", "")

        reply_markup = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.add(InlineKeyboardButton(
                text=button_text,
                url=button_url
            ))
            reply_markup = builder.as_markup()

        # Получаем список всех чатов и пользователей
        cursor.execute('SELECT chat_id FROM chat_stats')
        chats = [row[0] for row in cursor.fetchall()]
        
        cursor.execute('SELECT user_id FROM users')
        users = [row[0] for row in cursor.fetchall()]

        all_recipients = list(set(chats + users))
        total = len(all_recipients)
        success = 0
        errors = 0

        await callback.message.edit_text(
            f"🔄 Начинаю рассылку для {total} получателей...",
            reply_markup=None
        )

        for recipient in all_recipients:
            try:
                if photo_url:
                    await bot.send_photo(
                        chat_id=recipient,
                        photo=photo_url,
                        caption=text,
                        reply_markup=reply_markup
                    )
                elif 'photo_file_id' in locals():
                    await bot.send_photo(
                        chat_id=recipient,
                        photo=photo_file_id,
                        caption=text,
                        reply_markup=reply_markup
                    )
                else:
                    await bot.send_message(
                        chat_id=recipient,
                        text=text,
                        reply_markup=reply_markup
                    )
                success += 1
            except Exception as e:
                errors += 1
                logger.error(f"Error sending to {recipient}: {e}")

            if (success + errors) % 10 == 0:
                try:
                    await callback.message.edit_text(
                        f"🔄 Рассылка: {success + errors}/{total}\n"
                        f"✅ Успешно: {success}\n"
                        f"❌ Ошибок: {errors}",
                        reply_markup=None
                    )
                except:
                    pass

            await asyncio.sleep(0.1)

        await callback.message.answer(
            f"✅ Рассылка завершена!\n"
            f"Всего получателей: {total}\n"
            f"Успешно: {success}\n"
            f"Ошибок: {errors}"
        )

    except Exception as e:
        logger.error(f"Error in ad distribution: {e}")
        await callback.message.answer(f"❌ Ошибка при рассылке: {str(e)}")

@dp.callback_query(F.data.startswith("cancel_ad_"))
async def cancel_ad_callback(callback: CallbackQuery):
    await callback.answer()
    # Проверяем доступ
    if not await check_access(callback.message.chat.id, callback.from_user.id):
        return

    owner_id = int(callback.data.split("_")[2])
    if callback.from_user.id != owner_id:
        await callback.answer("Это не ваша рассылка!", show_alert=True)
        return

    await callback.message.edit_text("❌ Рассылка отменена", reply_markup=None)
    
import logging
from datetime import datetime, timedelta
from aiogram import F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
MOD_CHAT_ID = -1002817091376  # ID чата модерации
PAYMENT_LINK = "https://www.tinkoff.ru/rm/r_vHqSAwQQKI.HBDmlksyhH/tnBoZ48067"
ADMIN_ID = 963551489  # ID администратора

# Варианты премиума
PREMIUM_OPTIONS = {
    "premium_1": {"days": 1, "price": 50},
    "premium_3": {"days": 3, "price": 100},
    "premium_7": {"days": 7, "price": 200},
    "premium_30": {"days": 30, "price": 400}
}

# Варианты уборщика
CLEANER_OPTIONS = {
    "cleaner_1": {"days": 1, "price": 40},
    "cleaner_3": {"days": 3, "price": 90},
    "cleaner_7": {"days": 7, "price": 180},
    "cleaner_30": {"days": 30, "price": 350}
}

# Варианты инвесторов
INVESTOR_OPTIONS = {
    "investor_1": {"days": 1, "price": 50},
    "investor_3": {"days": 3, "price": 100},
    "investor_7": {"days": 7, "price": 200},
    "investor_30": {"days": 30, "price": 400}
}


def is_premium(user_id: int) -> bool:
    """Проверяет, есть ли у пользователя активный премиум"""
    try:
        cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            premium_until = datetime.fromisoformat(result[0])
            return premium_until > datetime.now()
        return False
    except Exception as e:
        logger.error(f"Error checking premium status: {e}")
        return False

def has_cleaner_booster(user_id: int) -> Tuple[bool, float, Optional[datetime]]:
    """Проверяет наличие бустера уборщика"""
    try:
        cursor.execute('SELECT cleaner_until, cleaner_bonus FROM user_boosters WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            until = datetime.fromisoformat(result[0])
            if until > datetime.now():
                return True, result[1], until
        return False, 0, None
    except Exception as e:
        logger.error(f"Error checking cleaner booster: {e}")
        return False, 0, None

def has_investor_booster(user_id: int) -> Tuple[bool, float, Optional[datetime]]:
    """Проверяет наличие бустера инвестора"""
    try:
        cursor.execute('SELECT investor_until, investor_bonus FROM user_boosters WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            until = datetime.fromisoformat(result[0])
            if until > datetime.now():
                return True, result[1], until
        return False, 0, None
    except Exception as e:
        logger.error(f"Error checking investor booster: {e}")
        return False, 0, None

def add_premium_time(user_id: int, days: int) -> bool:
    """Добавляет премиум время пользователю"""
    try:
        cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result and result[0]:
            current_until = datetime.fromisoformat(result[0])
            # Если премиум еще активен, добавляем дни к текущей дате
            if current_until > datetime.now():
                new_until = current_until + timedelta(days=days)
            else:
                new_until = datetime.now() + timedelta(days=days)
        else:
            new_until = datetime.now() + timedelta(days=days)
        
        cursor.execute('''
        INSERT OR REPLACE INTO premium_users (user_id, premium_until)
        VALUES (?, ?)
        ''', (user_id, new_until.isoformat()))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding premium time: {e}")
        conn.rollback()
        return False

def add_cleaner_booster(user_id: int, days: int) -> bool:
    """Добавляет новый бустер уборщика"""
    try:
        new_until = datetime.now() + timedelta(days=days)
        
        # Добавляем новый бустер
        cursor.execute('''
        INSERT INTO user_boosters 
        (user_id, booster_type, until, bonus) 
        VALUES (?, ?, ?, ?)
        ''', (user_id, 'cleaner', new_until.isoformat(), 0.25))
        
        # Пересчитываем суммарный бонус
        cursor.execute('''
        SELECT SUM(bonus) FROM user_boosters 
        WHERE user_id = ? AND booster_type = 'cleaner' AND until > ?
        ''', (user_id, datetime.now().isoformat()))
        
        total_bonus = cursor.fetchone()[0] or 0
        
        # Обновляем общий бонус
        cursor.execute('''
        UPDATE users SET cleaner_total_bonus = ? 
        WHERE user_id = ?
        ''', (total_bonus, user_id))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding cleaner booster: {e}")
        conn.rollback()
        return False

def add_investor_booster(user_id: int, days: int) -> bool:
    """Добавляет новый бустер инвестора"""
    try:
        new_until = datetime.now() + timedelta(days=days)
        
        # Добавляем новый бустер
        cursor.execute('''
        INSERT INTO user_boosters 
        (user_id, booster_type, until, bonus) 
        VALUES (?, ?, ?, ?)
        ''', (user_id, 'investor', new_until.isoformat(), 0.15))
        
        # Пересчитываем суммарный бонус
        cursor.execute('''
        SELECT SUM(bonus) FROM user_boosters 
        WHERE user_id = ? AND booster_type = 'investor' AND until > ?
        ''', (user_id, datetime.now().isoformat()))
        
        total_bonus = cursor.fetchone()[0] or 0
        
        # Обновляем общий бонус
        cursor.execute('''
        UPDATE users SET investor_total_bonus = ? 
        WHERE user_id = ?
        ''', (total_bonus, user_id))
        
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error adding investor booster: {e}")
        conn.rollback()
        return False



    

def calculate_business_income(user_id: int, business_id: int) -> float:
    """Расчет дохода бизнеса с учетом бонусов"""
    try:
        business = next((b for b in BUSINESSES if b['id'] == business_id), None)
        if not business:
            return 0.0
            
        cursor.execute('''
        SELECT level FROM user_businesses 
        WHERE user_id = ? AND business_id = ?
        ''', (user_id, business_id))
        result = cursor.fetchone()
        if not result:
            return 0.0
            
        level = result[0]
        base_income = business['base_income'] * (business['upgrade_multiplier'] ** (level - 1))
        
        # Бонус от премиума (добавляется как множитель)
        if is_premium(user_id):
            base_income *= 1.15
            
        # Получаем все активные бустеры инвесторов
        cursor.execute('''
        SELECT bonus FROM user_boosters 
        WHERE user_id = ? AND booster_type = 'investor' AND until > ?
        ''', (user_id, datetime.now().isoformat()))
        
        investor_bonuses = cursor.fetchall()
        total_investor_bonus = sum(bonus for (bonus,) in investor_bonuses)
        
        # Применяем бонус инвесторов (каждый бустер дает +15% к базовому доходу)
        if total_investor_bonus > 0:
            base_income *= (1 + total_investor_bonus)
        
        # ПРИМЕНЯЕМ БОНУС ОТ НАДЕТЫХ ПРЕДМЕТОВ (ДОБАВЛЕНО)
        farm_bonus, business_bonus = calculate_items_bonus(user_id)
        base_income *= (1 + business_bonus)
        
        # ПРИМЕНЯЕМ БОНУС ОТ СОЦИАЛЬНЫХ ПОДПИСОК (ДОБАВЛЕНО)
        social_bonus = get_social_bonus(user_id)
        base_income *= (1 + social_bonus)
            
        return base_income
    except Exception as e:
        logger.error(f"Error calculating business income: {e}")
        return 0.0
    
    
# Добавьте в раздел с константами (рядом с PREMIUM_OPTIONS и другими)
AUTOMATION_OPTIONS = {
    "automation_1": {"days": 1, "price": 25},
    "automation_3": {"days": 3, "price": 60},
    "automation_7": {"days": 7, "price": 130},
    "automation_30": {"days": 30, "price": 400}
}

# Добавьте в раздел с константами
ANTIVIRUS_OPTIONS = {
    "antivirus_1": {"days": 1, "price": 25},
    "antivirus_3": {"days": 3, "price": 60},
    "antivirus_7": {"days": 7, "price": 130},
    "antivirus_30": {"days": 30, "price": 400}
}

@dp.message(Command("donate"))
async def donate_command(message: Message):
    """Обработчик команды /donate"""
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 PREMIUM", callback_data="donate_premium")],
            [InlineKeyboardButton(text="🧹 Уборщик фермы", callback_data="donate_cleaner")],
            [InlineKeyboardButton(text="🏢 Инвесторы бизнесов", callback_data="donate_investor")],
            [InlineKeyboardButton(text="🤖 Автоматизация", callback_data="donate_automation")],
            [InlineKeyboardButton(text="🦠 Антивирус", callback_data="donate_antivirus")]  # Новая кнопка
        ])
        
        await message.answer(
            "💎 <b>Магазин бустеров</b>\n\n"
            "Выберите тип бустера для покупки:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        
    except Exception as e:
        logger.error(f"Error in donate_command: {e}")
        await message.answer("⚠️ Произошла ошибка. Попробуйте позже.")
      
@dp.callback_query(F.data == "donate_antivirus")
async def antivirus_menu_callback(callback: CallbackQuery):
    """Меню выбора антивируса"""
    try:
        user_id = callback.from_user.id
        
        # Проверяем, есть ли уже антивирус
        has_antivirus, antivirus_until = check_antivirus_access(user_id)
        
        antivirus_status = ""
        if has_antivirus:
            remaining = antivirus_until - datetime.now()
            days = remaining.days
            hours = remaining.seconds // 3600
            minutes = (remaining.seconds % 3600) // 60
            antivirus_status = f"\n\n🦠 <b>У вас активен антивирус (осталось {days}д {hours}ч {minutes}м)</b>"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 день - 25 ₽", callback_data="antivirus_1")],
            [InlineKeyboardButton(text="3 дня - 60 ₽", callback_data="antivirus_3")],
            [InlineKeyboardButton(text="1 неделя - 130 ₽", callback_data="antivirus_7")],
            [InlineKeyboardButton(text="1 месяц - 400 ₽", callback_data="antivirus_30")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="donate_back")]
        ])
        
        text = (
            "🦠 <b>Антивирус</b>\n\n"
            "✨ <b>Защита:</b>\n"
            "• 🔒 Полная защита от всех вирусов\n"
            "• ⚡ Автоматическая блокировка угроз\n"
            "• 🛡️ Гарантия безопасности фермы\n"
            "• 📊 Не влияет на доходность\n\n"
            "📅 <b>Выберите срок:</b>"
            f"{antivirus_status}"
        )
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in antivirus_menu: {e}")
        await callback.answer("⚠️ Ошибка при загрузке меню", show_alert=True)  
    
@dp.callback_query(F.data == "donate_back")
async def donate_back_callback(callback: CallbackQuery):
    """Возврат в главное меню доната"""
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 PREMIUM", callback_data="donate_premium")],
            [InlineKeyboardButton(text="🧹 Уборщик фермы", callback_data="donate_cleaner")],
            [InlineKeyboardButton(text="🏢 Инвесторы бизнесов", callback_data="donate_investor")],
            [InlineKeyboardButton(text="🤖 Автоматизация", callback_data="donate_automation")],
            [InlineKeyboardButton(text="🦠 Антивирус", callback_data="donate_antivirus")]  # Добавляем кнопку
        ])
        
        await callback.message.edit_text(
            "💎 <b>Магазин бустеров</b>\n\n"
            "Выберите тип бустера для покупки:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in donate_back: {e}")
        await callback.answer("⚠️ Ошибка", show_alert=True)
        
        
        
def grant_antivirus_access(user_id: int, days: int) -> bool:
    """Выдать доступ к антивирусу"""
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO antivirus_users 
            (user_id, antivirus_until) 
            VALUES (?, datetime('now', ? || ' days'))
        ''', (user_id, days))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error granting antivirus access: {e}")
        return False

def check_antivirus_access(user_id: int) -> tuple[bool, datetime]:
    """Проверить доступ к антивирусу"""
    try:
        cursor.execute(
            'SELECT antivirus_until FROM antivirus_users WHERE user_id = ?', 
            (user_id,)
        )
        result = cursor.fetchone()
        if result:
            antivirus_until = datetime.fromisoformat(result[0])
            return antivirus_until > datetime.now(), antivirus_until
        return False, datetime.now()
    except Exception as e:
        logger.error(f"Error checking antivirus access: {e}")
        return False, datetime.now()

def has_active_antivirus(user_id: int) -> bool:
    """Проверить, есть ли активный антивирус"""
    has_antivirus, _ = check_antivirus_access(user_id)
    return has_antivirus
        

@dp.callback_query(F.data == "donate_automation")
async def automation_menu_callback(callback: CallbackQuery):
    """Меню выбора автоматизации"""
    try:
        user_id = callback.from_user.id
        
        # Проверяем, есть ли уже доступ к автоматизации
        has_automation, automation_until = check_automation_access(user_id)
        
        automation_status = ""
        if has_automation:
            remaining = automation_until - datetime.now()
            days = remaining.days
            hours = remaining.seconds // 3600
            minutes = (remaining.seconds % 3600) // 60
            automation_status = f"\n\n🤖 <b>У вас активна автоматизация (осталось {days}д {hours}ч {minutes}м)</b>"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 день - 25 ₽", callback_data="automation_1")],
            [InlineKeyboardButton(text="3 дня - 60 ₽", callback_data="automation_3")],
            [InlineKeyboardButton(text="1 неделя - 130 ₽", callback_data="automation_7")],
            [InlineKeyboardButton(text="1 месяц - 400 ₽", callback_data="automation_30")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="donate_back")]
        ])
        
        text = (
            "🤖 <b>Автоворк и Автоналог</b>\n\n"
            "✨ <b>Возможности:</b>\n"
            "• ⚡ Автоматический сбор работы (/work)\n"
            "• 💰 Автоматическая оплата налогов (/nalog)\n"
            "• 🔄 Работает 24/7 без вашего участия\n"
            "• 📊 Уведомления о выполнении\n\n"
            "📅 <b>Выберите срок:</b>"
            f"{automation_status}"
        )
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in automation_menu: {e}")
        await callback.answer("⚠️ Ошибка при загрузке меню", show_alert=True)

@dp.callback_query(F.data == "donate_premium")
async def premium_menu_callback(callback: CallbackQuery):
    """Меню выбора премиум подписки"""
    try:
        user_id = callback.from_user.id
        is_premium_user = is_premium(user_id)
        
        premium_status = ""
        if is_premium_user:
            cursor.execute('SELECT premium_until FROM premium_users WHERE user_id = ?', (user_id,))
            premium_until = datetime.fromisoformat(cursor.fetchone()[0])
            remaining = premium_until - datetime.now()
            days = remaining.days
            hours = remaining.seconds // 3600
            premium_status = f"\n\n💎 <b>Ваш PREMIUM активен ещё {days}д {hours}ч</b>"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 день - 50 ₽", callback_data="premium_1")],
            [InlineKeyboardButton(text="3 дня - 100 ₽", callback_data="premium_3")],
            [InlineKeyboardButton(text="1 неделя - 200 ₽", callback_data="premium_7")],
            [InlineKeyboardButton(text="1 месяц - 400 ₽", callback_data="premium_30")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="donate_back")]
        ])
        
        text = (
            "👑 <b>PREMIUM Статус</b>\n\n"
            "💫 <b>Бонусы:</b>\n"
            "• 🎛 +35% к доходу фермы\n"
            "• 🏢 +15% к доходу бизнесов\n"
            "• 🎁 Ежедневный бонус раз в 12 часов\n"
            "• ⚡ Приоритетная поддержка\n\n"
            "📅 <b>Выберите срок:</b>"
            f"{premium_status}"
        )
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in premium_menu: {e}")
        await callback.answer("⚠️ Ошибка при загрузке меню", show_alert=True)

@dp.callback_query(F.data == "donate_cleaner")
async def cleaner_menu_callback(callback: CallbackQuery):
    """Меню выбора уборщика"""
    try:
        user_id = callback.from_user.id
        has_cleaner, cleaner_bonus, cleaner_until = has_cleaner_booster(user_id)
        
        cleaner_status = ""
        if has_cleaner:
            remaining = cleaner_until - datetime.now()
            days = remaining.days
            hours = remaining.seconds // 3600
            cleaner_status = f"\n\n🧹 <b>У вас активен бустер +{int(cleaner_bonus*100)}% (осталось {days}д {hours}ч)</b>"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 день - 40 ₽", callback_data="cleaner_1")],
            [InlineKeyboardButton(text="3 дня - 90 ₽", callback_data="cleaner_3")],
            [InlineKeyboardButton(text="1 неделя - 180 ₽", callback_data="cleaner_7")],
            [InlineKeyboardButton(text="1 месяц - 350 ₽", callback_data="cleaner_30")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="donate_back")]
        ])
        
        text = (
            "🧹 <b>Уборщик фермы</b>\n\n"
            "✨ <b>Бонус:</b> +25% к доходу фермы\n\n"
            "📊 <b>Особенности:</b>\n"
            "• 🎯 Бонусы уборщиков суммируются\n"
            "• ⏱ Время действия берется максимальное\n"
            "• 🔄 Можно докупать для увеличения бонуса\n\n"
            "📅 <b>Выберите срок:</b>"
            f"{cleaner_status}"
        )
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in cleaner_menu: {e}")
        await callback.answer("⚠️ Ошибка при загрузке меню", show_alert=True)

@dp.callback_query(F.data == "donate_investor")
async def investor_menu_callback(callback: CallbackQuery):
    """Меню выбора инвесторов"""
    try:
        user_id = callback.from_user.id
        has_investor, investor_bonus, investor_until = has_investor_booster(user_id)
        
        investor_status = ""
        if has_investor:
            remaining = investor_until - datetime.now()
            days = remaining.days
            hours = remaining.seconds // 3600
            investor_status = f"\n\n🏢 <b>У вас активен бустер +{int(investor_bonus*100)}% (осталось {days}д {hours}ч)</b>"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 день - 50 ₽", callback_data="investor_1")],
            [InlineKeyboardButton(text="3 дня - 100 ₽", callback_data="investor_3")],
            [InlineKeyboardButton(text="1 неделя - 200 ₽", callback_data="investor_7")],
            [InlineKeyboardButton(text="1 месяц - 400 ₽", callback_data="investor_30")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="donate_back")]
        ])
        
        text = (
            "🏢 <b>Инвесторы бизнесов</b>\n\n"
            "✨ <b>Бонус:</b> +15% к доходу всех бизнесов\n\n"
            "📊 <b>Особенности:</b>\n"
            "• 🎯 Бонусы инвесторов суммируются\n"
            "• ⏱ Время действия берется максимальное\n"
            "• 🔄 Можно докупать для увеличения бонуса\n"
            "• 💼 Действует на все бизнесы\n\n"
            "📅 <b>Выберите срок:</b>"
            f"{investor_status}"
        )
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in investor_menu: {e}")
        await callback.answer("⚠️ Ошибка при загрузке меню", show_alert=True)

@dp.callback_query(F.data.startswith("approve:"))
async def approve_payment(callback: CallbackQuery, bot: Bot):
    """Обработчик подтверждения платежа администратором"""
    try:
        _, booster_type, user_id, days = callback.data.split(":")
        user_id = int(user_id)
        days = int(days)

        # Определяем бонусную валюту в зависимости от типа бустера и длительности
        event_currency_bonus = calculate_event_currency_bonus(booster_type, days)
        
        if booster_type == "premium":
            success = add_premium_time(user_id, days)
            name = "👑 PREMIUM статус"
        elif booster_type == "cleaner":
            success = add_cleaner_booster(user_id, days)
            name = "🧹 Уборщик фермы"
        elif booster_type == "investor":
            success = add_investor_booster(user_id, days)
            name = "🏢 Инвесторы бизнесов"
        elif booster_type == "automation":
            success = grant_automation_access(user_id, days)
            name = "🤖 Автоматизация"
        elif booster_type == "antivirus":
            success = grant_antivirus_access(user_id, days)
            name = "🦠 Антивирус"
        else:
            await callback.answer("Неизвестный тип бустера", show_alert=True)
            return

        if success:
            # Выдаем бонусную ивент валюту пользователю
            event_currency_success = False
            if event_currency_bonus > 0:
                event_currency_success = add_event_currency(user_id, event_currency_bonus, is_earned=False)
            
            await callback.message.edit_text(
                f"✅ <b>{name} активирован</b>\n\n"
                f"Пользователю: <code>{user_id}</code>\n"
                f"Срок: {days} дней\n"
                f"🎁 Бонус: +{event_currency_bonus} 🍁 {'✅' if event_currency_success else '❌'}",
                parse_mode="HTML"
            )
            
            try:
                # Формируем сообщение для пользователя
                user_message = (
                    f"🎉 <b>Ваш {name} на {days} дней активирован!</b>\n\n"
                    f"Спасибо за поддержку! 💫"
                )
                
                # Добавляем информацию о бонусной валюте, если она была выдана
                if event_currency_success and event_currency_bonus > 0:
                    current_balance = get_event_currency(user_id)
                    user_message += f"\n\n🎁 <b>Бонус за покупку:</b> +{event_currency_bonus} 🍁\n"
                    user_message += f"💰 Ваш баланс ивент валюты: {current_balance} 🍁\n"
                    user_message += f"💡 Используйте /top_ivent для просмотра топа"
                
                await bot.send_message(
                    chat_id=user_id,
                    text=user_message,
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления пользователя: {e}")
            
            await callback.answer("✅ Успешно активировано", show_alert=True)
        else:
            await callback.answer("❌ Ошибка активации", show_alert=True)

    except Exception as e:
        logger.error(f"Ошибка в approve_payment: {e}")
        await callback.answer("⚠️ Ошибка при обработке", show_alert=True)

def calculate_event_currency_bonus(booster_type: str, days: int) -> int:
    """Рассчитывает бонусную ивент валюту в зависимости от типа бустера и длительности"""
    
    if booster_type == "premium":
        # Премиум: 1 день - 5 🍁, 3 дня - 20 🍁, 1 неделя - 50 🍁, 1 месяц - 200 🍁
        if days == 1:
            return 5
        elif days == 3:
            return 20
        elif days == 7:
            return 50
        elif days == 30:
            return 200
        else:
            # Для других длительностей - пропорционально
            return max(5, days * 5)  # Минимум 5, пропорционально дням
    
    elif booster_type == "cleaner":
        # Уборщик фермы: 1 день - 4 🍁, 3 дня - 15 🍁, 1 неделя - 30 🍁, 1 месяц - 150 🍁
        if days == 1:
            return 4
        elif days == 3:
            return 15
        elif days == 7:
            return 30
        elif days == 30:
            return 150
        else:
            # Для других длительностей - пропорционально
            return max(4, days * 4)  # Минимум 4, пропорционально дням
    
    elif booster_type == "investor":
        # Инвестор бизнесов: 1 день - 5 🍁, 3 дня - 20 🍁, 1 неделя - 50 🍁, 1 месяц - 200 🍁
        if days == 1:
            return 5
        elif days == 3:
            return 20
        elif days == 7:
            return 50
        elif days == 30:
            return 200
        else:
            # Для других длительностей - пропорционально
            return max(5, days * 5)  # Минимум 5, пропорционально дням
    
    elif booster_type == "antivirus":
        # Антивирус: 1 день - 5 🍁, 3 дня - 15 🍁, 1 неделя - 50 🍁, 1 месяц - 200 🍁
        if days == 1:
            return 5
        elif days == 3:
            return 15
        elif days == 7:
            return 50
        elif days == 30:
            return 200
        else:
            # Для других длительностей - пропорционально
            return max(5, days * 5)  # Минимум 5, пропорционально дням
    
    elif booster_type == "automation":
        # Автоматизация: 1 день - 5 🍁, 3 дня - 15 🍁, 1 неделя - 50 🍁, 1 месяц - 200 🍁
        if days == 1:
            return 5
        elif days == 3:
            return 15
        elif days == 7:
            return 50
        elif days == 30:
            return 200
        else:
            # Для других длительностей - пропорционально
            return max(5, days * 5)  # Минимум 5, пропорционально дням
    
    # По умолчанию - без бонуса
    return 0
        
def grant_automation_access(user_id: int, days: int) -> bool:
    """Выдать доступ к автоматизации"""
    try:
        automation_until = (datetime.now() + timedelta(days=days)).isoformat()
        cursor.execute('''
        INSERT OR REPLACE INTO automation_access (user_id, access_until) 
        VALUES (?, ?)
        ''', (user_id, automation_until))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error granting automation access: {e}")
        conn.rollback()
        return False

def check_automation_access(user_id: int) -> tuple[bool, Optional[datetime]]:
    """Проверить доступ к автоматизации"""
    try:
        cursor.execute('SELECT access_until FROM automation_access WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if not result:
            return False, None
        
        access_until = datetime.fromisoformat(result[0])
        has_access = datetime.now() <= access_until
        return has_access, access_until
        
    except Exception as e:
        logger.error(f"Error checking automation access: {e}")
        return False, None

def get_automation_status(user_id: int) -> Dict:
    """Получить статус автоматизации"""
    try:
        has_access, access_until = check_automation_access(user_id)
        
        cursor.execute('SELECT auto_taxes, auto_work FROM user_automation WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        
        if result:
            return {
                "has_access": has_access,
                "access_until": access_until,
                "auto_taxes": bool(result[0]),
                "auto_work": bool(result[1])
            }
        else:
            cursor.execute('INSERT INTO user_automation (user_id) VALUES (?)', (user_id,))
            conn.commit()
            return {
                "has_access": has_access,
                "access_until": access_until,
                "auto_taxes": False,
                "auto_work": False
            }
    except Exception as e:
        logger.error(f"Error getting automation status: {e}")
        return {"has_access": False, "access_until": None, "auto_taxes": False, "auto_work": False}



@dp.callback_query(F.data.startswith(("premium_", "cleaner_", "investor_", "automation_", "antivirus_")))  # Добавляем antivirus_
async def payment_callback(callback: CallbackQuery):
    """Обработчик выбора варианта оплаты"""
    try:
        # Проверяем, если это группа - просим перейти в ЛС
        if callback.message.chat.type != 'private':
            await callback.message.edit_text(
                "💬 <b>Для оформления заказа перейдите в ЛС с ботом:</b>\n"
                "👉 @CryptoMiner_sBot",
                parse_mode="HTML"
            )
            await callback.answer()
            return
            
        option = callback.data
        
        # Определяем тип бустера и параметры
        if option.startswith("premium_"):
            if option not in PREMIUM_OPTIONS:
                await callback.answer("❌ Неверный вариант", show_alert=True)
                return
            days = PREMIUM_OPTIONS[option]["days"]
            price = PREMIUM_OPTIONS[option]["price"]
            booster_type = "premium"
            name = "👑 PREMIUM статус"
            emoji = "👑"
            
        elif option.startswith("cleaner_"):
            if option not in CLEANER_OPTIONS:
                await callback.answer("❌ Неверный вариант", show_alert=True)
                return
            days = CLEANER_OPTIONS[option]["days"]
            price = CLEANER_OPTIONS[option]["price"]
            booster_type = "cleaner"
            name = "🧹 Уборщик фермы"
            emoji = "🧹"
            
        elif option.startswith("investor_"):
            if option not in INVESTOR_OPTIONS:
                await callback.answer("❌ Неверный вариант", show_alert=True)
                return
            days = INVESTOR_OPTIONS[option]["days"]
            price = INVESTOR_OPTIONS[option]["price"]
            booster_type = "investor"
            name = "🏢 Инвесторы бизнесов"
            emoji = "🏢"
            
        elif option.startswith("automation_"):
            if option not in AUTOMATION_OPTIONS:
                await callback.answer("❌ Неверный вариант", show_alert=True)
                return
            days = AUTOMATION_OPTIONS[option]["days"]
            price = AUTOMATION_OPTIONS[option]["price"]
            booster_type = "automation"
            name = "🤖 Автоматизация"
            emoji = "🤖"
            
        elif option.startswith("antivirus_"):  # Добавляем обработку антивируса
            if option not in ANTIVIRUS_OPTIONS:
                await callback.answer("❌ Неверный вариант", show_alert=True)
                return
            days = ANTIVIRUS_OPTIONS[option]["days"]
            price = ANTIVIRUS_OPTIONS[option]["price"]
            booster_type = "antivirus"
            name = "🦠 Антивирус"
            emoji = "🦠"
            
        else:
            await callback.answer("❌ Неизвестный вариант", show_alert=True)
            return
        
        # Клавиатура с выбором способа оплаты
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💳 Картой", callback_data=f"payment_card:{booster_type}:{days}"),
                InlineKeyboardButton(text="📱 СБП", callback_data=f"payment_sbp:{booster_type}:{days}")
            ],
            [
                InlineKeyboardButton(text="💎 Crypto (TON/USDT)", callback_data=f"payment_crypto:{booster_type}:{days}")
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"donate_{booster_type}")]
        ])
        
        text = (
            f"{emoji} <b>Оплата {name}</b>\n\n"
            f"📅 <b>Срок:</b> {days} дней\n"
            f"💰 <b>Сумма:</b> {price} ₽\n\n"
            f"💳 <b>Выберите способ оплаты:</b>"
        )
        
        await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="HTML")
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in payment_callback: {e}")
        await callback.answer("⚠️ Ошибка при оформлении", show_alert=True)

@dp.callback_query(F.data.startswith("payment_"))
async def payment_method_callback(callback: CallbackQuery):
    """Обработчик выбора способа оплаты"""
    try:
        # Проверяем, если это группа - просим перейти в ЛС
        if callback.message.chat.type != 'private':
            await callback.message.edit_text(
                "💬 <b>Для оформления заказа перейдите в ЛС с ботом:</b>\n"
                "👉 @CryptoMiner_sBot",
                parse_mode="HTML"
            )
            await callback.answer()
            return
            
        method, booster_type, days = callback.data.replace("payment_", "").split(":")
        days = int(days)
        
        # Получаем информацию о товаре
        if booster_type == "premium":
            price = PREMIUM_OPTIONS[f"premium_{days}"]["price"]
            name = "👑 PREMIUM статус"
            emoji = "👑"
        elif booster_type == "cleaner":
            price = CLEANER_OPTIONS[f"cleaner_{days}"]["price"]
            name = "🧹 Уборщик фермы"
            emoji = "🧹"
        elif booster_type == "investor":
            price = INVESTOR_OPTIONS[f"investor_{days}"]["price"]
            name = "🏢 Инвесторы бизнесов"
            emoji = "🏢"
        elif booster_type == "automation":
            price = AUTOMATION_OPTIONS[f"automation_{days}"]["price"]
            name = "🤖 Автоматизация"
            emoji = "🤖"
        elif booster_type == "antivirus":  # Добавляем антивирус
            price = ANTIVIRUS_OPTIONS[f"antivirus_{days}"]["price"]
            name = "🦠 Антивирус"
            emoji = "🦠"
        else:
            await callback.answer("❌ Ошибка", show_alert=True)
            return
        
        payment_methods = {
            "card": {
                "name": "💳 Банковская карта",
                "text": (
                    f"{emoji} <b>Оплата картой - {name}</b>\n\n"
                    f"📅 <b>Срок:</b> {days} дней\n"
                    f"💰 <b>Сумма:</b> {price} ₽\n\n"
                    f"👉 <a href='{PAYMENT_LINK}'>Ссылка для оплаты</a>\n\n"
                    f"<b>⚠️ ВАЖНО!</b>\n"
                    f"• В комментарии укажите ваш USERNAME!\n"
                    f"• После оплаты нажмите '✅ Я оплатил'\n\n"
                    f"🔒 Оплата через защищенный сервис"
                )
            },
            "sbp": {
                "name": "📱 СБП",
                "text": (
                    f"{emoji} <b>Оплата по СБП - {name}</b>\n\n"
                    f"📅 <b>Срок:</b> {days} дней\n"
                    f"💰 <b>Сумма:</b> {price} ₽\n\n"
                    f"<b>📲 Реквизиты:</b>\n"
                    f"• Банк: <code>T-BANK</code>\n"
                    f"• Номер: <code>+79259101233</code>\n\n"
                    f"<b>⚠️ ВАЖНО!</b>\n"
                    f"• В комментарии укажите ваш USERNAME!\n"
                    f"• После оплаты нажмите '✅ Я оплатил'\n\n"
                    f"⚡ Перевод проходит мгновенно"
                )
            },
            "crypto": {
                "name": "💎 Crypto (TON/USDT)",
                "text": (
                    f"{emoji} <b>Оплата криптовалютой - {name}</b>\n\n"
                    f"📅 <b>Срок:</b> {days} дней\n"
                    f"💰 <b>Сумма:</b> {price} ₽\n\n"
                    f"<b>💎 Доступные валюты:</b>\n"
                    f"• TON\n"
                    f"<b>📝 Инструкция:</b>\n"
                    f"1. Напишите администратору @otttimict\n"
                    f"2. Укажите выбранный бустер\n"
                    f"3. Получите реквизиты для оплаты\n"
                    f"4. Совершите перевод\n\n"
                    f"<b>⚠️ ВАЖНО!</b>\n"
                    f"• После оплаты нажмите '✅ Я оплатил'"
                )
            }
        }
        
        if method not in payment_methods:
            await callback.answer("❌ Неизвестный способ оплаты", show_alert=True)
            return
        
        payment_info = payment_methods[method]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"confirm:{booster_type}:{days}:{method}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{booster_type}_{days}")]
        ])
        
        # Для крипты добавляем кнопку написания админу
        if method == "crypto":
            keyboard.inline_keyboard.insert(0, [
                InlineKeyboardButton(text="💌 Написать админу", url="https://t.me/TheLuni")
            ])
        
        await callback.message.edit_text(
            payment_info["text"],
            reply_markup=keyboard,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Error in payment_method_callback: {e}")
        await callback.answer("⚠️ Ошибка при выборе способа оплаты", show_alert=True)

@dp.callback_query(F.data.startswith("confirm:"))
async def confirm_payment(callback: CallbackQuery, bot: Bot):
    """Подтверждение оплаты пользователем"""
    try:
        # Проверяем, если это группа - просим перейти в ЛС
        if callback.message.chat.type != 'private':
            await callback.message.edit_text(
                "💬 <b>Для оформления заказа перейдите в ЛС с ботом:</b>\n"
                "👉 @CryptoMiner_sBot",
                parse_mode="HTML"
            )
            await callback.answer()
            return
            
        _, booster_type, days, payment_method = callback.data.split(":")
        days = int(days)
        user = callback.from_user
        
        # Получаем информацию о товаре
        if booster_type == "premium":
            price = PREMIUM_OPTIONS[f"premium_{days}"]["price"]
            name = "👑 PREMIUM статус"
        elif booster_type == "cleaner":
            price = CLEANER_OPTIONS[f"cleaner_{days}"]["price"]
            name = "🧹 Уборщик фермы"
        elif booster_type == "investor":
            price = INVESTOR_OPTIONS[f"investor_{days}"]["price"]
            name = "🏢 Инвесторы бизнесов"
        elif booster_type == "automation":  # Добавляем автоматизацию
            price = AUTOMATION_OPTIONS[f"automation_{days}"]["price"]
            name = "🤖 Автоматизация"
        elif booster_type == "antivirus":  # Добавляем антивирус
            price = ANTIVIRUS_OPTIONS[f"antivirus_{days}"]["price"]
            name = "🦠 Антивирус"
        else:
            await callback.answer("Неизвестный тип бустера", show_alert=True)
            return
        
        # Определяем способ оплаты для админа
        payment_methods = {
            "card": "💳 Карта",
            "sbp": "📱 СБП", 
            "crypto": "💎 Crypto (TON/USDT)"
        }
        payment_name = payment_methods.get(payment_method, "❓ Неизвестно")
        
        # Уведомление администратора
        mod_text = (
            f"🛒 <b>Новый заказ</b>\n\n"
            f"👤 <b>Пользователь:</b> {user.full_name}\n"
            f"🆔 <b>ID:</b> <code>{user.id}</code>\n"
            f"📦 <b>Товар:</b> {name}\n"
            f"📅 <b>Срок:</b> {days} дней\n"
            f"💰 <b>Сумма:</b> {price} ₽\n"
            f"💳 <b>Способ оплаты:</b> {payment_name}\n\n"
            f"🔎 <b>Подтвердите получение оплаты:</b>"
        )
        
        mod_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить", 
                    callback_data=f"approve:{booster_type}:{user.id}:{days}"
                ),
                InlineKeyboardButton(
                    text="❌ Отклонить", 
                    callback_data=f"reject:{booster_type}:{user.id}"
                )
            ]
        ])
        
        await bot.send_message(
            chat_id=MOD_CHAT_ID,
            text=mod_text,
            reply_markup=mod_keyboard,
            parse_mode="HTML"
        )
        
        await callback.message.edit_text(
            f"✅ <b>Заявка отправлена</b>\n\n"
            f"Ваш заказ на {name} отправлен на проверку администратору. "
            f"Ожидайте активации в течение 15-30 минут.\n\n"
            f"📞 По вопросам: @otttimict",
            parse_mode="HTML"
        )
        await callback.answer()
        
    except Exception as e:
        logger.error(f"Confirm payment error: {e}")
        await callback.answer("⚠️ Ошибка при подтверждении платежа", show_alert=True)


@dp.callback_query(F.data.startswith("approve:"))
async def approve_payment(callback: CallbackQuery, bot: Bot):
    """Обработчик подтверждения платежа администратором"""
    try:
        _, booster_type, user_id, days = callback.data.split(":")
        user_id = int(user_id)
        days = int(days)

        if booster_type == "premium":
            success = add_premium_time(user_id, days)
            name = "👑 PREMIUM статус"
        elif booster_type == "cleaner":
            success = add_cleaner_booster(user_id, days)
            name = "🧹 Уборщик фермы"
        elif booster_type == "investor":
            success = add_investor_booster(user_id, days)
            name = "🏢 Инвесторы бизнесов"
        elif booster_type == "automation":
            success = grant_automation_access(user_id, days)
            name = "🤖 Автоматизация"
        elif booster_type == "antivirus":  # Добавляем антивирус
            success = grant_antivirus_access(user_id, days)
            name = "🦠 Антивирус"
        else:
            await callback.answer("Неизвестный тип бустера", show_alert=True)
            return

        if success:
            await callback.message.edit_text(
                f"✅ <b>{name} активирован</b>\n\n"
                f"Пользователю: <code>{user_id}</code>\n"
                f"Срок: {days} дней",
                parse_mode="HTML"
            )
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"🎉 <b>Ваш {name} на {days} дней активирован!</b>\n\nСпасибо за поддержку! 💫",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка уведомления пользователя: {e}")
            
            await callback.answer("✅ Успешно активировано", show_alert=True)
        else:
            await callback.answer("❌ Ошибка активации", show_alert=True)

    except Exception as e:
        logger.error(f"Ошибка в approve_payment: {e}")
        await callback.answer("⚠️ Ошибка при обработке", show_alert=True)
        
@dp.callback_query(F.data.startswith("reject:"))
async def reject_payment(callback: CallbackQuery, bot: Bot):
    """Обработчик отклонения платежа администратором"""
    try:
        _, booster_type, user_id = callback.data.split(":")
        user_id = int(user_id)

        if booster_type == "premium":
            name = "👑 PREMIUM статус"
        elif booster_type == "cleaner":
            name = "🧹 Уборщик фермы"
        elif booster_type == "investor":
            name = "🏢 Инвесторы бизнесов"
        elif booster_type == "automation":
            name = "🤖 Автоматизация"
        elif booster_type == "antivirus":  # Добавляем антивирус
            name = "🦠 Антивирус"
        else:
            await callback.answer("Неизвестный тип бустера", show_alert=True)
            return

        await callback.message.edit_text(
            f"❌ <b>Заказ отклонен</b>\n\n"
            f"Пользователь: <code>{user_id}</code>\n"
            f"Товар: {name}",
            parse_mode="HTML"
        )
        
        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"⚠️ <b>Ваш заказ на {name} был отклонен.</b>\n\n"
                     f"Если вы произвели оплату, свяжитесь с администратором: @otttimict",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления пользователя: {e}")
            
        await callback.answer("❌ Заказ отклонен", show_alert=True)

    except Exception as e:
        logger.error(f"Ошибка в reject_payment: {e}")
        await callback.answer("⚠️ Ошибка при обработке", show_alert=True)
        
def log_chat_income(chat_id: int, user_id: int, income: float):
    """Логирует доход для чата (без учета бустеров и премиума)"""
    try:
        # Рассчитываем базовый доход (без бустеров и премиума)
        base_income = calculate_base_income(user_id)
        
        cursor.execute('''
        INSERT INTO chat_income_log (chat_id, user_id, btc_income, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (chat_id, user_id, base_income, datetime.now().isoformat()))
        
        cursor.execute('''
        UPDATE chat_stats 
        SET weekly_btc_earned = weekly_btc_earned + ?,
            last_updated = ?
        WHERE chat_id = ?
        ''', (base_income, datetime.now().isoformat(), chat_id))
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error logging chat income: {e}")
        conn.rollback()

@dp.message(Command("delete_chat"))
async def delete_chat_command(message: Message):
    # Проверяем, что команду вызывает владелец
    if message.from_user.id not in ADMINS:
        await message.answer("❌ У вас нет прав на эту команду")
        return

    # Проверяем, есть ли аргумент (ID чата)
    if len(message.text.split()) < 2:
        await message.answer("ℹ️ Использование: /delete_chat <chat_id>")
        return

    try:
        chat_id = int(message.text.split()[1])
    except ValueError:
        await message.answer("❌ Неверный формат ID. Укажите числовой ID чата")
        return

    try:
        # Удаляем все данные чата из всех таблиц
        with conn:
            # 1. Удаляем участников чата
            cursor.execute('DELETE FROM chat_members WHERE chat_id = ?', (chat_id,))
            
            # 2. Удаляем записи о доходах чата
            cursor.execute('DELETE FROM chat_income_log WHERE chat_id = ?', (chat_id,))
            
            # 3. Удаляем статистику чата
            cursor.execute('DELETE FROM chat_stats WHERE chat_id = ?', (chat_id,))
            
        # Получаем название чата для отчета
        cursor.execute('SELECT title FROM chat_stats WHERE chat_id = ?', (chat_id,))
        chat_title = cursor.fetchone()
        chat_name = chat_title[0] if chat_title else f"ID {chat_id}"
        
        await message.answer(f"✅ Чат '{chat_name}' (ID: {chat_id}) и все связанные данные успешно удалены")
        
    except Exception as e:
        logger.error(f"Error deleting chat: {e}")
        conn.rollback()
        await message.answer(f"❌ Ошибка при удалении чата {chat_id}: {str(e)}")
        
@dp.message(Command("add_booster"))
async def manual_add_booster(message: Message, bot: Bot):
    """Ручная выдача бустера администратором"""
    try:
        if message.from_user.id != ADMIN_ID:
            await message.answer("❌ У вас нет прав на эту команду")
            return
        
        parts = message.text.split()
        if len(parts) != 4:
            await message.answer(
                "ℹ️ Формат команды:\n"
                "/add_booster [booster_type] [user_id] [days]\n\n"
                "Доступные типы бустеров:\n"
                "premium - Премиум статус\n"
                "cleaner - Уборщик фермы\n"
                "investor - Инвесторы бизнесов"
            )
            return
            
        try:
            booster_type = parts[1]
            user_id = int(parts[2])
            days = int(parts[3])
        except ValueError:
            await message.answer("❌ Неверный формат. user_id и days должны быть числами")
            return
            
        success = False
        name = ""
        
        if booster_type == "premium":
            success = add_premium_time(user_id, days)
            name = "PREMIUM статус"
        elif booster_type == "cleaner":
            success = add_cleaner_booster(user_id, days)
            name = "Уборщик фермы"
        elif booster_type == "investor":
            success = add_investor_booster(user_id, days)
            name = "Инвесторы бизнесов"
        else:
            await message.answer("❌ Неизвестный тип бустера")
            return
            
        if success:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"🎉 Вам был выдан {name} на {days} дней администратором!"
                )
            except Exception as e:
                logger.error(f"Error notifying user: {e}")
                await message.answer(f"✅ {name} выдан, но не удалось уведомить пользователя: {e}")
            else:
                await message.answer(f"✅ {name} на {days} дней успешно выдан пользователю {user_id}")
        else:
            await message.answer(f"❌ Ошибка при выдаче {name}")
            
    except Exception as e:
        logger.error(f"Error in manual_add_booster: {e}")
        await message.answer("❌ Произошла ошибка при выполнении команды")
        
async def get_chat_title(chat_id: int) -> str:
    """Получает текущее название чата через API Telegram"""
    try:
        chat = await bot.get_chat(chat_id)
        return chat.title
    except Exception as e:
        logger.error(f"Error getting chat title for {chat_id}: {e}")
        return None

async def on_chat_title_update(message: types.Message):
    """Обработчик изменения названия чата"""
    try:
        # Проверяем что это группа/супергруппа и есть новое название
        if message.chat.type not in ["group", "supergroup"]:
            return
        if not message.new_chat_title:
            return

        chat_id = message.chat.id
        new_title = message.new_chat_title
        
        # Обновляем название в базе данных
        cursor.execute('''
            UPDATE chat_stats 
            SET title = ?
            WHERE chat_id = ?
        ''', (new_title, chat_id))
        conn.commit()
        
        logger.info(f"Chat {chat_id} title updated to '{new_title}'")
    except Exception as e:
        logger.error(f"Error handling chat title update: {e}")

# Регистрируем обработчик с фильтрами через F
dp.message.register(
    on_chat_title_update,
    F.chat.type.in_({"group", "supergroup"}) & F.new_chat_title
)


async def update_chat_titles():
    """Обновляет названия чатов в базе данных с пакетной обработкой"""
    try:
        # Получаем только необходимые данные
        cursor.execute('SELECT chat_id FROM chat_stats WHERE members_count > 0')
        active_chat_ids = [row[0] for row in cursor.fetchall()]
        
        if not active_chat_ids:
            return
            
        updates = []
        
        # Используем asyncio.gather для параллельного получения названий
        tasks = [get_chat_title(chat_id) for chat_id in active_chat_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for chat_id, new_title in zip(active_chat_ids, results):
            if isinstance(new_title, Exception) or not new_title:
                continue
                
            # Получаем текущее название только для чатов с изменением
            cursor.execute('SELECT title FROM chat_stats WHERE chat_id = ?', (chat_id,))
            current_title = cursor.fetchone()[0]
            
            if new_title != current_title:
                updates.append((new_title, chat_id))
                logger.debug(f"Chat {chat_id} title changed from '{current_title}' to '{new_title}'")
        
        # Массовое обновление измененных названий
        if updates:
            cursor.executemany('UPDATE chat_stats SET title = ? WHERE chat_id = ?', updates)
            conn.commit()
            logger.info(f"Updated {len(updates)} chat titles")
            
    except Exception as e:
        logger.error(f"Error updating chat titles: {e}")
        conn.rollback()            
            

async def chat_income_task():
    """Основная задача для начисления дохода чатам"""
    try:
        # Получаем все активные чаты
        cursor.execute('SELECT chat_id FROM chat_stats WHERE members_count > 0')
        active_chats = cursor.fetchall()
        
        if not active_chats:
            logger.info("No active chats found for income distribution")
            return
            
        logger.info(f"Processing income for {len(active_chats)} chats")
        
        for (chat_id,) in active_chats:
            members = get_chat_members(chat_id)
            if not members:
                continue
                
            total_chat_income = 0.0
            
            for user_id in members:
                # Рассчитываем базовый доход без премиум-бонусов
                user_income = calculate_base_income(user_id)
                total_chat_income += user_income
                log_chat_income(chat_id, user_id, user_income)
                
                logger.debug(f"Added {user_income} BTC from user {user_id} to chat {chat_id}")
            
            # Обновляем общий доход чата
            cursor.execute('''
                UPDATE chat_stats 
                SET weekly_btc_earned = weekly_btc_earned + ?,
                    last_updated = ?
                WHERE chat_id = ?
            ''', (total_chat_income, datetime.now().isoformat(), chat_id))
            
            logger.info(f"Added {total_chat_income} BTC to chat {chat_id}")
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error in chat income task: {e}")
        conn.rollback()

async def run_chat_income():
    """Бесконечный цикл для начисления дохода чатам каждые 10 минут"""
    while True:
        try:
            now = datetime.now()
            # Вычисляем следующее время запуска (кратное 10 минутам)
            next_run = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0) + timedelta(minutes=10)
            wait_seconds = (next_run - now).total_seconds()
            
            if wait_seconds > 0:
                logger.debug(f"Waiting {wait_seconds} seconds until next income distribution")
                await asyncio.sleep(wait_seconds)
            
            await chat_income_task()
            
            # Дополнительно обновляем названия чатов каждый час
            if datetime.now().minute == 0:
                await update_chat_titles()
                
        except Exception as e:
            logger.error(f"Error in chat income loop: {e}")
            await asyncio.sleep(60)  # Подождем минуту при ошибке

from aiogram import types
from aiogram import F
from aiogram.filters import Command



import aiohttp
        
BTC_PRICE = 100000.0  # Значение по умолчанию
LAST_BTC_UPDATE = datetime.min

async def update_btc_price():
    """Обновляет курс BTC каждые 15 минут"""
    global BTC_PRICE, LAST_BTC_UPDATE
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd',
                    timeout=10
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        new_price = float(data['bitcoin']['usd'])
                        if new_price != BTC_PRICE:
                            BTC_PRICE = new_price
                            LAST_BTC_UPDATE = datetime.now()
                            logger.info(f"BTC price updated to: {BTC_PRICE}")
                            # Принудительно выводим новый курс в лог
                            print(f"NEW BTC PRICE: {BTC_PRICE}")
                    else:
                        logger.warning(f"CoinGecko API returned status {response.status}")
        except Exception as e:
            logger.error(f"Error updating BTC price: {e}")
        
        await asyncio.sleep(900)

async def run_btc_updater():
    """Запускает обновление курса BTC в фоне"""
    asyncio.create_task(update_btc_price())

def get_btc_price() -> float:
    """Возвращает текущий курс BTC"""
    # Если данные устарели (больше 30 минут), логируем предупреждение
    if (datetime.now() - LAST_BTC_UPDATE).total_seconds() > 1800:
        logger.warning(f"Using potentially stale BTC price (last update: {LAST_BTC_UPDATE})")
    return BTC_PRICE
    
def reset_monthly_bp_progress():
    """Сбрасывает уровень BP на 1 для всех пользователей в начале месяца"""
    try:
        cursor.execute('''
        UPDATE user_bp_progress
        SET current_level = 1, current_exp = 0
        ''')
        conn.commit()
        logger.info("Monthly BP progress reset: all levels set to 1")
        return True
    except Exception as e:
        logger.error(f"Error in reset_monthly_bp_progress: {e}")
        conn.rollback()
        return False

async def monthly_bp_reset_task():
    """Проверяет и сбрасывает BP каждое 1 число месяца в 00:00"""
    while True:
        try:
            now = datetime.now()
            # Если сегодня 1-е число и время между 00:00 и 00:05
            if now.day == 1 and now.hour == 0 and now.minute <= 5:
                reset_monthly_bp_progress()
                logger.info("Monthly BP progress reset completed")
                # Ждем до следующего дня, чтобы не сбросить повторно
                await asyncio.sleep(24 * 60 * 60)
            else:
                # Проверяем каждый час
                await asyncio.sleep(60 * 60)
        except Exception as e:
            logger.error(f"Error in monthly_bp_reset_task: {e}")
            await asyncio.sleep(60 * 60)                         
        


def get_active_boosters(user_id: int) -> Dict[str, float]:
    """Возвращает активные бустеры с их множителями"""
    boosters = {}
    now = datetime.now().isoformat()
    
    # Проверяем премиум
    if is_premium(user_id):
        boosters['premium'] = 0.35  # +35% к доходу фермы
    
    # Проверяем активные бустеры
    cursor.execute('''
    SELECT booster_type FROM user_boosters 
    WHERE user_id = ? AND until > ?
    ''', (user_id, now))
    
    for (booster_type,) in cursor.fetchall():
        if booster_type == 'cleaner':
            boosters['cleaner'] = 0.25  # +25% к доходу фермы
        elif booster_type == 'investor':
            boosters['investor'] = 0.15  # +15% к доходу бизнесов
    
    return boosters
    
async def check_expired_boosters():
    """Проверяет истекшие бустеры и пересчитывает доходы"""
    try:
        now = datetime.now().isoformat()
        # Находим пользователей, у которых только что истекли бустеры
        cursor.execute('''
        SELECT DISTINCT user_id FROM user_boosters 
        WHERE until <= ? 
        AND user_id IN (SELECT user_id FROM users)
        ''', (now,))
        
        users_to_update = [row[0] for row in cursor.fetchall()]
        
        # Удаляем истекшие бустеры
        cursor.execute('DELETE FROM user_boosters WHERE until <= ?', (now,))
        
        # Пересчитываем доход для этих пользователей
        for user_id in users_to_update:
            calculate_income(user_id)
        
        conn.commit()
    except Exception as e:
        logger.error(f"Error checking expired boosters: {e}")
        conn.rollback()

@dp.message(Command("clear_boosters"))
async def clear_all_boosters(message: Message):
    """Команда для очистки всех бустеров пользователя (только для владельца)"""
    if message.from_user.id not in ADMINS:  # Проверка на владельца (замените на ваш ID)
        await message.answer("❌ У вас нет прав на эту команду")
        return

    try:
        # Проверяем формат команды: /clear_boosters [user_id]
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer(
                "ℹ️ Формат команды:\n"
                "/clear_boosters [user_id]\n\n"
                "Пример:\n"
                "/clear_boosters 123456789 - очистит все бустеры у пользователя с ID 123456789"
            )
            return

        target_user_id = int(parts[1])

        # Удаляем все бустеры
        with conn:
            # Удаляем уборщиков и инвесторов
            cursor.execute('''
            DELETE FROM user_boosters 
            WHERE user_id = ? AND booster_type IN ('cleaner', 'investor')
            ''', (target_user_id,))

            # Удаляем премиум статус
            cursor.execute('''
            DELETE FROM premium_users 
            WHERE user_id = ?
            ''', (target_user_id,))

            # Пересчитываем доход пользователя
            calculate_income(target_user_id)

        await message.answer(f"✅ Все бустеры пользователя {target_user_id} успешно удалены")

    except ValueError:
        await message.answer("❌ Неверный формат ID пользователя. Укажите числовой ID")
    except Exception as e:
        logger.error(f"Error clearing boosters: {e}")
        await message.answer(f"❌ Произошла ошибка при очистке бустеров: {str(e)}")

async def auto_rembp_job():
    """Автоматическое обновление BP заданий в 21:00 по Москве"""
    try:
        moscow_tz = pytz.timezone('Europe/Moscow')
        now = datetime.now(moscow_tz)
        logger.info(f"Auto BP update job triggered at {now}")

        # Проверяем, нужно ли сбросить уровень BP (1 число месяца)
        is_first_of_month = now.day == 1

        cursor.execute('SELECT user_id FROM users')
        users = cursor.fetchall()

        # Обрабатываем пользователей пакетами по 100 для избежания переполнения БД
        batch_size = 100
        for idx, (user_id,) in enumerate(users):
            # Проверяем текущие данные в user_bp_progress
            cursor.execute('SELECT current_level, current_exp FROM user_bp_progress WHERE user_id = ?', (user_id,))
            result = cursor.fetchone()

            # Получаем доступные задания
            available_tasks = BP_TASKS.copy()

            # Исключаем задания, выполненные в текущем месяце
            cursor.execute('''
                SELECT DISTINCT task_id FROM user_bp_history
                WHERE user_id = ? AND strftime('%Y-%m', completion_date) = ?
            ''', (user_id, now.strftime('%Y-%m')))
            completed_tasks = [row[0] for row in cursor.fetchall()]

            if completed_tasks:
                available_tasks = [t for t in available_tasks if t['id'] not in completed_tasks]

            # Если все задания выполнены, используем все доступные
            if not available_tasks:
                available_tasks = BP_TASKS

            # Выбираем случайное задание
            if available_tasks:
                task = random.choice(available_tasks)
            else:
                logger.error(f"No available tasks for user {user_id}")
                continue

            if not result:
                # Создаем новую запись для нового пользователя с уровнем по умолчанию
                cursor.execute('''
                    INSERT INTO user_bp_progress
                    (user_id, current_level, current_exp, current_task_id, task_progress, last_task_date, completed_tasks)
                    VALUES (?, 1, 0, ?, 0, ?, 0)
                ''', (user_id, task['id'], now.isoformat()))
                logger.info(f"Created BP progress for user {user_id} with level 1, task_id: {task['id']}")
            else:
                # Если 1 число месяца - сбрасываем уровень на 0
                if is_first_of_month:
                    cursor.execute('''
                        UPDATE user_bp_progress
                        SET
                            current_level = 0,
                            current_exp = 0,
                            current_task_id = ?,
                            task_progress = 0,
                            last_task_date = ?
                        WHERE user_id = ?
                    ''', (task['id'], now.isoformat(), user_id))
                    logger.info(f"MONTHLY RESET for user {user_id}: BP level reset to 0, new task_id: {task['id']}")
                else:
                    # Обычное обновление - сохраняем current_level и current_exp
                    old_level, old_exp = result
                    cursor.execute('''
                        UPDATE user_bp_progress
                        SET
                            current_task_id = ?,
                            task_progress = 0,
                            last_task_date = ?
                        WHERE user_id = ?
                    ''', (task['id'], now.isoformat(), user_id))
                    logger.info(f"Updated task for user {user_id}, level remains {old_level}, exp remains {old_exp}, new task_id: {task['id']}")

            # Делаем commit после каждых batch_size пользователей
            if (idx + 1) % batch_size == 0:
                conn.commit()
                logger.info(f"Committed batch {idx + 1}/{len(users)}")

        conn.commit()
        
        # Отправляем уведомление администраторам
        try:
            for admin_id in ADMINS:
                await bot.send_message(admin_id, "🔄 BP UPDATE: Задания для всех пользователей успешно обновлены автоматически в 21:00")
            logger.info("BP update notification sent to admins")
        except Exception as e:
            logger.error(f"Failed to send BP update notification: {e}")
            
        logger.info("Auto BP update completed successfully")

    except Exception as e:
        logger.error(f"Error in auto BP update job: {e}", exc_info=True)
        conn.rollback()
        
        # Отправляем уведомление об ошибке администраторам
        try:
            for admin_id in ADMINS:
                await bot.send_message(admin_id, f"❌ BP UPDATE ERROR: {str(e)}")
        except Exception as notify_error:
            logger.error(f"Failed to send error notification: {notify_error}")

@dp.message(Command("force_bp_update"))
async def force_bp_update(message: Message):
    """Принудительное обновление BP заданий для всех пользователей (только для админа)"""

    if message.from_user.id not in ADMINS:
        await message.answer("❌ Эта команда только для администраторов")
        return
    
    try:
        await message.answer("🔄 Запускаю принудительное обновление BP заданий...")
        
        # Вызываем функцию обновления BP заданий
        await auto_rembp_job()
        
        await message.answer("✅ Принудительное обновление BP заданий успешно завершено!")
        
    except Exception as e:
        logger.error(f"Error in force BP update: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка при обновлении BP заданий: {str(e)}")            
                                    
def initialize_tax_system():
    """Инициализирует налоговую систему при запуске бота"""
    try:
        logger.info("Налоговая система инициализирована")
    except Exception as e:
        logger.error(f"Error initializing tax system: {e}")
                    

async def main():
    global BOT_START_TIME
    BOT_START_TIME = datetime.now()   
    
    # Запуск фоновых задач без await (они должны работать параллельно)
    asyncio.create_task(start_virus_checker())
    asyncio.create_task(run_btc_updater())
    asyncio.create_task(start_social_bonus_checker())
    asyncio.create_task(start_scavenger_scheduler())
    asyncio.create_task(start_tax_checker())
    asyncio.create_task(start_daily_bank_processing())  # ✅ ДОБАВЛЕНО: Ежедневная обработка банковских операций
    asyncio.create_task(start_automation_scheduler())   # ✅ ДОБАВЛЕНО: Планировщик автоматизации "Рабы"
    asyncio.create_task(monthly_bp_reset_task())  # ✅ ДОБАВЛЕНО: Сброс BP каждое 1-е число месяца

    # Автоматические еженедельные функции
    async def auto_weekly_reset():
        """Автоматический сброс топа чатов и сбора каждое воскресенье в 18:00"""
        try:
            logger.info("🔄 Starting automatic weekly reset (chats + fund)")

            # 1. Сброс статистики чатов
            reset_weekly_stats()
            logger.info("✅ Chat stats reset completed")

            # 2. Сброс сбора
            cursor.execute('''
            UPDATE server_fund
            SET status = 'cancelled', end_date = ?
            WHERE status = 'active'
            ''', (datetime.now().isoformat(),))

            create_new_fund()
            conn.commit()
            logger.info("✅ Fund reset completed")

            # Уведомляем админов
            for admin_id in ADMINS:
                try:
                    await bot.send_message(
                        admin_id,
                        "🔄 <b>Автоматический недельный сброс завершён</b>\n\n"
                        "✅ Топ чатов сброшен\n"
                        "✅ Сбор сброшен и создан новый\n\n"
                        "⏰ Время: воскресенье 18:00 МСК",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Error sending reset notification to admin {admin_id}: {e}")

        except Exception as e:
            logger.error(f"Error in auto_weekly_reset: {e}")
            conn.rollback()

    async def auto_generate_promo():
        """Автоматическая генерация промокодов каждое воскресенье в 18:05"""
        try:
            logger.info("🎁 Starting automatic promo code generation")

            # Генерируем первый промокод (300 использований, 10-40 часов)
            hours1 = random.randint(10, 40)
            multiplier1 = hours1 * 6
            code1 = create_promo_code(ADMINS[0], 300, multiplier1)

            # Генерируем второй промокод (5 использований, 40-70 часов)
            hours2 = random.randint(40, 70)
            multiplier2 = hours2 * 6
            code2 = create_promo_code(ADMINS[0], 5, multiplier2)

            # Формируем текст для публикации (как в примере)
            promo_text = (
                f"✨ Новые промокоды на этой неделе!\n\n"
                f"🔹 Промокод на {hours1}ч. заработка (300 использований):\n"
                f"/promo {code1}\n\n"
                f"🔹 Промокод для быстрых на {hours2}ч. заработка (5 использований):\n"
                f"{code2}"
            )

            logger.info(f"✅ Promo codes generated: {code1}, {code2}")

            # Отправляем админам
            for admin_id in ADMINS:
                try:
                    await bot.send_message(
                        admin_id,
                        "🎁 <b>АВТОМАТИЧЕСКИЕ ПРОМОКОДЫ</b>\n\n"
                        "Промокоды сгенерированы!\n"
                        "Скопируй и опубликуй этот текст:\n\n"
                        "━━━━━━━━━━━━━━━\n\n" +
                        promo_text,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Error sending promo to admin {admin_id}: {e}")

        except Exception as e:
            logger.error(f"Error in auto_generate_promo: {e}")

    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Europe/Moscow'))

    # Добавление задач в планировщик
    scheduler.add_job(check_expired_boosters, 'interval', minutes=10)
    scheduler.add_job(reset_daily_bp_tasks, 'cron', hour=21, minute=2)
    scheduler.add_job(background_business_income, 'interval', minutes=10)
    
    # ✅ ДОБАВЛЕНО: Автоматическое обновление BP в 21:00
    scheduler.add_job(auto_rembp_job, 'cron', hour=0, minute=0, timezone=pytz.timezone('Europe/Moscow'))
    
    # ✅ ДОБАВЛЕНО: Банковские задачи
    scheduler.add_job(process_loan_payments, 'cron', day_of_week='mon', hour=0, minute=0)  # Платежи по кредитам
    scheduler.add_job(process_deposit_interests, 'cron', day_of_week='mon', hour=0, minute=5)  # Проценты по вкладам
    scheduler.add_job(check_deposit_maturity, 'cron', day_of_week='mon', hour=0, minute=10)  # Проверка зрелости вкладов
    
    # ✅ ДОБАВЛЕНО: Еженедельные задачи
    scheduler.add_job(distribute_premium_rewards, 'cron', day_of_week='sun', hour=18, minute=0)  # Награды за чаты в 18:00 воскресенье
    scheduler.add_job(auto_weekly_reset, 'cron', day_of_week='sun', hour=18, minute=1)  # Сброс чатов и сбора в 18:01 воскресенье
    scheduler.add_job(auto_generate_promo, 'cron', day_of_week='sun', hour=18, minute=5)  # Генерация промокодов в 18:05 воскресенье
    
    # Запуск планировщика
    scheduler.start()
    initialize_items()
    start_captcha_cleaner()
    # Инициализация систем
    initialize_tax_system()  # ✅ ДОБАВЛЕНО: Инициализация таблиц налогов
    initialize_fund_system()  # ✅ ДОБАВЛЕНО: Инициализация системы сборов
    
    # Запуск задачи для чатов как асинхронной задачи (не через планировщик)
    asyncio.create_task(run_chat_income())
    
    # Удаление вебхука
    await bot.delete_webhook(drop_pending_updates=True)
    await update_chat_titles()
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Bot error: {e}")
    finally:
        scheduler.shutdown()
        conn.close()
        await bot.session.close()     
        
if __name__ == '__main__':
    asyncio.run(main())