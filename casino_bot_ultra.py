"""
casino_bot_ultra.py

Функции:
- Своя ставка: /setbet 500
- Слоты / Рулетка / Блэкджек
- Кубики между игроками:
    /dice user_id сумма
    или ответом на сообщение: /dice сумма
- Работы:
    /work taxi     -> +2500
    /work captcha  -> +1000
- Переводы денег с комментарием:
    /pay user_id сумма комментарий
    /pay сумма комментарий   (ответом на сообщение)
- Баланс / Топ / Топ недели
- Недельная награда: топ-3 получают по 50 000
- Админка:
    /admin
    /give user_id сумма
    /setbal user_id сумма
    /ban user_id причина
    /unban user_id
    /odds
    /setodds slots 40
    /setodds roulette 48
    /setodds blackjack 45

Установка:
    python -m pip install aiogram aiosqlite

Для Railway:
- Background Worker
- Start Command: python casino_bot_ultra.py
- Variable: BOT_TOKEN=...
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_TOKEN_HERE")
ADMIN_IDS = {123456789}  # замени на свой Telegram ID
DB_PATH = "casino.db"

START_BALANCE = 5000
DEFAULT_BET = 100
WEEKLY_REWARD = 50000
WEEKLY_WINNERS_COUNT = 3

TAXI_REWARD = 2500
CAPTCHA_REWARD = 1000
TAXI_COOLDOWN = 60 * 30
CAPTCHA_COOLDOWN = 60 * 15

DEFAULT_ODDS = {
    "slots": 40,
    "roulette": 48,
    "blackjack": 45,
}

dp = Dispatcher()
db = None
blackjack_games = {}
dice_games = {}


@dataclass
class BJGame:
    bet: int
    player_total: int
    dealer_total: int


class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = None

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance INTEGER NOT NULL DEFAULT 0,
                current_bet INTEGER NOT NULL DEFAULT 100,
                weekly_profit INTEGER NOT NULL DEFAULT 0,
                total_profit INTEGER NOT NULL DEFAULT 0,
                is_banned INTEGER NOT NULL DEFAULT 0,
                ban_reason TEXT,
                last_taxi INTEGER NOT NULL DEFAULT 0,
                last_captcha INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        await self.conn.commit()

        for key, value in DEFAULT_ODDS.items():
            await self.set_state_if_missing(f"odds_{key}", str(value))

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    async def ensure_user(self, user_id: int, username: str | None) -> None:
        cur = await self.conn.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()

        if row is None:
            await self.conn.execute(
                """
                INSERT INTO users (
                    user_id, username, balance, current_bet, weekly_profit,
                    total_profit, is_banned, ban_reason, last_taxi, last_captcha
                )
                VALUES (?, ?, ?, ?, 0, 0, 0, NULL, 0, 0)
                """,
                (user_id, username, START_BALANCE, DEFAULT_BET),
            )
        else:
            await self.conn.execute(
                "UPDATE users SET username = ? WHERE user_id = ?",
                (username, user_id),
            )
        await self.conn.commit()

    async def get_balance(self, user_id: int) -> int:
        cur = await self.conn.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def get_bet(self, user_id: int) -> int:
        cur = await self.conn.execute("SELECT current_bet FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else DEFAULT_BET

    async def set_bet(self, user_id: int, amount: int) -> int:
        await self.conn.execute("UPDATE users SET current_bet = ? WHERE user_id = ?", (amount, user_id))
        await self.conn.commit()
        return await self.get_bet(user_id)

    async def add_balance(self, user_id: int, amount: int) -> int:
        await self.conn.execute(
            "UPDATE users SET balance = balance + ? WHERE user_id = ?",
            (amount, user_id),
        )
        await self.conn.commit()
        return await self.get_balance(user_id)

    async def set_balance(self, user_id: int, amount: int) -> int:
        await self.conn.execute(
            "UPDATE users SET balance = ? WHERE user_id = ?",
            (amount, user_id),
        )
        await self.conn.commit()
        return await self.get_balance(user_id)

    async def add_profit(self, user_id: int, amount: int) -> None:
        await self.conn.execute(
            """
            UPDATE users
            SET weekly_profit = weekly_profit + ?, total_profit = total_profit + ?
            WHERE user_id = ?
            """,
            (amount, amount, user_id),
        )
        await self.conn.commit()

    async def transfer(self, sender_id: int, target_id: int, amount: int) -> bool:
        if amount <= 0:
            return False
        sender_balance = await self.get_balance(sender_id)
        if sender_balance < amount:
            return False

        await self.conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, sender_id))
        await self.conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, target_id))
        await self.conn.commit()
        return True

    async def top_balance(self, limit: int = 10):
        cur = await self.conn.execute(
            "SELECT user_id, username, balance FROM users ORDER BY balance DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [(int(r[0]), r[1], int(r[2])) for r in rows]

    async def top_week(self, limit: int = 10):
        cur = await self.conn.execute(
            """
            SELECT user_id, username, weekly_profit
            FROM users
            ORDER BY weekly_profit DESC, balance DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [(int(r[0]), r[1], int(r[2])) for r in rows]

    async def get_state(self, key: str):
        cur = await self.conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None

    async def set_state(self, key: str, value: str) -> None:
        await self.conn.execute(
            """
            INSERT INTO bot_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        await self.conn.commit()

    async def set_state_if_missing(self, key: str, value: str) -> None:
        cur = await self.conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            await self.set_state(key, value)

    async def reward_weekly_top(self):
        cur = await self.conn.execute(
            """
            SELECT user_id, username
            FROM users
            WHERE weekly_profit > 0
            ORDER BY weekly_profit DESC, balance DESC
            LIMIT ?
            """,
            (WEEKLY_WINNERS_COUNT,),
        )
        rows = await cur.fetchall()
        await cur.close()

        winners = []
        for row in rows:
            user_id = int(row[0])
            username = row[1]
            winners.append((user_id, username))
            await self.conn.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (WEEKLY_REWARD, user_id),
            )

        await self.conn.execute("UPDATE users SET weekly_profit = 0")
        await self.conn.commit()
        return winners

    async def ban_user(self, user_id: int, reason: str | None) -> None:
        await self.conn.execute(
            "UPDATE users SET is_banned = 1, ban_reason = ? WHERE user_id = ?",
            (reason, user_id),
        )
        await self.conn.commit()

    async def unban_user(self, user_id: int) -> None:
        await self.conn.execute(
            "UPDATE users SET is_banned = 0, ban_reason = NULL WHERE user_id = ?",
            (user_id,),
        )
        await self.conn.commit()

    async def is_banned(self, user_id: int) -> bool:
        cur = await self.conn.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return bool(row[0]) if row else False

    async def ban_reason(self, user_id: int):
        cur = await self.conn.execute("SELECT ban_reason FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return row[0] if row else None

    async def get_last_job_time(self, user_id: int, job_name: str) -> int:
        column = "last_taxi" if job_name == "taxi" else "last_captcha"
        cur = await self.conn.execute(f"SELECT {column} FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def set_last_job_time(self, user_id: int, job_name: str, ts: int) -> None:
        column = "last_taxi" if job_name == "taxi" else "last_captcha"
        await self.conn.execute(f"UPDATE users SET {column} = ? WHERE user_id = ?", (ts, user_id))
        await self.conn.commit()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def card_value() -> int:
    return random.choice([2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 10, 10, 11])


def fmt_money(value: int) -> str:
    return f"{value:,}".replace(",", " ") + " 💵"


def fmt_name(user_id: int, username: str | None) -> str:
    return f"@{username}" if username else f"<code>{user_id}</code>"


def current_week_key() -> str:
    now = datetime.now(timezone.utc)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def sec_to_text(sec: int) -> str:
    minutes = sec // 60
    hours = minutes // 60
    minutes %= 60
    if hours > 0:
        return f"{hours} ч {minutes} мин"
    return f"{minutes} мин"


async def get_odds(game: str) -> int:
    value = await db.get_state(f"odds_{game}")
    return int(value) if value else DEFAULT_ODDS[game]


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎰 Слоты", callback_data="menu:slots"),
             InlineKeyboardButton(text="🎡 Рулетка", callback_data="menu:roulette")],
            [InlineKeyboardButton(text="🃏 Блэкджек", callback_data="menu:blackjack"),
             InlineKeyboardButton(text="🎲 Кубики", callback_data="menu:dice")],
            [InlineKeyboardButton(text="💼 Работы", callback_data="menu:jobs"),
             InlineKeyboardButton(text="💰 Баланс", callback_data="menu:balance")],
            [InlineKeyboardButton(text="🏆 Топ", callback_data="menu:top"),
             InlineKeyboardButton(text="📅 Топ недели", callback_data="menu:topweek")],
            [InlineKeyboardButton(text="🎯 Ставка", callback_data="menu:bet"),
             InlineKeyboardButton(text="📘 Помощь", callback_data="menu:help")],
            [InlineKeyboardButton(text="🛠 Админ", callback_data="menu:admin")],
        ]
    )


def bet_select_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="100", callback_data="setbet:100"),
             InlineKeyboardButton(text="500", callback_data="setbet:500"),
             InlineKeyboardButton(text="1000", callback_data="setbet:1000")],
            [InlineKeyboardButton(text="2500", callback_data="setbet:2500"),
             InlineKeyboardButton(text="5000", callback_data="setbet:5000")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")],
        ]
    )


def roulette_color_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔴 Красное", callback_data="roulette:red")],
            [InlineKeyboardButton(text="⚫ Чёрное", callback_data="roulette:black")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")],
        ]
    )


def blackjack_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Ещё", callback_data="blackjack:hit"),
             InlineKeyboardButton(text="✋ Стоп", callback_data="blackjack:stand")],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="menu:home")],
        ]
    )


def jobs_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚕 Таксист", callback_data="job:taxi")],
            [InlineKeyboardButton(text="🔐 Капча", callback_data="job:captcha")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:home")],
        ]
    )


async def ensure_user_from_message(message: Message) -> int:
    if message.from_user is None:
        raise RuntimeError("Не удалось определить пользователя")
    await db.ensure_user(message.from_user.id, message.from_user.username)
    if await db.is_banned(message.from_user.id):
        reason = await db.ban_reason(message.from_user.id)
        text = "⛔ Ты забанен."
        if reason:
            text += f"\nПричина: <b>{reason}</b>"
        await message.answer(text)
        raise RuntimeError("User banned")
    return message.from_user.id


async def ensure_user_from_callback(call: CallbackQuery) -> int:
    await db.ensure_user(call.from_user.id, call.from_user.username)
    if await db.is_banned(call.from_user.id):
        reason = await db.ban_reason(call.from_user.id)
        text = "⛔ Ты забанен."
        if reason:
            text += f"\nПричина: <b>{reason}</b>"
        await call.answer(text, show_alert=True)
        raise RuntimeError("User banned")
    return call.from_user.id


async def safe_edit_or_send(call: CallbackQuery, text: str, reply_markup=None) -> None:
    try:
        await call.message.edit_text(text, reply_markup=reply_markup)
    except Exception:
        await call.message.answer(text, reply_markup=reply_markup)
    await call.answer()


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    try:
        user_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    balance = await db.get_balance(user_id)
    bet = await db.get_bet(user_id)
    await message.answer(
        "🎰 <b>Добро пожаловать в Casino Bot</b>\n\n"
        f"Баланс: <b>{fmt_money(balance)}</b>\n"
        f"Текущая ставка: <b>{fmt_money(bet)}</b>\n\n"
        "Выбирай игру ниже.",
        reply_markup=main_menu(),
    )


@dp.message(Command("menu"))
async def cmd_menu(message: Message) -> None:
    try:
        await ensure_user_from_message(message)
    except RuntimeError:
        return
    await message.answer("🏠 <b>Главное меню</b>", reply_markup=main_menu())


@dp.message(Command("balance"))
async def cmd_balance(message: Message) -> None:
    try:
        user_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    balance = await db.get_balance(user_id)
    bet = await db.get_bet(user_id)
    await message.answer(f"💰 Баланс: <b>{fmt_money(balance)}</b>\n🎯 Текущая ставка: <b>{fmt_money(bet)}</b>")


@dp.message(Command("setbet"))
async def cmd_setbet(message: Message) -> None:
    try:
        user_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: <code>/setbet сумма</code>")
        return
    try:
        amount = int(parts[1])
    except ValueError:
        await message.answer("❌ Ставка должна быть числом.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть больше нуля.")
        return
    balance = await db.get_balance(user_id)
    if amount > balance:
        await message.answer("❌ Ставка не может быть больше баланса.")
        return
    await db.set_bet(user_id, amount)
    await message.answer(f"✅ Новая ставка: <b>{fmt_money(amount)}</b>")


@dp.message(Command("work"))
async def cmd_work(message: Message) -> None:
    try:
        user_id = await ensure_user_from_message(message)
    except RuntimeError:
        return

    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование:\n<code>/work taxi</code>\n<code>/work captcha</code>")
        return

    job = parts[1].lower()
    now = int(time.time())

    if job == "taxi":
        last = await db.get_last_job_time(user_id, "taxi")
        left = TAXI_COOLDOWN - (now - last)
        if left > 0:
            await message.answer(f"🚕 Работа таксиста будет доступна через <b>{sec_to_text(left)}</b>")
            return
        await db.set_last_job_time(user_id, "taxi", now)
        await db.add_balance(user_id, TAXI_REWARD)
        await db.add_profit(user_id, TAXI_REWARD)
        await message.answer(f"🚕 Ты отвёз пассажира-бота.\nЗаработано: <b>{fmt_money(TAXI_REWARD)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>")
        return

    if job == "captcha":
        last = await db.get_last_job_time(user_id, "captcha")
        left = CAPTCHA_COOLDOWN - (now - last)
        if left > 0:
            await message.answer(f"🔐 Работа с капчей будет доступна через <b>{sec_to_text(left)}</b>")
            return
        await db.set_last_job_time(user_id, "captcha", now)
        await db.add_balance(user_id, CAPTCHA_REWARD)
        await db.add_profit(user_id, CAPTCHA_REWARD)
        await message.answer(f"🔐 Ты решил капчу.\nЗаработано: <b>{fmt_money(CAPTCHA_REWARD)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>")
        return

    await message.answer("❌ Неизвестная работа.")


@dp.message(Command("top"))
async def cmd_top(message: Message) -> None:
    try:
        await ensure_user_from_message(message)
    except RuntimeError:
        return
    rows = await db.top_balance(10)
    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    for i, (user_id, username, balance) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(balance)}</b>\n"
    await message.answer(text)


@dp.message(Command("topweek"))
async def cmd_topweek(message: Message) -> None:
    try:
        await ensure_user_from_message(message)
    except RuntimeError:
        return
    rows = await db.top_week(10)
    text = f"📅 <b>Топ недели</b>\nТоп-{WEEKLY_WINNERS_COUNT} в конце недели получают по <b>{fmt_money(WEEKLY_REWARD)}</b>\n\n"
    for i, (user_id, username, weekly_profit) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(weekly_profit)}</b>\n"
    await message.answer(text)


@dp.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    try:
        user_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(user_id):
        await message.answer("⛔ Нет доступа.")
        return
    await message.answer(
        "🛠 <b>Админ-панель</b>\n\n"
        "<code>/give user_id сумма</code>\n"
        "<code>/setbal user_id сумма</code>\n"
        "<code>/ban user_id причина</code>\n"
        "<code>/unban user_id</code>\n"
        "<code>/odds</code>\n"
        "<code>/setodds slots 40</code>\n"
        "<code>/setodds roulette 48</code>\n"
        "<code>/setodds blackjack 45</code>"
    )


@dp.message(Command("give"))
async def cmd_give(message: Message) -> None:
    try:
        admin_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Использование: <code>/give user_id сумма</code>")
        return
    try:
        target_id = int(parts[1])
        amount = int(parts[2])
    except ValueError:
        await message.answer("❌ user_id и сумма должны быть числами.")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return
    await db.ensure_user(target_id, None)
    new_balance = await db.add_balance(target_id, amount)
    await message.answer(f"✅ Игроку <code>{target_id}</code> выдано <b>{fmt_money(amount)}</b>\nНовый баланс: <b>{fmt_money(new_balance)}</b>")


@dp.message(Command("setbal"))
async def cmd_setbal(message: Message) -> None:
    try:
        admin_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Использование: <code>/setbal user_id сумма</code>")
        return
    try:
        target_id = int(parts[1])
        amount = int(parts[2])
    except ValueError:
        await message.answer("❌ user_id и сумма должны быть числами.")
        return
    if amount < 0:
        await message.answer("❌ Баланс не может быть отрицательным.")
        return
    await db.ensure_user(target_id, None)
    new_balance = await db.set_balance(target_id, amount)
    await message.answer(f"✅ Баланс игрока <code>{target_id}</code>: <b>{fmt_money(new_balance)}</b>")


@dp.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    try:
        admin_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: <code>/ban user_id причина</code>")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.answer("❌ user_id должен быть числом.")
        return
    reason = parts[2] if len(parts) > 2 else "Без причины"
    await db.ensure_user(target_id, None)
    await db.ban_user(target_id, reason)
    await message.answer(f"⛔ Игрок <code>{target_id}</code> забанен.\nПричина: <b>{reason}</b>")


@dp.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    try:
        admin_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    parts = (message.text or "").split()
    if len(parts) != 2:
        await message.answer("Использование: <code>/unban user_id</code>")
        return
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.answer("❌ user_id должен быть числом.")
        return
    await db.ensure_user(target_id, None)
    await db.unban_user(target_id)
    await message.answer(f"✅ Игрок <code>{target_id}</code> разбанен.")


@dp.message(Command("odds"))
async def cmd_odds(message: Message) -> None:
    try:
        admin_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    slots = await get_odds("slots")
    roulette = await get_odds("roulette")
    blackjack = await get_odds("blackjack")
    await message.answer(f"🎛 <b>Текущие шансы</b>\n\nСлоты: <b>{slots}%</b>\nРулетка: <b>{roulette}%</b>\nБлэкджек: <b>{blackjack}%</b>")


@dp.message(Command("setodds"))
async def cmd_setodds(message: Message) -> None:
    try:
        admin_id = await ensure_user_from_message(message)
    except RuntimeError:
        return
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Использование: <code>/setodds slots|roulette|blackjack число</code>")
        return
    game = parts[1].lower()
    if game not in ("slots", "roulette", "blackjack"):
        await message.answer("❌ Игра должна быть: slots, roulette или blackjack.")
        return
    try:
        value = int(parts[2])
    except ValueError:
        await message.answer("❌ Шанс должен быть числом.")
        return
    if value < 0 or value > 100:
        await message.answer("❌ Шанс должен быть от 0 до 100.")
        return
    await db.set_state(f"odds_{game}", str(value))
    await message.answer(f"✅ Для <b>{game}</b> установлен шанс <b>{value}%</b>")


@dp.message(Command("pay"))
async def cmd_pay(message: Message) -> None:
    try:
        sender_id = await ensure_user_from_message(message)
    except RuntimeError:
        return

    parts = (message.text or "").split(maxsplit=3)
    target_id = None
    amount = None
    comment = ""

    if message.reply_to_message and len(parts) >= 2:
        try:
            amount = int(parts[1])
        except ValueError:
            await message.answer("❌ Сумма должна быть числом.")
            return
        if message.reply_to_message.from_user is None:
            await message.answer("❌ Не удалось определить получателя.")
            return
        target_id = message.reply_to_message.from_user.id
        await db.ensure_user(target_id, message.reply_to_message.from_user.username)
        if len(parts) >= 3:
            comment = " ".join(parts[2:])
    elif len(parts) >= 3:
        try:
            target_id = int(parts[1])
            amount = int(parts[2])
        except ValueError:
            await message.answer("❌ user_id и сумма должны быть числами.")
            return
        await db.ensure_user(target_id, None)
        if len(parts) == 4:
            comment = parts[3]
    else:
        await message.answer(
            "Использование:\n"
            "<code>/pay user_id сумма комментарий</code>\n"
            "или ответом:\n"
            "<code>/pay сумма комментарий</code>"
        )
        return

    if target_id == sender_id:
        await message.answer("❌ Нельзя переводить самому себе.")
        return
    if amount is None or amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return
    if await db.is_banned(target_id):
        await message.answer("❌ Нельзя переводить деньги забаненному игроку.")
        return

    ok = await db.transfer(sender_id, target_id, amount)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return

    sender_balance = await db.get_balance(sender_id)
    text = f"💸 Перевод выполнен\nКому: <code>{target_id}</code>\nСумма: <b>{fmt_money(amount)}</b>\n"
    if comment:
        text += f"Комментарий: <b>{comment}</b>\n"
    text += f"Твой баланс: <b>{fmt_money(sender_balance)}</b>"
    await message.answer(text)

    try:
        notify = f"💸 Тебе перевели <b>{fmt_money(amount)}</b>"
        if comment:
            notify += f"\nКомментарий: <b>{comment}</b>"
        await message.bot.send_message(target_id, notify)
    except Exception:
        pass


@dp.message(Command("dice"))
async def cmd_dice(message: Message) -> None:
    try:
        challenger_id = await ensure_user_from_message(message)
    except RuntimeError:
        return

    target_id = None
    amount = None
    parts = (message.text or "").split()

    if message.reply_to_message and len(parts) == 2:
        try:
            amount = int(parts[1])
        except ValueError:
            await message.answer("❌ Ставка должна быть числом.")
            return
        if message.reply_to_message.from_user is None:
            await message.answer("❌ Не удалось определить игрока.")
            return
        target_id = message.reply_to_message.from_user.id
        await db.ensure_user(target_id, message.reply_to_message.from_user.username)
    elif len(parts) == 3:
        try:
            target_id = int(parts[1])
            amount = int(parts[2])
        except ValueError:
            await message.answer("❌ Используй: <code>/dice user_id сумма</code>")
            return
        await db.ensure_user(target_id, None)
    else:
        await message.answer("Использование: <code>/dice user_id сумма</code> или ответом <code>/dice сумма</code>")
        return

    if target_id == challenger_id:
        await message.answer("❌ Нельзя играть с самим собой.")
        return
    if amount is None or amount <= 0:
        await message.answer("❌ Ставка должна быть больше нуля.")
        return
    if await db.is_banned(target_id):
        await message.answer("❌ Этот игрок забанен.")
        return

    challenger_balance = await db.get_balance(challenger_id)
    if challenger_balance < amount:
        await message.answer("❌ У тебя недостаточно денег.")
        return

    dice_games[target_id] = {"from": challenger_id, "bet": amount}
    await message.answer(f"🎲 Вызов отправлен игроку <code>{target_id}</code>\nСтавка: <b>{fmt_money(amount)}</b>\n\nИгроку нужно написать:\n<code>/acceptdice</code>")
    try:
        await message.bot.send_message(target_id, f"🎲 Тебя вызвали на кубики!\nИгрок: <code>{challenger_id}</code>\nСтавка: <b>{fmt_money(amount)}</b>\n\nНапиши <code>/acceptdice</code>, чтобы сыграть.")
    except Exception:
        pass


@dp.message(Command("acceptdice"))
async def cmd_acceptdice(message: Message) -> None:
    try:
        target_id = await ensure_user_from_message(message)
    except RuntimeError:
        return

    game = dice_games.get(target_id)
    if not game:
        await message.answer("❌ Для тебя нет активного вызова.")
        return

    challenger_id = game["from"]
    amount = game["bet"]

    if await db.is_banned(challenger_id):
        dice_games.pop(target_id, None)
        await message.answer("❌ Игрок, который вызвал тебя, забанен.")
        return

    challenger_balance = await db.get_balance(challenger_id)
    target_balance = await db.get_balance(target_id)

    if challenger_balance < amount:
        dice_games.pop(target_id, None)
        await message.answer("❌ У соперника уже нет нужной суммы.")
        return
    if target_balance < amount:
        await message.answer("❌ У тебя недостаточно денег для этой ставки.")
        return

    await db.add_balance(challenger_id, -amount)
    await db.add_balance(target_id, -amount)
    await db.add_profit(challenger_id, -amount)
    await db.add_profit(target_id, -amount)

    challenger_roll = random.randint(1, 6)
    target_roll = random.randint(1, 6)

    if challenger_roll > target_roll:
        winner_id = challenger_id
        winner_text = f"<code>{challenger_id}</code>"
    elif target_roll > challenger_roll:
        winner_id = target_id
        winner_text = f"<code>{target_id}</code>"
    else:
        await db.add_balance(challenger_id, amount)
        await db.add_balance(target_id, amount)
        await db.add_profit(challenger_id, amount)
        await db.add_profit(target_id, amount)
        dice_games.pop(target_id, None)
        text = (
            "🎲 <b>Кубики</b>\n\n"
            f"Игрок <code>{challenger_id}</code>: <b>{challenger_roll}</b>\n"
            f"Игрок <code>{target_id}</code>: <b>{target_roll}</b>\n\n"
            "🤝 Ничья. Ставки возвращены."
        )
        await message.answer(text)
        try:
            await message.bot.send_message(challenger_id, text)
        except Exception:
            pass
        return

    await db.add_balance(winner_id, amount * 2)
    await db.add_profit(winner_id, amount * 2)
    dice_games.pop(target_id, None)

    text = (
        "🎲 <b>Кубики</b>\n\n"
        f"Игрок <code>{challenger_id}</code>: <b>{challenger_roll}</b>\n"
        f"Игрок <code>{target_id}</code>: <b>{target_roll}</b>\n\n"
        f"🏆 Победитель: {winner_text}\n"
        f"Приз: <b>{fmt_money(amount * 2)}</b>"
    )
    await message.answer(text)
    try:
        await message.bot.send_message(challenger_id, text)
    except Exception:
        pass


@dp.callback_query(F.data == "menu:home")
async def cb_home(call: CallbackQuery) -> None:
    try:
        await ensure_user_from_callback(call)
    except RuntimeError:
        return
    await safe_edit_or_send(call, "🏠 <b>Главное меню</b>", main_menu())


@dp.callback_query(F.data == "menu:balance")
async def cb_balance(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    balance = await db.get_balance(user_id)
    bet = await db.get_bet(user_id)
    await safe_edit_or_send(call, f"💰 Баланс: <b>{fmt_money(balance)}</b>\n🎯 Текущая ставка: <b>{fmt_money(bet)}</b>", main_menu())


@dp.callback_query(F.data == "menu:help")
async def cb_help(call: CallbackQuery) -> None:
    text = (
        "📘 <b>Помощь</b>\n\n"
        "/setbet сумма — своя ставка\n"
        "/work taxi — работа таксистом\n"
        "/work captcha — решать капчу\n"
        "/dice user_id сумма — кубики\n"
        "/acceptdice — принять вызов\n"
        "/pay user_id сумма комментарий — перевод с комментарием\n"
        "/top — общий топ\n"
        "/topweek — недельный топ\n\n"
        f"Топ-{WEEKLY_WINNERS_COUNT} недели получают по <b>{fmt_money(WEEKLY_REWARD)}</b>."
    )
    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:admin")
async def cb_admin(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    if not is_admin(user_id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await safe_edit_or_send(call, "🛠 <b>Админ-панель</b>\n\n<code>/give user_id сумма</code>\n<code>/setbal user_id сумма</code>\n<code>/ban user_id причина</code>\n<code>/unban user_id</code>\n<code>/odds</code>\n<code>/setodds slots 40</code>", main_menu())


@dp.callback_query(F.data == "menu:top")
async def cb_top(call: CallbackQuery) -> None:
    try:
        await ensure_user_from_callback(call)
    except RuntimeError:
        return
    rows = await db.top_balance(10)
    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    for i, (user_id, username, balance) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(balance)}</b>\n"
    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:topweek")
async def cb_topweek(call: CallbackQuery) -> None:
    try:
        await ensure_user_from_callback(call)
    except RuntimeError:
        return
    rows = await db.top_week(10)
    text = f"📅 <b>Топ недели</b>\nТоп-{WEEKLY_WINNERS_COUNT} получают по <b>{fmt_money(WEEKLY_REWARD)}</b>\n\n"
    for i, (user_id, username, weekly_profit) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(weekly_profit)}</b>\n"
    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:bet")
async def cb_bet_menu(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    bet = await db.get_bet(user_id)
    text = f"🎯 <b>Текущая ставка</b>\n\nСейчас: <b>{fmt_money(bet)}</b>\n\nВыбери кнопку или напиши:\n<code>/setbet сумма</code>"
    await safe_edit_or_send(call, text, bet_select_menu())


@dp.callback_query(F.data == "menu:jobs")
async def cb_jobs_menu(call: CallbackQuery) -> None:
    try:
        await ensure_user_from_callback(call)
    except RuntimeError:
        return
    await safe_edit_or_send(call, "💼 <b>Работы</b>\nВыбери работу:", jobs_menu())


@dp.callback_query(F.data == "menu:dice")
async def cb_dice_menu(call: CallbackQuery) -> None:
    try:
        await ensure_user_from_callback(call)
    except RuntimeError:
        return
    await safe_edit_or_send(call, "🎲 <b>Кубики с игроками</b>\n\nИспользуй:\n<code>/dice user_id сумма</code>\nили ответом:\n<code>/dice сумма</code>\n\nСоперник принимает вызов командой <code>/acceptdice</code>", main_menu())


@dp.callback_query(F.data.startswith("setbet:"))
async def cb_setbet(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    amount = int(call.data.split(":")[1])
    balance = await db.get_balance(user_id)
    if amount > balance:
        await call.answer("Ставка больше баланса", show_alert=True)
        return
    await db.set_bet(user_id, amount)
    await safe_edit_or_send(call, f"✅ Новая ставка: <b>{fmt_money(amount)}</b>", main_menu())


@dp.callback_query(F.data == "job:taxi")
async def cb_job_taxi(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    now = int(time.time())
    last = await db.get_last_job_time(user_id, "taxi")
    left = TAXI_COOLDOWN - (now - last)
    if left > 0:
        await call.answer(f"Доступно через {sec_to_text(left)}", show_alert=True)
        return
    await db.set_last_job_time(user_id, "taxi", now)
    await db.add_balance(user_id, TAXI_REWARD)
    await db.add_profit(user_id, TAXI_REWARD)
    await safe_edit_or_send(call, f"🚕 Ты отвёз пассажира-бота.\nНаграда: <b>{fmt_money(TAXI_REWARD)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>", main_menu())


@dp.callback_query(F.data == "job:captcha")
async def cb_job_captcha(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    now = int(time.time())
    last = await db.get_last_job_time(user_id, "captcha")
    left = CAPTCHA_COOLDOWN - (now - last)
    if left > 0:
        await call.answer(f"Доступно через {sec_to_text(left)}", show_alert=True)
        return
    await db.set_last_job_time(user_id, "captcha", now)
    await db.add_balance(user_id, CAPTCHA_REWARD)
    await db.add_profit(user_id, CAPTCHA_REWARD)
    await safe_edit_or_send(call, f"🔐 Ты решил капчу.\nНаграда: <b>{fmt_money(CAPTCHA_REWARD)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>", main_menu())


@dp.callback_query(F.data == "menu:slots")
async def cb_slots(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    bet = await db.get_bet(user_id)
    balance = await db.get_balance(user_id)
    if balance < bet:
        await call.answer("Недостаточно денег", show_alert=True)
        return

    await db.add_balance(user_id, -bet)
    await db.add_profit(user_id, -bet)

    symbols = ["🍒", "🍋", "💎", "7️⃣", "⭐"]
    try:
        await call.message.edit_text("🎰 Крутим...")
        for _ in range(3):
            spin = " | ".join(random.choice(symbols) for _ in range(3))
            await asyncio.sleep(0.3)
            await call.message.edit_text(f"🎰 <b>Слоты</b>\n\n<code>{spin}</code>")
    except Exception:
        pass

    odds = await get_odds("slots")
    win_event = random.randint(1, 100) <= odds

    if win_event:
        jackpot = random.randint(1, 100) <= 20
        if jackpot:
            result = ["7️⃣", "7️⃣", "7️⃣"]
            mult = 8
        else:
            sym = random.choice(["🍒", "🍋", "💎", "⭐"])
            if random.randint(1, 100) <= 60:
                result = [sym, sym, sym]
                mult = 5
            else:
                other = random.choice(["🍒", "🍋", "💎", "⭐"])
                result = [sym, sym, other]
                mult = 2
        line = " | ".join(result)
        win = bet * mult
        await db.add_balance(user_id, win)
        await db.add_profit(user_id, win)
        text = f"🎰 <b>Слоты</b>\n\n<code>{line}</code>\n\n🎉 Выигрыш: <b>{fmt_money(win)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"
    else:
        result = ["🍒", "🍋", "💎"]
        random.shuffle(result)
        line = " | ".join(result)
        text = f"🎰 <b>Слоты</b>\n\n<code>{line}</code>\n\n😢 Не повезло.\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"

    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:roulette")
async def cb_roulette_menu(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    bet = await db.get_bet(user_id)
    await safe_edit_or_send(call, f"🎡 <b>Рулетка</b>\nТекущая ставка: <b>{fmt_money(bet)}</b>\nВыбери цвет:", roulette_color_menu())


@dp.callback_query(F.data.startswith("roulette:"))
async def cb_roulette_play(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    color = call.data.split(":")[1]
    bet = await db.get_bet(user_id)
    balance = await db.get_balance(user_id)
    if balance < bet:
        await call.answer("Недостаточно денег", show_alert=True)
        return
    await db.add_balance(user_id, -bet)
    await db.add_profit(user_id, -bet)

    try:
        await call.message.edit_text("🎡 Крутим рулетку...")
        await asyncio.sleep(0.8)
    except Exception:
        pass

    odds = await get_odds("roulette")
    player_wins = random.randint(1, 100) <= odds
    real_color = color if player_wins else ("black" if color == "red" else "red")
    number = random.randint(0, 36)
    color_text = "🔴 Красное" if real_color == "red" else "⚫ Чёрное"

    if color == real_color:
        win = bet * 2
        await db.add_balance(user_id, win)
        await db.add_profit(user_id, win)
        text = f"🎡 <b>Рулетка</b>\n\nВыпало: <b>{number}</b> — {color_text}\n🎉 Выигрыш: <b>{fmt_money(win)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"
    else:
        text = f"🎡 <b>Рулетка</b>\n\nВыпало: <b>{number}</b> — {color_text}\n😢 Проигрыш.\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"

    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:blackjack")
async def cb_blackjack_start(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    bet = await db.get_bet(user_id)
    balance = await db.get_balance(user_id)
    if balance < bet:
        await call.answer("Недостаточно денег", show_alert=True)
        return

    await db.add_balance(user_id, -bet)
    await db.add_profit(user_id, -bet)

    player = card_value() + card_value()
    dealer = card_value() + card_value()
    odds = await get_odds("blackjack")
    favored = random.randint(1, 100) <= odds

    if favored and player <= dealer:
        player = min(21, dealer + random.randint(1, 3))
    elif not favored and player > dealer:
        dealer = min(21, player + random.randint(1, 3))

    blackjack_games[user_id] = BJGame(bet=bet, player_total=player, dealer_total=dealer)

    if player == 21:
        win = int(bet * 2.5)
        await db.add_balance(user_id, win)
        await db.add_profit(user_id, win)
        blackjack_games.pop(user_id, None)
        await safe_edit_or_send(call, f"🃏 <b>Блэкджек</b>\n\nУ тебя: <b>{player}</b>\nУ дилера: <b>{dealer}</b>\n\n🔥 Натуральный блэкджек! <b>{fmt_money(win)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>", main_menu())
        return

    await safe_edit_or_send(call, f"🃏 <b>Блэкджек</b>\n\nТвоя сумма: <b>{player}</b>\nУ дилера скрыто. Видно: <b>{max(2, dealer - 1)}</b>+\nСтавка: <b>{fmt_money(bet)}</b>", blackjack_menu())


@dp.callback_query(F.data == "blackjack:hit")
async def cb_blackjack_hit(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    game = blackjack_games.get(user_id)
    if game is None:
        await call.answer("Игра не найдена", show_alert=True)
        return
    game.player_total += card_value()
    if game.player_total > 21:
        blackjack_games.pop(user_id, None)
        await safe_edit_or_send(call, f"🃏 <b>Блэкджек</b>\n\nТвоя сумма: <b>{game.player_total}</b>\n💥 Перебор.\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>", main_menu())
        return
    await safe_edit_or_send(call, f"🃏 <b>Блэкджек</b>\n\nТвоя сумма: <b>{game.player_total}</b>\nУ дилера скрыто. Видно: <b>{max(2, game.dealer_total - 1)}</b>+\nСтавка: <b>{fmt_money(game.bet)}</b>", blackjack_menu())


@dp.callback_query(F.data == "blackjack:stand")
async def cb_blackjack_stand(call: CallbackQuery) -> None:
    try:
        user_id = await ensure_user_from_callback(call)
    except RuntimeError:
        return
    game = blackjack_games.get(user_id)
    if game is None:
        await call.answer("Игра не найдена", show_alert=True)
        return
    while game.dealer_total < 17:
        game.dealer_total += card_value()

    if game.dealer_total > 21 or game.player_total > game.dealer_total:
        win = game.bet * 2
        await db.add_balance(user_id, win)
        await db.add_profit(user_id, win)
        result = f"🎉 Победа! Выигрыш: <b>{fmt_money(win)}</b>"
    elif game.player_total == game.dealer_total:
        await db.add_balance(user_id, game.bet)
        await db.add_profit(user_id, game.bet)
        result = f"🤝 Ничья. Возврат: <b>{fmt_money(game.bet)}</b>"
    else:
        result = "😢 Проигрыш."

    blackjack_games.pop(user_id, None)
    await safe_edit_or_send(call, f"🃏 <b>Блэкджек</b>\n\nТвоя сумма: <b>{game.player_total}</b>\nСумма дилера: <b>{game.dealer_total}</b>\n\n{result}\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>", main_menu())


@dp.message()
async def fallback(message: Message) -> None:
    try:
        await ensure_user_from_message(message)
    except RuntimeError:
        return
    await message.answer("Не понял сообщение. Используй /start или /menu")


async def weekly_rewards_loop(bot: Bot) -> None:
    stored_week = await db.get_state("current_week")
    if stored_week is None:
        await db.set_state("current_week", current_week_key())

    while True:
        await asyncio.sleep(60)
        now_week = current_week_key()
        stored_week = await db.get_state("current_week")
        if stored_week != now_week:
            winners = await db.reward_weekly_top()
            await db.set_state("current_week", now_week)
            if winners:
                text = f"📅 <b>Недельные награды выданы!</b>\n\nТоп-{WEEKLY_WINNERS_COUNT} получили по <b>{fmt_money(WEEKLY_REWARD)}</b>\n\n"
                for i, (user_id, username) in enumerate(winners, start=1):
                    text += f"{i}. {fmt_name(user_id, username)}\n"
                for user_id, _ in winners:
                    try:
                        await bot.send_message(user_id, text)
                    except Exception:
                        pass


async def main() -> None:
    global db
    if BOT_TOKEN in ("", "PASTE_TOKEN_HERE", None):
        raise RuntimeError("Вставь BOT_TOKEN в переменные окружения")

    db = Database(DB_PATH)
    await db.connect()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    rewards_task = asyncio.create_task(weekly_rewards_loop(bot))
    print("BOT STARTED")

    try:
        await dp.start_polling(bot)
    finally:
        rewards_task.cancel()
        await bot.session.close()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
