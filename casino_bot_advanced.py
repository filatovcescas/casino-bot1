"""
casino_bot_advanced.py

Функции:
- своя ставка: /setbet 500
- слоты
- рулетка
- блэкджек
- баланс
- перевод денег: /pay user_id сумма или ответом /pay сумма
- админка: /give, /setbal
- общий топ: /top
- топ недели: /topweek
- в начале новой недели топ-3 по weekly_profit получают по 50 000

Установка:
    python -m pip install aiogram aiosqlite

Для Render:
- Background Worker
- Start Command: python casino_bot_advanced.py
- Environment variable: BOT_TOKEN=...
"""

from __future__ import annotations

import asyncio
import os
import random
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

dp = Dispatcher()
db = None
blackjack_games: dict[int, "BJGame"] = {}


@dataclass
class BJGame:
    bet: int
    player_total: int
    dealer_total: int


class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn: aiosqlite.Connection | None = None

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
                total_profit INTEGER NOT NULL DEFAULT 0
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
                INSERT INTO users (user_id, username, balance, current_bet, weekly_profit, total_profit)
                VALUES (?, ?, ?, ?, 0, 0)
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

    async def top_balance(self, limit: int = 10) -> list[tuple[int, str | None, int]]:
        cur = await self.conn.execute(
            "SELECT user_id, username, balance FROM users ORDER BY balance DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [(int(r[0]), r[1], int(r[2])) for r in rows]

    async def top_week(self, limit: int = 10) -> list[tuple[int, str | None, int]]:
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

    async def get_state(self, key: str) -> str | None:
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

    async def reward_weekly_top(self) -> list[tuple[int, str | None]]:
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

        winners: list[tuple[int, str | None]] = []
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


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🎰 Слоты", callback_data="menu:slots"),
                InlineKeyboardButton(text="🎡 Рулетка", callback_data="menu:roulette"),
            ],
            [
                InlineKeyboardButton(text="🃏 Блэкджек", callback_data="menu:blackjack"),
                InlineKeyboardButton(text="💰 Баланс", callback_data="menu:balance"),
            ],
            [
                InlineKeyboardButton(text="🏆 Топ", callback_data="menu:top"),
                InlineKeyboardButton(text="📅 Топ недели", callback_data="menu:topweek"),
            ],
            [
                InlineKeyboardButton(text="🎯 Ставка", callback_data="menu:bet"),
                InlineKeyboardButton(text="📘 Помощь", callback_data="menu:help"),
            ],
            [
                InlineKeyboardButton(text="🛠 Админ", callback_data="menu:admin"),
            ],
        ]
    )


def bet_select_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="100", callback_data="setbet:100"),
                InlineKeyboardButton(text="500", callback_data="setbet:500"),
                InlineKeyboardButton(text="1000", callback_data="setbet:1000"),
            ],
            [
                InlineKeyboardButton(text="2500", callback_data="setbet:2500"),
                InlineKeyboardButton(text="5000", callback_data="setbet:5000"),
            ],
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
            [
                InlineKeyboardButton(text="➕ Ещё", callback_data="blackjack:hit"),
                InlineKeyboardButton(text="✋ Стоп", callback_data="blackjack:stand"),
            ],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="menu:home")],
        ]
    )


async def ensure_user_from_message(message: Message) -> int:
    if message.from_user is None:
        raise RuntimeError("Не удалось определить пользователя")
    await db.ensure_user(message.from_user.id, message.from_user.username)
    return message.from_user.id


async def ensure_user_from_callback(call: CallbackQuery) -> int:
    await db.ensure_user(call.from_user.id, call.from_user.username)
    return call.from_user.id


async def safe_edit_or_send(call: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    try:
        await call.message.edit_text(text, reply_markup=reply_markup)
    except Exception:
        await call.message.answer(text, reply_markup=reply_markup)
    await call.answer()


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user_id = await ensure_user_from_message(message)
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
    await ensure_user_from_message(message)
    await message.answer("🏠 <b>Главное меню</b>", reply_markup=main_menu())


@dp.message(Command("balance"))
async def cmd_balance(message: Message) -> None:
    user_id = await ensure_user_from_message(message)
    balance = await db.get_balance(user_id)
    bet = await db.get_bet(user_id)
    await message.answer(
        f"💰 Баланс: <b>{fmt_money(balance)}</b>\n"
        f"🎯 Текущая ставка: <b>{fmt_money(bet)}</b>"
    )


@dp.message(Command("setbet"))
async def cmd_setbet(message: Message) -> None:
    user_id = await ensure_user_from_message(message)
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


@dp.message(Command("top"))
async def cmd_top(message: Message) -> None:
    await ensure_user_from_message(message)
    rows = await db.top_balance(10)
    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    for i, (user_id, username, balance) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(balance)}</b>\n"
    await message.answer(text)


@dp.message(Command("topweek"))
async def cmd_topweek(message: Message) -> None:
    await ensure_user_from_message(message)
    rows = await db.top_week(10)
    text = (
        "📅 <b>Топ недели</b>\n"
        f"Топ-{WEEKLY_WINNERS_COUNT} в конце недели получают по <b>{fmt_money(WEEKLY_REWARD)}</b>\n\n"
    )
    for i, (user_id, username, weekly_profit) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(weekly_profit)}</b>\n"
    await message.answer(text)


@dp.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    user_id = await ensure_user_from_message(message)
    if not is_admin(user_id):
        await message.answer("⛔ Нет доступа.")
        return
    await message.answer(
        "🛠 <b>Админ-панель</b>\n\n"
        "<code>/give user_id сумма</code>\n"
        "<code>/setbal user_id сумма</code>"
    )


@dp.message(Command("give"))
async def cmd_give(message: Message) -> None:
    admin_id = await ensure_user_from_message(message)
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
    admin_id = await ensure_user_from_message(message)
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


@dp.message(Command("pay"))
async def cmd_pay(message: Message) -> None:
    sender_id = await ensure_user_from_message(message)
    parts = (message.text or "").split()
    target_id = None
    amount = None

    if message.reply_to_message and len(parts) == 2:
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
    elif len(parts) == 3:
        try:
            target_id = int(parts[1])
            amount = int(parts[2])
        except ValueError:
            await message.answer("❌ user_id и сумма должны быть числами.")
            return
        await db.ensure_user(target_id, None)
    else:
        await message.answer(
            "Использование:\n"
            "<code>/pay user_id сумма</code>\n"
            "или ответом:\n"
            "<code>/pay сумма</code>"
        )
        return

    if target_id == sender_id:
        await message.answer("❌ Нельзя переводить самому себе.")
        return
    if amount is None or amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return

    ok = await db.transfer(sender_id, target_id, amount)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return

    sender_balance = await db.get_balance(sender_id)
    await message.answer(
        f"💸 Перевод выполнен\n"
        f"Кому: <code>{target_id}</code>\n"
        f"Сумма: <b>{fmt_money(amount)}</b>\n"
        f"Твой баланс: <b>{fmt_money(sender_balance)}</b>"
    )


@dp.callback_query(F.data == "menu:home")
async def cb_home(call: CallbackQuery) -> None:
    await ensure_user_from_callback(call)
    await safe_edit_or_send(call, "🏠 <b>Главное меню</b>", main_menu())


@dp.callback_query(F.data == "menu:balance")
async def cb_balance(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
    balance = await db.get_balance(user_id)
    bet = await db.get_bet(user_id)
    await safe_edit_or_send(call, f"💰 Баланс: <b>{fmt_money(balance)}</b>\n🎯 Текущая ставка: <b>{fmt_money(bet)}</b>", main_menu())


@dp.callback_query(F.data == "menu:help")
async def cb_help(call: CallbackQuery) -> None:
    text = (
        "📘 <b>Помощь</b>\n\n"
        "/setbet сумма — поставить свою ставку\n"
        "/pay user_id сумма — перевод\n"
        "/pay сумма — перевод ответом\n"
        "/top — общий топ\n"
        "/topweek — топ недели\n\n"
        f"Топ-{WEEKLY_WINNERS_COUNT} недели получают по <b>{fmt_money(WEEKLY_REWARD)}</b>."
    )
    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:admin")
async def cb_admin(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
    if not is_admin(user_id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await safe_edit_or_send(call, "🛠 <b>Админ-панель</b>\n\n<code>/give user_id сумма</code>\n<code>/setbal user_id сумма</code>", main_menu())


@dp.callback_query(F.data == "menu:top")
async def cb_top(call: CallbackQuery) -> None:
    rows = await db.top_balance(10)
    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    for i, (user_id, username, balance) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(balance)}</b>\n"
    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:topweek")
async def cb_topweek(call: CallbackQuery) -> None:
    rows = await db.top_week(10)
    text = f"📅 <b>Топ недели</b>\nТоп-{WEEKLY_WINNERS_COUNT} получают по <b>{fmt_money(WEEKLY_REWARD)}</b>\n\n"
    for i, (user_id, username, weekly_profit) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(weekly_profit)}</b>\n"
    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:bet")
async def cb_bet_menu(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
    bet = await db.get_bet(user_id)
    text = (
        "🎯 <b>Текущая ставка</b>\n\n"
        f"Сейчас: <b>{fmt_money(bet)}</b>\n\n"
        "Выбери кнопку или напиши:\n"
        "<code>/setbet сумма</code>"
    )
    await safe_edit_or_send(call, text, bet_select_menu())


@dp.callback_query(F.data.startswith("setbet:"))
async def cb_setbet(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
    amount = int(call.data.split(":")[1])

    balance = await db.get_balance(user_id)
    if amount > balance:
        await call.answer("Ставка больше баланса", show_alert=True)
        return

    await db.set_bet(user_id, amount)
    await safe_edit_or_send(call, f"✅ Новая ставка: <b>{fmt_money(amount)}</b>", main_menu())


@dp.callback_query(F.data == "menu:slots")
async def cb_slots(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
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

    result = [random.choice(symbols) for _ in range(3)]
    line = " | ".join(result)

    if len(set(result)) == 1:
        mult = 8 if result[0] == "7️⃣" else 5
        win = bet * mult
        await db.add_balance(user_id, win)
        await db.add_profit(user_id, win)
        text = f"🎰 <b>Слоты</b>\n\n<code>{line}</code>\n\n🔥 Джекпот: <b>{fmt_money(win)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"
    elif len(set(result)) == 2:
        win = bet * 2
        await db.add_balance(user_id, win)
        await db.add_profit(user_id, win)
        text = f"🎰 <b>Слоты</b>\n\n<code>{line}</code>\n\n✨ Совпадение: <b>{fmt_money(win)}</b>\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"
    else:
        text = f"🎰 <b>Слоты</b>\n\n<code>{line}</code>\n\n😢 Не повезло.\nБаланс: <b>{fmt_money(await db.get_balance(user_id))}</b>"

    await safe_edit_or_send(call, text, main_menu())


@dp.callback_query(F.data == "menu:roulette")
async def cb_roulette_menu(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
    bet = await db.get_bet(user_id)
    await safe_edit_or_send(call, f"🎡 <b>Рулетка</b>\nТекущая ставка: <b>{fmt_money(bet)}</b>\nВыбери цвет:", roulette_color_menu())


@dp.callback_query(F.data.startswith("roulette:"))
async def cb_roulette_play(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
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

    number = random.randint(0, 36)
    real_color = random.choice(["red", "black"])
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
    user_id = await ensure_user_from_callback(call)
    bet = await db.get_bet(user_id)
    balance = await db.get_balance(user_id)

    if balance < bet:
        await call.answer("Недостаточно денег", show_alert=True)
        return

    await db.add_balance(user_id, -bet)
    await db.add_profit(user_id, -bet)

    player = card_value() + card_value()
    dealer = card_value() + card_value()
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
    user_id = await ensure_user_from_callback(call)
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
    user_id = await ensure_user_from_callback(call)
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
    await ensure_user_from_message(message)
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
