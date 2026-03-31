"""
casino_bot_advanced.py

Функции:
- своя ставка: /setbet 500
- слоты
- рулетка
- блэкджек
- баланс
- перевод денег с комментарием: /pay user_id сумма комментарий
- админка: /give, /setbal, /ban, /unban, /setchance
- бан игроков
- общий топ: /top
- топ недели: /topweek
- работа таксистом: /taxi (2500 за поездку)
- капча: /captcha (1000 за ввод)
- кости: /dice @user сумма
- в начале новой недели топ-3 по weekly_profit получают по 50 000

Установка:
    python -m pip install aiogram aiosqlite
"""

from __future__ import annotations

import asyncio
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_TOKEN_HERE")
ADMIN_IDS = {8039924340}  # замени на свой Telegram ID
DB_PATH = "casino.db"

START_BALANCE = 5000
DEFAULT_BET = 100
WEEKLY_REWARD = 50000
WEEKLY_WINNERS_COUNT = 3

dp = Dispatcher()
db = None
blackjack_games: dict[int, "BJGame"] = {}
pending_dice_games: dict[int, "DiceGame"] = {}
pending_captcha: dict[int, dict] = {}


@dataclass
class BJGame:
    bet: int
    player_total: int
    dealer_total: int


@dataclass
class DiceGame:
    opponent_id: int
    opponent_name: str
    bet: int
    challenger_roll: int | None = None
    opponent_roll: int | None = None


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
                total_profit INTEGER NOT NULL DEFAULT 0,
                banned BOOLEAN DEFAULT 0,
                ban_reason TEXT DEFAULT ''
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
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS casino_settings (
                setting_name TEXT PRIMARY KEY,
                value REAL
            )
            """
        )
        # Настройки шансов по умолчанию
        default_settings = {
            'roulette_win_chance': 0.48,
            'blackjack_win_chance': 0.49,
            'slots_win_chance': 0.40
        }
        for name, val in default_settings.items():
            await self.conn.execute(
                "INSERT OR IGNORE INTO casino_settings (setting_name, value) VALUES (?, ?)",
                (name, val)
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
                INSERT INTO users (user_id, username, balance, current_bet, weekly_profit, total_profit, banned, ban_reason)
                VALUES (?, ?, ?, ?, 0, 0, 0, '')
                """,
                (user_id, username, START_BALANCE, DEFAULT_BET),
            )
        else:
            await self.conn.execute(
                "UPDATE users SET username = ? WHERE user_id = ?",
                (username, user_id),
            )
        await self.conn.commit()

    async def is_banned(self, user_id: int) -> tuple[bool, str]:
        cur = await self.conn.execute("SELECT banned, ban_reason FROM users WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
        await cur.close()
        if row and row[0]:
            return True, row[1] or "Не указана"
        return False, ""

    async def ban_user(self, user_id: int, reason: str = "") -> None:
        await self.conn.execute(
            "UPDATE users SET banned = 1, ban_reason = ? WHERE user_id = ?",
            (reason, user_id)
        )
        await self.conn.commit()

    async def unban_user(self, user_id: int) -> None:
        await self.conn.execute(
            "UPDATE users SET banned = 0, ban_reason = '' WHERE user_id = ?",
            (user_id,)
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

    async def transfer(self, sender_id: int, target_id: int, amount: int, comment: str = "") -> bool:
        if amount <= 0:
            return False
        sender_balance = await self.get_balance(sender_id)
        if sender_balance < amount:
            return False

        await self.conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, sender_id))
        await self.conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, target_id))
        if comment:
            await self.conn.execute(
                "INSERT INTO transfers (sender_id, target_id, amount, comment, timestamp) VALUES (?, ?, ?, ?, ?)",
                (sender_id, target_id, amount, comment, datetime.now(timezone.utc).isoformat())
            )
        await self.conn.commit()
        return True

    async def top_balance(self, limit: int = 10) -> list[tuple[int, str | None, int]]:
        cur = await self.conn.execute(
            "SELECT user_id, username, balance FROM users WHERE banned = 0 ORDER BY balance DESC LIMIT ?",
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
            WHERE banned = 0
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

    async def get_setting(self, setting_name: str) -> float:
        cur = await self.conn.execute("SELECT value FROM casino_settings WHERE setting_name = ?", (setting_name,))
        row = await cur.fetchone()
        await cur.close()
        return float(row[0]) if row else 0.5

    async def set_setting(self, setting_name: str, value: float) -> None:
        await self.conn.execute(
            "UPDATE casino_settings SET value = ? WHERE setting_name = ?",
            (value, setting_name)
        )
        await self.conn.commit()

    async def reward_weekly_top(self) -> list[tuple[int, str | None]]:
        cur = await self.conn.execute(
            """
            SELECT user_id, username
            FROM users
            WHERE weekly_profit > 0 AND banned = 0
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


def roll_dice() -> int:
    return random.randint(1, 6)


def fmt_money(value: int) -> str:
    return f"{value:,}".replace(",", " ") + " 💵"


def fmt_name(user_id: int, username: str | None) -> str:
    return f"@{username}" if username else f"<code>{user_id}</code>"


def current_week_key() -> str:
    now = datetime.now(timezone.utc)
    year, week, _ = now.isocalendar()
    return f"{year}-W{week:02d}"


def generate_captcha() -> tuple[str, str]:
    a = random.randint(1, 50)
    b = random.randint(1, 50)
    operators = ['+', '-']
    op = random.choice(operators)
    if op == '+':
        result = a + b
    else:
        result = a - b
    question = f"{a} {op} {b}"
    return question, str(result)


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
                InlineKeyboardButton(text="🚖 Такси", callback_data="menu:taxi"),
                InlineKeyboardButton(text="🎲 Кости", callback_data="menu:dice"),
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


def admin_settings_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎰 Шансы слотов", callback_data="admin:slots_chance")],
            [InlineKeyboardButton(text="🎡 Шансы рулетки", callback_data="admin:roulette_chance")],
            [InlineKeyboardButton(text="🃏 Шансы блэкджека", callback_data="admin:blackjack_chance")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:admin")],
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


async def check_ban(message: Message) -> bool:
    banned, reason = await db.is_banned(message.from_user.id)
    if banned:
        await message.answer(f"❌ Вы забанены.\nПричина: {reason}")
        return True
    return False


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    user_id = await ensure_user_from_message(message)
    banned, reason = await db.is_banned(user_id)
    if banned:
        await message.answer(f"❌ Вы забанены.\nПричина: {reason}")
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
    if await check_ban(message):
        return
    await ensure_user_from_message(message)
    await message.answer("🏠 <b>Главное меню</b>", reply_markup=main_menu())


@dp.message(Command("balance"))
async def cmd_balance(message: Message) -> None:
    if await check_ban(message):
        return
    user_id = await ensure_user_from_message(message)
    balance = await db.get_balance(user_id)
    bet = await db.get_bet(user_id)
    await message.answer(
        f"💰 Баланс: <b>{fmt_money(balance)}</b>\n"
        f"🎯 Текущая ставка: <b>{fmt_money(bet)}</b>"
    )


@dp.message(Command("setbet"))
async def cmd_setbet(message: Message) -> None:
    if await check_ban(message):
        return
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


@dp.message(Command("taxi"))
async def cmd_taxi(message: Message) -> None:
    """Работа таксистом: 2500 за поездку"""
    if await check_ban(message):
        return
    user_id = await ensure_user_from_message(message)
    earnings = 2500
    await db.add_balance(user_id, earnings)
    await db.add_profit(user_id, earnings)
    await message.answer(f"🚖 Вы выполнили заказ такси и заработали {fmt_money(earnings)}!")


@dp.message(Command("captcha"))
async def cmd_captcha(message: Message) -> None:
    """Капча: 1000 за правильный ввод"""
    if await check_ban(message):
        return
    user_id = await ensure_user_from_message(message)
    question, answer = generate_captcha()
    pending_captcha[user_id] = {"answer": answer, "timestamp": time.time()}
    await message.answer(f"🔐 <b>Капча</b>\n\nРешите пример: <code>{question}</code>\n\nЗа правильный ответ вы получите 1000 монет.\nУ вас 30 секунд.")


@dp.message(Command("dice"))
async def cmd_dice(message: Message) -> None:
    """Игра в кости: /dice @user сумма"""
    if await check_ban(message):
        return
    user_id = await ensure_user_from_message(message)
    parts = (message.text or "").split()
    
    if len(parts) < 3:
        await message.answer("Использование: <code>/dice @username сумма</code> или <code>/dice user_id сумма</code>")
        return
    
    try:
        amount = int(parts[-1])
    except ValueError:
        await message.answer("❌ Сумма должна быть числом.")
        return
    
    if amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return
    
    balance = await db.get_balance(user_id)
    if balance < amount:
        await message.answer("❌ Недостаточно средств для ставки.")
        return
    
    # Определяем оппонента
    target_username = None
    target_id = None
    
    if message.entities:
        for entity in message.entities:
            if entity.type == "mention":
                username = message.text[entity.offset:entity.offset + entity.length][1:]
                target_username = username
                break
    
    if not target_username and len(parts) >= 2:
        try:
            target_id = int(parts[1])
        except ValueError:
            target_username = parts[1].replace("@", "")
    
    if target_username:
        cur = await db.conn.execute("SELECT user_id FROM users WHERE username = ?", (target_username,))
        row = await cur.fetchone()
        await cur.close()
        if row:
            target_id = row[0]
    
    if target_id is None or target_id == user_id:
        await message.answer("❌ Не удалось найти оппонента или вы указали себя.")
        return
    
    await db.ensure_user(target_id, None)
    
    target_balance = await db.get_balance(target_id)
    if target_balance < amount:
        await message.answer("❌ У оппонента недостаточно средств.")
        return
    
    # Создаём игру
    target_name = fmt_name(target_id, None)
    pending_dice_games[target_id] = DiceGame(
        opponent_id=user_id,
        opponent_name=fmt_name(user_id, message.from_user.username),
        bet=amount
    )
    
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Принять игру", callback_data=f"dice:accept:{user_id}:{amount}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dice:decline:{user_id}")]
        ]
    )
    
    await message.answer(
        f"🎲 <b>Игра в кости</b>\n\n"
        f"Игрок {fmt_name(user_id, message.from_user.username)} бросает вызов!\n"
        f"Ставка: {fmt_money(amount)}\n"
        f"У кого выпадет больше число, тот забирает ставку.",
        reply_markup=keyboard
    )
    await message.bot.send_message(target_id, f"🎲 Вам бросают вызов на {fmt_money(amount)}!\nНажмите кнопку, чтобы принять.", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("dice:"))
async def cb_dice_response(call: CallbackQuery) -> None:
    user_id = await ensure_user_from_callback(call)
    parts = call.data.split(":")
    
    if parts[1] == "accept":
        challenger_id = int(parts[2])
        bet = int(parts[3])
        
        if user_id == challenger_id:
            await call.answer("Нельзя принять свой вызов", show_alert=True)
            return
        
        balance = await db.get_balance(user_id)
        if balance < bet:
            await call.answer("Недостаточно средств для ставки", show_alert=True)
            await call.message.edit_text(f"❌ {fmt_name(user_id, call.from_user.username)} не может принять игру: недостаточно средств.")
            return
        
        # Бросаем кости
        challenger_roll = roll_dice()
        opponent_roll = roll_dice()
        
        # Списываем ставки
        await db.add_balance(challenger_id, -bet)
        await db.add_balance(user_id, -bet)
        
        # Определяем победителя
        if challenger_roll > opponent_roll:
            winner_id = challenger_id
            winner_roll = challenger_roll
            loser_roll = opponent_roll
            winnings = bet * 2
            await db.add_balance(winner_id, winnings)
            await db.add_profit(winner_id, winnings)
            result_text = f"🎉 Победил {fmt_name(challenger_id, None)}!"
        elif opponent_roll > challenger_roll:
            winner_id = user_id
            winner_roll = opponent_roll
            loser_roll = challenger_roll
            winnings = bet * 2
            await db.add_balance(winner_id, winnings)
            await db.add_profit(winner_id, winnings)
            result_text = f"🎉 Победил {fmt_name(user_id, call.from_user.username)}!"
        else:
            # Ничья - возврат ставок
            await db.add_balance(challenger_id, bet)
            await db.add_balance(user_id, bet)
            result_text = f"🤝 Ничья! Возврат ставок."
            winner_roll = challenger_roll
            loser_roll = opponent_roll
        
        await call.message.edit_text(
            f"🎲 <b>Результат игры в кости</b>\n\n"
            f"{fmt_name(challenger_id, None)} выбросил: <b>{challenger_roll}</b>\n"
            f"{fmt_name(user_id, call.from_user.username)} выбросил: <b>{opponent_roll}</b>\n\n"
            f"{result_text}\n\n"
            f"Баланс {fmt_name(challenger_id, None)}: {fmt_money(await db.get_balance(challenger_id))}\n"
            f"Баланс {fmt_name(user_id, call.from_user.username)}: {fmt_money(await db.get_balance(user_id))}"
        )
        
    elif parts[1] == "decline":
        challenger_id = int(parts[2])
        if user_id == challenger_id:
            await call.answer("Нельзя отклонить свой вызов", show_alert=True)
            return
        await call.message.edit_text(f"❌ Игра отклонена {fmt_name(user_id, call.from_user.username)}.")
        await call.answer()


@dp.message(Command("pay"))
async def cmd_pay(message: Message) -> None:
    """Перевод денег с комментарием: /pay user_id сумма комментарий"""
    if await check_ban(message):
        return
    sender_id = await ensure_user_from_message(message)
    parts = (message.text or "").split(maxsplit=2)
    
    target_id = None
    amount = None
    comment = ""
    
    if message.reply_to_message and len(parts) >= 2:
        try:
            amount = int(parts[1])
            if len(parts) > 2:
                comment = parts[2]
        except ValueError:
            await message.answer("❌ Сумма должна быть числом.")
            return
        if message.reply_to_message.from_user is None:
            await message.answer("❌ Не удалось определить получателя.")
            return
        target_id = message.reply_to_message.from_user.id
        await db.ensure_user(target_id, message.reply_to_message.from_user.username)
    elif len(parts) >= 3:
        try:
            target_id = int(parts[1])
            amount = int(parts[2])
            if len(parts) > 3:
                comment = parts[3]
        except ValueError:
            await message.answer("❌ user_id и сумма должны быть числами.")
            return
        await db.ensure_user(target_id, None)
    else:
        await message.answer(
            "Использование:\n"
            "<code>/pay user_id сумма [комментарий]</code>\n"
            "или ответом:\n"
            "<code>/pay сумма [комментарий]</code>"
        )
        return

    if target_id == sender_id:
        await message.answer("❌ Нельзя переводить самому себе.")
        return
    if amount is None or amount <= 0:
        await message.answer("❌ Сумма должна быть больше нуля.")
        return

    ok = await db.transfer(sender_id, target_id, amount, comment)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return

    sender_balance = await db.get_balance(sender_id)
    comment_text = f"\n📝 Комментарий: {comment}" if comment else ""
    await message.answer(
        f"💸 Перевод выполнен\n"
        f"Кому: <code>{target_id}</code>\n"
        f"Сумма: <b>{fmt_money(amount)}</b>{comment_text}\n"
        f"Твой баланс: <b>{fmt_money(sender_balance)}</b>"
    )


@dp.message(Command("top"))
async def cmd_top(message: Message) -> None:
    if await check_ban(message):
        return
    await ensure_user_from_message(message)
    rows = await db.top_balance(10)
    text = "🏆 <b>Топ игроков по балансу</b>\n\n"
    for i, (user_id, username, balance) in enumerate(rows, start=1):
        text += f"{i}. {fmt_name(user_id, username)} — <b>{fmt_money(balance)}</b>\n"
    await message.answer(text)


@dp.message(Command("topweek"))
async def cmd_topweek(message: Message) -> None:
    if await check_ban(message):
        return
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
        "<code>/setbal user_id сумма</code>\n"
        "<code>/ban user_id [причина]</code>\n"
        "<code>/unban user_id</code>\n"
        "<code>/setchance slots|roulette|blackjack 0.XX</code>"
    )


@dp.message(Command("ban"))
async def cmd_ban(message: Message) -> None:
    admin_id = await ensure_user_from_message(message)
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: <code>/ban user_id [причина]</code>")
        return
    
    try:
        target_id = int(parts[1])
    except ValueError:
        await message.answer("❌ user_id должен быть числом.")
        return
    
    reason = parts[2] if len(parts) > 2 else "Нарушение правил"
    
    await db.ensure_user(target_id, None)
    await db.ban_user(target_id, reason)
    await message.answer(f"✅ Игрок <code>{target_id}</code> забанен.\nПричина: {reason}")


@dp.message(Command("unban"))
async def cmd_unban(message: Message) -> None:
    admin_id = await ensure_user_from_message(message)
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
    
    await db.unban_user(target_id)
    await message.answer(f"✅ Игрок <code>{target_id}</code> разбанен.")


@dp.message(Command("setchance"))
async def cmd_setchance(message: Message) -> None:
    admin_id = await ensure_user_from_message(message)
    if not is_admin(admin_id):
        await message.answer("⛔ Только админ.")
        return
    
    parts = (message.text or "").split()
    if len(parts) != 3:
        await message.answer("Использование: <code>/setchance slots|roulette|blackjack 0.XX</code>\nПример: <code>/setchance slots 0.35</code>")
        return
    
    game_type = parts[1].lower()
    try:
        chance = float(parts[2])
    except ValueError:
        await message.answer("❌ Шанс должен быть числом (0.0-1.0).")
        return
    
    if not 0 <= chance <= 1:
        await message.answer("❌ Шанс должен быть от 0 до 1.")
        return
    
    setting_map = {
        "slots": "slots_win_chance",
        "roulette": "roulette_win_chance",
        "blackjack": "blackjack_win_chance"
    }
    
    if game_type not in setting_map:
        await message.answer("❌ Доступные игры: slots, roulette, blackjack")
        return
    
    await db.set_setting(setting_map[game_type], chance)
    await message.answer(f"✅ Шанс победы в {game_type} изменён на {chance*100:.1f}%")


@dp.message(Command("give"))
async def cmd_give(message: Message) -> None:
    admin_id = await ensure
    
