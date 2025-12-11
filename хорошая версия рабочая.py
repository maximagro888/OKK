import logging
import sqlite3
import asyncio
import os
import aiohttp
import math
import random
from datetime import datetime
from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

# --- КОНФИГУРАЦИЯ ---
API_TOKEN = '8467950733:AAGDBNb_HbbVJRUdGk4NxscuJo-BT7lLFjo'
ADMIN_ID = 2116037251  
LTC_WALLET = 'ltc1quvr9zna0mkzz0dw0n0mya6c0qjfsp9a3twe47c' 
LTC_API_URL = "https://api.blockcypher.com/v1/ltc/main/addrs/{}/full"
RENDER_APP_URL = os.getenv('RENDER_EXTERNAL_URL', None)
LTC_PRICE_API = "https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=usd"

# Контакты и ссылки
ADMIN_USERNAME = "okkoads"
CHANNEL_LINK = "https://t.me/+Cxztw399MUk5ZTRi"
SUPPORT_LINK = f"https://t.me/{ADMIN_USERNAME}" 

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- БАЗА ДАННЫХ ---
def db_start():
    conn = sqlite3.connect('shop.db')
    cur = conn.cursor()
    
    cur.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, balance REAL DEFAULT 0, purchases INTEGER DEFAULT 0)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS cities (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price_usd REAL, city_id INTEGER)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY AUTOINCREMENT, product_id INTEGER, content_text TEXT, content_photo TEXT, status TEXT DEFAULT 'active')''')
    cur.execute('''CREATE TABLE IF NOT EXISTS reviews (id INTEGER PRIMARY KEY AUTOINCREMENT, text TEXT, author TEXT)''')
    
    # Обновленная таблица заказов (добавлен item_id и дата)
    cur.execute('''CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        user_id INTEGER, 
        amount_usd REAL, 
        amount_ltc REAL, 
        type TEXT, 
        status TEXT, 
        tx_hash TEXT, 
        product_id INTEGER,
        item_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )''')
    
    cur.execute('''CREATE TABLE IF NOT EXISTS promocodes (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, amount REAL, activations INTEGER)''')
    cur.execute('''CREATE TABLE IF NOT EXISTS promo_activations (user_id INTEGER, code_id INTEGER)''')
    
    conn.commit()
    return conn, cur

conn, cur = db_start()

# --- FSM (СОСТОЯНИЯ) ---
class AppStates(StatesGroup):
    add_city = State()
    prod_city = State()
    prod_name = State()
    prod_price = State()
    prod_desc = State()
    prod_photo = State()
    add_promo_code = State()
    add_promo_amount = State()
    add_promo_uses = State()
    add_rev_author = State()
    add_rev_text = State()
    activate_promo = State()
    topup_amount = State()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
async def get_ltc_rate():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(LTC_PRICE_API) as resp:
                data = await resp.json()
                return float(data['litecoin']['usd'])
        except:
            return 100.0 

async def check_transaction(amount_needed_ltc, user_id):
    if user_id == ADMIN_ID: return "TEST_HASH_ADMIN_PASS"
    url = LTC_API_URL.format(LTC_WALLET)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200: return False
                data = await resp.json()
                if 'txs' in data:
                    for tx in data['txs']:
                        if tx['confirmations'] >= 1:
                            for output in tx['outputs']:
                                val = output['value'] / 100000000
                                if 'addresses' in output and LTC_WALLET in output['addresses']:
                                    if abs(val - amount_needed_ltc) < 0.0005:
                                        cur.execute("SELECT * FROM orders WHERE tx_hash = ?", (tx['hash'],))
                                        if not cur.fetchone():
                                            return tx['hash']
        except Exception as e:
            logging.error(f"Tx Check Error: {e}")
    return None

# --- СТАРТ И МЕНЮ ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    cur.execute("INSERT OR IGNORE INTO users (user_id, username) VALUES (?, ?)", (uid, message.from_user.username))
    conn.commit()
    
    kb = ReplyKeyboardBuilder()
    kb.button(text="🛍 Товары")
    kb.button(text="👤 Профиль")
    kb.button(text="🎟 Промокод") 
    kb.button(text="⭐️ Отзывы")
    kb.button(text="🆘 Поддержка")
    kb.button(text="🏠 Главное меню") 
    if uid == ADMIN_ID:
        kb.button(text="🔧 Админ-панель")
    kb.adjust(2)

    caption = (
        "👁 <b>OKKO STORE — BEST IN MOLDOVA</b> 🇲🇩\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "Добро пожаловать в пространство абсолютного качества.\n\n"
        "💠 <b>Наш сервис</b> — это эталон надежности.\n"
        "💠 <b>Наш продукт</b> — самый чистый и качественный товар.\n\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "🔥 <b>АКТУАЛЬНЫЕ НОВОСТИ И БОНУСЫ:</b>\n\n"
        "🎁 <b>Бонус +5$ на депозит:</b>\n"
        "Ищите промокоды в нашей рекламе для первых пользователей!\n\n"
        "👥 <b>Бонус +5$ за друга:</b>\n"
        f"1. Вступите в канал: <a href='{CHANNEL_LINK}'>OKKO GROUP</a>\n"
        "2. Пригласите друзей по ссылке.\n"
        f"3. Напишите админу @{ADMIN_USERNAME} для получения кода.\n"
        "▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n"
        "💵 Оплата: <b>LTC / Баланс</b>\n"
        "🚀 Выдача: <b>Моментальная 24/7</b>"
    )
    
    try:
        video = FSInputFile("okko.mov")
        await message.answer_video(video, caption=caption, parse_mode='HTML', reply_markup=kb.as_markup(resize_keyboard=True))
    except:
        await message.answer(caption, parse_mode='HTML', reply_markup=kb.as_markup(resize_keyboard=True))

@dp.message(F.text == "🏠 Главное меню")
async def main_menu_btn(message: types.Message, state: FSMContext):
    await cmd_start(message, state)

@dp.message(F.text == "🆘 Поддержка")
async def support_btn(message: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="👨‍💻 Написать Админу", url=SUPPORT_LINK)
    await message.answer(f"По всем вопросам обращайтесь к администратору: @{ADMIN_USERNAME}", reply_markup=kb.as_markup())

# --- ПРОФИЛЬ И ИСТОРИЯ ПОКУПОК ---

@dp.message(F.text == "👤 Профиль")
async def profile_handler(message: types.Message):
    uid = message.from_user.id
    cur.execute("SELECT purchases, balance FROM users WHERE user_id=?", (uid,))
    res = cur.fetchone()
    
    bal_txt = f"{round(res[1], 2)} USD"
    if uid == ADMIN_ID: bal_txt += " (∞ ADMIN)"

    text = (
        f"<b>👤 ЛИЧНЫЙ КАБИНЕТ</b>\n"
        f"➖➖➖➖➖➖➖➖➖\n"
        f"💳 <b>ID:</b> <code>{uid}</code>\n"
        f"💰 <b>Баланс:</b> {bal_txt}\n"
        f"📦 <b>Покупок:</b> {res[0]} шт.\n"
        f"➖➖➖➖➖➖➖➖➖"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📦 Мои покупки", callback_data="my_orders")
    kb.button(text="💰 Пополнить баланс", callback_data="topup_balance")
    kb.adjust(1)
    
    try:
        photo = FSInputFile("okko.png")
        await message.answer_photo(photo, caption=text, parse_mode='HTML', reply_markup=kb.as_markup())
    except:
        await message.answer(text, parse_mode='HTML', reply_markup=kb.as_markup())

@dp.callback_query(F.data == "my_orders")
async def show_my_orders(call: types.CallbackQuery):
    uid = call.from_user.id
    # Берем последние 10 успешных покупок (где есть item_id)
    cur.execute("""
        SELECT o.id, p.name, o.created_at 
        FROM orders o
        JOIN products p ON o.product_id = p.id
        WHERE o.user_id = ? AND o.status = 'COMPLETED' AND o.item_id IS NOT NULL
        ORDER BY o.id DESC LIMIT 10
    """, (uid,))
    orders = cur.fetchall()
    
    kb = InlineKeyboardBuilder()
    if not orders:
        await call.answer("У вас пока нет покупок.", show_alert=True)
        return

    for order in orders:
        # Форматируем дату
        dt = datetime.strptime(order[2], "%Y-%m-%d %H:%M:%S")
        date_str = dt.strftime("%d.%m %H:%M")
        kb.button(text=f"{order[1]} | {date_str}", callback_data=f"order_{order[0]}")
    
    kb.adjust(1)
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_profile"))
    
    await call.message.edit_caption(caption="📦 <b>История ваших покупок:</b>\n<i>Нажмите на покупку, чтобы увидеть данные снова.</i>", parse_mode='HTML', reply_markup=kb.as_markup())

@dp.callback_query(F.data == "back_to_profile")
async def back_to_prof(call: types.CallbackQuery):
    await call.message.delete()
    await profile_handler(call.message)

@dp.callback_query(F.data.startswith("order_"))
async def show_order_details(call: types.CallbackQuery):
    oid = call.data.split('_')[1]
    # Достаем данные товара из item_id, который сохранен в заказе
    cur.execute("""
        SELECT i.content_text, i.content_photo, p.name 
        FROM orders o
        JOIN items i ON o.item_id = i.id
        JOIN products p ON o.product_id = p.id
        WHERE o.id = ?
    """, (oid,))
    data = cur.fetchone()
    
    if data:
        msg = f"📦 <b>Покупка: {data[2]}</b>\n\n📄 <b>Данные:</b>\n{data[0]}"
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 К списку", callback_data="my_orders")
        
        await call.message.delete()
        if data[1]:
             await bot.send_photo(call.from_user.id, photo=data[1], caption=msg, parse_mode='HTML', reply_markup=kb.as_markup())
        else:
             await bot.send_message(call.from_user.id, msg, parse_mode='HTML', reply_markup=kb.as_markup())
    else:
        await call.answer("Ошибка загрузки заказа.", show_alert=True)

# --- ПОПОЛНЕНИЕ БАЛАНСА ---

@dp.callback_query(F.data == "topup_balance")
async def topup_start(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введите сумму пополнения в USD (например: 50):")
    await state.set_state(AppStates.topup_amount)

@dp.message(AppStates.topup_amount)
async def topup_calc(message: types.Message, state: FSMContext):
    try:
        usd_amount = float(message.text)
        ltc_rate = await get_ltc_rate()
        ltc_amount = round(usd_amount / ltc_rate, 5)
        
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Я перевел", callback_data=f"check_topup_{usd_amount}_{ltc_amount}")
        
        msg = (
            f"📥 <b>Пополнение баланса</b>\n"
            f"Сумма: <b>${usd_amount}</b>\n"
            f"К переводу: <code>{ltc_amount}</code> LTC\n\n"
            f"Адрес: <code>{LTC_WALLET}</code>\n\n"
            f"⚠️ <i>После 1 подтверждения нажмите кнопку.</i>"
        )
        await message.answer(msg, parse_mode='HTML', reply_markup=kb.as_markup())
        await state.clear()
    except:
        await message.answer("Введите корректное число.")

@dp.callback_query(F.data.startswith("check_topup_"))
async def check_topup(call: types.CallbackQuery):
    _, _, usd, ltc = call.data.split('_')
    usd, ltc = float(usd), float(ltc)
    uid = call.from_user.id
    
    await call.answer("Проверка транзакции...", show_alert=True)
    tx = await check_transaction(ltc, uid)
    
    if tx:
        cur.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (usd, uid))
        cur.execute("INSERT INTO orders (user_id, amount_usd, amount_ltc, type, status, tx_hash) VALUES (?, ?, ?, ?, ?, ?)", 
                    (uid, usd, ltc, 'TOPUP', 'COMPLETED', tx))
        conn.commit()
        await call.message.delete()
        await call.message.answer(f"✅ Баланс пополнен на ${usd}!")
    else:
        await call.message.answer("❌ Транзакция не найдена. Подождите подтверждения сети.")

# --- ПРОМОКОДЫ ---

@dp.message(F.text == "🎟 Промокод")
async def promo_menu(message: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Активировать промокод", callback_data="enter_promo")
    kb.button(text="🎁 Получить промокод", url=SUPPORT_LINK)
    kb.adjust(1)
    await message.answer("Выберите действие:", reply_markup=kb.as_markup())

@dp.callback_query(F.data == "enter_promo")
async def promo_input(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введите промокод:")
    await state.set_state(AppStates.activate_promo)

@dp.message(AppStates.activate_promo)
async def promo_process(message: types.Message, state: FSMContext):
    code = message.text.strip()
    uid = message.from_user.id
    
    cur.execute("SELECT id, amount, activations FROM promocodes WHERE code=?", (code,))
    promo = cur.fetchone()
    
    if not promo:
        await message.answer("❌ Неверный промокод.")
    elif promo[2] <= 0:
        await message.answer("❌ Промокод закончился.")
    else:
        cur.execute("SELECT * FROM promo_activations WHERE user_id=? AND code_id=?", (uid, promo[0]))
        if cur.fetchone():
            await message.answer("❌ Вы уже активировали этот код.")
        else:
            cur.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (promo[1], uid))
            cur.execute("UPDATE promocodes SET activations = activations - 1 WHERE id=?", (promo[0],))
            cur.execute("INSERT INTO promo_activations VALUES (?, ?)", (uid, promo[0]))
            conn.commit()
            await message.answer(f"✅ Промокод активирован! +${promo[1]}")
    await state.clear()

# --- МАГАЗИН И ПОКУПКИ ---

@dp.message(F.text == "🛍 Товары")
async def shop_cities(message: types.Message):
    cur.execute("SELECT * FROM cities")
    cities = cur.fetchall()
    if not cities:
        await message.answer("Товары скоро появятся.")
        return
    kb = InlineKeyboardBuilder()
    for c in cities:
        kb.button(text=f"🏙 {c[1]}", callback_data=f"city_{c[0]}")
    kb.adjust(2)
    await message.answer("📍 Выберите город:", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("city_"))
async def shop_products(call: types.CallbackQuery):
    cid = call.data.split('_')[1]
    cur.execute("SELECT id, name, price_usd FROM products WHERE city_id=?", (cid,))
    prods = cur.fetchall()
    
    kb = InlineKeyboardBuilder()
    has_items = False
    
    for p in prods:
        cur.execute("SELECT count(*) FROM items WHERE product_id=? AND status='active'", (p[0],))
        count = cur.fetchone()[0]
        text_btn = f"{p[1]} | ${p[2]}"
        if count > 0:
            text_btn += f" | ({count} шт)"
            kb.button(text=text_btn, callback_data=f"prod_{p[0]}")
            has_items = True
            
    kb.adjust(1)
    # Кнопка НАЗАД теперь отдельной строкой
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_cities"))
    
    text = "💠 <b>Витрина товаров:</b>"
    if not has_items:
        text = "В этом городе пока пусто."
        
    try:
        photo = FSInputFile("okko.png")
        await call.message.delete() 
        await call.message.answer_photo(photo, caption=text, parse_mode='HTML', reply_markup=kb.as_markup())
    except:
        await call.message.answer(text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "back_to_cities")
async def back_shop(call: types.CallbackQuery):
    await call.message.delete()
    await shop_cities(call.message)

@dp.callback_query(F.data.startswith("prod_"))
async def product_view(call: types.CallbackQuery):
    pid = call.data.split('_')[1]
    cur.execute("SELECT name, price_usd, city_id FROM products WHERE id=?", (pid,))
    prod = cur.fetchone()
    
    cur.execute("SELECT count(*) FROM items WHERE product_id=? AND status='active'", (pid,))
    count = cur.fetchone()[0]
    
    if count == 0:
        await call.answer("Товар закончился!", show_alert=True)
        return

    kb = InlineKeyboardBuilder()
    kb.button(text=f"💳 Купить (${prod[1]})", callback_data=f"buyopts_{pid}")
    # Кнопка назад для товара
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data=f"city_{prod[2]}"))
    
    msg = (
        f"📦 <b>{prod[0]}</b>\n"
        f"💰 Цена: <b>${prod[1]}</b>\n"
        f"📊 В наличии: {count} шт.\n\n"
        f"<i>Нажмите купить, чтобы выбрать способ оплаты.</i>"
    )
    
    try:
        await call.message.edit_caption(caption=msg, parse_mode='HTML', reply_markup=kb.as_markup())
    except:
        await call.message.answer(msg, parse_mode='HTML', reply_markup=kb.as_markup())

# --- ЛОГИКА ОПЛАТЫ ---

@dp.callback_query(F.data.startswith("buyopts_"))
async def payment_options(call: types.CallbackQuery):
    pid = call.data.split('_')[1]
    cur.execute("SELECT price_usd, city_id FROM products WHERE id=?", (pid,))
    prod = cur.fetchone()
    price = prod[0]
    
    kb = InlineKeyboardBuilder()
    kb.button(text="💰 С баланса", callback_data=f"paybal_{pid}")
    kb.button(text="⚡️ Криптовалюта (LTC)", callback_data=f"payltc_{pid}")
    kb.button(text="🔙 Отмена", callback_data=f"city_{prod[1]}")
    kb.adjust(1)
    
    await call.message.edit_caption(caption=f"💳 Выберите способ оплаты (${price}):", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("paybal_"))
async def pay_balance(call: types.CallbackQuery):
    pid = int(call.data.split('_')[1])
    uid = call.from_user.id
    
    cur.execute("SELECT name, price_usd FROM products WHERE id=?", (pid,))
    prod = cur.fetchone()
    price = prod[1]
    
    cur.execute("SELECT balance FROM users WHERE user_id=?", (uid,))
    balance = cur.fetchone()[0]
    
    cur.execute("SELECT id FROM items WHERE product_id=? AND status='active' LIMIT 1", (pid,))
    item = cur.fetchone()
    if not item:
        await call.answer("Товар только что закончился.", show_alert=True)
        return

    if balance >= price:
        new_bal = balance - price
        cur.execute("UPDATE users SET balance=?, purchases=purchases+1 WHERE user_id=?", (new_bal, uid))
        cur.execute("UPDATE items SET status='sold' WHERE id=?", (item[0],))
        # Записываем item_id в заказ для истории
        cur.execute("INSERT INTO orders (user_id, amount_usd, type, status, product_id, item_id) VALUES (?, ?, 'BALANCE', 'COMPLETED', ?, ?)", (uid, price, pid, item[0]))
        conn.commit()
        await deliver_item(call, item[0], prod[0])
    else:
        missing = round(price - balance, 2)
        ltc_rate = await get_ltc_rate()
        missing_ltc = round(missing / ltc_rate, 5)
        
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Я доплатил", callback_data=f"check_part_{pid}_{missing}_{missing_ltc}")
        
        msg = (
            f"❌ Недостаточно средств.\n"
            f"Баланс: ${round(balance, 2)}\n"
            f"Нужно: ${price}\n\n"
            f"🔹 <b>Доплатите разницу:</b>\n"
            f"Сумма: <b>${missing}</b> ({missing_ltc} LTC)\n"
            f"Кошелек: <code>{LTC_WALLET}</code>"
        )
        await call.message.edit_caption(caption=msg, parse_mode='HTML', reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("payltc_"))
async def pay_direct_ltc(call: types.CallbackQuery):
    pid = int(call.data.split('_')[1])
    cur.execute("SELECT name, price_usd FROM products WHERE id=?", (pid,))
    prod = cur.fetchone()
    
    ltc_rate = await get_ltc_rate()
    ltc_sum = round(prod[1] / ltc_rate, 5)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Я оплатил", callback_data=f"check_full_{pid}_{ltc_sum}")
    
    msg = (
        f"💳 <b>Оплата товара: {prod[0]}</b>\n"
        f"Сумма: <b>${prod[1]}</b>\n"
        f"Переведите: <code>{ltc_sum}</code> LTC\n\n"
        f"Кошелек: <code>{LTC_WALLET}</code>"
    )
    await call.message.edit_caption(caption=msg, parse_mode='HTML', reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("check_part_"))
async def check_partial(call: types.CallbackQuery):
    _, _, pid, usd, ltc = call.data.split('_')
    pid, usd, ltc = int(pid), float(usd), float(ltc)
    uid = call.from_user.id
    
    await call.answer("Проверяем блокчейн...", show_alert=True)
    tx = await check_transaction(ltc, uid)
    
    if tx:
        cur.execute("UPDATE users SET balance=0, purchases=purchases+1 WHERE user_id=?", (uid,))
        cur.execute("SELECT id FROM items WHERE product_id=? AND status='active' LIMIT 1", (pid,))
        item = cur.fetchone()
        
        if item:
            cur.execute("UPDATE items SET status='sold' WHERE id=?", (item[0],))
            # Записываем item_id в заказ
            cur.execute("INSERT INTO orders (user_id, amount_usd, type, status, tx_hash, product_id, item_id) VALUES (?, ?, 'PARTIAL', 'COMPLETED', ?, ?, ?)", (uid, usd, tx, pid, item[0]))
            conn.commit()
            await deliver_item(call, item[0], "Товар") 
        else:
            cur.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (usd, uid))
            conn.commit()
            await call.message.answer("⚠️ Товар закончился пока вы платили. Сумма зачислена на баланс.")
    else:
        await call.message.answer("❌ Оплата не найдена.")

@dp.callback_query(F.data.startswith("check_full_"))
async def check_full(call: types.CallbackQuery):
    _, _, pid, ltc = call.data.split('_')
    pid, ltc = int(pid), float(ltc)
    uid = call.from_user.id
    
    await call.answer("Проверяем...", show_alert=True)
    tx = await check_transaction(ltc, uid)
    
    if tx:
        cur.execute("SELECT id FROM items WHERE product_id=? AND status='active' LIMIT 1", (pid,))
        item = cur.fetchone()
        if item:
            cur.execute("UPDATE items SET status='sold' WHERE id=?", (item[0],))
            # Записываем item_id в заказ
            cur.execute("INSERT INTO orders (user_id, amount_ltc, type, status, tx_hash, product_id, item_id) VALUES (?, ?, 'DIRECT', 'COMPLETED', ?, ?, ?)", (uid, ltc, tx, pid, item[0]))
            cur.execute("UPDATE users SET purchases=purchases+1 WHERE user_id=?", (uid,))
            conn.commit()
            await deliver_item(call, item[0], "Товар")
        else:
             usd_val = ltc * (await get_ltc_rate())
             cur.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (usd_val, uid))
             conn.commit()
             await call.message.answer("⚠️ Товар закончился. Средства зачислены на баланс.")
    else:
         await call.message.answer("❌ Оплата не найдена.")

async def deliver_item(call: types.CallbackQuery, item_id, prod_name):
    cur.execute("SELECT content_text, content_photo FROM items WHERE id=?", (item_id,))
    data = cur.fetchone()
    await call.message.delete()
    msg = f"✅ <b>Оплата успешна!</b>\nВаш товар: <b>{prod_name}</b>\n\n📄 <b>Данные:</b>\n{data[0]}"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 В меню", callback_data="back_to_cities")
    
    if data[1]: 
        await bot.send_photo(call.from_user.id, photo=data[1], caption=msg, parse_mode='HTML', reply_markup=kb.as_markup())
    else:
        await bot.send_message(call.from_user.id, msg, parse_mode='HTML', reply_markup=kb.as_markup())

# --- ОТЗЫВЫ ---
@dp.message(F.text == "⭐️ Отзывы")
async def reviews_view(message: types.Message):
    cur.execute("SELECT * FROM reviews ORDER BY id DESC LIMIT 5")
    revs = cur.fetchall()
    if not revs:
        await message.answer("Отзывов пока нет.")
        return
    txt = "⭐️ <b>Отзывы клиентов:</b>\n\n"
    for r in revs:
        txt += f"👤 <b>{r[2]}:</b> {r[1]}\n\n"
    await message.answer(txt, parse_mode='HTML')

# --- АДМИН ПАНЕЛЬ ---

@dp.message(F.text == "🔧 Админ-панель", F.from_user.id == ADMIN_ID)
async def admin_panel(message: types.Message):
    kb = ReplyKeyboardBuilder()
    kb.button(text="➕ Добавить товар")
    kb.button(text="➕ Город")
    kb.button(text="🎫 Создать Промо")
    kb.button(text="💬 Добавить Отзыв")
    kb.button(text="⬅️ Выход")
    kb.adjust(2)
    await message.answer("Админ-панель", reply_markup=kb.as_markup(resize_keyboard=True))

@dp.message(F.text == "⬅️ Выход")
async def admin_exit(message: types.Message, state: FSMContext):
    await state.clear()
    await cmd_start(message, state)

@dp.message(F.text == "➕ Добавить товар", F.from_user.id == ADMIN_ID)
async def admin_add_prod_start(message: types.Message, state: FSMContext):
    cur.execute("SELECT * FROM cities")
    cities = cur.fetchall()
    if not cities:
        await message.answer("Сначала создайте города!")
        return
    kb = InlineKeyboardBuilder()
    for c in cities:
        kb.button(text=c[1], callback_data=f"adm_city_{c[0]}")
    kb.adjust(2)
    await message.answer("Выберите город для товара:", reply_markup=kb.as_markup())
    await state.set_state(AppStates.prod_city)

@dp.callback_query(AppStates.prod_city)
async def admin_prod_city(call: types.CallbackQuery, state: FSMContext):
    cid = int(call.data.split('_')[2])
    await state.update_data(city_id=cid)
    await call.message.edit_text("Введите НАЗВАНИЕ товара (например: Сахар 1г):")
    await state.set_state(AppStates.prod_name)

@dp.message(AppStates.prod_name)
async def admin_prod_name(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Введите ЦЕНУ в USD (например: 30):")
    await state.set_state(AppStates.prod_price)

@dp.message(AppStates.prod_price)
async def admin_prod_price(message: types.Message, state: FSMContext):
    try:
        price = float(message.text)
        await state.update_data(price=price)
        await message.answer("Теперь отправьте ОПИСАНИЕ/КЛАД для этой единицы:")
        await state.set_state(AppStates.prod_desc)
    except:
        await message.answer("Введите число.")

@dp.message(AppStates.prod_desc)
async def admin_prod_desc(message: types.Message, state: FSMContext):
    await state.update_data(desc=message.text)
    await message.answer("Отправьте ФОТО для этого товара (или напишите 'нет'):")
    await state.set_state(AppStates.prod_photo)

@dp.message(AppStates.prod_photo)
async def admin_prod_fin(message: types.Message, state: FSMContext):
    photo = message.photo[-1].file_id if message.photo else None
    data = await state.get_data()
    
    cur.execute("SELECT id FROM products WHERE name=? AND price_usd=? AND city_id=?", 
                (data['name'], data['price'], data['city_id']))
    existing_prod = cur.fetchone()
    
    if existing_prod:
        prod_id = existing_prod[0]
        status_msg = f"✅ Товар '{data['name']}' уже был, добавлена новая единица на склад."
    else:
        cur.execute("INSERT INTO products (name, price_usd, city_id) VALUES (?, ?, ?)",
                    (data['name'], data['price'], data['city_id']))
        prod_id = cur.lastrowid
        status_msg = f"✅ Создан новый товар '{data['name']}' и добавлена первая единица."
    
    cur.execute("INSERT INTO items (product_id, content_text, content_photo) VALUES (?, ?, ?)",
                (prod_id, data['desc'], photo))
    conn.commit()
    await message.answer(status_msg)
    await state.clear()

@dp.message(F.text == "🎫 Создать Промо", F.from_user.id == ADMIN_ID)
async def add_promo_s(message: types.Message, state: FSMContext):
    await message.answer("Введите код (слово):")
    await state.set_state(AppStates.add_promo_code)

@dp.message(AppStates.add_promo_code)
async def add_promo_c(message: types.Message, state: FSMContext):
    await state.update_data(code=message.text)
    await message.answer("Сумма ($):")
    await state.set_state(AppStates.add_promo_amount)

@dp.message(AppStates.add_promo_amount)
async def add_promo_a(message: types.Message, state: FSMContext):
    await state.update_data(amount=float(message.text))
    await message.answer("Количество активаций:")
    await state.set_state(AppStates.add_promo_uses)

@dp.message(AppStates.add_promo_uses)
async def add_promo_fin(message: types.Message, state: FSMContext):
    d = await state.get_data()
    cur.execute("INSERT INTO promocodes (code, amount, activations) VALUES (?, ?, ?)", 
                (d['code'], d['amount'], int(message.text)))
    conn.commit()
    await message.answer("✅ Промокод создан.")
    await state.clear()

@dp.message(F.text == "➕ Город", F.from_user.id == ADMIN_ID)
async def add_city(message: types.Message, state: FSMContext):
    await message.answer("Название города:")
    await state.set_state(AppStates.add_city)

@dp.message(AppStates.add_city)
async def add_city_f(message: types.Message, state: FSMContext):
    cur.execute("INSERT INTO cities (name) VALUES (?)", (message.text,))
    conn.commit()
    await message.answer("Город добавлен.")
    await state.clear()

@dp.message(F.text == "💬 Добавить Отзыв", F.from_user.id == ADMIN_ID)
async def add_rev(message: types.Message, state: FSMContext):
    await message.answer("Автор:")
    await state.set_state(AppStates.add_rev_author)

@dp.message(AppStates.add_rev_author)
async def add_rev_text(message: types.Message, state: FSMContext):
    await state.update_data(author=message.text)
    await message.answer("Текст:")
    await state.set_state(AppStates.add_rev_text)

@dp.message(AppStates.add_rev_text)
async def add_rev_fin(message: types.Message, state: FSMContext):
    d = await state.get_data()
    cur.execute("INSERT INTO reviews (text, author) VALUES (?, ?)", (message.text, d['author']))
    conn.commit()
    await message.answer("Отзыв добавлен.")
    await state.clear()

async def health_check(request): return web.Response(text="OKKO RUNNING")
async def main():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except:
        pass