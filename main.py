"""
🚀 MATH_TEKSHIRUVCHI_BOT — ENTERPRISE EDTECH ECOSYSTEM
------------------------------------------------------
Arxitektor: FAANG Level Senior Software Architect
Texnologiyalar: Aiogram 3.x, FastAPI, SQLAlchemy (Sync), Groq LPU, Render
Maqsad: 24/7 Barqaror, Kengayuvchan va Gamifikatsiyalashgan Platforma
"""

import os
import logging
import asyncio
import json
import random
import datetime
import time
import io
import re
from typing import List, Optional, Dict, Any, Union
from dataclasses import dataclass
from threading import Thread

# Web & API Frameworks
import uvicorn
from fastapi import FastAPI, Request, Response

# Telegram Bot Framework
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, StateFilter, BaseFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile,
    InputMediaPhoto, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# Ma'lumotlar bazasi (SQLAlchemy - Sinxron drayver orqali)
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, 
    DateTime, ForeignKey, Text, Boolean, desc, func, update, and_
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Session
from sqlalchemy.exc import SQLAlchemyError

# AI Integration
from openai import AsyncOpenAI

# ==================================================
# 1. KONFIGURATSIYA (ENVIRONMENT VARIABLES)
# ==================================================

# Logger sozlamalari
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger("MathPlatform")

class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
    
    # Database URL Fix: MissingGreenlet xatosini oldini olish uchun psycopg2 ishlatamiz
    _raw_db_url = os.getenv("DATABASE_URL", "sqlite:///math_db.sqlite3")
    if _raw_db_url.startswith("postgres://"):
        DATABASE_URL = _raw_db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif _raw_db_url.startswith("postgresql://"):
        DATABASE_URL = _raw_db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        DATABASE_URL = _raw_db_url

    # Groq AI sozlamalari
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile")
    AI_ENABLED = os.getenv("AI_ENABLED", "True").lower() == "true"

    # Server sozlamalari
    PORT = int(os.getenv("PORT", 8080))
    WEBHOOK_URL = os.getenv("WEBHOOK_URL") # Masalan: https://app-name.onrender.com
    WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
    
    # Biznes mantiq qoidalari
    MIN_PASS_PERCENT = 70.0
    XP_UNIT = 10 # Har bir to'g'ri javob uchun
    LEVEL_UP_BASE = 500 # Keyingi daraja uchun kerakli XP
    DAILY_STREAK_BONUS = 50

# ==================================================
# 2. DATABASE MODELS (ORM)
# ==================================================

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, index=True)
    full_name = Column(String(255))
    username = Column(String(255), nullable=True)
    
    # Gamification
    xp = Column(Integer, default=0)
    level = Column(Integer, default=1)
    streak = Column(Integer, default=0)
    last_active = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Status
    is_admin = Column(Boolean, default=False)
    is_banned = Column(Boolean, default=False)
    joined_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    results = relationship("Result", back_populates="user", cascade="all, delete-orphan")
    favorites = relationship("Favorite", back_populates="user")

class Test(Base):
    __tablename__ = "tests"
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, index=True)
    title = Column(String(255))
    category = Column(String(100))
    difficulty = Column(String(20)) # Oson, O'rta, Qiyin
    description = Column(Text, nullable=True)
    pdf_file_id = Column(String(255), nullable=True)
    
    # Javoblar kaliti: {"1":"A", "2":"C"}
    correct_answers = Column(Text) 
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    is_active = Column(Boolean, default=True)

class Result(Base):
    __tablename__ = "results"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.telegram_id"))
    test_code = Column(String(50))
    
    score = Column(Integer)
    total = Column(Integer)
    percent = Column(Float)
    
    # Xatolar ro'yxati JSON formatda
    mistakes_data = Column(Text, nullable=True) 
    completed_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    user = relationship("User", back_populates="results")

class Favorite(Base):
    __tablename__ = "favorites"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.telegram_id"))
    test_code = Column(String(50))
    user = relationship("User", back_populates="favorites")

# Engine yaratish (Sinxron Pool bilan)
engine = create_engine(
    Config.DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Ma'lumotlar bazasi sxemasi muvaffaqiyatli yaratildi.")
    except Exception as e:
        logger.error(f"❌ Bazani yaratishda xatolik: {e}")

# ==================================================
# 3. AI SERVICES (GROQ INTEGRATION)
# ==================================================

class GroqService:
    def __init__(self):
        self.client = AsyncOpenAI(
            api_key=Config.GROQ_API_KEY,
            base_url="https://api.groq.com/openai/v1"
        ) if Config.GROQ_API_KEY else None

    async def explain_problem(self, problem_text: str) -> str:
        if not self.client or not Config.AI_ENABLED:
            return "⚠️ AI tizimi vaqtincha faol emas. Iltimos, keyinroq urinib ko'ring."

        system_prompt = (
            "Siz professional matematika o'qituvchisiz. "
            "O'quvchining savoliga o'zbek tilida, juda sodda va bosqichma-bosqich javob bering. "
            "Matematik formulalar uchun Markdown formatidan foydalaning."
        )

        try:
            chat_completion = await self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": problem_text}
                ],
                model=Config.GROQ_MODEL,
                temperature=0.2,
                max_tokens=3000
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            logger.error(f"Groq API Error: {e}")
            return "❌ Kechirasi, masalani tahlil qilishda texnik xatolik yuz berdi."

# ==================================================
# 4. UI/UX COMPONENTS & KEYBOARDS
# ==================================================

class UI:
    @staticmethod
    def main_menu(is_admin=False):
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="📚 Testlar Ro'yxati"), KeyboardButton(text="✅ Test Tekshirish"))
        builder.row(KeyboardButton(text="🤖 AI Ustoz"), KeyboardButton(text="📊 Natijalarim"))
        builder.row(KeyboardButton(text="👤 Profilim"), KeyboardButton(text="🏆 Leaderboard"))
        builder.row(KeyboardButton(text="⭐️ Saralanganlar"), KeyboardButton(text="📞 Bog'lanish"))
        
        if is_admin:
            builder.row(KeyboardButton(text="⚙️ Admin Panel"))
            
        return builder.as_markup(resize_keyboard=True)

    @staticmethod
    def inline_back(callback_data="go_home"):
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text="⬅️ Orqaga", callback_data=callback_data))
        builder.add(InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="go_home"))
        return builder.as_markup()

    @staticmethod
    def premium_card(title: str, content: str):
        divider = "━━━━━━━━━━━━━━"
        return f"💠 **{title.upper()}**\n{divider}\n\n{content}\n\n{divider}\n✨ _Math Platform AI v2.0_"

# ==================================================
# 5. FSM STATES
# ==================================================

class BotStates(StatesGroup):
    # User States
    WAITING_TEST_CODE = State()
    WAITING_AI_QUESTION = State()
    WAITING_FEEDBACK = State()
    
    # Admin States
    ADMIN_BROADCAST = State()
    ADMIN_ADD_TEST_META = State()
    ADMIN_ADD_TEST_KEYS = State()

# ==================================================
# 6. BOT HANDLERS (CORE LOGIC)
# ==================================================

router = Router()
groq_ai = GroqService()

# --- START HANDLER ---
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_id == message.from_user.id).first()
        if not user:
            user = User(
                telegram_id=message.from_user.id,
                full_name=message.from_user.full_name,
                username=message.from_user.username,
                is_admin=(message.from_user.id == Config.ADMIN_ID)
            )
            db.add(user)
            db.commit()
            logger.info(f"Yangi foydalanuvchi: {message.from_user.id}")

    welcome_msg = UI.premium_card(
        "Xush kelibsiz",
        f"Assalomu alaykum, **{message.from_user.first_name}**!\n\n"
        "Men sizning professional matematika yordamchingizman. "
        "Bilimingizni sinash va yangi cho'qqilarni zabt etishga tayyormisiz?"
    )
    
    await message.answer(
        welcome_msg, 
        reply_markup=UI.main_menu(message.from_user.id == Config.ADMIN_ID),
        parse_mode="Markdown"
    )

# --- TESTLAR RO'YXATI ---
@router.message(F.text == "📚 Testlar Ro'yxati")
async def show_categories(message: Message):
    with SessionLocal() as db:
        categories = db.query(Test.category).distinct().all()
    
    if not categories:
        return await message.answer("😔 Hozircha testlar mavjud emas.")

    builder = InlineKeyboardBuilder()
    for cat in categories:
        builder.add(InlineKeyboardButton(text=f"📁 {cat[0]}", callback_data=f"cat_{cat[0]}"))
    
    builder.adjust(2)
    await message.answer("📑 **Mavzu yoki kategoriyani tanlang:**", reply_markup=builder.as_markup(), parse_mode="Markdown")

@router.callback_query(F.data.startswith("cat_"))
async def list_tests(callback: CallbackQuery):
    cat_name = callback.data.split("_")[1]
    with SessionLocal() as db:
        tests = db.query(Test).filter(Test.category == cat_name, Test.is_active == True).all()
    
    text = f"📂 **{cat_name} bo'yicha testlar:**\n\n"
    builder = InlineKeyboardBuilder()
    
    for t in tests:
        text += f"🔹 `{t.code}` | {t.title} ({t.difficulty})\n"
        builder.add(InlineKeyboardButton(text=f"📖 {t.code}", callback_data=f"view_test_{t.code}"))
    
    builder.adjust(2)
    builder.row(InlineKeyboardButton(text="⬅️ Orqaga", callback_data="back_to_cats"))
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# --- TEST TEKSHIRISH (LOGIC) ---
@router.message(F.text == "✅ Test Tekshirish")
async def start_test_check(message: Message, state: FSMContext):
    await state.set_state(BotStates.WAITING_TEST_CODE)
    
    text = (
        "📝 **Testni tekshirish uchun quyidagi formatda yuboring:**\n\n"
        "`kod*javoblar` (masalan: `105*abcd...`)\n\n"
        "⚠️ _Eslatma: Faqat kichik harflarda, bo'shliqlarsiz yozing._"
    )
    await message.answer(text, reply_markup=UI.inline_back(), parse_mode="Markdown")

@router.message(BotStates.WAITING_TEST_CODE)
async def process_test_submission(message: Message, state: FSMContext):
    if "*" not in message.text:
        return await message.answer("❌ Xato format. Misol: `105*abcd`")

    parts = message.text.split("*")
    test_code = parts[0].strip()
    user_answers = parts[1].strip().lower()

    with SessionLocal() as db:
        # 1. Test mavjudligini tekshirish
        test = db.query(Test).filter(Test.code == test_code).first()
        if not test:
            return await message.answer(f"🚫 `{test_code}` kodli test topilmadi.")

        # 2. Oldin ishlaganini tekshirish
        existing = db.query(Result).filter(
            Result.user_id == message.from_user.id, 
            Result.test_code == test_code
        ).first()
        
        if existing:
            return await message.answer("⚠️ Siz bu testni allaqachon topshirgansiz!")

        # 3. Tekshirish mantiqi
        correct_data = json.loads(test.correct_answers)
        correct_count = 0
        mistakes = []
        
        for i, char in enumerate(user_answers, 1):
            key = str(i)
            if key in correct_data:
                if char == correct_data[key].lower():
                    correct_count += 1
                else:
                    mistakes.append(f"{i}-savol (Siz: {char.upper()}, To'g'ri: {correct_data[key].upper()})")
        
        total_q = len(correct_data)
        percent = (correct_count / total_q) * 100
        
        # 4. Natijani saqlash
        new_result = Result(
            user_id=message.from_user.id,
            test_code=test_code,
            score=correct_count,
            total=total_q,
            percent=percent,
            mistakes_data=json.dumps(mistakes)
        )
        db.add(new_result)
        
        # 5. Gamification (XP va Level)
        user = db.query(User).filter(User.telegram_id == message.from_user.id).first()
        earned_xp = correct_count * Config.XP_UNIT
        user.xp += earned_xp
        
        # Streak hisoblash
        today = datetime.datetime.utcnow().date()
        if (today - user.last_active.date()).days == 1:
            user.streak += 1
            user.xp += Config.DAILY_STREAK_BONUS
        elif (today - user.last_active.date()).days > 1:
            user.streak = 1
        
        user.last_active = datetime.datetime.utcnow()
        
        # Level up tekshiruvi
        new_lvl = (user.xp // Config.LEVEL_UP_BASE) + 1
        if new_lvl > user.level:
            user.level = new_lvl
            await message.answer(f"🎊 **TABRIKLAYMIZ!** Siz {new_lvl}-darajaga ko'tarildingiz!")

        db.commit()

        # 6. Natija kartasini chiqarish
        status_emoji = "✅" if percent >= Config.MIN_PASS_PERCENT else "❌"
        result_card = UI.premium_card(
            "Test Natijasi",
            f"📖 Test: {test.title}\n"
            f"{status_emoji} Natija: {correct_count}/{total_q}\n"
            f"📊 Foiz: {percent:.1f}%\n"
            f"💎 XP: +{earned_xp}\n"
            f"🔥 Streak: {user.streak} kun"
        )
        
        kb = InlineKeyboardBuilder()
        if mistakes:
            kb.add(InlineKeyboardButton(text="🧐 Xatolarni ko'rish", callback_data=f"mistakes_{new_result.id}"))
        kb.row(InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="go_home"))
        
        await message.answer(result_card, reply_markup=kb.as_markup(), parse_mode="Markdown")
        await state.clear()

# --- AI USTOZ (GROQ) ---
@router.message(F.text == "🤖 AI Ustoz")
async def ai_tutor_start(message: Message, state: FSMContext):
    await state.set_state(BotStates.WAITING_AI_QUESTION)
    text = (
        "🧠 **AI Ustoz xizmati yoqildi.**\n\n"
        "Menga istalgan matematik masala, formula yoki tushunmagan mavzuyingizni yuboring. "
        "Men uni sizga eng sodda tilda tushuntirib beraman."
    )
    await message.answer(text, reply_markup=UI.inline_back(), parse_mode="Markdown")

@router.message(BotStates.WAITING_AI_QUESTION)
async def handle_ai_request(message: Message, state: FSMContext):
    if not message.text:
        return await message.answer("Iltimos, savolingizni matn shaklida yuboring.")
    
    wait_msg = await message.answer("🤔 _AI tahlil qilmoqda, bir oz kuting..._", parse_mode="Markdown")
    
    explanation = await groq_ai.explain_problem(message.text)
    
    await wait_msg.delete()
    await message.answer(f"🎓 **Ustozning javobi:**\n\n{explanation}", parse_mode="Markdown")

# --- PROFIL VA STATISTIKA ---
@router.message(F.text == "👤 Profilim")
async def show_profile(message: Message):
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_id == message.from_user.id).first()
        res_stats = db.query(
            func.count(Result.id),
            func.avg(Result.percent)
        ).filter(Result.user_id == message.from_user.id).first()

    total_tests = res_stats[0] or 0
    avg_score = float(res_stats[1] or 0)
    
    profile_data = (
        f"👤 **Ism:** {user.full_name}\n"
        f"🎭 **Daraja:** {user.level}-LVL\n"
        f"💎 **XP:** {user.xp}\n"
        f"🔥 **Streak:** {user.streak} kun\n\n"
        f"📊 **Umumiy testlar:** {total_tests} ta\n"
        f"📈 **O'rtacha ko'rsatkich:** {avg_score:.1f}%"
    )
    
    await message.answer(UI.premium_card("Sizning Profilingiz", profile_data), parse_mode="Markdown")

# --- REYTING (LEADERBOARD) ---
@router.message(F.text == "🏆 Leaderboard")
async def show_leaderboard(message: Message):
    with SessionLocal() as db:
        top_users = db.query(User).order_by(User.xp.desc()).limit(10).all()
    
    text = "🏆 **GLOBAL REYTING (TOP 10)**\n\n"
    for i, u in enumerate(top_users, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👤"
        text += f"{medal} {i}. {u.full_name} — `{u.xp}` XP\n"
    
    await message.answer(text, parse_mode="Markdown")

# --- ADMIN PANEL (RESTRICTED) ---
@router.message(F.text == "⚙️ Admin Panel")
async def admin_dashboard(message: Message):
    if message.from_user.id != Config.ADMIN_ID:
        return
    
    with SessionLocal() as db:
        total_u = db.query(User).count()
        total_t = db.query(Test).count()
        total_r = db.query(Result).count()
        
    admin_text = (
        "🛠 **Admin Boshqaruv Markazi**\n\n"
        f"👥 Jami foydalanuvchilar: {total_u}\n"
        f"📝 Jami testlar: {total_t}\n"
        f"✅ Jami urinishlar: {total_r}\n\n"
        "Server holati: 🟢 Online (24/7)"
    )
    
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📢 Xabar yuborish", callback_data="adm_broadcast"))
    kb.row(InlineKeyboardButton(text="➕ Yangi test qo'shish", callback_data="adm_add_test"))
    
    await message.answer(admin_text, reply_markup=kb.as_markup(), parse_mode="Markdown")

# ==================================================
# 7. WEB SERVER & DEPLOYMENT (FASTAPI)
# ==================================================

app = FastAPI()

@app.get("/")
async def root():
    return {
        "status": "online", 
        "engine": "MathPlatform AI",
        "uptime": str(datetime.datetime.now()),
        "port_status": "listening"
    }

@app.post(Config.WEBHOOK_PATH)
async def bot_webhook(request: Request):
    update_data = await request.json()
    update_obj = types.Update(**update_data)
    await dp.feed_update(bot, update_obj)
    return Response(status_code=200)

async def on_startup():
    # DB yaratish
    init_db()
    
    # Webhookni sozlash
    if Config.WEBHOOK_URL:
        await bot.set_webhook(f"{Config.WEBHOOK_URL}{Config.WEBHOOK_PATH}")
        logger.info(f"✅ Webhook o'rnatildi: {Config.WEBHOOK_URL}")
    else:
        await bot.delete_webhook()
        logger.info("ℹ️ Polling rejimi yoqildi.")

async def on_shutdown():
    await bot.session.close()

# Bot va Dispatcher obyektlari
bot = Bot(token=Config.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

def start_web_server():
    """FastAPI serverini alohida thread'da ishga tushirish (Render Port Binding uchun)"""
    uvicorn.run(app, host="0.0.0.0", port=Config.PORT)

# --- CALLBACK HANDLERS (NAVIGATION) ---
@router.callback_query(F.data == "go_home")
async def cb_home(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    # Asosiy menyu xabarini qaytadan yuborish
    await cmd_start(callback.message, state)

@router.callback_query(F.data.startswith("mistakes_"))
async def cb_show_mistakes(callback: CallbackQuery):
    res_id = int(callback.data.split("_")[1])
    with SessionLocal() as db:
        res = db.query(Result).filter(Result.id == res_id).first()
        if not res:
            return await callback.answer("Natija topilmadi.")
        
        mistakes = json.loads(res.mistakes_data)
        if not mistakes:
            return await callback.message.answer("🎉 Ajoyib! Sizda xatolar yo'q.")
        
        text = "🧐 **Xatolar tahlili:**\n\n" + "\n".join(mistakes)
        await callback.message.answer(text, parse_mode="Markdown")
        await callback.answer()

# ==================================================
# 8. EXECUTION POINT
# ==================================================

if __name__ == "__main__":
    # 1. Web serverni (FastAPI) orqa fonda boshlash
    server_thread = Thread(target=start_web_server, daemon=True)
    server_thread.start()
    logger.info(f"🚀 FastAPI server {Config.PORT}-portda ishlamoqda.")

    # 2. Botni asinxron ishga tushirish
    async def main_runner():
        await on_startup()
        try:
            if Config.WEBHOOK_URL:
                # Webhook rejimida botni kutish holatida ushlab turish
                while True:
                    await asyncio.sleep(3600)
            else:
                await dp.start_polling(bot)
        finally:
            await on_shutdown()

    try:
        asyncio.run(main_runner())
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 Bot to'xtatildi.")
