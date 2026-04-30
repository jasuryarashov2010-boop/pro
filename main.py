import os
import logging
import asyncio
import datetime
import json
import random
from typing import List, Optional, Union, Dict
from abc import ABC, abstractmethod

# Web Server & Bot Framework
from fastapi import FastAPI, Request
from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, 
    InlineKeyboardButton, Message, CallbackQuery, BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# Database & AI
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, ForeignKey, 
    Text, Boolean, create_engine, select, update, desc, func
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship, Session
import google.generativeai as genai
import openai

# --- CONFIGURATION (ENV) ---
class Config:
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///math_bot.db")
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    PORT = int(os.getenv("PORT", 8080))
    MIN_PASS_PERCENT = float(os.getenv("MIN_PASS_PERCENT", 70.0))
    AI_ENABLED = os.getenv("AI_ENABLED", "True").lower() == "true"
    WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# --- DATABASE MODELS ---
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, index=True)
    full_name = Column(String(255))
    username = Column(String(255))
    xp = Column(Integer, default=0)
    level = Column(Integer, default=1)
    streak = Column(Integer, default=0)
    last_active = Column(DateTime, default=datetime.datetime.utcnow)
    is_admin = Column(Boolean, default=False)
    
class Test(Base):
    __tablename__ = "tests"
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, index=True)
    title = Column(String(255))
    category = Column(String(100))
    difficulty = Column(String(20)) # Easy, Medium, Hard
    content_url = Column(String(500)) # PDF/Image link
    correct_answers = Column(Text) # JSON string: {"1":"A", "2":"B"}
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

class Result(Base):
    __tablename__ = "results"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.telegram_id"))
    test_code = Column(String(50))
    score = Column(Integer)
    total_questions = Column(Integer)
    percent = Column(Float)
    mistakes = Column(Text) # JSON string
    completed_at = Column(DateTime, default=datetime.datetime.utcnow)

class Achievement(Base):
    __tablename__ = "achievements"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.telegram_id"))
    badge_name = Column(String(100))
    earned_at = Column(DateTime, default=datetime.datetime.utcnow)

# Database Engine Setup
engine = create_engine(Config.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)

# --- AI SERVICE ---
class AIService:
    def __init__(self):
        if Config.GEMINI_API_KEY:
            genai.configure(api_key=Config.GEMINI_API_KEY)
            self.gemini = genai.GenerativeModel('gemini-1.5-flash')
        
    async def analyze_math(self, query: str, image_path: Optional[str] = None) -> str:
        prompt = f"""Siz tajribali Matematika o'qituvchisiz. 
        O'quvchining quyidagi savoliga o'zbek tilida, juda sodda va bosqichma-bosqich javob bering.
        Savol: {query}
        Format:
        1. Muammoning mohiyati
        2. Qadam-baqadam yechim
        3. Yakuniy javob va xulosa.
        Agar savol matematikaga oid bo'lmasa, uni muloyimlik bilan rad eting."""

        try:
            if Config.AI_ENABLED:
                response = self.gemini.generate_content(prompt)
                return response.text
            return "AI xizmati vaqtincha o'chirilgan."
        except Exception as e:
            logging.error(f"AI Error: {e}")
            return "Kechirasiz, AI hozirda band. Keyinroq urinib ko'ring."

# --- UI HELPERS & KEYBOARDS ---
class UI:
    @staticmethod
    def main_menu(is_admin=False):
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="📚 Testlar Ro'yxati"), KeyboardButton(text="✅ Test Tekshirish"))
        builder.row(KeyboardButton(text="🤖 AI Ustoz"), KeyboardButton(text="📊 Natijalarim"))
        builder.row(KeyboardButton(text="👤 Profilim"), KeyboardButton(text="📞 Bog'lanish"))
        if is_admin:
            builder.row(KeyboardButton(text="⚙️ Admin Panel"))
        return builder.as_markup(resize_keyboard=True)

    @staticmethod
    def back_home():
        builder = InlineKeyboardBuilder()
        builder.add(InlineKeyboardButton(text="⬅️ Orqaga", callback_data="go_back"))
        builder.add(InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="go_home"))
        return builder.as_markup()

    @staticmethod
    def premium_card(title: str, body: str):
        return f"✨ **{title}**\n\n{body}\n\n━━━━━━━━━━━━━━"

# --- STATES ---
class BotStates(StatesGroup):
    MAIN_MENU = State()
    TEST_LIST = State()
    TEST_CHECKING = State()
    AI_CHAT = State()
    ADMIN_COMMANDS = State()
    SEND_TEST_ANSWERS = State()

# --- BOT LOGIC (HANDLERS) ---
router = Router()
ai_service = AIService()

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
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
    
    welcome_text = UI.premium_card(
        "Xush kelibsiz!", 
        f"Assalomu alaykum, {message.from_user.first_name}!\n"
        "Men sizning intellektual matematika yordamchingizman. "
        "Bilim darajangizni oshirishga tayyormisiz?"
    )
    await message.answer(welcome_text, reply_markup=UI.main_menu(message.from_user.id == Config.ADMIN_ID), parse_mode="Markdown")

# --- TEST LISITNG ---
@router.message(F.text == "📚 Testlar Ro'yxati")
async def show_tests(message: Message):
    with SessionLocal() as db:
        tests = db.query(Test).limit(10).all()
        
    if not tests:
        return await message.answer("Hozircha testlar mavjud emas.")
    
    text = "📂 **Mavjud testlar:**\n\n"
    builder = InlineKeyboardBuilder()
    for test in tests:
        text += f"🔹 {test.code} | {test.title} ({test.difficulty})\n"
        builder.add(InlineKeyboardButton(text=f"📖 {test.code}", callback_data=f"view_test_{test.code}"))
    
    builder.adjust(2)
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# --- TEST CHECKING LOGIC ---
@router.message(F.text == "✅ Test Tekshirish")
async def test_check_start(message: Message, state: FSMContext):
    await state.set_state(BotStates.TEST_CHECKING)
    await message.answer(
        "📝 Test kodini va javoblaringizni quyidagi formatda yuboring:\n\n"
        "`1001*abcd...` (yoki `1001*1a2b3c...`)",
        parse_mode="Markdown",
        reply_markup=UI.back_home()
    )

@router.message(BotStates.TEST_CHECKING)
async def process_test_submission(message: Message, state: FSMContext):
    if "*" not in message.text:
        return await message.answer("❌ Xato format. Misol: `1001*abcd...`")
    
    code, answers = message.text.split("*", 1)
    code = code.strip()
    answers = answers.strip().lower()
    
    with SessionLocal() as db:
        # Check if already solved
        existing = db.query(Result).filter(
            Result.user_id == message.from_user.id, 
            Result.test_code == code
        ).first()
        if existing:
            return await message.answer("⚠️ Siz bu testni allaqachon topshirgansiz!")
            
        test = db.query(Test).filter(Test.code == code).first()
        if not test:
            return await message.answer("❌ Bunday kodli test topilmadi.")
        
        correct = json.loads(test.correct_answers)
        score = 0
        mistakes = []
        
        # Checking logic
        for i, char in enumerate(answers, 1):
            key = str(i)
            if key in correct:
                if char == correct[key].lower():
                    score += 1
                else:
                    mistakes.append(f"{i}-savol (Siz: {char.upper()}, To'g'ri: {correct[key].upper()})")
        
        total = len(correct)
        percent = (score / total) * 100
        
        # Save Result
        res = Result(
            user_id=message.from_user.id,
            test_code=code,
            score=score,
            total_questions=total,
            percent=percent,
            mistakes=json.dumps(mistakes)
        )
        db.add(res)
        
        # XP & Level System
        user = db.query(User).filter(User.telegram_id == message.from_user.id).first()
        earned_xp = score * 10
        user.xp += earned_xp
        if user.xp >= user.level * 100:
            user.level += 1
            await message.answer(f"🎊 TABRIKLAYMIZ! Siz {user.level}-darajaga ko'tarildingiz!")
        
        db.commit()
        
        # Premium Result Card
        status = "✅ Muvaffaqiyatli" if percent >= Config.MIN_PASS_PERCENT else "❌ Yetarsiz"
        result_text = UI.premium_card(
            f"Natija: {test.title}",
            f"👤 O'quvchi: {message.from_user.full_name}\n"
            f"📈 Ball: {score}/{total}\n"
            f"📊 Foiz: {percent:.1f}%\n"
            f"🏆 XP: +{earned_xp}\n"
            f"📌 Status: {status}"
        )
        
        await message.answer(result_text, parse_mode="Markdown", reply_markup=UI.main_menu(user.is_admin))
        await state.clear()

# --- AI TUTOR HANDLER ---
@router.message(F.text == "🤖 AI Ustoz")
async def ai_tutor_start(message: Message, state: FSMContext):
    await state.set_state(BotStates.AI_CHAT)
    await message.answer(
        "🧠 **Men AI Matematika ustoziman.**\n"
        "Menga istalgan misol yoki masalani yuboring (matn yoki rasm ko'rinishida).\n"
        "Men sizga uni tushuntirib beraman.",
        parse_mode="Markdown",
        reply_markup=UI.back_home()
    )

@router.message(BotStates.AI_CHAT)
async def handle_ai_query(message: Message):
    wait_msg = await message.answer("🤔 O'ylayapman... Bir oz kutib turing.")
    
    query_text = message.text or message.caption or "Ushbu masalani yechishda yordam ber."
    response = await ai_service.analyze_math(query_text)
    
    await wait_msg.delete()
    await message.answer(f"🎓 **AI Ustoz javobi:**\n\n{response}", parse_mode="Markdown")

# --- PROFILE & STATS ---
@router.message(F.text == "👤 Profilim")
async def show_profile(message: Message):
    with SessionLocal() as db:
        user = db.query(User).filter(User.telegram_id == message.from_user.id).first()
        results = db.query(Result).filter(Result.user_id == message.from_user.id).all()
    
    total_tests = len(results)
    avg_score = sum(r.percent for r in results) / total_tests if total_tests > 0 else 0
    
    profile_text = UI.premium_card(
        "Sizning Profilingiz",
        f"🆔 ID: `{user.telegram_id}`\n"
        f"🎭 Daraja: {user.level}-LVL\n"
        f"💎 XP: {user.xp}\n"
        f"🔥 Streak: {user.streak} kun\n\n"
        f"📊 Umumiy testlar: {total_tests}\n"
        f"📈 O'rtacha foiz: {avg_score:.1f}%"
    )
    
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="📜 Oxirgi natijalar", callback_data="my_results"))
    builder.add(InlineKeyboardButton(text="🏆 Leaderboard", callback_data="leaderboard"))
    
    await message.answer(profile_text, parse_mode="Markdown", reply_markup=builder.as_markup())

# --- ADMIN PANEL ---
@router.message(F.text == "⚙️ Admin Panel")
async def admin_panel(message: Message):
    if message.from_user.id != Config.ADMIN_ID:
        return
    
    with SessionLocal() as db:
        user_count = db.query(User).count()
        test_count = db.query(Test).count()
    
    admin_text = UI.premium_card(
        "Admin Boshqaruvi",
        f"👥 Foydalanuvchilar: {user_count}\n"
        f"📝 Mavjud testlar: {test_count}\n"
        f"🚀 Server Status: Online (24/7)"
    )
    
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="➕ Test Qo'shish", callback_data="admin_add_test"))
    builder.add(InlineKeyboardButton(text="📢 Xabar Yuborish", callback_data="admin_broadcast"))
    builder.row(InlineKeyboardButton(text="📊 Batafsil Statistika", callback_data="admin_stats"))
    
    await message.answer(admin_text, reply_markup=builder.as_markup(), parse_mode="Markdown")

# --- CALLBACK HANDLERS (Navigation) ---
@router.callback_query(F.data == "go_home")
async def cb_home(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Bosh menyuga qaytdingiz.", reply_markup=None)
    await cmd_start(callback.message, state)

@router.callback_query(F.data == "leaderboard")
async def cb_leaderboard(callback: CallbackQuery):
    with SessionLocal() as db:
        top_users = db.query(User).order_by(desc(User.xp)).limit(10).all()
    
    text = "🏆 **Global Leaderboard**\n\n"
    for i, u in enumerate(top_users, 1):
        medal = "🥇" if i==1 else "🥈" if i==2 else "🥉" if i==3 else "👤"
        text += f"{medal} {i}. {u.full_name} - {u.xp} XP\n"
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=UI.back_home())

# --- SERVER & DEPLOYMENT ---
app = FastAPI()

@app.get("/")
async def health_check():
    return {"status": "ok", "timestamp": datetime.datetime.utcnow().isoformat()}

@app.post("/webhook")
async def bot_webhook(request: Request):
    update = types.Update(**await request.json())
    await dp.feed_update(bot, update)
    return {"ok": True}

async def main():
    global bot, dp
    logging.basicConfig(level=logging.INFO)
    
    bot = Bot(token=Config.BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    
    # Webhook set up for production
    if Config.WEBHOOK_URL:
        await bot.set_webhook(f"{Config.WEBHOOK_URL}/webhook")
        logging.info(f"Webhook set to: {Config.WEBHOOK_URL}")
    else:
        logging.info("Starting Polling mode...")
        await bot.delete_webhook()
        await dp.start_polling(bot)

if __name__ == "__main__":
    # Render requires a non-blocking way to run both FastAPI and Aiogram
    # For a simple main.py deployment:
    import uvicorn
    from threading import Thread
    
    def run_fastapi():
        uvicorn.run(app, host="0.0.0.0", port=Config.PORT)

    Thread(target=run_fastapi, daemon=True).start()
    asyncio.run(main())
