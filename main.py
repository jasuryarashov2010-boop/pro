#!/usr/bin/env python3
# app_combined.py
"""
Combined single-file implementation of math_tekshiruvchi_bot (minimal, runnable).
Features:
- Env-based config (via python-dotenv / os.environ)
- FastAPI health endpoints (Render-ready)
- aiogram bot with polling fallback
- Async SQLAlchemy (SQLite default) with minimal models: users, tests, attempts, messages
- Basic handlers: /start, main menu, tests list, AI (/ai), admin /broadcast (admin only)
- Certificate PDF generator (WeasyPrint optional; falls back to text file)
- Graceful error handling, logging, and safe env usage
Note: Extend modules by splitting into packages for production.
"""
import os
import sys
import asyncio
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

# Load env from .env if present
from dotenv import load_dotenv
load_dotenv()

# ====== CONFIG ======
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    BOT_TOKEN: str = Field(..., env="BOT_TOKEN")
    ADMIN_ID: int = Field(..., env="ADMIN_ID")
    DATABASE_URL: str = Field("sqlite+aiosqlite:///./data.db", env="DATABASE_URL")
    WEBHOOK_MODE: bool = Field(False, env="WEBHOOK_MODE")
    WEBHOOK_URL: Optional[str] = Field(None, env="WEBHOOK_URL")
    WEBHOOK_PATH: str = Field("/webhook", env="WEBHOOK_PATH")
    PORT: int = Field(8000, env="PORT")
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    RATE_LIMIT_PER_MINUTE: int = Field(30, env="RATE_LIMIT_PER_MINUTE")

settings = Settings()

# ====== LOGGING ======
LOG_LEVEL = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("math_tekshiruvchi_bot")

# ====== ASYNC DB SETUP (SQLAlchemy) ======
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, relationship

DATABASE_URL = settings.DATABASE_URL
engine = create_async_engine(DATABASE_URL, future=True, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# Models
class User(Base):
    __tablename__ = "users"
    id = sa.Column(sa.Integer, primary_key=True, index=True)
    tg_id = sa.Column(sa.Integer, unique=True, index=True, nullable=False)
    username = sa.Column(sa.String(64))
    first_name = sa.Column(sa.String(128))
    last_name = sa.Column(sa.String(128))
    created_at = sa.Column(sa.DateTime, default=datetime.utcnow)
    xp = sa.Column(sa.Integer, default=0)
    level = sa.Column(sa.Integer, default=1)
    streak = sa.Column(sa.Integer, default=0)
    badges = sa.Column(sa.JSON, default=[])
    settings = sa.Column(sa.JSON, default={})

class Test(Base):
    __tablename__ = "tests"
    id = sa.Column(sa.Integer, primary_key=True, index=True)
    code = sa.Column(sa.String(64), unique=True, index=True, nullable=False)
    title = sa.Column(sa.String(255), nullable=False)
    description = sa.Column(sa.Text)
    pdf_url = sa.Column(sa.String(1024))
    category = sa.Column(sa.String(128), index=True)
    topic = sa.Column(sa.String(128), index=True)
    difficulty = sa.Column(sa.String(32), index=True)
    created_at = sa.Column(sa.DateTime, default=datetime.utcnow)
    meta = sa.Column(sa.JSON, default={})

class Attempt(Base):
    __tablename__ = "attempts"
    id = sa.Column(sa.Integer, primary_key=True, index=True)
    user_id = sa.Column(sa.Integer, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    test_id = sa.Column(sa.Integer, sa.ForeignKey("tests.id", ondelete="CASCADE"), nullable=False, index=True)
    started_at = sa.Column(sa.DateTime, default=datetime.utcnow)
    finished_at = sa.Column(sa.DateTime)
    duration = sa.Column(sa.Integer)
    answers = sa.Column(sa.JSON, default={})
    score = sa.Column(sa.Float, default=0.0)
    percentage = sa.Column(sa.Float, default=0.0)
    passed = sa.Column(sa.Boolean, default=False)
    user = relationship("User")
    test = relationship("Test")
    __table_args__ = (sa.UniqueConstraint("user_id", "test_id", name="uix_user_test"),)

class MessageLog(Base):
    __tablename__ = "messages"
    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Integer, sa.ForeignKey("users.id", ondelete="SET NULL"))
    text = sa.Column(sa.Text)
    created_at = sa.Column(sa.DateTime, default=datetime.utcnow)
    meta = sa.Column(sa.JSON, default={})

# DB helpers
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_session():
    async with AsyncSessionLocal() as session:
        yield session

# ====== TELEGRAM BOT (aiogram v3) ======
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage

# Bot init
bot = Bot(token=settings.BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Keyboards
def main_menu_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📝 Testlar roʻyxati", callback_data="menu:tests"),
        InlineKeyboardButton("🔍 Test tekshirish", callback_data="menu:check"),
    )
    kb.add(
        InlineKeyboardButton("🤖 AI Ustoz", callback_data="menu:ai"),
        InlineKeyboardButton("📊 Natijalarim", callback_data="menu:results"),
    )
    kb.add(
        InlineKeyboardButton("🧾 Sertifikatlar", callback_data="menu:certs"),
        InlineKeyboardButton("⚙️ Profilim", callback_data="menu:profile"),
    )
    return kb

def back_and_home_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⬅️ Orqaga", callback_data="nav:back"))
    kb.add(InlineKeyboardButton("🏠 Bosh menyu", callback_data="nav:home"))
    return kb

# Simple rate-limiter in memory (per-process)
from collections import defaultdict
_rate_counts = defaultdict(list)  # tg_id -> list of timestamps

def is_rate_limited(tg_id: int) -> bool:
    import time
    window = 60
    limit = settings.RATE_LIMIT_PER_MINUTE
    now = time.time()
    lst = _rate_counts[tg_id]
    # drop old
    _rate_counts[tg_id] = [t for t in lst if now - t < window]
    if len(_rate_counts[tg_id]) >= limit:
        return True
    _rate_counts[tg_id].append(now)
    return False

# AI adapter (fallback)
async def ai_explain_text(prompt: str) -> str:
    # If real OpenAI/Gemini keys are configured, plug their client here.
    # For now, return a simple Uzbek explanatory template.
    await asyncio.sleep(0.2)  # simulate latency
    return (
        f"AI Ustoz:\nSiz so'radingiz: {prompt}\n\n"
        "Qisqacha tushuntirish:\n1) Savol komponentlarini aniqlang.\n2) Asosiy formulani yozing.\n3) Bosqichma-bosqich yechimni bajaring.\n\n"
        "Agar rasm yoki ovoz bo'lsa, iltimos /ai bilan matnni yuboring yoki rasmni ilova qiling."
    )

# Certificate generator
def generate_certificate_bytes(name: str, test_title: str, percent: float) -> bytes:
    # Try to use WeasyPrint if installed; otherwise return plain text bytes as fallback.
    tpl = f"SERTIFIKAT\n\n{name} ga beriladi\nTest: {test_title}\nFoiz: {percent}%\nSana: {datetime.utcnow().date()}\n"
    try:
        from jinja2 import Template
        from weasyprint import HTML
        html = Template("""
        <html><body style="font-family: DejaVu Sans, sans-serif; text-align:center;">
        <h1>SERTIFIKAT</h1>
        <p><strong>{{ name }}</strong> ga beriladi</p>
        <p>Test: {{ test_title }}</p>
        <p>Foiz: {{ percent }}%</p>
        <p>Sana: {{ date }}</p>
        </body></html>
        """).render(name=name, test_title=test_title, percent=percent, date=str(datetime.utcnow().date()))
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
        HTML(string=html).write_pdf(tmp.name)
        tmp.seek(0)
        data = tmp.read()
        tmp.close()
        os.unlink(tmp.name)
        return data
    except Exception as e:
        logger.debug("WeasyPrint not available or failed: %s", e)
        return tpl.encode("utf-8")

# ====== HANDLERS ======

# Startup: create DB
@dp.startup()
async def on_startup():
    logger.info("Bot starting up...")
    await init_db()
    # If WEBHOOK_MODE true, you'd set webhook here (not implemented in single-file polling fallback)
    if settings.WEBHOOK_MODE:
        logger.info("WEBHOOK_MODE is enabled but full webhook wiring is not implemented in this single-file. Use polling or extend.")

# Global error handler
@dp.errors()
async def global_error_handler(update, exception):
    logger.exception("Unhandled exception: %s", exception)
    try:
        if hasattr(update, "message") and update.message:
            await update.message.answer("Kechirasiz, xatolik yuz berdi. Iltimos qayta urinib ko'ring yoki /help buyrug'idan foydalaning.")
    except Exception:
        logger.exception("Failed to notify user about the error")
    return True

# /start
@dp.message(Command(commands=["start", "help"]))
async def cmd_start(message: types.Message):
    if is_rate_limited(message.from_user.id):
        await message.reply("Siz juda tez-tez so'rov yuboryapsiz. Iltimos biroz kuting.")
        return
    # Upsert user into DB
    async with AsyncSessionLocal() as session:
        q = await session.execute(sa.select(User).where(User.tg_id == message.from_user.id))
        user = q.scalars().first()
        if not user:
            user = User(
                tg_id=message.from_user.id,
                username=message.from_user.username,
                first_name=message.from_user.first_name,
                last_name=message.from_user.last_name,
            )
            session.add(user)
            await session.commit()
    text = (
        "Assalomu alaykum! math_tekshiruvchi_bot ga xush kelibsiz.\n\n"
        "🏠 Asosiy menyu uchun pastdagi tugmalardan birini tanlang."
    )
    await message.answer(text, reply_markup=main_menu_kb())

# Main menu callbacks
@dp.callback_query(lambda c: c.data and c.data.startswith("menu:"))
async def menu_callback(query: types.CallbackQuery):
    if is_rate_limited(query.from_user.id):
        await query.answer("Tizim band. Sekinroq urinib ko'ring.", show_alert=True)
        return
    data = query.data.split(":")[1]
    if data == "tests":
        await show_tests(query)
    elif data == "ai":
        await query.message.answer("AI Ustozga savolingizni yuboring. Matn uchun /ai buyruqidan foydalaning yoki ovoz/rasm yuboring.", reply_markup=back_and_home_kb())
        await query.answer()
    elif data == "check":
        await query.message.answer("Test kodini kiriting:", reply_markup=back_and_home_kb())
        await query.answer()
    elif data == "profile":
        await show_profile(query)
    else:
        await query.answer()

# Show tests (paginated simple)
async def show_tests(query: types.CallbackQuery, page: int = 1):
    async with AsyncSessionLocal() as session:
        stmt = sa.select(Test).order_by(Test.created_at.desc()).limit(8).offset((page-1)*8)
        res = await session.execute(stmt)
        items = res.scalars().all()
        if not items:
            await query.message.answer("Testlar topilmadi. Yaqinda qoʻshamiz!", reply_markup=back_and_home_kb())
            await query.answer()
            return
        lines = []
        for t in items:
            lines.append(f"🧾 <b>{t.title}</b>\nKod: <code>{t.code}</code>\nKategoriya: {t.category} • {t.difficulty}")
        text = "\n\n".join(lines)
        await query.message.answer(text, reply_markup=back_and_home_kb())
        await query.answer()

# Show profile
async def show_profile(query: types.CallbackQuery):
    async with AsyncSessionLocal() as session:
        q = await session.execute(sa.select(User).where(User.tg_id == query.from_user.id))
        user = q.scalars().first()
        if not user:
            await query.message.answer("Profil topilmadi. /start buyrug'ini qayta bajaring.", reply_markup=back_and_home_kb())
            await query.answer()
            return
        text = (
            f"👤 Profil\n\nIsm: {user.first_name or ''} {user.last_name or ''}\n"
            f"Foydalanuvchi: @{user.username or '—'}\nID: <code>{user.tg_id}</code>\n\n"
            f"XP: {user.xp} • Level: {user.level} • Streak: {user.streak}\n"
        )
        await query.message.answer(text, reply_markup=back_and_home_kb())
        await query.answer()

# /ai text command
@dp.message(Command(commands=["ai"]))
async def cmd_ai(message: types.Message):
    prompt = message.text or ""
    prompt = prompt.removeprefix("/ai").strip()
    if not prompt:
        await message.reply("Iltimos, savolingizni yozing. Misol: /ai integrallarni tushuntiring")
        return
    await message.reply("AI ustoz javob tayyorlamoqda…")
    try:
        resp = await ai_explain_text(prompt)
        await message.reply(resp, reply_markup=back_and_home_kb())
    except Exception as e:
        logger.exception("AI explain failed: %s", e)
        await message.reply("Kechirasiz, AI xizmatiga ulanish imkoni yo'q. Iltimos keyinroq urinib ko'ring.", reply_markup=back_and_home_kb())

# Voice & photo handlers (stubs)
@dp.message(lambda m: m.content_type in ("voice", "audio"))
async def voice_handler(message: types.Message):
    await message.reply("Ovoz qabul qilindi. Matnga aylantirish va tahlil qilinmoqda…")
    # TODO: download file, convert, call speech-to-text, then ai_explain_text
    await message.reply("Tahlil yakunlandi. (Bu stub)")

@dp.message(lambda m: m.content_type in ("photo", "document"))
async def photo_handler(message: types.Message):
    await message.reply("Rasm qabul qilindi. Tahlil qilinmoqda…")
    # TODO: OCR / handwriting recognition -> ai_explain_text
    await message.reply("Tahlil yakunlandi. (Bu stub)")

# Admin broadcast (admin only)
@dp.message(Command(commands=["broadcast"]))
async def cmd_broadcast(message: types.Message):
    if str(message.from_user.id) != str(settings.ADMIN_ID):
        await message.reply("Sizda bu buyruq uchun ruxsat yo'q.")
        return
    text = message.get_args()
    if not text:
        await message.reply("Iltimos, yuboriladigan xabar matnini kiriting: /broadcast Xabar matni")
        return
    await message.reply("Xabar qatorga qoʻyildi. Foydalanuvchilarga yuborish fon rejimida amalga oshiriladi.")
    # For demo: broadcast to all users sequentially (small DB). In prod: push to queue.
    async with AsyncSessionLocal() as session:
        res = await session.execute(sa.select(User.tg_id))
        tg_ids = [r[0] for r in res.all()]
    count = 0
    for uid in tg_ids:
        try:
            await bot.send_message(uid, f"📢 Administrator xabari:\n\n{text}")
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            logger.exception("Failed to send broadcast to %s", uid)
    await message.reply(f"Xabar yuborildi: {count} foydalanuvchiga.")

# Simple command to request certificate (demo)
@dp.message(Command(commands=["certificate"]))
async def cmd_certificate(message: types.Message):
    # args: name|test|percent
    args = message.get_args()
    parts = [p.strip() for p in args.split("|")] if args else []
    if len(parts) < 3:
        await message.reply("Sertifikat generatsiyasi uchun format: /certificate Ism | Test nomi | Foiz\nMisol: /certificate Ali | Algebra 1 | 92")
        return
    name, test_title, percent = parts[0], parts[1], parts[2]
    try:
        percent_f = float(percent)
    except:
        percent_f = 0.0
    await message.reply("Sertifikat yaratilmoqda…")
    pdf_bytes = generate_certificate_bytes(name, test_title, percent_f)
    try:
        await bot.send_document(message.from_user.id, (f"certificate_{name}.pdf", pdf_bytes))
    except Exception:
        # fallback: send as text file
        await bot.send_document(message.from_user.id, (f"certificate_{name}.txt", pdf_bytes))

# Navigation callbacks
@dp.callback_query(lambda c: c.data and c.data.startswith("nav:"))
async def nav_callback(query: types.CallbackQuery):
    cmd = query.data.split(":")[1]
    if cmd == "back":
        await query.message.answer("⬅️ Orqaga", reply_markup=main_menu_kb())
    elif cmd == "home":
        await query.message.answer("🏠 Bosh menyu", reply_markup=main_menu_kb())
    await query.answer()

# Catch-all text logs
@dp.message()
async def all_messages(message: types.Message):
    # store message short log
    try:
        async with AsyncSessionLocal() as session:
            mlog = MessageLog(user_id=message.from_user.id, text=(message.text or "")[:200])
            session.add(mlog)
            await session.commit()
    except Exception:
        logger.exception("Failed to log message")
    # helpful hint
    if not message.text or message.text.startswith("/"):
        return
    await message.answer("Men AI Ustoz va testlarni boshqaraman. /help yoki bosh menyudan tanlang.", reply_markup=main_menu_kb())

# ====== FASTAPI APP (health + optional webhook receiver) ======
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # ensure DB exists
    await init_db()
    logger.info("HTTP app startup complete")

@app.get("/health")
async def health():
    return JSONResponse({"status": "ok", "app": "math_tekshiruvchi_bot"})

@app.post(settings.WEBHOOK_PATH)
async def webhook_receiver(request: Request):
    # For production: integrate aiogram webhook processing here; this stub accepts updates
    try:
        data = await request.json()
        logger.debug("Webhook update received: keys=%s", list(data.keys()))
    except Exception as e:
        logger.exception("Invalid webhook payload: %s", e)
    return JSONResponse({"status": "received"})

# ====== RUN LOGIC ======
async def start_polling():
    logger.info("Starting aiogram polling...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

def run():
    # If running as main, choose mode: polling (default) or start only HTTP app (via uvicorn)
    mode = os.environ.get("RUN_MODE", "polling")  # "polling" or "http"
    if mode == "http":
        # run only HTTP (for Render web service). Use: uvicorn app_combined:app --host 0.0.0.0 --port $PORT
        logger.info("RUN_MODE=http — serve FastAPI (use uvicorn to run).")
    else:
        # run polling in current process
        try:
            asyncio.run(start_polling())
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down...")

if __name__ == "__main__":
    run()
