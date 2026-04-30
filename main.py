
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import tempfile
import textwrap
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse, FileResponse
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    and_,
    desc,
    func,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncAttrs, async_sessionmaker, create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# Optional AI / PDF libraries
with contextlib.suppress(Exception):
    from openai import AsyncOpenAI  # type: ignore
with contextlib.suppress(Exception):
    import google.generativeai as genai  # type: ignore
REPORTLAB_AVAILABLE = False
with contextlib.suppress(Exception):
    from reportlab.lib.pagesizes import A4  # type: ignore
    from reportlab.pdfgen import canvas  # type: ignore
    REPORTLAB_AVAILABLE = True

APP_DIR = Path(__file__).resolve().parent
TZ = timezone.utc

# ----------------------------- Env & Config -----------------------------
def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def env_bool(name: str, default: str = "false") -> bool:
    return env(name, default).lower() in {"1", "true", "yes", "on"}

def env_int(name: str, default: str = "0") -> int:
    try:
        return int(env(name, default))
    except Exception:
        return int(default)

BOT_TOKEN = env("BOT_TOKEN")
ADMIN_ID = int(env("ADMIN_ID", "0") or "0")
WEBHOOK_URL = env("WEBHOOK_URL")
WEBHOOK_SECRET = env("WEBHOOK_SECRET", "change_me")
DATABASE_URL_RAW = env("DATABASE_URL", "sqlite+aiosqlite:///./math_tekshiruvchi_bot.db")
REDIS_URL = env("REDIS_URL")
DASHBOARD_SECRET = env("DASHBOARD_SECRET", "dashboard_secret")
APP_ROLE = env("APP_ROLE", "webhook").lower()
AI_ENABLED = env_bool("AI_ENABLED", "true")
MIN_PASS_PERCENT = max(1, min(100, env_int("MIN_PASS_PERCENT", "60")))
GEMINI_API_KEY = env("GEMINI_API_KEY")
OPENAI_API_KEY = env("OPENAI_API_KEY")
PORT = env_int("PORT", "8000")
PYTHON_VERSION = env("PYTHON_VERSION", "")

BOT_NAME = "math_tekshiruvchi_bot"

def normalize_db_url(url: str) -> str:
    if not url:
        return "sqlite+aiosqlite:///./math_tekshiruvchi_bot.db"
    if url.startswith("postgres://"):
        return "postgresql+asyncpg://" + url[len("postgres://"):]
    if url.startswith("sqlite:///"):
        return "sqlite+aiosqlite:///" + url[len("sqlite:///"):]
    if url.startswith("sqlite://"):
        return "sqlite+aiosqlite://" + url[len("sqlite://"):]
    return url

DATABASE_URL = normalize_db_url(DATABASE_URL_RAW)
IS_SQLITE = DATABASE_URL.startswith("sqlite")

# ----------------------------- Logging -----------------------------
logger = logging.getLogger(BOT_NAME)
logging.basicConfig(
    level=getattr(logging, env("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
for noisy in ("httpx", "sqlalchemy.engine", "sqlalchemy.pool"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

def mask_secret(value: str, visible: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= visible * 2:
        return "*" * len(value)
    return value[:visible] + "*" * (len(value) - visible * 2) + value[-visible:]

# ----------------------------- Database -----------------------------
class Base(AsyncAttrs, DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True, nullable=False)
    first_name: Mapped[str] = mapped_column(String(255), default="")
    username: Mapped[str] = mapped_column(String(255), default="")
    language_code: Mapped[str] = mapped_column(String(32), default="uz")
    level: Mapped[int] = mapped_column(Integer, default=1)
    xp: Mapped[int] = mapped_column(Integer, default=0)
    streak: Mapped[int] = mapped_column(Integer, default=0)
    best_percent: Mapped[float] = mapped_column(Float, default=0.0)
    avg_percent: Mapped[float] = mapped_column(Float, default=0.0)
    total_tests: Mapped[int] = mapped_column(Integer, default=0)
    badge: Mapped[str] = mapped_column(String(64), default="Newbie")
    state: Mapped[str] = mapped_column(String(64), default="")
    pending_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class Test(Base):
    __tablename__ = "tests"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(128), default="Umumiy")
    topic: Mapped[str] = mapped_column(String(128), default="Matematika")
    difficulty: Mapped[str] = mapped_column(String(32), default="Oson")
    description: Mapped[str] = mapped_column(Text, default="")
    pdf_url: Mapped[str] = mapped_column(Text, default="")
    answer_key: Mapped[dict] = mapped_column(JSON, default=dict)
    total_questions: Mapped[int] = mapped_column(Integer, default=0)
    pass_percent: Mapped[int] = mapped_column(Integer, default=MIN_PASS_PERCENT)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class Result(Base):
    __tablename__ = "results"
    __table_args__ = (
        UniqueConstraint("user_id", "test_id", name="uq_one_user_one_test"),
        Index("ix_results_user_created", "user_id", "created_at"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    test_id: Mapped[int] = mapped_column(ForeignKey("tests.id", ondelete="CASCADE"), nullable=False)
    test_code: Mapped[str] = mapped_column(String(64), index=True)
    total: Mapped[int] = mapped_column(Integer, default=0)
    correct: Mapped[int] = mapped_column(Integer, default=0)
    wrong: Mapped[int] = mapped_column(Integer, default=0)
    skipped: Mapped[int] = mapped_column(Integer, default=0)
    percent: Mapped[float] = mapped_column(Float, default=0.0)
    score: Mapped[float] = mapped_column(Float, default=0.0)
    time_spent: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="pending")
    answers_json: Mapped[dict] = mapped_column(JSON, default=dict)
    ai_analysis: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))
    reviewed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

class Attempt(Base):
    __tablename__ = "attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    test_code: Mapped[str] = mapped_column(String(64), index=True)
    raw_input: Mapped[str] = mapped_column(Text, default="")
    duplicate: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class MessageLog(Base):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    direction: Mapped[str] = mapped_column(String(16), default="in")
    text: Mapped[str] = mapped_column(Text, default="")
    media_type: Mapped[str] = mapped_column(String(32), default="text")
    telegram_message_id: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class AdminLog(Base):
    __tablename__ = "admin_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    actor_id: Mapped[int] = mapped_column(BigInteger, default=0)
    action: Mapped[str] = mapped_column(String(128), default="")
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class AnalyticsEvent(Base):
    __tablename__ = "analytics"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    meta: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class Certificate(Base):
    __tablename__ = "certificates"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    result_id: Mapped[int] = mapped_column(Integer, index=True)
    serial: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    file_path: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class Badge(Base):
    __tablename__ = "badges"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    name: Mapped[str] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class Streak(Base):
    __tablename__ = "streaks"
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    current: Mapped[int] = mapped_column(Integer, default=0)
    best: Mapped[int] = mapped_column(Integer, default=0)
    last_active: Mapped[date] = mapped_column(Date, nullable=True)

class Favorite(Base):
    __tablename__ = "favorites"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, index=True)
    test_id: Mapped[int] = mapped_column(Integer, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))
    __table_args__ = (UniqueConstraint("user_id", "test_id", name="uq_favorite_user_test"),)

class Notification(Base):
    __tablename__ = "notifications"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(255), default="")
    body: Mapped[str] = mapped_column(Text, default="")
    is_global: Mapped[bool] = mapped_column(Boolean, default=False)
    is_read: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

class StateStore(Base):
    __tablename__ = "state_store"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    state: Mapped[str] = mapped_column(String(64), default="")
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(TZ))

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
    pool_recycle=1800,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ----------------------------- Cache / Redis -----------------------------
class MemoryCache:
    def __init__(self):
        self.store: Dict[str, Tuple[float, Any]] = {}
        self.rate: Dict[int, deque] = defaultdict(deque)

    async def get(self, key: str) -> Any:
        item = self.store.get(key)
        if not item:
            return None
        exp, val = item
        if exp and exp < time.time():
            self.store.pop(key, None)
            return None
        return val

    async def set(self, key: str, value: Any, ttl: int = 300):
        self.store[key] = (time.time() + ttl, value)

    async def delete(self, key: str):
        self.store.pop(key, None)

    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None

MEM = MemoryCache()

try:
    import redis.asyncio as redis  # type: ignore
    REDIS = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
except Exception:
    REDIS = None

async def cache_get(key: str) -> Any:
    if REDIS:
        try:
            raw = await REDIS.get(key)
            return json.loads(raw) if raw else None
        except Exception:
            return await MEM.get(key)
    return await MEM.get(key)

async def cache_set(key: str, value: Any, ttl: int = 300):
    if REDIS:
        try:
            await REDIS.set(key, json.dumps(value, default=str), ex=ttl)
            return
        except Exception:
            pass
    await MEM.set(key, value, ttl)

async def cache_delete(key: str):
    if REDIS:
        with contextlib.suppress(Exception):
            await REDIS.delete(key)
    await MEM.delete(key)

# ----------------------------- Telegram Helpers -----------------------------
HTTP = httpx.AsyncClient(timeout=30.0)

def api_url(method: str) -> str:
    if not BOT_TOKEN:
        return f"https://api.telegram.org/bot{method}"
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"

async def tg(method: str, payload: dict) -> dict:
    if not BOT_TOKEN:
        return {"ok": False, "description": "BOT_TOKEN missing"}
    try:
        r = await HTTP.post(api_url(method), json=payload)
        data = r.json()
        if not data.get("ok"):
            logger.warning("Telegram API error on %s: %s", method, data)
        return data
    except Exception as e:
        logger.exception("Telegram request failed: %s", e)
        return {"ok": False, "description": str(e)}

async def send_message(chat_id: int, text: str, reply_markup: Optional[dict] = None, parse_mode: str = "HTML", disable_web_page_preview: bool = True) -> dict:
    payload = {
        "chat_id": chat_id,
        "text": text[:4096],
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await tg("sendMessage", payload)

async def edit_message(chat_id: int, message_id: int, text: str, reply_markup: Optional[dict] = None, parse_mode: str = "HTML") -> dict:
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text[:4096], "parse_mode": parse_mode}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await tg("editMessageText", payload)

async def answer_callback(callback_query_id: str, text: str = "", show_alert: bool = False) -> dict:
    payload = {"callback_query_id": callback_query_id, "text": text[:200], "show_alert": show_alert}
    return await tg("answerCallbackQuery", payload)

async def send_document(chat_id: int, file_path: str, caption: str = "") -> dict:
    if not Path(file_path).exists():
        return await send_message(chat_id, caption or "Fayl topilmadi.")
    try:
        with open(file_path, "rb") as f:
            files = {"document": (Path(file_path).name, f)}
            data = {"chat_id": str(chat_id), "caption": caption[:1024]}
            r = await HTTP.post(api_url("sendDocument"), data=data, files=files)
            return r.json()
    except Exception as e:
        logger.exception("send_document failed: %s", e)
        return {"ok": False, "description": str(e)}

async def send_photo(chat_id: int, photo_url: str, caption: str = "", reply_markup: Optional[dict] = None) -> dict:
    payload = {"chat_id": chat_id, "photo": photo_url, "caption": caption[:1024], "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await tg("sendPhoto", payload)

# ----------------------------- Keyboards -----------------------------
def kb(rows: List[List[Tuple[str, str]]]) -> dict:
    return {
        "inline_keyboard": [
            [{"text": txt, "callback_data": data} for txt, data in row]
            for row in rows
        ]
    }

def main_menu_keyboard() -> dict:
    return kb([
        [("📝 Testlar ro‘yxati", "menu:tests"), ("✅ Test tekshirish", "menu:check")],
        [("🤖 AI ustoz", "menu:ai"), ("📊 Natijalarim", "menu:results")],
        [("👤 Profilim", "menu:profile"), ("⭐ Favorites", "menu:favorites")],
        [("🏆 Reyting", "menu:leaderboard"), ("🎯 Daily challenge", "menu:daily")],
        [("📩 Bog‘lanish", "menu:contact"), ("ℹ️ Yordam", "menu:help")],
        [("🏠 Bosh menyu", "menu:home")],
    ])

def back_home_keyboard(back: str = "menu:home") -> dict:
    return kb([
        [("⬅️ Orqaga", back), ("🏠 Bosh menyu", "menu:home")]
    ])

def tests_nav_keyboard(page: int, total_pages: int, q: str = "", category: str = "", difficulty: str = "") -> dict:
    row = []
    if page > 1:
        row.append(("⬅️ Oldingi", f"tests:list:{page-1}:{category}:{difficulty}:{q[:24]}"))
    if page < total_pages:
        row.append(("Keyingi ➡️", f"tests:list:{page+1}:{category}:{difficulty}:{q[:24]}"))
    rows = [
        row if row else [("🔄 Yangilash", f"tests:list:{page}:{category}:{difficulty}:{q[:24]}")],
        [("🔎 Qidiruv", "tests:search"), ("🎚 Filter", "tests:filter")],
        [("🏠 Bosh menyu", "menu:home")],
    ]
    return kb(rows)

def test_preview_keyboard(code: str, favorite: bool = False) -> dict:
    fav_text = "⭐ Favoritdan olish" if favorite else "⭐ Favoritga qo‘shish"
    fav_action = f"fav:toggle:{code}"
    return kb([
        [("▶️ Tekshirish", f"test:check:{code}"), ("📄 PDF", f"test:pdf:{code}")],
        [(fav_text, fav_action), ("🤖 AI tahlil", f"test:ai:{code}")],
        [("⬅️ Orqaga", "menu:tests"), ("🏠 Bosh menyu", "menu:home")],
    ])

def results_keyboard(result_id: int, test_code: str) -> dict:
    return kb([
        [("🔍 Review mode", f"result:review:{result_id}"), ("🤖 AI tahlil", f"result:ai:{result_id}")],
        [("🏅 Sertifikat", f"result:cert:{result_id}"), ("📌 Keyingi test", "menu:tests")],
        [("🏠 Bosh menyu", "menu:home")],
    ])

def admin_keyboard() -> dict:
    return kb([
        [("➕ Test qo‘shish", "admin:add_test"), ("🗑 Test o‘chirish", "admin:del_test")],
        [("📣 Ommaviy xabar", "admin:broadcast"), ("📈 Analytics", "admin:analytics")],
        [("👥 Foydalanuvchilar", "admin:users"), ("🏅 Top userlar", "admin:top")],
        [("🧾 Loglar", "admin:logs"), ("🏠 Bosh menyu", "menu:home")],
    ])

# ----------------------------- Utils -----------------------------
def now() -> datetime:
    return datetime.now(TZ)

def pct(correct: int, total: int) -> float:
    return round((correct / total * 100.0) if total else 0.0, 2)

def safe_text(v: Any, limit: int = 120) -> str:
    s = str(v or "")
    s = re.sub(r"[\x00-\x1f\x7f]", " ", s)
    return s[:limit]

def user_full_name(u: Any) -> str:
    parts = [safe_text(getattr(u, "first_name", ""), 32), safe_text(getattr(u, "last_name", ""), 32)]
    return " ".join([p for p in parts if p]).strip() or "Foydalanuvchi"

def level_from_xp(xp: int) -> int:
    return max(1, xp // 100 + 1)

def badge_from_percent(p: float) -> str:
    if p >= 95:
        return "Legend"
    if p >= 85:
        return "Pro"
    if p >= 70:
        return "Skilled"
    if p >= 50:
        return "Rising"
    return "Newbie"

def progress_bar(value: float, total: float = 100.0, length: int = 10) -> str:
    if total <= 0:
        return "▫️" * length
    filled = int(round((value / total) * length))
    return "█" * filled + "░" * (length - filled)

def clean_command(text: str) -> str:
    return (text or "").strip().split()[0].lower()

def parse_int_or_none(v: str) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

# ----------------------------- DB Helpers -----------------------------
async def _table_exists(conn, table_name: str) -> bool:
    q = await conn.execute(
        text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = :table_name
            LIMIT 1
        """),
        {"table_name": table_name},
    )
    return q.first() is not None


async def _get_table_columns(conn, table_name: str) -> list[str]:
    q = await conn.execute(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :table_name
            ORDER BY ordinal_position
        """),
        {"table_name": table_name},
    )
    return [row[0] for row in q.fetchall()]

async def _get_column_type(conn, table_name: str, column_name: str) -> str:
    q = await conn.execute(
        text("""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :table_name
              AND column_name = :column_name
            LIMIT 1
        """),
        {"table_name": table_name, "column_name": column_name},
    )
    row = q.first()
    return str(row[0]).lower() if row else ""

async def _table_exists(conn, table_name: str) -> bool:
    q = await conn.execute(
        text("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = :table_name
            LIMIT 1
        """),
        {"table_name": table_name},
    )
    return q.first() is not None

async def _safe_add_column(conn, table_name: str, column_name: str, ddl: str) -> None:
    columns = set(await _get_table_columns(conn, table_name))
    if column_name not in columns:
        await conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {ddl}'))

async def _repair_tests_table(conn) -> None:
    """Repair the tests table in place so old Render databases do not crash startup.

    This function avoids rename/copy migrations because Render startup can be
    interrupted by legacy schema mismatches. Instead, it adds missing columns,
    upgrades answer_key to JSON when needed, and backfills ids when absent.
    """
    if not await _table_exists(conn, "tests"):
        await conn.run_sync(Base.metadata.create_all)
        return

    # Add missing columns safely.
    await _safe_add_column(conn, "tests", "id", "BIGSERIAL")
    await _safe_add_column(conn, "tests", "code", "VARCHAR(64)")
    await _safe_add_column(conn, "tests", "title", "VARCHAR(255)")
    await _safe_add_column(conn, "tests", "category", "VARCHAR(128) DEFAULT 'Umumiy'")
    await _safe_add_column(conn, "tests", "topic", "VARCHAR(128) DEFAULT 'Matematika'")
    await _safe_add_column(conn, "tests", "difficulty", "VARCHAR(32) DEFAULT 'Oson'")
    await _safe_add_column(conn, "tests", "description", "TEXT DEFAULT ''")
    await _safe_add_column(conn, "tests", "pdf_url", "TEXT DEFAULT ''")
    await _safe_add_column(conn, "tests", "answer_key", "JSON DEFAULT '{}'::json")
    await _safe_add_column(conn, "tests", "total_questions", "INTEGER DEFAULT 0")
    await _safe_add_column(conn, "tests", "pass_percent", "INTEGER DEFAULT 60")
    await _safe_add_column(conn, "tests", "active", "BOOLEAN DEFAULT TRUE")
    await _safe_add_column(conn, "tests", "created_at", "TIMESTAMP WITH TIME ZONE DEFAULT NOW()")

    # Backfill id if it exists but is still NULL for legacy rows.
    columns = set(await _get_table_columns(conn, "tests"))
    if "id" in columns:
        try:
            await conn.execute(text("UPDATE tests SET id = nextval(pg_get_serial_sequence('tests', 'id')) WHERE id IS NULL"))
        except Exception:
            # If the sequence cannot be resolved, we still keep startup alive.
            pass

        # Try to set PK only if the table does not already have one.
        try:
            q = await conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM information_schema.table_constraints
                    WHERE table_schema = current_schema()
                      AND table_name = 'tests'
                      AND constraint_type = 'PRIMARY KEY'
                """)
            )
            has_pk = (q.scalar_one() or 0) > 0
            if not has_pk:
                await conn.execute(text("ALTER TABLE tests ADD PRIMARY KEY (id)"))
        except Exception:
            pass

    # Upgrade answer_key type when the old table stored it as text.
    try:
        current_type = await _get_column_type(conn, "tests", "answer_key")
        if current_type in {"character varying", "text", "varchar", "jsonb"}:
            if current_type in {"character varying", "text", "varchar"}:
                await conn.execute(text(
                    """
                    ALTER TABLE tests
                    ALTER COLUMN answer_key TYPE JSON
                    USING CASE
                        WHEN answer_key IS NULL OR answer_key = '' THEN '{}'::json
                        ELSE answer_key::json
                    END
                    """
                ))
    except Exception:
        # If the existing data is malformed, do not kill startup.
        pass

    # Enforce sane defaults on frequently used columns.
    for ddl in [
        "ALTER TABLE tests ALTER COLUMN code SET DEFAULT ''",
        "ALTER TABLE tests ALTER COLUMN title SET DEFAULT ''",
        "ALTER TABLE tests ALTER COLUMN category SET DEFAULT 'Umumiy'",
        "ALTER TABLE tests ALTER COLUMN topic SET DEFAULT 'Matematika'",
        "ALTER TABLE tests ALTER COLUMN difficulty SET DEFAULT 'Oson'",
        "ALTER TABLE tests ALTER COLUMN description SET DEFAULT ''",
        "ALTER TABLE tests ALTER COLUMN pdf_url SET DEFAULT ''",
        "ALTER TABLE tests ALTER COLUMN total_questions SET DEFAULT 0",
        "ALTER TABLE tests ALTER COLUMN pass_percent SET DEFAULT 60",
        "ALTER TABLE tests ALTER COLUMN active SET DEFAULT TRUE",
        "ALTER TABLE tests ALTER COLUMN created_at SET DEFAULT NOW()",
    ]:
        try:
            await conn.execute(text(ddl))
        except Exception:
            pass

async def init_db():
    async with engine.begin() as conn:
        try:
            await conn.run_sync(Base.metadata.create_all)
            await _repair_tests_table(conn)
            await conn.run_sync(Base.metadata.create_all)
        except Exception as e:
            logger.exception("DB init/migration failed but app will keep running: %s", e)
async def session_scope():
    async with SessionLocal() as session:
        yield session

async def get_or_create_user(session: AsyncSession, tg_user: dict) -> User:
    tg_id = tg_user["id"]
    q = await session.execute(select(User).where(User.tg_id == tg_id))
    user = q.scalar_one_or_none()
    if user:
        user.first_name = tg_user.get("first_name", user.first_name or "")
        user.username = tg_user.get("username", user.username or "")
        user.language_code = tg_user.get("language_code", user.language_code or "uz")
        user.last_seen = now()
        user.updated_at = now()
        return user
    user = User(
        tg_id=tg_id,
        first_name=tg_user.get("first_name", ""),
        username=tg_user.get("username", ""),
        language_code=tg_user.get("language_code", "uz"),
        last_seen=now(),
    )
    session.add(user)
    await session.flush()
    await ensure_streak_row(session, user.tg_id)
    return user

async def ensure_streak_row(session: AsyncSession, tg_id: int) -> Streak:
    q = await session.execute(select(Streak).where(Streak.user_id == tg_id))
    row = q.scalar_one_or_none()
    if row:
        return row
    row = Streak(user_id=tg_id, current=0, best=0, last_active=None)
    session.add(row)
    await session.flush()
    return row

async def get_state(session: AsyncSession, tg_id: int) -> Tuple[str, dict]:
    q = await session.execute(select(StateStore).where(StateStore.user_id == tg_id))
    row = q.scalar_one_or_none()
    if not row:
        return "", {}
    return row.state or "", row.payload or {}

async def set_state(session: AsyncSession, tg_id: int, state: str, payload: Optional[dict] = None):
    q = await session.execute(select(StateStore).where(StateStore.user_id == tg_id))
    row = q.scalar_one_or_none()
    if not row:
        row = StateStore(user_id=tg_id, state=state, payload=payload or {})
        session.add(row)
    else:
        row.state = state
        row.payload = payload or {}
        row.updated_at = now()

async def clear_state(session: AsyncSession, tg_id: int):
    q = await session.execute(select(StateStore).where(StateStore.user_id == tg_id))
    row = q.scalar_one_or_none()
    if row:
        row.state = ""
        row.payload = {}
        row.updated_at = now()

async def add_log(session: AsyncSession, actor_id: int, action: str, payload: dict):
    session.add(AdminLog(actor_id=actor_id, action=action, payload=payload))

async def log_analytics(session: AsyncSession, name: str, meta: dict):
    session.add(AnalyticsEvent(name=name, meta=meta))

async def add_message_log(session: AsyncSession, user_id: int, text: str, direction: str = "in", media_type: str = "text", telegram_message_id: int = 0):
    session.add(MessageLog(user_id=user_id, text=text, direction=direction, media_type=media_type, telegram_message_id=telegram_message_id))

# ----------------------------- Seed Demo Tests -----------------------------
DEMO_TESTS = [
    {
        "code": "MATH-101",
        "title": "Asosiy arifmetika",
        "category": "Boshlang‘ich",
        "topic": "Sonlar",
        "difficulty": "Oson",
        "description": "Qo‘shish, ayirish, ko‘paytirish va bo‘lish bo‘yicha tezkor test.",
        "pdf_url": "",
        "answer_key": {"1": "B", "2": "C", "3": "A", "4": "D"},
        "total_questions": 4,
    },
    {
        "code": "MATH-201",
        "title": "Kasrlar va nisbatlar",
        "category": "O‘rta",
        "topic": "Kasrlar",
        "difficulty": "O‘rtacha",
        "description": "Kasrlar ustida amallar va solishtirish.",
        "pdf_url": "",
        "answer_key": {"1": "A", "2": "D", "3": "B", "4": "C"},
        "total_questions": 4,
    },
    {
        "code": "MATH-301",
        "title": "Algebra va tenglamalar",
        "category": "Yuqori",
        "topic": "Algebra",
        "difficulty": "Qiyin",
        "description": "Tenglama yechish va mantiqiy tahlil.",
        "pdf_url": "",
        "answer_key": {"1": "C", "2": "B", "3": "D", "4": "A"},
        "total_questions": 4,
    },
]

async def seed_tests(session: AsyncSession):
    # Existence check uses raw SQL so it never references a specific column shape.
    try:
        q = await session.execute(text("SELECT 1 FROM tests LIMIT 1"))
        if q.first() is not None:
            return
    except Exception as e:
        logger.warning("Seed check skipped due to tests table issue: %s", e)
        return

    for t in DEMO_TESTS:
        try:
            session.add(Test(**t, pass_percent=MIN_PASS_PERCENT, active=True))
            await session.flush()
        except Exception as e:
            await session.rollback()
            logger.warning("Skipping demo seed row %s due to schema issue: %s", t.get("code"), e)
            return

# ----------------------------- Bot Content -----------------------------
def welcome_text(user: Optional[User] = None) -> str:
    name = safe_text(user.first_name if user else "", 24)
    return (
        f"👋 <b>Xush kelibsiz, {name or 'do‘stim'}!</b>\n\n"
        f"Bu platforma testlar, natijalar, AI yordam va profil statistikasini bir joyga jamlaydi.\n\n"
        f"🎯 Tayyor bo‘lsangiz, boshlaymiz."
    )

def main_menu_text(user: Optional[User] = None) -> str:
    xp = user.xp if user else 0
    level = user.level if user else 1
    badge = user.badge if user else "Newbie"
    return (
        f"🏠 <b>Bosh menyu</b>\n"
        f"• Level: <b>{level}</b>\n"
        f"• XP: <b>{xp}</b>\n"
        f"• Badge: <b>{badge}</b>\n\n"
        f"Kerakli bo‘limni tanlang."
    )

def test_preview_text(t: Test) -> str:
    return (
        f"🧪 <b>{t.code}</b>\n"
        f"📌 {t.title}\n"
        f"🏷 Kategoriya: <b>{t.category}</b>\n"
        f"📚 Mavzu: <b>{t.topic}</b>\n"
        f"⚙ Qiyinlik: <b>{t.difficulty}</b>\n"
        f"🧾 Savollar: <b>{t.total_questions}</b>\n"
        f"✅ O‘tish balli: <b>{t.pass_percent}%</b>\n\n"
        f"{safe_text(t.description, 450)}"
    )

def result_card(r: Result, t: Test) -> str:
    return (
        f"📊 <b>Natija kartasi</b>\n"
        f"🧪 Test: <b>{t.code}</b> — {t.title}\n"
        f"✅ To‘g‘ri: <b>{r.correct}</b>\n"
        f"❌ Noto‘g‘ri: <b>{r.wrong}</b>\n"
        f"⏱ Vaqt: <b>{r.time_spent}s</b>\n"
        f"🎯 Foiz: <b>{r.percent}%</b>\n"
        f"🏁 Status: <b>{r.status}</b>\n"
        f"💯 Ball: <b>{r.score}</b>\n\n"
        f"{progress_bar(r.percent)}"
    )

def profile_text(user: User, streak: Optional[Streak], badges: List[Badge]) -> str:
    full = user_full_name(user)
    uname = f"@{user.username}" if user.username else "—"
    st = streak.current if streak else 0
    best = streak.best if streak else 0
    badge_line = ", ".join([b.name for b in badges[:5]]) or user.badge
    return (
        f"👤 <b>Profilim</b>\n"
        f"Ism: <b>{full}</b>\n"
        f"Username: <b>{uname}</b>\n"
        f"User ID: <code>{user.tg_id}</code>\n\n"
        f"📈 Level: <b>{user.level}</b>\n"
        f"✨ XP: <b>{user.xp}</b>\n"
        f"🔥 Streak: <b>{st}</b> (best: {best})\n"
        f"🏅 Badge: <b>{user.badge}</b>\n"
        f"🎖 Yutuqlar: <b>{badge_line}</b>\n\n"
        f"🧠 O‘rtacha foiz: <b>{round(user.avg_percent, 2)}%</b>\n"
        f"🏆 Eng yaxshi natija: <b>{round(user.best_percent, 2)}%</b>\n"
        f"🧪 Tekshirilgan testlar: <b>{user.total_tests}</b>"
    )

def help_text() -> str:
    return (
        "ℹ️ <b>Yordam</b>\n\n"
        "• /tests — testlar ro‘yxati\n"
        "• /check — test kodini tekshirish\n"
        "• /ai — AI ustoz\n"
        "• /results — natijalar\n"
        "• /profile — profil\n"
        "• /contact — bog‘lanish\n"
    )

def admin_help_text() -> str:
    return (
        "🛠 <b>Admin panel</b>\n\n"
        "Buyruqlar:\n"
        "• /admin\n"
        "• /addtest\n"
        "• /deltest <code>\n"
        "• /broadcast\n"
        "• /reply <user_id> <text>\n"
    )

# ----------------------------- AI Layer -----------------------------
async def ai_fallback(prompt: str) -> str:
    prompt = safe_text(prompt, 500)
    return (
        "🤖 <b>AI tahlil (fallback)</b>\n\n"
        "Men hozir avtomatik modelga ulanmadim, lekin savolni tushuntirishga urinaman.\n\n"
        f"Siz yozgan matn: <i>{prompt}</i>\n\n"
        "Boshlash uchun: ma’lumotni qismlarga ajrating, berilganlar va so‘ralgan narsani yozing, so‘ng formula yoki mantiqiy qadamlar bilan yeching."
    )

async def call_openai(prompt: str) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    try:
        client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        resp = await client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": "Siz o‘zbek tilida tushuntiradigan matematik AI ustozsiz. Javoblar sodda, bosqichma-bosqich va aniq bo‘lsin."},
                {"role": "user", "content": prompt},
            ],
        )
        text_out = getattr(resp, "output_text", None)
        return text_out or None
    except Exception as e:
        logger.warning("OpenAI call failed: %s", e)
        return None

async def call_gemini(prompt: str) -> Optional[str]:
    if not GEMINI_API_KEY:
        return None
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-1.5-flash")
        resp = await asyncio.to_thread(model.generate_content, prompt)
        text_out = getattr(resp, "text", None)
        return text_out or None
    except Exception as e:
        logger.warning("Gemini call failed: %s", e)
        return None

async def ai_answer(prompt: str) -> str:
    if not AI_ENABLED:
        return await ai_fallback(prompt)
    answer = await call_openai(prompt)
    if not answer:
        answer = await call_gemini(prompt)
    return answer or await ai_fallback(prompt)

# ----------------------------- Business Logic -----------------------------
async def ensure_metrics(session: AsyncSession, user: User):
    user.level = level_from_xp(user.xp)
    user.badge = badge_from_percent(user.best_percent or 0)
    user.updated_at = now()
    q = await session.execute(select(Result.percent).where(Result.user_id == user.id))
    vals = [float(row[0]) for row in q.all()]
    if vals:
        user.avg_percent = round(sum(vals) / len(vals), 2)
        user.best_percent = round(max(max(vals), user.best_percent or 0), 2)
        user.total_tests = len(vals)

async def grant_xp(session: AsyncSession, user: User, percent: float, time_spent: int):
    base = 20
    bonus = int(percent / 5)
    speed_bonus = 10 if time_spent and time_spent < 60 else 0
    user.xp += base + bonus + speed_bonus
    user.level = level_from_xp(user.xp)
    user.badge = badge_from_percent(max(user.best_percent, percent))
    await ensure_streak_row(session, user.tg_id)

async def update_streak(session: AsyncSession, user: User):
    row = await ensure_streak_row(session, user.tg_id)
    today = date.today()
    if row.last_active == today:
        return row
    if row.last_active == today.fromordinal(today.toordinal() - 1):
        row.current += 1
    else:
        row.current = 1
    row.best = max(row.best, row.current)
    row.last_active = today
    user.streak = row.current
    return row

async def toggle_favorite(session: AsyncSession, user: User, test: Test) -> bool:
    q = await session.execute(select(Favorite).where(and_(Favorite.user_id == user.tg_id, Favorite.test_id == test.id)))
    row = q.scalar_one_or_none()
    if row:
        await session.delete(row)
        return False
    session.add(Favorite(user_id=user.tg_id, test_id=test.id))
    return True

async def fetch_test_by_code(session: AsyncSession, code: str) -> Optional[Test]:
    q = await session.execute(select(Test).where(func.lower(Test.code) == code.lower().strip()))
    return q.scalar_one_or_none()

async def tests_query(session: AsyncSession, page: int = 1, q: str = "", category: str = "", difficulty: str = "", per_page: int = 5):
    stmt = select(Test).where(Test.active == True)
    if q:
        like = f"%{q.lower()}%"
        stmt = stmt.where(
            func.lower(Test.code).like(like) |
            func.lower(Test.title).like(like) |
            func.lower(Test.category).like(like) |
            func.lower(Test.topic).like(like)
        )
    if category:
        stmt = stmt.where(func.lower(Test.category) == category.lower())
    if difficulty:
        stmt = stmt.where(func.lower(Test.difficulty) == difficulty.lower())
    total_q = await session.execute(select(func.count()).select_from(stmt.subquery()))
    total = total_q.scalar_one()
    pages = max(1, (total + per_page - 1) // per_page)
    stmt = stmt.order_by(desc(Test.created_at)).offset((page - 1) * per_page).limit(per_page)
    rows = (await session.execute(stmt)).scalars().all()
    return rows, total, pages

def fmt_test_row(t: Test) -> str:
    return f"• <b>{t.code}</b> — {safe_text(t.title, 50)}\n  {safe_text(t.category, 20)} | {safe_text(t.topic, 20)} | {safe_text(t.difficulty, 12)}"

def tests_list_text(rows: List[Test], page: int, pages: int, q: str, category: str, difficulty: str) -> str:
    header = f"📝 <b>Testlar ro‘yxati</b>\nSahifa: <b>{page}/{pages}</b>"
    if q or category or difficulty:
        header += f"\nFiltr: <b>{safe_text(q or '-', 20)}</b> / <b>{safe_text(category or '-', 20)}</b> / <b>{safe_text(difficulty or '-', 20)}</b>"
    if not rows:
        return header + "\n\nHech narsa topilmadi."
    body = "\n\n".join(fmt_test_row(t) for t in rows)
    return header + "\n\n" + body

async def record_attempt(session: AsyncSession, user_id: int, code: str, raw_input: str, duplicate: bool):
    session.add(Attempt(user_id=user_id, test_code=code, raw_input=raw_input, duplicate=duplicate))

async def process_test_code(session: AsyncSession, user: User, code: str, raw_answer: str = "", time_spent: int = 0) -> Tuple[str, Optional[Result]]:
    test = await fetch_test_by_code(session, code)
    if not test:
        return "❌ Bunday test kodi topilmadi.", None
    q = await session.execute(select(Result).where(and_(Result.user_id == user.id, Result.test_id == test.id)))
    existing = q.scalar_one_or_none()
    if existing:
        await record_attempt(session, user.tg_id, code, raw_answer, True)
        return "⛔ Bu test avval ishlangan.", existing

    # Demo scoring: answers like A B C D or 1:A, 2:B...
    submitted = {}
    tokens = re.findall(r"(\d+)\s*[:=-]?\s*([ABCD])", raw_answer.upper())
    if tokens:
        submitted = {k: v for k, v in tokens}
    else:
        letters = re.findall(r"\b([ABCD])\b", raw_answer.upper())
        submitted = {str(i + 1): letters[i] for i in range(min(len(letters), test.total_questions))}

    correct = 0
    wrong = 0
    skipped = 0
    review = []
    for i in range(1, test.total_questions + 1):
        key = str(i)
        right = str(test.answer_key.get(key, "")).upper()
        got = str(submitted.get(key, "")).upper()
        if not got:
            skipped += 1
            review.append(f"{i}) ⏭ javob berilmadi | to‘g‘ri: <b>{right}</b>")
        elif got == right:
            correct += 1
            review.append(f"{i}) ✅ {got} | to‘g‘ri")
        else:
            wrong += 1
            review.append(f"{i}) ❌ {got} | to‘g‘ri: <b>{right}</b>")
    percent = pct(correct, test.total_questions)
    score = round(percent)
    status = "passed" if percent >= test.pass_percent else "failed"
    result = Result(
        user_id=user.id,
        test_id=test.id,
        test_code=test.code,
        total=test.total_questions,
        correct=correct,
        wrong=wrong,
        skipped=skipped,
        percent=percent,
        score=score,
        time_spent=time_spent,
        status=status,
        answers_json={"submitted": submitted, "review": review},
    )
    session.add(result)
    await update_streak(session, user)
    await grant_xp(session, user, percent, time_spent)
    await ensure_metrics(session, user)
    await record_attempt(session, user.tg_id, code, raw_answer, False)
    await log_analytics(session, "test_completed", {"code": code, "percent": percent, "user_id": user.tg_id})
    if percent >= test.pass_percent:
        session.add(Badge(user_id=user.tg_id, name=f"{test.code}-winner"))
    return "", result

async def result_review_text(result: Result, test: Test) -> str:
    lines = []
    review = (result.answers_json or {}).get("review", [])
    if not review:
        for i in range(1, test.total_questions + 1):
            right = test.answer_key.get(str(i), "-")
            lines.append(f"{i}) to‘g‘ri javob: <b>{right}</b>")
    else:
        lines.extend(review)
    return "🔎 <b>Review mode</b>\n\n" + "\n".join(lines[:40])

async def generate_certificate(result: Result, user: User, test: Test) -> Optional[str]:
    if result.percent < max(test.pass_percent, MIN_PASS_PERCENT):
        return None
    out_dir = APP_DIR / "certificates"
    out_dir.mkdir(exist_ok=True)
    file_path = out_dir / f"certificate_{result.id}_{user.tg_id}.pdf"
    if not REPORTLAB_AVAILABLE:
        html_path = out_dir / f"certificate_{result.id}_{user.tg_id}.txt"
        html_path.write_text(
            f"SERTIFIKAT\nIsm: {user_full_name(user)}\nTest: {test.title}\nFoiz: {result.percent}%\nSana: {result.created_at.strftime('%Y-%m-%d')}\n",
            encoding="utf-8",
        )
        return str(html_path)
    try:
        c = canvas.Canvas(str(file_path), pagesize=A4)
        width, height = A4
        c.setFont("Helvetica-Bold", 22)
        c.drawString(70, height - 90, "SERTIFIKAT")
        c.setFont("Helvetica", 14)
        c.drawString(70, height - 130, f"Ism: {user_full_name(user)}")
        c.drawString(70, height - 155, f"Test: {test.title} ({test.code})")
        c.drawString(70, height - 180, f"Foiz: {result.percent}%")
        c.drawString(70, height - 205, f"Sana: {result.created_at.strftime('%Y-%m-%d')}")
        c.drawString(70, height - 230, f"Serial: {uuid.uuid4().hex[:12].upper()}")
        c.showPage()
        c.save()
        return str(file_path)
    except Exception as e:
        logger.exception("Certificate generation failed: %s", e)
        return None

# ----------------------------- Navigation / Messages -----------------------------
async def send_home(session: AsyncSession, chat_id: int, user: User):
    await send_message(chat_id, main_menu_text(user), main_menu_keyboard())

async def send_tests_page(session: AsyncSession, chat_id: int, page: int = 1, q: str = "", category: str = "", difficulty: str = ""):
    rows, total, pages = await tests_query(session, page, q, category, difficulty)
    text_out = tests_list_text(rows, page, pages, q, category, difficulty)
    rows_buttons = []
    for t in rows:
        rows_buttons.append([(f"🧩 {t.code}", f"test:open:{t.code}")])
    rows_buttons.extend([
        [("🔎 Qidiruv", "tests:search"), ("🎚 Filter", "tests:filter")],
    ])
    nav = []
    if page > 1:
        nav.append(("⬅️", f"tests:list:{page-1}:{category}:{difficulty}:{q[:20]}"))
    if page < pages:
        nav.append(("➡️", f"tests:list:{page+1}:{category}:{difficulty}:{q[:20]}"))
    if nav:
        rows_buttons.append(nav)
    rows_buttons.append([("🏠 Bosh menyu", "menu:home")])
    await send_message(chat_id, text_out, {"inline_keyboard": [[{"text": txt, "callback_data": data} for txt, data in row] for row in rows_buttons]})

async def handle_start(session: AsyncSession, tg_msg: dict, user: User):
    await send_message(tg_msg["chat"]["id"], welcome_text(user), main_menu_keyboard())

async def handle_check_request(session: AsyncSession, user: User, chat_id: int, args: str = ""):
    await set_state(session, user.tg_id, "await_test_code", {})
    await send_message(chat_id, "✅ Test kodini yuboring. Keyin javobingizni qabul qilaman.", back_home_keyboard())

async def handle_ai_request(session: AsyncSession, user: User, chat_id: int):
    await set_state(session, user.tg_id, "await_ai_question", {})
    await send_message(chat_id, "🤖 Savolingizni yozing. Men o‘zbekcha bosqichma-bosqich tushuntiraman.", back_home_keyboard())

async def handle_contact_request(session: AsyncSession, user: User, chat_id: int):
    await set_state(session, user.tg_id, "await_contact_message", {})
    await send_message(chat_id, "📩 Xabaringizni yuboring. Men uni adminga yetkazaman.", back_home_keyboard())

async def handle_admin_panel(session: AsyncSession, user: User, chat_id: int):
    if user.tg_id != ADMIN_ID:
        await send_message(chat_id, "⛔ Bu bo‘lim faqat admin uchun.", back_home_keyboard())
        return
    await send_message(chat_id, admin_help_text(), admin_keyboard())

async def ensure_sample_fallback(session: AsyncSession):
    # Add a few notifications or welcome states if needed in future.
    return

# ----------------------------- Update Handling -----------------------------
async def handle_message(session: AsyncSession, update: dict):
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return
    chat = msg.get("chat") or {}
    user_obj = msg.get("from") or {}
    chat_id = chat.get("id")
    if not chat_id:
        return

    user = await get_or_create_user(session, user_obj)
    await add_message_log(session, user.tg_id, msg.get("text", "") or "", "in", "text", msg.get("message_id", 0))

    text_msg = (msg.get("text") or "").strip()
    state, payload = await get_state(session, user.tg_id)

    if text_msg.startswith("/start"):
        await clear_state(session, user.tg_id)
        await handle_start(session, msg, user)
        return

    if text_msg.startswith("/menu"):
        await clear_state(session, user.tg_id)
        await send_home(session, chat_id, user)
        return

    if text_msg.startswith("/tests"):
        await clear_state(session, user.tg_id)
        await send_tests_page(session, chat_id, 1)
        return

    if text_msg.startswith("/check"):
        await handle_check_request(session, user, chat_id)
        return

    if text_msg.startswith("/ai"):
        await handle_ai_request(session, user, chat_id)
        return

    if text_msg.startswith("/results"):
        await show_results(session, user, chat_id)
        return

    if text_msg.startswith("/profile"):
        await show_profile(session, user, chat_id)
        return

    if text_msg.startswith("/contact"):
        await handle_contact_request(session, user, chat_id)
        return

    if text_msg.startswith("/leaderboard"):
        await show_leaderboard(session, chat_id)
        return

    if text_msg.startswith("/daily"):
        await show_daily(session, user, chat_id)
        return

    if text_msg.startswith("/favorites"):
        await show_favorites(session, user, chat_id)
        return

    if text_msg.startswith("/admin"):
        await handle_admin_panel(session, user, chat_id)
        return

    if text_msg.startswith("/reply") and user.tg_id == ADMIN_ID:
        parts = text_msg.split(maxsplit=2)
        if len(parts) < 3:
            await send_message(chat_id, "Foydalanish: /reply <user_id> <text>", back_home_keyboard())
            return
        target = parse_int_or_none(parts[1])
        if not target:
            await send_message(chat_id, "User ID noto‘g‘ri.", back_home_keyboard())
            return
        reply_text = parts[2]
        await send_message(target, f"📨 <b>Admindan javob</b>\n\n{reply_text}", main_menu_keyboard())
        await add_log(session, user.tg_id, "reply_user", {"target": target})
        await send_message(chat_id, "✅ Javob yuborildi.", admin_keyboard())
        return

    if state == "await_test_code":
        await clear_state(session, user.tg_id)
        code = text_msg.upper().strip()
        await set_state(session, user.tg_id, "await_test_answers", {"code": code, "start_ts": int(time.time())})
        test = await fetch_test_by_code(session, code)
        if not test:
            await send_message(chat_id, "❌ Test kodi topilmadi. Yana urinib ko‘ring.", back_home_keyboard())
            return
        await send_message(
            chat_id,
            "✍️ Endi javobingizni yozing. Masalan: <code>1:A 2:C 3:B 4:D</code>",
            back_home_keyboard(),
        )
        return

    if state == "await_test_answers":
        code = (payload or {}).get("code", "")
        start_ts = int((payload or {}).get("start_ts", time.time()))
        time_spent = max(0, int(time.time()) - start_ts)
        await clear_state(session, user.tg_id)
        _, result = await process_test_code(session, user, code, text_msg, time_spent=time_spent)
        if not result:
            await send_message(chat_id, "⛔ Bu test avval ishlangan yoki topilmadi.", back_home_keyboard())
            return
        test = await fetch_test_by_code(session, code)
        await session.flush()
        await session.commit()
        await send_message(chat_id, result_card(result, test), results_keyboard(result.id, test.code))
        return

    if state == "await_ai_question":
        await clear_state(session, user.tg_id)
        response = await ai_answer(text_msg)
        await send_message(chat_id, response, back_home_keyboard())
        return

    if state == "await_search":
        await clear_state(session, user.tg_id)
        await send_tests_page(session, chat_id, 1, q=text_msg)
        return

    if state == "await_contact_message":
        await clear_state(session, user.tg_id)
        if ADMIN_ID:
            await send_message(ADMIN_ID, f"📩 <b>Yangi xabar</b>\nFrom: <code>{user.tg_id}</code>\n\n{text_msg}")
        await send_message(chat_id, "✅ Xabaringiz adminga yuborildi.", main_menu_keyboard())
        return

    if text_msg:
        # Default AI fallback for natural questions
        if len(text_msg) > 2:
            response = await ai_answer(text_msg)
            await send_message(chat_id, response, back_home_keyboard())
            return

    await send_message(chat_id, "Iltimos, menyudan kerakli bo‘limni tanlang.", main_menu_keyboard())

async def show_results(session: AsyncSession, user: User, chat_id: int):
    q = await session.execute(
        select(Result, Test).join(Test, Result.test_id == Test.id).where(Result.user_id == user.id).order_by(desc(Result.created_at)).limit(10)
    )
    rows = q.all()
    if not rows:
        await send_message(chat_id, "Hozircha natija yo‘q.", back_home_keyboard())
        return
    lines = []
    for result, test in rows:
        lines.append(f"• <b>{test.code}</b> — {result.percent}% | {result.status}")
    await send_message(chat_id, "📊 <b>Natijalarim</b>\n\n" + "\n".join(lines), back_home_keyboard())

async def show_profile(session: AsyncSession, user: User, chat_id: int):
    streak_q = await session.execute(select(Streak).where(Streak.user_id == user.tg_id))
    streak = streak_q.scalar_one_or_none()
    badges_q = await session.execute(select(Badge).where(Badge.user_id == user.tg_id).order_by(desc(Badge.created_at)).limit(10))
    badges = badges_q.scalars().all()
    await send_message(chat_id, profile_text(user, streak, badges), back_home_keyboard())

async def show_leaderboard(session: AsyncSession, chat_id: int):
    q = await session.execute(select(User).order_by(desc(User.xp)).limit(10))
    users = q.scalars().all()
    if not users:
        await send_message(chat_id, "Hozircha reyting bo‘sh.", back_home_keyboard())
        return
    lines = []
    for i, u in enumerate(users, 1):
        lines.append(f"{i}. <b>{safe_text(u.first_name, 20)}</b> — XP {u.xp} | lvl {u.level}")
    await send_message(chat_id, "🏆 <b>Global leaderboard</b>\n\n" + "\n".join(lines), back_home_keyboard())

async def show_daily(session: AsyncSession, user: User, chat_id: int):
    q = await session.execute(select(Test).where(Test.active == True).order_by(desc(Test.created_at)).limit(1))
    test = q.scalar_one_or_none()
    if not test:
        await send_message(chat_id, "Buguncha challenge yo‘q.", back_home_keyboard())
        return
    await send_message(chat_id, f"🎯 <b>Daily challenge</b>\n\n{test_preview_text(test)}", test_preview_keyboard(test.code))

async def show_favorites(session: AsyncSession, user: User, chat_id: int):
    q = await session.execute(
        select(Test).join(Favorite, Favorite.test_id == Test.id).where(Favorite.user_id == user.tg_id).order_by(desc(Favorite.created_at))
    )
    tests = q.scalars().all()
    if not tests:
        await send_message(chat_id, "⭐ Favoritlar bo‘sh.", back_home_keyboard())
        return
    text_out = "⭐ <b>Favorites</b>\n\n" + "\n\n".join(fmt_test_row(t) for t in tests[:10])
    await send_message(chat_id, text_out, back_home_keyboard())

async def handle_callback(session: AsyncSession, update: dict):
    cq = update.get("callback_query")
    if not cq:
        return
    user_obj = cq.get("from") or {}
    user = await get_or_create_user(session, user_obj)
    data = cq.get("data", "")
    message = cq.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    try:
        if data.startswith("menu:home"):
            await answer_callback(cq["id"], "Bosh menyu")
            await send_home(session, chat_id, user)
        elif data == "menu:tests":
            await answer_callback(cq["id"], "Testlar")
            await send_tests_page(session, chat_id, 1)
        elif data == "menu:check":
            await answer_callback(cq["id"], "Tekshirish")
            await handle_check_request(session, user, chat_id)
        elif data == "menu:ai":
            await answer_callback(cq["id"], "AI ustoz")
            await handle_ai_request(session, user, chat_id)
        elif data == "menu:results":
            await answer_callback(cq["id"], "Natijalar")
            await show_results(session, user, chat_id)
        elif data == "menu:profile":
            await answer_callback(cq["id"], "Profil")
            await show_profile(session, user, chat_id)
        elif data == "menu:contact":
            await answer_callback(cq["id"], "Bog‘lanish")
            await handle_contact_request(session, user, chat_id)
        elif data == "menu:help":
            await answer_callback(cq["id"], "Yordam")
            await send_message(chat_id, help_text(), back_home_keyboard())
        elif data == "menu:leaderboard":
            await answer_callback(cq["id"], "Reyting")
            await show_leaderboard(session, chat_id)
        elif data == "menu:daily":
            await answer_callback(cq["id"], "Daily")
            await show_daily(session, user, chat_id)
        elif data == "menu:favorites":
            await answer_callback(cq["id"], "Favorites")
            await show_favorites(session, user, chat_id)
        elif data.startswith("tests:list:"):
            _, _, page_s, category, difficulty, qtxt = data.split(":", 5)
            await answer_callback(cq["id"], "Ro‘yxat")
            await send_tests_page(session, chat_id, int(page_s), qtxt, category, difficulty)
        elif data == "tests:search":
            await answer_callback(cq["id"], "Qidiruv")
            await set_state(session, user.tg_id, "await_search", {})
            await send_message(chat_id, "🔎 Qidiruv uchun test kodi, mavzu yoki sarlavha yozing.", back_home_keyboard())
        elif data == "tests:filter":
            await answer_callback(cq["id"], "Filter")
            await send_message(chat_id, "🎚 Filter: keyin /tests va qidiruv bilan birga ishlating.", back_home_keyboard())
        elif data.startswith("test:open:"):
            code = data.split(":", 2)[2]
            test = await fetch_test_by_code(session, code)
            if not test:
                await answer_callback(cq["id"], "Topilmadi", True)
                return
            favorite = False
            q = await session.execute(select(Favorite).where(and_(Favorite.user_id == user.tg_id, Favorite.test_id == test.id)))
            favorite = q.scalar_one_or_none() is not None
            await answer_callback(cq["id"], "Test")
            await send_message(chat_id, test_preview_text(test), test_preview_keyboard(test.code, favorite))
        elif data.startswith("test:check:"):
            code = data.split(":", 2)[2]
            await answer_callback(cq["id"], "Tekshirish")
            await set_state(session, user.tg_id, "await_test_answers", {"code": code, "start_ts": int(time.time())})
            await send_message(chat_id, f"✍️ {code} uchun javoblarni yuboring.", back_home_keyboard())
        elif data.startswith("test:pdf:"):
            code = data.split(":", 2)[2]
            test = await fetch_test_by_code(session, code)
            await answer_callback(cq["id"], "PDF")
            if test and test.pdf_url:
                await send_message(chat_id, f"📄 PDF: {test.pdf_url}", back_home_keyboard())
            else:
                await send_message(chat_id, "📄 Bu test uchun PDF hozircha yo‘q.", back_home_keyboard())
        elif data.startswith("test:ai:"):
            code = data.split(":", 2)[2]
            test = await fetch_test_by_code(session, code)
            await answer_callback(cq["id"], "AI tahlil")
            if test:
                txt = await ai_answer(f"{test.title} ({test.code}) testini o‘zbek tilida tushuntir. Asosiy mavzu: {test.topic}. Qiyinlik: {test.difficulty}.")
                await send_message(chat_id, txt, back_home_keyboard())
        elif data.startswith("fav:toggle:"):
            code = data.split(":", 2)[2]
            test = await fetch_test_by_code(session, code)
            if not test:
                await answer_callback(cq["id"], "Topilmadi", True)
                return
            added = await toggle_favorite(session, user, test)
            await answer_callback(cq["id"], "Saqlandi" if added else "O‘chirildi")
            await session.commit()
            await send_message(chat_id, "⭐ Favorit yangilandi.", back_home_keyboard())
        elif data.startswith("result:review:"):
            rid = int(data.split(":")[-1])
            q = await session.execute(select(Result).where(Result.id == rid, Result.user_id == user.id))
            result = q.scalar_one_or_none()
            if not result:
                await answer_callback(cq["id"], "Topilmadi", True)
                return
            t = await session.get(Test, result.test_id)
            await answer_callback(cq["id"], "Review")
            await send_message(chat_id, await result_review_text(result, t), back_home_keyboard())
        elif data.startswith("result:ai:"):
            rid = int(data.split(":")[-1])
            q = await session.execute(select(Result).where(Result.id == rid, Result.user_id == user.id))
            result = q.scalar_one_or_none()
            if not result:
                await answer_callback(cq["id"], "Topilmadi", True)
                return
            t = await session.get(Test, result.test_id)
            await answer_callback(cq["id"], "AI")
            txt = await ai_answer(
                f"Ushbu test natijasini tahlil qil: {t.title} ({t.code}). "
                f"To‘g‘ri: {result.correct}, noto‘g‘ri: {result.wrong}, foiz: {result.percent}. "
                f"Xatolarni oddiy o‘zbek tilida tushuntir."
            )
            await send_message(chat_id, txt, back_home_keyboard())
        elif data.startswith("result:cert:"):
            rid = int(data.split(":")[-1])
            q = await session.execute(select(Result).where(Result.id == rid, Result.user_id == user.id))
            result = q.scalar_one_or_none()
            if not result:
                await answer_callback(cq["id"], "Topilmadi", True)
                return
            t = await session.get(Test, result.test_id)
            cert = await generate_certificate(result, user, t)
            await answer_callback(cq["id"], "Sertifikat")
            if cert:
                await send_document(chat_id, cert, "🏅 Sertifikat tayyor.")
            else:
                await send_message(chat_id, "Sertifikat uchun yetarli ball yo‘q.", back_home_keyboard())
        elif data.startswith("admin:"):
            if user.tg_id != ADMIN_ID:
                await answer_callback(cq["id"], "Ruxsat yo‘q", True)
                return
            action = data.split(":", 1)[1]
            if action == "add_test":
                await set_state(session, user.tg_id, "await_admin_add_test", {})
                await answer_callback(cq["id"], "Test qo‘shish")
                await send_message(chat_id, "➕ Yangi test uchun format yuboring: code|title|category|topic|difficulty|qsonlar|A1/B1/C1/D1", admin_keyboard())
            elif action == "del_test":
                await set_state(session, user.tg_id, "await_admin_del_test", {})
                await answer_callback(cq["id"], "Test o‘chirish")
                await send_message(chat_id, "🗑 O‘chirish uchun test kodini yuboring.", admin_keyboard())
            elif action == "broadcast":
                await set_state(session, user.tg_id, "await_admin_broadcast", {})
                await answer_callback(cq["id"], "Broadcast")
                await send_message(chat_id, "📣 Ommaviy xabar matnini yuboring.", admin_keyboard())
            elif action == "analytics":
                await answer_callback(cq["id"], "Analytics")
                await show_admin_analytics(session, chat_id)
            elif action == "users":
                await answer_callback(cq["id"], "Users")
                await show_admin_users(session, chat_id)
            elif action == "top":
                await answer_callback(cq["id"], "Top")
                await show_leaderboard(session, chat_id)
            elif action == "logs":
                await answer_callback(cq["id"], "Logs")
                await show_admin_logs(session, chat_id)
        else:
            await answer_callback(cq["id"], "OK")
    except Exception as e:
        logger.exception("Callback error: %s", e)
        with contextlib.suppress(Exception):
            await answer_callback(cq["id"], "Xatolik", True)
            await send_message(chat_id, "⚠️ Kutilmagan xatolik yuz berdi. Iltimos qayta urinib ko‘ring.", back_home_keyboard())

async def show_admin_analytics(session: AsyncSession, chat_id: int):
    users = (await session.execute(select(func.count(User.id)))).scalar_one()
    results = (await session.execute(select(func.count(Result.id)))).scalar_one()
    tests = (await session.execute(select(func.count()).select_from(Test))).scalar_one()
    top_test_q = await session.execute(select(Result.test_code, func.count(Result.id)).group_by(Result.test_code).order_by(desc(func.count(Result.id))).limit(5))
    top_tests = top_test_q.all()
    body = (
        "📈 <b>Analytics</b>\n\n"
        f"👥 Users: <b>{users}</b>\n"
        f"🧪 Tests: <b>{tests}</b>\n"
        f"📊 Results: <b>{results}</b>\n\n"
        "Top ishlangan testlar:\n" + ("\n".join([f"• {c} — {n}" for c, n in top_tests]) or "—")
    )
    await send_message(chat_id, body, admin_keyboard())

async def show_admin_users(session: AsyncSession, chat_id: int):
    q = await session.execute(select(User).order_by(desc(User.xp)).limit(20))
    users = q.scalars().all()
    lines = [f"• <code>{u.tg_id}</code> | {safe_text(u.first_name, 20)} | XP {u.xp} | lvl {u.level}" for u in users]
    await send_message(chat_id, "👥 <b>Foydalanuvchilar</b>\n\n" + "\n".join(lines), admin_keyboard())

async def show_admin_logs(session: AsyncSession, chat_id: int):
    q = await session.execute(select(AdminLog).order_by(desc(AdminLog.created_at)).limit(10))
    logs = q.scalars().all()
    lines = [f"• {l.action} | {l.actor_id} | {l.created_at.strftime('%Y-%m-%d %H:%M')}" for l in logs]
    await send_message(chat_id, "🧾 <b>Loglar</b>\n\n" + "\n".join(lines), admin_keyboard())

async def broadcast_message(session: AsyncSession, text_msg: str):
    q = await session.execute(select(User.tg_id))
    ids = [r[0] for r in q.all()]
    sent = 0
    for uid in ids:
        with contextlib.suppress(Exception):
            await send_message(uid, f"📣 <b>Bildirishnoma</b>\n\n{text_msg}")
            sent += 1
            await asyncio.sleep(0.03)
    return sent

async def handle_stateful_admin(session: AsyncSession, user: User, chat_id: int, text_msg: str, state: str):
    if user.tg_id != ADMIN_ID:
        return False
    if state == "await_admin_del_test":
        code = text_msg.strip().upper()
        test = await fetch_test_by_code(session, code)
        if not test:
            await send_message(chat_id, "Bunday test yo‘q.", admin_keyboard())
            return True
        await session.delete(test)
        await add_log(session, user.tg_id, "delete_test", {"code": code})
        await send_message(chat_id, f"✅ {code} o‘chirildi.", admin_keyboard())
        return True
    if state == "await_admin_broadcast":
        await clear_state(session, user.tg_id)
        sent = await broadcast_message(session, text_msg)
        await add_log(session, user.tg_id, "broadcast", {"count": sent})
        await session.commit()
        await send_message(chat_id, f"✅ Broadcast yuborildi. {sent} ta userga yetdi.", admin_keyboard())
        return True
    if state == "await_admin_add_test":
        # code|title|category|topic|difficulty|qsonlar|A1/B1/C1/D1
        parts = [p.strip() for p in text_msg.split("|")]
        if len(parts) < 7:
            await send_message(chat_id, "Format noto‘g‘ri.", admin_keyboard())
            return True
        code, title, category, topic, difficulty, qcount, answers = parts[:7]
        try:
            qcount_i = int(qcount)
        except Exception:
            qcount_i = 4
        ans_list = re.split(r"[,\s/]+", answers.upper())
        answer_key = {str(i + 1): (ans_list[i] if i < len(ans_list) and ans_list[i] in {"A", "B", "C", "D"} else "A") for i in range(qcount_i)}
        session.add(Test(
            code=code.upper(),
            title=title,
            category=category,
            topic=topic,
            difficulty=difficulty,
            description="Admin tomonidan qo‘shilgan test.",
            pdf_url="",
            answer_key=answer_key,
            total_questions=qcount_i,
            pass_percent=MIN_PASS_PERCENT,
            active=True,
        ))
        await add_log(session, user.tg_id, "add_test", {"code": code, "title": title})
        await clear_state(session, user.tg_id)
        await send_message(chat_id, f"✅ {code} qo‘shildi.", admin_keyboard())
        return True
    return False

# ----------------------------- Polling (fallback) -----------------------------
async def get_updates(offset: int) -> dict:
    return await tg("getUpdates", {"offset": offset, "timeout": 30, "allowed_updates": ["message", "callback_query"]})

async def polling_loop():
    if not BOT_TOKEN:
        logger.warning("Polling disabled: BOT_TOKEN missing")
        return
    offset = 0
    while True:
        try:
            resp = await get_updates(offset)
            if not resp.get("ok"):
                await asyncio.sleep(2)
                continue
            for upd in resp.get("result", []):
                offset = max(offset, upd["update_id"] + 1)
                await process_update(upd)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Polling loop error: %s", e)
            await asyncio.sleep(2)

# ----------------------------- App & Routes -----------------------------
app = FastAPI(title=BOT_NAME, version="1.0.0")
polling_task: Optional[asyncio.Task] = None

@app.on_event("startup")
async def on_startup():
    global polling_task
    logger.info("Starting %s", BOT_NAME)
    logger.info("DB: %s", DATABASE_URL)
    logger.info("BOT_TOKEN: %s", mask_secret(BOT_TOKEN))
    try:
        await init_db()
    except Exception as e:
        logger.exception("Init DB failed, continuing in degraded mode: %s", e)

    try:
        async with SessionLocal() as session:
            try:
                await seed_tests(session)
                await ensure_sample_fallback(session)
                await session.commit()
            except Exception as e:
                logger.exception("Startup seeding skipped: %s", e)
                await session.rollback()
    except Exception as e:
        logger.exception("Session bootstrap skipped: %s", e)

    if APP_ROLE == "polling" or not WEBHOOK_URL:
        polling_task = asyncio.create_task(polling_loop())
    else:
        await set_webhook()

@app.on_event("shutdown")
async def on_shutdown():
    global polling_task
    if polling_task:
        polling_task.cancel()
        with contextlib.suppress(Exception):
            await polling_task
    await HTTP.aclose()

async def set_webhook():
    if not BOT_TOKEN or not WEBHOOK_URL:
        return
    url = WEBHOOK_URL.rstrip("/") + f"/webhook/{WEBHOOK_SECRET}"
    try:
        await tg("setWebhook", {"url": url, "drop_pending_updates": True})
        logger.info("Webhook set to %s", url)
    except Exception as e:
        logger.exception("Failed to set webhook: %s", e)

@app.get("/health")
async def health():
    return {"ok": True, "bot": BOT_NAME, "role": APP_ROLE, "ai_enabled": AI_ENABLED, "db": DATABASE_URL.split("@")[-1]}

@app.get("/")
async def root():
    return HTMLResponse("<h3>math_tekshiruvchi_bot is running</h3>")

@app.head("/")
async def root_head():
    return Response(status_code=200)

@app.get(f"/dashboard/{{secret}}")
async def dashboard(secret: str):
    if secret != DASHBOARD_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    async with SessionLocal() as session:
        users = (await session.execute(select(func.count(User.id)))).scalar_one()
        tests = (await session.execute(select(func.count()).select_from(Test))).scalar_one()
        results = (await session.execute(select(func.count(Result.id)))).scalar_one()
        return {"users": users, "tests": tests, "results": results, "min_pass_percent": MIN_PASS_PERCENT}

@app.post(f"/webhook/{{secret}}")
async def webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    update = await request.json()
    async with SessionLocal() as session:
        await process_update(update, session)
        await session.commit()
    return {"ok": True}

async def process_update(update: dict, session: Optional[AsyncSession] = None):
    own_session = False
    if session is None:
        own_session = True
        session = SessionLocal()
    try:
        assert session is not None
        # duplicate protection
        uid = update.get("update_id")
        cache_key = f"upd:{uid}"
        if uid is not None and await cache_get(cache_key):
            return
        if uid is not None:
            await cache_set(cache_key, True, ttl=120)
        if "message" in update or "edited_message" in update:
            msg = update.get("message") or update.get("edited_message")
            user_obj = msg.get("from") or {}
            user = await get_or_create_user(session, user_obj)
            state, _payload = await get_state(session, user.tg_id)
            if state and await handle_stateful_admin(session, user, (msg.get("chat") or {}).get("id", 0), (msg.get("text") or "").strip(), state):
                await session.commit()
                return
            await handle_message(session, update)
        elif "callback_query" in update:
            await handle_callback(session, update)
        await session.commit()
    except Exception as e:
        logger.exception("Update processing failed: %s", e)
        # attempt graceful feedback
        try:
            if "message" in update:
                chat_id = (update["message"].get("chat") or {}).get("id")
                if chat_id:
                    await send_message(chat_id, "⚠️ Kutilmagan xatolik. Keyinroq qayta urinib ko‘ring.", main_menu_keyboard())
        except Exception:
            pass
    finally:
        if own_session:
            await session.close()

# ----------------------------- Startup Hook -----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False, log_level=env("LOG_LEVEL", "info").lower())
