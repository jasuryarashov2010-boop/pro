import asyncio
import csv
import io
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

load_dotenv()

# =========================
# ENV
# =========================
APP_ROLE = os.getenv("APP_ROLE", "all").lower().strip()   # all | web | bot
PORT = int(os.getenv("PORT", "10000"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./bot.db")
REDIS_URL = os.getenv("REDIS_URL", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
DASHBOARD_SECRET = os.getenv("DASHBOARD_SECRET", str(ADMIN_ID))
MIN_PASS_PERCENT = int(os.getenv("MIN_PASS_PERCENT", "50"))
AI_ENABLED = os.getenv("AI_ENABLED", "1") == "1"

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://") and "+asyncpg" not in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("sqlite:///"):
    DATABASE_URL = DATABASE_URL.replace("sqlite:///", "sqlite+aiosqlite:///", 1)

engine_kwargs = {"echo": False, "pool_pre_ping": True, "future": True}
if DATABASE_URL.startswith("sqlite"):
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine: AsyncEngine = create_async_engine(DATABASE_URL, **engine_kwargs)

bot: Optional[Bot] = None
if APP_ROLE in ("all", "bot"):
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN yo‘q.")
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# Optional Redis cache
redis_client = None
if REDIS_URL:
    try:
        import redis  # type: ignore
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        redis_client = None

# In-memory AI memory
ai_memory: dict[int, list[str]] = defaultdict(list)

# =========================
# STATES
# =========================
class AIFlow(StatesGroup):
    waiting_query = State()

class ContactFlow(StatesGroup):
    waiting_message = State()

class TestCheckFlow(StatesGroup):
    waiting_code = State()
    waiting_answers = State()

class AdminAddTestFlow(StatesGroup):
    waiting_title = State()
    waiting_code = State()
    waiting_subject = State()
    waiting_difficulty = State()
    waiting_answers = State()
    waiting_pdf = State()

class AdminDeleteTestFlow(StatesGroup):
    waiting_code = State()

class AdminBroadcastFlow(StatesGroup):
    waiting_message = State()

class AdminStatsFlow(StatesGroup):
    waiting_code = State()

# =========================
# HELPERS
# =========================
def now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def clamp_text(value: str, limit: int = 40) -> str:
    value = value or ""
    return value if len(value) <= limit else value[: limit - 1] + "…"

def normalize_answers(raw: str) -> str:
    return "".join(ch.upper() for ch in raw.strip() if ch.upper() in "ABCD")

def calc_level(xp: int) -> int:
    return max(1, xp // 500 + 1)

def make_back_kb(back_cb: str = "menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=back_cb)],
        [InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="menu")]
    ])

def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📚 Testlar ro‘yxati", callback_data="tests:1")],
        [InlineKeyboardButton(text="📝 Test tekshirish", callback_data="check:start")],
        [InlineKeyboardButton(text="🤖 AI ustoz", callback_data="ai:start")],
        [InlineKeyboardButton(text="📊 Natijalarim", callback_data="results:list")],
        [InlineKeyboardButton(text="👤 Profilim", callback_data="profile")],
        [InlineKeyboardButton(text="📩 Bog‘lanish", callback_data="contact:start")],
        [InlineKeyboardButton(text="🏆 Reyting", callback_data="leaderboard")],
        [InlineKeyboardButton(text="📜 Sertifikat", callback_data="cert:latest")],
    ]
    if is_admin(user_id):
        rows.append([InlineKeyboardButton(text="🛠 Admin paneli", callback_data="admin:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Statistikalar", callback_data="admin:statsmenu")],
        [InlineKeyboardButton(text="📝 Test boshqaruvi", callback_data="admin:tests")],
        [InlineKeyboardButton(text="👥 Foydalanuvchilar", callback_data="admin:users")],
        [InlineKeyboardButton(text="📢 Xabar yuborish", callback_data="admin:broadcast")],
        [InlineKeyboardButton(text="🧠 AI nazorat", callback_data="admin:ai")],
        [InlineKeyboardButton(text="⚙️ Sozlamalar", callback_data="admin:settings")],
        [InlineKeyboardButton(text="📁 Loglar", callback_data="admin:logs")],
        [InlineKeyboardButton(text="🩺 System Health", callback_data="admin:health")],
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu")],
    ])

def tests_kb(tests: list[dict[str, Any]], page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows = []
    for t in tests:
        rows.append([InlineKeyboardButton(text=f"{t['code']} • {clamp_text(t['title'], 28)}", callback_data=f"test:{t['code']}")])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️ Oldingi", callback_data=f"tests:{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="Keyingi ▶️", callback_data=f"tests:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="menu")])
    rows.append([InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def result_kb(test_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 AI tahlil", callback_data=f"ai:result:{test_code}")],
        [InlineKeyboardButton(text="📜 Sertifikat", callback_data="cert:latest")],
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="results:list")],
        [InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="menu")],
    ])

def admin_test_item_kb(code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Batafsil statistika", callback_data=f"admin:teststats:{code}")],
        [InlineKeyboardButton(text="🗑 O‘chirish", callback_data=f"admin:del:{code}")],
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data="admin:tests")],
        [InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="menu")],
    ])

def cache_get(key: str) -> Optional[str]:
    if redis_client is None:
        return None
    try:
        return redis_client.get(key)
    except Exception:
        return None

def cache_set(key: str, value: str, ttl: int = 300) -> None:
    if redis_client is None:
        return
    try:
        redis_client.set(key, value, ex=ttl)
    except Exception:
        pass

# =========================
# DATABASE HELPERS
# =========================
async def db_fetchall(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        result = await conn.execute(text(sql), params or {})
        return [dict(row._mapping) for row in result.fetchall()]

async def db_fetchone(sql: str, params: dict[str, Any] | None = None) -> Optional[dict[str, Any]]:
    rows = await db_fetchall(sql, params)
    return rows[0] if rows else None

async def db_value(sql: str, params: dict[str, Any] | None = None) -> Any:
    row = await db_fetchone(sql, params)
    if not row:
        return None
    return list(row.values())[0]

async def db_exec(sql: str, params: dict[str, Any] | None = None) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(sql), params or {})

async def init_db() -> None:
    if DATABASE_URL.startswith("sqlite"):
        await db_exec("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            xp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            streak INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS tests(
            code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            subject TEXT DEFAULT '',
            difficulty TEXT DEFAULT 'medium',
            answer_key TEXT NOT NULL,
            pdf_file_id TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS results(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            test_code TEXT,
            score INTEGER DEFAULT 0,
            correct_count INTEGER DEFAULT 0,
            wrong_count INTEGER DEFAULT 0,
            elapsed_seconds INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, test_code)
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS contacts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            message TEXT,
            admin_reply TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            replied_at TEXT DEFAULT ''
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT,
            user_id INTEGER,
            detail TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
    else:
        await db_exec("""
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            xp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            streak INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS tests(
            code TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            subject TEXT DEFAULT '',
            difficulty TEXT DEFAULT 'medium',
            answer_key TEXT NOT NULL,
            pdf_file_id TEXT DEFAULT '',
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS results(
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT,
            test_code TEXT,
            score INTEGER DEFAULT 0,
            correct_count INTEGER DEFAULT 0,
            wrong_count INTEGER DEFAULT 0,
            elapsed_seconds INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, test_code)
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS contacts(
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT,
            message TEXT,
            admin_reply TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            replied_at TIMESTAMPTZ
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""")
        await db_exec("""
        CREATE TABLE IF NOT EXISTS logs(
            id BIGSERIAL PRIMARY KEY,
            kind TEXT,
            user_id BIGINT,
            detail TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""")

    # settings defaults
    if DATABASE_URL.startswith("sqlite"):
        await db_exec("INSERT OR IGNORE INTO settings(key, value) VALUES ('min_pass_percent', :v)", {"v": str(MIN_PASS_PERCENT)})
        await db_exec("INSERT OR IGNORE INTO settings(key, value) VALUES ('ai_enabled', :v)", {"v": "1" if AI_ENABLED else "0"})
    else:
        await db_exec("INSERT INTO settings(key, value) VALUES ('min_pass_percent', :v) ON CONFLICT (key) DO NOTHING", {"v": str(MIN_PASS_PERCENT)})
        await db_exec("INSERT INTO settings(key, value) VALUES ('ai_enabled', :v) ON CONFLICT (key) DO NOTHING", {"v": "1" if AI_ENABLED else "0"})

# =========================
# BUSINESS LOGIC
# =========================
async def log_event(kind: str, user_id: int | None, detail: str) -> None:
    try:
        await db_exec(
            "INSERT INTO logs(kind, user_id, detail) VALUES (:k, :u, :d)",
            {"k": kind, "u": user_id, "d": detail[:1000]},
        )
    except Exception:
        pass

async def upsert_user(user_id: int, username: str | None, full_name: str | None) -> None:
    if DATABASE_URL.startswith("sqlite"):
        await db_exec("""
            INSERT INTO users(user_id, username, full_name, created_at, updated_at)
            VALUES (:id, :u, :f, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name, updated_at=CURRENT_TIMESTAMP
        """, {"id": user_id, "u": username or "", "f": full_name or ""})
    else:
        await db_exec("""
            INSERT INTO users(user_id, username, full_name, created_at, updated_at)
            VALUES (:id, :u, :f, NOW(), NOW())
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name, updated_at=NOW()
        """, {"id": user_id, "u": username or "", "f": full_name or ""})

async def add_xp(user_id: int, xp: int) -> None:
    current_xp = int(await db_value("SELECT COALESCE(xp,0) FROM users WHERE user_id=:u", {"u": user_id}) or 0)
    new_xp = current_xp + xp
    new_level = calc_level(new_xp)
    await db_exec(
        "UPDATE users SET xp = :x, level = :l, updated_at = CURRENT_TIMESTAMP WHERE user_id = :u"
        if DATABASE_URL.startswith("sqlite")
        else "UPDATE users SET xp = :x, level = :l, updated_at = NOW() WHERE user_id = :u",
        {"x": new_xp, "l": new_level, "u": user_id},
    )

async def get_setting(key: str, default: str = "") -> str:
    value = await db_value("SELECT value FROM settings WHERE key=:k", {"k": key})
    return str(value) if value is not None else default

async def set_setting_value(key: str, value: str) -> None:
    if DATABASE_URL.startswith("sqlite"):
        await db_exec("INSERT OR REPLACE INTO settings(key, value) VALUES (:k, :v)", {"k": key, "v": value})
    else:
        await db_exec("""
        INSERT INTO settings(key, value) VALUES (:k, :v)
        ON CONFLICT (key) DO UPDATE SET value=excluded.value
        """, {"k": key, "v": value})

async def get_user_stats(user_id: int) -> dict[str, Any]:
    user = await db_fetchone("SELECT * FROM users WHERE user_id=:u", {"u": user_id}) or {}
    res = await db_fetchall("SELECT * FROM results WHERE user_id=:u ORDER BY created_at DESC", {"u": user_id})
    scores = [int(r["score"]) for r in res] if res else []
    avg = round(sum(scores) / len(scores), 1) if scores else 0.0
    best = max(scores) if scores else 0
    last = scores[0] if scores else 0
    return {
        "user": user,
        "count": len(scores),
        "avg": avg,
        "best": best,
        "last": last,
        "xp": int(user.get("xp", 0) or 0),
        "level": int(user.get("level", 1) or 1),
    }

async def recommend_path(user_id: int) -> str:
    stats = await get_user_stats(user_id)
    avg = stats["avg"]
    if stats["count"] == 0:
        return "Boshlang‘ich testlardan boshlang."
    if avg < 50:
        return "📘 Sizga oson testlar va mini darslar tavsiya qilinadi."
    if avg < 80:
        return "📗 Sizga o‘rta daraja testlar va xato tahlili kerak."
    return "📕 Sizga qiyin testlar va tezlik mashqlari tavsiya qilinadi."

# =========================
# AI HELPERS
# =========================
async def ask_ai(text_in: str, user_id: int) -> str:
    if not AI_ENABLED:
        return "🤖 AI hozir o‘chiq. Keyinroq urinib ko‘ring."

    memory = "\n".join(ai_memory[user_id][-5:])
    prompt = f"""
Sen matematika ustozisan. Javobni o‘zbek tilida, qisqa, aniq va bosqichma-bosqich ber.

Oldingi savollar:
{memory}

Yangi savol:
{text_in}
"""
    ai_memory[user_id].append(text_in)
    if len(ai_memory[user_id]) > 5:
        ai_memory[user_id].pop(0)

    if GEMINI_API_KEY:
        try:
            import google.generativeai as genai  # type: ignore
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel("gemini-1.5-flash")
            resp = await asyncio.to_thread(model.generate_content, prompt)
            txt = getattr(resp, "text", "") or ""
            if txt.strip():
                return txt.strip()
        except Exception:
            pass

    if OPENAI_API_KEY:
        try:
            from openai import AsyncOpenAI  # type: ignore
            client = AsyncOpenAI(api_key=OPENAI_API_KEY)
            resp = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Sen matematika ustozisan. O‘zbek tilida tushuntir."},
                    {"role": "user", "content": prompt},
                ],
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            pass

    return (
        f"🧠 Tushuntirish:\n{text_in}\n\n"
        "1) Masalani bo‘lib o‘qiymiz.\n"
        "2) Berilganlarni ajratamiz.\n"
        "3) Formula yoki mantiqni qo‘llaymiz.\n"
        "4) Javobni tekshiramiz."
    )

async def ocr_or_voice_stub(kind: str) -> str:
    return f"📎 {kind} qabul qilindi. OCR/STT modulini ulasangiz, real tahlil ishlaydi."

# =========================
# CHARTS / DASHBOARD
# =========================
async def chart_bytes() -> bytes:
    rows = await db_fetchall("""
        SELECT username, xp
        FROM users
        ORDER BY xp DESC
        LIMIT 10
    """)
    labels = [r["username"] or str(r.get("user_id", "user")) for r in rows] if rows else []
    values = [int(r["xp"] or 0) for r in rows] if rows else []

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(111)
    ax.bar(labels, values)
    ax.set_title("Top 10 foydalanuvchi XP")
    ax.set_xlabel("Foydalanuvchi")
    ax.set_ylabel("XP")
    ax.tick_params(axis="x", rotation=35)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()

async def dashboard_stats() -> dict[str, Any]:
    users = await db_value("SELECT COUNT(*) FROM users") or 0
    tests = await db_value("SELECT COUNT(*) FROM tests") or 0
    results = await db_value("SELECT COUNT(*) FROM results") or 0
    avg = await db_value("SELECT COALESCE(ROUND(AVG(score), 1), 0) FROM results") or 0
    top = await db_fetchall("""
        SELECT username, xp, level
        FROM users
        ORDER BY xp DESC
        LIMIT 5
    """)
    recent = await db_fetchall("""
        SELECT user_id, test_code, score, created_at
        FROM results
        ORDER BY created_at DESC
        LIMIT 10
    """)
    return {"users": users, "tests": tests, "results": results, "avg": avg, "top": top, "recent": recent}

def dashboard_html(stats: dict[str, Any], secret: str) -> str:
    top_rows = "".join(
        f"<tr><td>{i+1}</td><td>@{(r['username'] or 'user')}</td><td>{r['xp']}</td><td>{r['level']}</td></tr>"
        for i, r in enumerate(stats["top"])
    ) or "<tr><td colspan='4'>Ma'lumot yo'q</td></tr>"

    recent_rows = "".join(
        f"<tr><td>{r['user_id']}</td><td>{r['test_code']}</td><td>{r['score']}%</td><td>{r['created_at']}</td></tr>"
        for r in stats["recent"]
    ) or "<tr><td colspan='4'>Ma'lumot yo'q</td></tr>"

    return f"""
    <html>
    <head>
      <title>math_tekshiruvchi_bot - Dashboard</title>
      <style>
        body{{font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:24px}}
        .grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px}}
        .card{{background:#111827;border:1px solid #243244;border-radius:18px;padding:18px}}
        .title{{font-size:28px;font-weight:700;margin-bottom:12px}}
        .muted{{color:#94a3b8}}
        table{{width:100%;border-collapse:collapse;margin-top:8px}}
        th,td{{border-bottom:1px solid #243244;padding:10px;text-align:left}}
        .wrap{{max-width:1200px;margin:auto}}
        .btn{{display:inline-block;background:#2563eb;color:white;padding:10px 14px;border-radius:10px;text-decoration:none;margin-right:8px}}
      </style>
    </head>
    <body>
      <div class="wrap">
        <div class="title">math_tekshiruvchi_bot — Control Center</div>
        <div class="muted">Microservice-ready: web/API va bot worker alohida ishlashi mumkin.</div>
        <div style="margin:12px 0 18px 0;">
          <a class="btn" href="/dashboard?secret={secret}">Refresh</a>
          <a class="btn" href="/dashboard/chart.png?secret={secret}">Chart PNG</a>
          <a class="btn" href="/api/metrics?secret={secret}">JSON</a>
          <a class="btn" href="/api/export.csv?secret={secret}">CSV</a>
        </div>
        <div class="grid">
          <div class="card"><div class="muted">Foydalanuvchilar</div><h2>{stats["users"]}</h2></div>
          <div class="card"><div class="muted">Testlar</div><h2>{stats["tests"]}</h2></div>
          <div class="card"><div class="muted">Natijalar</div><h2>{stats["results"]}</h2></div>
          <div class="card"><div class="muted">O‘rtacha ball</div><h2>{stats["avg"]}%</h2></div>
        </div>

        <div class="card" style="margin-top:16px;">
          <h3>Top foydalanuvchilar</h3>
          <table>
            <thead><tr><th>#</th><th>User</th><th>XP</th><th>Level</th></tr></thead>
            <tbody>{top_rows}</tbody>
          </table>
        </div>

        <div class="card" style="margin-top:16px;">
          <h3>So‘nggi natijalar</h3>
          <table>
            <thead><tr><th>User ID</th><th>Test</th><th>Score</th><th>Sana</th></tr></thead>
            <tbody>{recent_rows}</tbody>
          </table>
        </div>

        <div class="card" style="margin-top:16px;">
          <h3>Load balancing eslatmasi</h3>
          <p class="muted">Web xizmatini alohida worker bilan, bot worker’ni esa alohida process bilan ishga tushirish tavsiya qilinadi.</p>
        </div>
      </div>
    </body>
    </html>
    """

# =========================
# BOT STARTUP
# =========================
async def start_bot_polling() -> None:
    if bot is None:
        return
    await bot.delete_webhook(drop_pending_updates=True)
    while True:
        try:
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        except asyncio.CancelledError:
            break
        except Exception as e:
            await asyncio.sleep(5)
            await log_event("BOT_ERROR", None, str(e))

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.bot_task = None

    # webhook mode
    if APP_ROLE in ("all", "bot") and bot is not None and WEBHOOK_URL and WEBHOOK_SECRET:
        webhook_url = f"{WEBHOOK_URL.rstrip('/')}/webhook/{WEBHOOK_SECRET}"
        try:
            await bot.set_webhook(webhook_url, drop_pending_updates=True)
        except Exception as e:
            await log_event("WEBHOOK_SET_ERROR", None, str(e))

    # polling fallback
    elif APP_ROLE in ("all", "bot") and bot is not None:
        app.state.bot_task = asyncio.create_task(start_bot_polling())

    yield

    task = getattr(app.state, "bot_task", None)
    if task:
        task.cancel()
        try:
            await task
        except Exception:
            pass

    if bot is not None:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass

app = FastAPI(title="math_tekshiruvchi_bot Dashboard", lifespan=lifespan)

# =========================
# BOT HANDLERS
# =========================
@router.message(F.text == "/start")
async def cmd_start(message: Message):
    await upsert_user(
        message.from_user.id,
        message.from_user.username,
        message.from_user.full_name
    )
    await log_event("START", message.from_user.id, "/start")
    await message.answer(
        "🚀 <b>Xush kelibsiz!</b>\n\nBu botda testlar, AI ustoz, natijalar va profil bor.",
        reply_markup=main_menu_kb(message.from_user.id)
    )

@router.callback_query(F.data == "menu")
async def menu_cb(call: CallbackQuery):
    await call.message.edit_text(
        "🏠 <b>Bosh menyu</b>\n\nKerakli bo‘limni tanlang:",
        reply_markup=main_menu_kb(call.from_user.id)
    )
    await call.answer()

@router.callback_query(F.data == "tests:1")
async def tests_page(call: CallbackQuery):
    page = 1
    per_page = 5
    total = await db_value("SELECT COUNT(*) FROM tests WHERE active=1") or 0
    total_pages = max(1, (int(total) + per_page - 1) // per_page)
    offset = (page - 1) * per_page
    tests = await db_fetchall("""
        SELECT code, title, subject, difficulty, pdf_file_id
        FROM tests
        WHERE active=1
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """, {"limit": per_page, "offset": offset})
    items = [{"code": t["code"], "title": t["title"]} for t in tests]
    text = f"📚 <b>Testlar ro‘yxati</b>\n\nJami: {total}\nSahifa: {page}/{total_pages}\n\nTestni tanlang:"
    await call.message.edit_text(text, reply_markup=tests_kb(items, page, total_pages))
    await call.answer()

@router.callback_query(F.data.startswith("tests:"))
async def tests_pages(call: CallbackQuery):
    _, page_s = call.data.split(":")
    page = max(1, int(page_s))
    per_page = 5
    total = await db_value("SELECT COUNT(*) FROM tests WHERE active=1") or 0
    total_pages = max(1, (int(total) + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    tests = await db_fetchall("""
        SELECT code, title, subject, difficulty, pdf_file_id
        FROM tests
        WHERE active=1
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """, {"limit": per_page, "offset": offset})
    items = [{"code": t["code"], "title": t["title"]} for t in tests]
    text = f"📚 <b>Testlar ro‘yxati</b>\n\nJami: {total}\nSahifa: {page}/{total_pages}\n\nTestni tanlang:"
    await call.message.edit_text(text, reply_markup=tests_kb(items, page, total_pages))
    await call.answer()

@router.callback_query(F.data.startswith("test:"))
async def test_detail(call: CallbackQuery):
    code = call.data.split(":", 1)[1]
    test = await db_fetchone("SELECT * FROM tests WHERE code=:c", {"c": code})
    if not test:
        await call.answer("Test topilmadi", show_alert=True)
        return

    text = (
        f"📘 <b>{test['title']}</b>\n"
        f"🆔 Kod: <code>{test['code']}</code>\n"
        f"📌 Fan: {test['subject']}\n"
        f"📈 Qiyinlik: {test['difficulty']}\n"
        f"✅ Holat: {'Faol' if int(test.get('active', 1) or 1) == 1 else 'Nofaol'}\n"
    )
    if test.get("pdf_file_id"):
        try:
            await call.message.answer_document(test["pdf_file_id"], caption=text, reply_markup=make_back_kb("tests:1"))
        except Exception:
            await call.message.answer(text, reply_markup=make_back_kb("tests:1"))
    else:
        await call.message.answer(text, reply_markup=make_back_kb("tests:1"))
    await call.answer()

@router.callback_query(F.data == "check:start")
async def start_check(call: CallbackQuery, state: FSMContext):
    await state.set_state(TestCheckFlow.waiting_code)
    await call.message.answer("📝 Test kodini yuboring:", reply_markup=make_back_kb("menu"))
    await call.answer()

@router.message(TestCheckFlow.waiting_code, F.text)
async def check_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    test = await db_fetchone("SELECT * FROM tests WHERE code=:c AND active=1", {"c": code})
    if not test:
        await message.answer("❌ Test topilmadi. Qaytadan yuboring.", reply_markup=make_back_kb("menu"))
        return

    existed = await db_fetchone(
        "SELECT 1 FROM results WHERE user_id=:u AND test_code=:c",
        {"u": message.from_user.id, "c": code}
    )
    if existed:
        await state.clear()
        await message.answer("⚠️ Siz bu testni allaqachon ishlagansiz.", reply_markup=make_back_kb("menu"))
        return

    await state.update_data(test_code=code, answer_key=test["answer_key"])
    await state.set_state(TestCheckFlow.waiting_answers)
    await message.answer(
        "✍️ Javoblaringizni bitta qatorda yuboring.\nMasalan: <code>ABCDABCD</code>",
        reply_markup=make_back_kb("menu")
    )

@router.message(TestCheckFlow.waiting_answers, F.text)
async def check_answers(message: Message, state: FSMContext):
    data = await state.get_data()
    code = data["test_code"]
    key = normalize_answers(data["answer_key"])
    user_ans = normalize_answers(message.text)

    if not key:
        await message.answer("❌ Test javoblari noto‘g‘ri saqlangan.")
        await state.clear()
        return

    correct = sum(1 for a, b in zip(user_ans, key) if a == b)
    total = max(len(key), len(user_ans), 1)
    wrong = total - correct
    score = round((correct / total) * 100)
    elapsed = 0

    await db_exec("""
        INSERT INTO results(user_id, test_code, score, correct_count, wrong_count, elapsed_seconds)
        VALUES (:u, :c, :s, :cc, :wc, :e)
    """, {"u": message.from_user.id, "c": code, "s": score, "cc": correct, "wc": wrong, "e": elapsed})

    await add_xp(message.from_user.id, score)
    await log_event("TEST_CHECK", message.from_user.id, f"{code} => {score}%")
    await state.clear()

    text = (
        f"✅ <b>Natija</b>\n\n"
        f"🆔 Test: <code>{code}</code>\n"
        f"✔️ To‘g‘ri: {correct}\n"
        f"❌ Xato: {wrong}\n"
        f"📊 Foiz: {score}%\n"
        f"🏅 Ball: {score}\n"
    )
    await message.answer(text, reply_markup=result_kb(code))

@router.callback_query(F.data == "results:list")
async def results_list(call: CallbackQuery):
    rows = await db_fetchall("""
        SELECT test_code, score, correct_count, wrong_count, created_at
        FROM results
        WHERE user_id=:u
        ORDER BY created_at DESC
        LIMIT 20
    """, {"u": call.from_user.id})

    if not rows:
        await call.message.answer("📭 Hali natijalar yo‘q.", reply_markup=make_back_kb("menu"))
        await call.answer()
        return

    text = "📊 <b>Sizning natijalaringiz</b>\n\n"
    for r in rows:
        text += f"• <code>{r['test_code']}</code> — {r['score']}% (✔️ {r['correct_count']} / ❌ {r['wrong_count']})\n"
    await call.message.answer(text, reply_markup=make_back_kb("menu"))
    await call.answer()

@router.callback_query(F.data.startswith("ai:result:"))
async def ai_result(call: CallbackQuery):
    test_code = call.data.split(":")[-1]
    result = await db_fetchone(
        "SELECT * FROM results WHERE user_id=:u AND test_code=:c",
        {"u": call.from_user.id, "c": test_code}
    )
    if not result:
        await call.answer("Natija topilmadi", show_alert=True)
        return
    test = await db_fetchone("SELECT * FROM tests WHERE code=:c", {"c": test_code})
    prompt = f"""Quyidagi test natijasini tahlil qil:
Test: {test_code} - {test['title'] if test else ''}
Ball: {result['score']}%
To'g'ri: {result['correct_count']}
Xato: {result['wrong_count']}
O'zbekcha, qisqa va foydali tavsiya ber."""
    answer = await ask_ai(prompt, call.from_user.id)
    await call.message.answer(f"🤖 <b>AI tahlil</b>\n\n{answer}", reply_markup=make_back_kb("results:list"))
    await call.answer()

@router.callback_query(F.data == "cert:latest")
async def cert_latest(call: CallbackQuery):
    latest = await db_fetchone("""
        SELECT r.score, r.test_code, t.title
        FROM results r
        LEFT JOIN tests t ON t.code = r.test_code
        WHERE r.user_id=:u
        ORDER BY r.created_at DESC
        LIMIT 1
    """, {"u": call.from_user.id})
    if not latest:
        await call.answer("Sertifikat uchun natija yo‘q", show_alert=True)
        return

    if latest["score"] < MIN_PASS_PERCENT:
        await call.answer("Sertifikat uchun ball yetarli emas", show_alert=True)
        return

    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
    except Exception:
        await call.answer("PDF kutubxonasi yo‘q", show_alert=True)
        return

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    w, h = A4
    c.setFont("Helvetica-Bold", 22)
    c.drawString(80, h - 100, "CERTIFICATE")
    c.setFont("Helvetica", 14)
    c.drawString(80, h - 150, f"Name: {call.from_user.full_name}")
    c.drawString(80, h - 175, f"Test: {latest['title'] or latest['test_code']}")
    c.drawString(80, h - 200, f"Score: {latest['score']}%")
    c.drawString(80, h - 225, f"Date: {datetime.utcnow().strftime('%Y-%m-%d')}")
    c.showPage()
    c.save()
    buffer.seek(0)
    await call.message.answer_document(
        document=buffer.getvalue(),
        caption="📜 Sizning sertifikatingiz"
    )
    await call.answer()

@router.callback_query(F.data == "profile")
async def profile(call: CallbackQuery):
    stats = await get_user_stats(call.from_user.id)
    path = await recommend_path(call.from_user.id)
    text = (
        f"👤 <b>Profilim</b>\n\n"
        f"🆔 ID: <code>{call.from_user.id}</code>\n"
        f"👤 Username: @{call.from_user.username or 'yo‘q'}\n"
        f"📝 Ism: {call.from_user.full_name}\n"
        f"📚 Testlar: {stats['count']}\n"
        f"📊 O‘rtacha: {stats['avg']}%\n"
        f"🔥 XP: {stats['xp']}\n"
        f"⭐ Level: {stats['level']}\n"
        f"🏅 Eng yaxshi: {stats['best']}%\n"
        f"🎯 Tavsiya: {path}"
    )
    await call.message.answer(text, reply_markup=make_back_kb("menu"))
    await call.answer()

@router.callback_query(F.data == "leaderboard")
async def leaderboard(call: CallbackQuery):
    rows = await db_fetchall("""
        SELECT username, xp, level
        FROM users
        ORDER BY xp DESC
        LIMIT 10
    """)
    text = "🏆 <b>Top 10 reyting</b>\n\n"
    for i, r in enumerate(rows, 1):
        text += f"{i}. @{r['username'] or 'user'} — {r['xp']} XP (Lv {r['level']})\n"
    await call.message.answer(text or "Ma’lumot yo‘q", reply_markup=make_back_kb("menu"))
    await call.answer()

@router.callback_query(F.data == "ai:start")
async def ai_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(AIFlow.waiting_query)
    await call.message.answer(
        "🤖 Savolingizni yozing.\nRasm yoki ovoz yuborsangiz ham bo‘ladi.",
        reply_markup=make_back_kb("menu")
    )
    await call.answer()

@router.message(AIFlow.waiting_query, F.text)
async def ai_text(message: Message, state: FSMContext):
    if message.text.startswith("/"):
        return
    answer = await ask_ai(message.text, message.from_user.id)
    await message.answer(f"🤖 <b>AI ustoz javobi</b>\n\n{answer}")
    await state.clear()

@router.message(AIFlow.waiting_query, F.photo)
async def ai_photo(message: Message, state: FSMContext):
    await message.answer(await ocr_or_voice_stub("Rasm"), reply_markup=make_back_kb("menu"))
    await state.clear()

@router.message(AIFlow.waiting_query, F.voice)
async def ai_voice(message: Message, state: FSMContext):
    await message.answer(await ocr_or_voice_stub("Ovoz"), reply_markup=make_back_kb("menu"))
    await state.clear()

@router.callback_query(F.data == "contact:start")
async def contact_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(ContactFlow.waiting_message)
    await call.message.answer("📩 Adminga xabar yozing:", reply_markup=make_back_kb("menu"))
    await call.answer()

@router.message(ContactFlow.waiting_message, F.text)
async def contact_save(message: Message, state: FSMContext):
    await db_exec("""
        INSERT INTO contacts(user_id, message, created_at)
        VALUES (:u, :m, CURRENT_TIMESTAMP)
    """, {"u": message.from_user.id, "m": message.text[:4000]})
    await log_event("CONTACT", message.from_user.id, message.text)
    await state.clear()
    await message.answer("✅ Xabaringiz adminga yuborildi.", reply_markup=make_back_kb("menu"))

@router.message(F.text)
async def fallback_text(message: Message):
    if message.text.startswith("/"):
        return
    await message.answer("Menyu orqali bo‘lim tanlang yoki /start yuboring.", reply_markup=main_menu_kb(message.from_user.id))

# =========================
# ADMIN HANDLERS
# =========================
@router.callback_query(F.data == "admin:home")
async def admin_home(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    await call.message.answer("🛠 <b>Admin paneli</b>\n\nBo‘limni tanlang:", reply_markup=admin_menu_kb())
    await call.answer()

@router.callback_query(F.data == "admin:tests")
async def admin_tests(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    rows = await db_fetchall("SELECT code, title, subject, difficulty, active FROM tests ORDER BY created_at DESC LIMIT 20")
    if not rows:
        await call.message.answer("Testlar yo‘q.", reply_markup=admin_menu_kb())
        await call.answer()
        return
    text = "📝 <b>Test boshqaruvi</b>\n\n"
    kb_rows = []
    for r in rows:
        text += f"• <code>{r['code']}</code> — {r['title']} ({r['difficulty']})\n"
        kb_rows.append([InlineKeyboardButton(text=f"{r['code']} • {clamp_text(r['title'], 22)}", callback_data=f"test:{r['code']}")])
    kb_rows.append([InlineKeyboardButton(text="➕ Test qo‘shish", callback_data="admin:addtest")])
    kb_rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="admin:home")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Bosh menyu", callback_data="menu")])
    await call.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()

@router.callback_query(F.data == "admin:addtest")
async def admin_addtest(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    await state.set_state(AdminAddTestFlow.waiting_title)
    await call.message.answer("1/6 — Test nomini kiriting:", reply_markup=make_back_kb("admin:tests"))
    await call.answer()

@router.message(AdminAddTestFlow.waiting_title, F.text)
async def admin_add_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminAddTestFlow.waiting_code)
    await message.answer("2/6 — Test kodini kiriting (unique):", reply_markup=make_back_kb("admin:tests"))

@router.message(AdminAddTestFlow.waiting_code, F.text)
async def admin_add_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    exists = await db_fetchone("SELECT 1 FROM tests WHERE code=:c", {"c": code})
    if exists:
        await message.answer("Bu kod allaqachon bor. Yangi kod kiriting.")
        return
    await state.update_data(code=code)
    await state.set_state(AdminAddTestFlow.waiting_subject)
    await message.answer("3/6 — Fan / kategoriya kiriting:", reply_markup=make_back_kb("admin:tests"))

@router.message(AdminAddTestFlow.waiting_subject, F.text)
async def admin_add_subject(message: Message, state: FSMContext):
    await state.update_data(subject=message.text.strip())
    await state.set_state(AdminAddTestFlow.waiting_difficulty)
    await message.answer("4/6 — Qiyinlik: easy / medium / hard", reply_markup=make_back_kb("admin:tests"))

@router.message(AdminAddTestFlow.waiting_difficulty, F.text)
async def admin_add_difficulty(message: Message, state: FSMContext):
    diff = message.text.strip().lower()
    if diff not in {"easy", "medium", "hard"}:
        await message.answer("Faqat: easy / medium / hard")
        return
    await state.update_data(difficulty=diff)
    await state.set_state(AdminAddTestFlow.waiting_answers)
    await message.answer("5/6 — To‘g‘ri javoblar ketma-ketligini yuboring. Masalan: ABCDABCD", reply_markup=make_back_kb("admin:tests"))

@router.message(AdminAddTestFlow.waiting_answers, F.text)
async def admin_add_answers(message: Message, state: FSMContext):
    answers = normalize_answers(message.text)
    if len(answers) < 1:
        await message.answer("Javoblar noto‘g‘ri. Qaytadan yuboring.")
        return
    await state.update_data(answers=answers)
    await state.set_state(AdminAddTestFlow.waiting_pdf)
    await message.answer("6/6 — PDF fayl yuboring (document). Agar bo‘lmasa `skip` yozing.", reply_markup=make_back_kb("admin:tests"))

@router.message(AdminAddTestFlow.waiting_pdf, F.document)
async def admin_add_pdf(message: Message, state: FSMContext):
    data = await state.get_data()
    file_id = message.document.file_id
    await db_exec("""
        INSERT INTO tests(code, title, subject, difficulty, answer_key, pdf_file_id, active, created_at)
        VALUES (:c, :t, :s, :d, :a, :p, 1, CURRENT_TIMESTAMP)
    """, {
        "c": data["code"],
        "t": data["title"],
        "s": data["subject"],
        "d": data["difficulty"],
        "a": data["answers"],
        "p": file_id,
    })
    await log_event("ADMIN_ADD_TEST", message.from_user.id, data["code"])
    await state.clear()
    await message.answer("✅ Test qo‘shildi.", reply_markup=admin_menu_kb())

@router.message(AdminAddTestFlow.waiting_pdf, F.text)
async def admin_add_pdf_skip(message: Message, state: FSMContext):
    if message.text.strip().lower() != "skip":
        await message.answer("PDF yuboring yoki `skip` yozing.")
        return
    data = await state.get_data()
    await db_exec("""
        INSERT INTO tests(code, title, subject, difficulty, answer_key, pdf_file_id, active, created_at)
        VALUES (:c, :t, :s, :d, :a, '', 1, CURRENT_TIMESTAMP)
    """, {
        "c": data["code"],
        "t": data["title"],
        "s": data["subject"],
        "d": data["difficulty"],
        "a": data["answers"],
    })
    await log_event("ADMIN_ADD_TEST", message.from_user.id, data["code"])
    await state.clear()
    await message.answer("✅ Test qo‘shildi (PDFsiz).", reply_markup=admin_menu_kb())

@router.callback_query(F.data.startswith("admin:del:"))
async def admin_del_prompt(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    code = call.data.split(":")[-1]
    await state.set_state(AdminDeleteTestFlow.waiting_code)
    await state.update_data(code=code)
    await call.message.answer(
        f"🗑 <code>{code}</code> testini o‘chirishni tasdiqlang.\nTasdiqlash uchun kodni qayta yuboring:",
        reply_markup=make_back_kb("admin:tests")
    )
    await call.answer()

@router.message(AdminDeleteTestFlow.waiting_code, F.text)
async def admin_delete(message: Message, state: FSMContext):
    data = await state.get_data()
    code = data["code"]
    if message.text.strip().upper() != code:
        await message.answer("Kod mos kelmadi. Bekor qilindi.")
        await state.clear()
        return
    await db_exec("UPDATE tests SET active=0 WHERE code=:c", {"c": code})
    await log_event("ADMIN_DELETE_TEST", message.from_user.id, code)
    await state.clear()
    await message.answer("✅ Test o‘chirildi (nofaol qilindi).", reply_markup=admin_menu_kb())

@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    await state.set_state(AdminBroadcastFlow.waiting_message)
    await call.message.answer("📢 Hammaga yuboriladigan xabarni kiriting:", reply_markup=make_back_kb("admin:home"))
    await call.answer()

@router.message(AdminBroadcastFlow.waiting_message, F.text)
async def admin_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    users = await db_fetchall("SELECT user_id FROM users")
    sent = 0
    failed = 0
    for u in users:
        try:
            if bot is not None:
                await bot.send_message(int(u["user_id"]), message.text)
            sent += 1
        except Exception:
            failed += 1
    await log_event("ADMIN_BROADCAST", message.from_user.id, f"sent={sent};failed={failed}")
    await state.clear()
    await message.answer(f"✅ Yuborildi: {sent}\n❌ Fail: {failed}", reply_markup=admin_menu_kb())

@router.callback_query(F.data == "admin:statsmenu")
async def admin_stats_menu(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    await state.set_state(AdminStatsFlow.waiting_code)
    await call.message.answer("📊 Statistika uchun test kodini yuboring:", reply_markup=make_back_kb("admin:home"))
    await call.answer()

@router.message(AdminStatsFlow.waiting_code, F.text)
async def admin_stats_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    test = await db_fetchone("SELECT * FROM tests WHERE code=:c", {"c": code})
    if not test:
        await message.answer("Test topilmadi.")
        return
    rows = await db_fetchall("SELECT * FROM results WHERE test_code=:c ORDER BY score DESC", {"c": code})
    scores = [int(r["score"]) for r in rows] if rows else []
    avg = round(sum(scores) / len(scores), 1) if scores else 0
    best = max(scores) if scores else 0
    worst = min(scores) if scores else 0
    total = len(scores)
    pass_count = len([s for s in scores if s >= MIN_PASS_PERCENT])
    fail_rate = round(((total - pass_count) / total) * 100, 1) if total else 0
    text = (
        f"📊 <b>{test['title']}</b>\n"
        f"🆔 <code>{code}</code>\n\n"
        f"👥 Ishlaganlar: {total}\n"
        f"📈 O‘rtacha: {avg}%\n"
        f"🔥 Eng yuqori: {best}%\n"
        f"❄️ Eng past: {worst}%\n"
        f"✅ O‘tganlar: {pass_count}\n"
        f"⚠️ O‘tmaganlar ulushi: {fail_rate}%\n"
    )
    await message.answer(text, reply_markup=admin_test_item_kb(code))
    await state.clear()

@router.callback_query(F.data == "admin:users")
async def admin_users(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    users = await db_fetchall("""
        SELECT user_id, username, xp, level, streak, created_at
        FROM users
        ORDER BY xp DESC
        LIMIT 20
    """)
    text = "👥 <b>Foydalanuvchilar</b>\n\n"
    for u in users:
        text += f"• <code>{u['user_id']}</code> @{u['username'] or 'yo‘q'} — {u['xp']} XP, Lv {u['level']}\n"
    await call.message.answer(text or "Ma'lumot yo‘q", reply_markup=admin_menu_kb())
    await call.answer()

@router.callback_query(F.data == "admin:ai")
async def admin_ai(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    reqs = await db_value("SELECT COUNT(*) FROM logs WHERE kind='AI'") or 0
    errors = await db_value("SELECT COUNT(*) FROM logs WHERE kind='BOT_ERROR'") or 0
    await call.message.answer(
        f"🧠 <b>AI nazorat</b>\n\n"
        f"So‘rovlar: {reqs}\n"
        f"Xatolar: {errors}\n"
        f"AI holati: {'YONIQ' if AI_ENABLED else 'O‘CHIQ'}",
        reply_markup=admin_menu_kb()
    )
    await call.answer()

@router.callback_query(F.data == "admin:settings")
async def admin_settings(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    min_pass = await get_setting("min_pass_percent", str(MIN_PASS_PERCENT))
    ai_state = await get_setting("ai_enabled", "1")
    text = (
        "⚙️ <b>Sozlamalar</b>\n\n"
        f"Minimal o‘tish foizi: <b>{min_pass}%</b>\n"
        f"AI: <b>{'YONIQ' if ai_state == '1' else 'O‘CHIQ'}</b>\n\n"
        "O‘zgartirish uchun:\n"
        "<code>/set key value</code>\n"
        "Masalan: <code>/set min_pass_percent 60</code>"
    )
    await call.message.answer(text, reply_markup=admin_menu_kb())
    await call.answer()

@router.message(F.text.startswith("/set "))
async def admin_set(message: Message):
    if not is_admin(message.from_user.id):
        return
    try:
        _, key, value = message.text.split(maxsplit=2)
        await set_setting_value(key, value)
        await log_event("ADMIN_SET", message.from_user.id, f"{key}={value}")
        await message.answer("✅ Saqlandi.", reply_markup=admin_menu_kb())
    except Exception:
        await message.answer("Format: /set key value")

@router.callback_query(F.data == "admin:logs")
async def admin_logs(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    rows = await db_fetchall("""
        SELECT kind, user_id, detail, created_at
        FROM logs
        ORDER BY created_at DESC
        LIMIT 20
    """)
    text = "📁 <b>Loglar</b>\n\n"
    for r in rows:
        text += f"• [{r['kind']}] <code>{r['user_id']}</code> — {clamp_text(r['detail'], 60)}\n"
    await call.message.answer(text or "Log yo‘q", reply_markup=admin_menu_kb())
    await call.answer()

@router.callback_query(F.data == "admin:health")
async def admin_health(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    db_ok = "OK"
    ai_ok = "OK" if AI_ENABLED else "OFF"
    redis_ok = "OK" if redis_client else "OFF"
    await call.message.answer(
        f"🩺 <b>System Health</b>\n\n"
        f"DB: {db_ok}\n"
        f"AI: {ai_ok}\n"
        f"Redis: {redis_ok}\n"
        f"Bot role: {APP_ROLE}\n",
        reply_markup=admin_menu_kb()
    )
    await call.answer()

@router.callback_query(F.data.startswith("admin:teststats:"))
async def admin_teststats(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("Sizda ruxsat yo‘q", show_alert=True)
        return
    code = call.data.split(":")[-1]
    test = await db_fetchone("SELECT * FROM tests WHERE code=:c", {"c": code})
    if not test:
        await call.answer("Test topilmadi", show_alert=True)
        return
    rows = await db_fetchall("""
        SELECT user_id, score, correct_count, wrong_count, created_at
        FROM results
        WHERE test_code=:c
        ORDER BY score DESC
    """, {"c": code})
    text = f"📊 <b>{test['title']}</b>\n<code>{code}</code>\n\n"
    for r in rows[:20]:
        text += f"• {r['user_id']} — {r['score']}% (✔️ {r['correct_count']} / ❌ {r['wrong_count']})\n"
    if not rows:
        text += "Ma'lumot yo‘q.\n"
    await call.message.answer(text, reply_markup=admin_test_item_kb(code))
    await call.answer()

@router.message(F.text == "/reply")
async def admin_reply_help(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("Foydalanuvchiga javob: <code>/reply user_id xabar</code>")

@router.message(F.text.startswith("/reply "))
async def admin_reply_send(message: Message):
    if not is_admin(message.from_user.id):
        return
    try:
        _, uid, reply_text = message.text.split(maxsplit=2)
        uid_int = int(uid)
        if bot is not None:
            await bot.send_message(uid_int, f"📩 <b>Admin javobi</b>\n\n{reply_text}")
        await db_exec("""
            INSERT INTO contacts(user_id, message, admin_reply, replied_at)
            VALUES (:u, '', :r, CURRENT_TIMESTAMP)
        """, {"u": uid_int, "r": reply_text[:4000]})
        await log_event("ADMIN_REPLY", message.from_user.id, f"to={uid_int}")
        await message.answer("✅ Javob yuborildi.", reply_markup=admin_menu_kb())
    except Exception:
        await message.answer("Format: /reply user_id xabar")

# =========================
# DASHBOARD / API
# =========================
@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "role": APP_ROLE}

@app.get("/")
async def root() -> HTMLResponse:
    return HTMLResponse("<h1>math_tekshiruvchi_bot</h1><p>Dashboard: /dashboard?secret=...</p>")

@app.get("/dashboard")
async def dashboard(secret: str = "") -> HTMLResponse:
    if secret != DASHBOARD_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    stats = await dashboard_stats()
    return HTMLResponse(dashboard_html(stats, secret))

@app.get("/dashboard/chart.png")
async def dashboard_chart(secret: str = "") -> StreamingResponse:
    if secret != DASHBOARD_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    img = await chart_bytes()
    return StreamingResponse(io.BytesIO(img), media_type="image/png")

@app.get("/api/metrics")
async def api_metrics(secret: str = "") -> JSONResponse:
    if secret != DASHBOARD_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    stats = await dashboard_stats()
    return JSONResponse(stats)

@app.get("/api/export.csv")
async def export_csv(secret: str = "") -> Response:
    if secret != DASHBOARD_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    rows = await db_fetchall("""
        SELECT user_id, test_code, score, correct_count, wrong_count, created_at
        FROM results
        ORDER BY created_at DESC
        LIMIT 1000
    """)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["user_id", "test_code", "score", "correct_count", "wrong_count", "created_at"])
    for r in rows:
        writer.writerow([r["user_id"], r["test_code"], r["score"], r["correct_count"], r["wrong_count"], r["created_at"]])
    return Response(buf.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=results.csv"})

@app.post("/webhook/{secret}")
async def telegram_webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    if bot is None:
        raise HTTPException(status_code=503, detail="bot disabled")

    data = await request.json()
    update = Update.model_validate(data)
    await dp.feed_update(bot, update)
    return {"ok": True}

# =========================
# MAIN
# =========================
async def main_bot_only():
    await init_db()
    if bot is None:
        raise RuntimeError("BOT_TOKEN kerak.")
    await start_bot_polling()

if __name__ == "__main__":
    if APP_ROLE == "bot":
        asyncio.run(main_bot_only())
    else:
        import uvicorn
        uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
