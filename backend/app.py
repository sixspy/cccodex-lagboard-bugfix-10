import hashlib, os, secrets, sqlite3, time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
DOCS = ROOT / "docs"
CUTOFF_CARD = DOCS / "脏写切断卡.md"
CUTOFF_REGISTER = DOCS / "批量超时脏写重试切断簿册.md"
SESSIONS: dict[str, str] = {}

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

# 脏写切断：只走切断簿册择定的一种（乙：批次幂等键条件插入）。
# 甲路径（整批事务回退）不得混用：本仓不实现甲路径分支。
CUTOFF_PATH = "乙"
CUTOFF_LOCK_DATE = "2026-11-19"
CUTOFF_SEAL = "ba72383994cfd6a8b78a47ed642289ebf5a9cec6736849b38694947697f11130"


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


def require_session(authorization: str | None) -> tuple[str, str]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    tok = authorization.removeprefix("Bearer ").strip()
    u = SESSIONS.get(tok)
    if not u:
        raise HTTPException(401, "登录已失效")
    return u, tok


app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def _ensure_column(conn, table: str, column: str, decl: str):
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          sla_minutes INTEGER NOT NULL,
          output_mode TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS arrivals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          source_clock TEXT NOT NULL,
          ingest_clock TEXT NOT NULL,
          lag_minutes INTEGER NOT NULL,
          level TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS catchups(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          status TEXT NOT NULL,
          restored_mode TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_key TEXT NOT NULL,
          source TEXT NOT NULL,
          seat TEXT NOT NULL,
          status TEXT NOT NULL,
          row_count INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS batch_locks(
          batch_key TEXT PRIMARY KEY,
          seat TEXT NOT NULL,
          acquired_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS batch_contentions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_key TEXT NOT NULL,
          seat TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS catchup_queue(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_key TEXT NOT NULL,
          source TEXT NOT NULL,
          seat TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS reconciliation_entries(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          ref_key TEXT NOT NULL,
          source TEXT NOT NULL,
          summary TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dirty_write_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          reason TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        _ensure_column(conn, "arrivals", "batch_key", "TEXT")
        _ensure_column(conn, "arrivals", "batch_seq", "INTEGER")
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode) VALUES (?,?,?)", [
                ("订单源", 15, "可写"), ("支付源", 20, "降级只读"), ("物流源", 30, "冻结"),
            ])
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("支付源", "2026-11-16T08:00:00", "2026-11-16T08:35:00", 35, "告警"))
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("物流源", "2026-11-16T07:00:00", "2026-11-16T09:10:00", 130, "违约"))


def level_of(lag, sla):
    if lag <= sla:
        return "正常"
    if lag <= sla * 2:
        return "告警"
    return "违约"


def mode_of(level):
    return {"正常": "可写", "告警": "降级只读", "违约": "冻结"}[level]


def _severity(level: str) -> int:
    return {"正常": 0, "告警": 1, "违约": 2}[level]


def _fault_timeout_after():
    raw = os.environ.get("LAGBOARD_TIMEOUT_AFTER")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fault_delay_seconds() -> float:
    raw = os.environ.get("LAGBOARD_BATCH_DELAY")
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def _record_deviation(conn, reason: str, detail: str) -> None:
    conn.execute("INSERT INTO dirty_write_deviations(reason,detail,created_at) VALUES (?,?,?)",
                 (reason, detail, _now()))


def cutoff_card_ok() -> tuple[bool, str]:
    """核对切断卡是否与簿册择定一致且已封档；不一致即视为偏离。"""
    if not CUTOFF_CARD.exists():
        return False, "脏写切断卡缺失"
    text = CUTOFF_CARD.read_text(encoding="utf-8")
    if "择定路径：乙" not in text:
        return False, "切断卡择定路径非乙"
    if "封档状态：已封档" not in text:
        return False, "切断卡未封档"
    if CUTOFF_PATH != "乙":
        return False, "运行路径与簿册择定不一致"
    return True, ""


def _batch_already_committed(conn, batch_key: str) -> bool:
    row = conn.execute("SELECT 1 FROM batches WHERE batch_key=? AND status='完成' LIMIT 1",
                       (batch_key,)).fetchone()
    return row is not None


def _record_contention(conn, batch_key: str, seat: str, detail: str) -> None:
    conn.execute("INSERT INTO batch_contentions(batch_key,seat,detail,created_at) VALUES (?,?,?,?)",
                 (batch_key, seat, detail, _now()))


def _acquire_dirty_lock(conn, batch_key: str, seat: str) -> bool:
    """抢占脏写锁；后到者返回 False，只记批量脏写争用。"""
    now = _now()
    try:
        conn.execute("INSERT INTO batch_locks(batch_key,seat,acquired_at) VALUES (?,?,?)",
                     (batch_key, seat, now))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        conn.rollback()
        return False


@app.on_event("startup")
def _s():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?",
                        (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/sources")
def sources(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM sources ORDER BY id").fetchall()]}


class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str


@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
        if not src:
            raise HTTPException(404, "未知源")
        lag = int((datetime.fromisoformat(body.ingest_clock) - datetime.fromisoformat(body.source_clock)).total_seconds() // 60)
        if lag < 0:
            lag = 0
        lv = level_of(lag, src["sla_minutes"])
        mode = mode_of(lv)
        c.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                  (body.source, body.source_clock, body.ingest_clock, lag, lv))
        c.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode, body.source))
        return {"lag_minutes": lag, "level": lv, "output_mode": mode}


@app.get("/api/arrivals")
def arrivals(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM arrivals ORDER BY id DESC").fetchall()]}


class BatchRowIn(BaseModel):
    source_clock: str
    ingest_clock: str


class BatchIn(BaseModel):
    batch_key: str
    source: str
    rows: list[BatchRowIn]


@app.post("/api/batches")
def add_batch(body: BatchIn, authorization: str | None = Header(default=None)):
    """乙路径：批次幂等键条件插入。整批单事务提交；遇重复键拒绝第二次并保留首批正式行。"""
    user, tok = require_session(authorization)
    seat = f"{user}#{tok[:6]}"
    if not body.rows:
        raise HTTPException(400, "空批次")

    ok, why = cutoff_card_ok()
    if not ok:
        # 偏离脏写切断卡：拦下并记「脏写偏了」，不落任何数据。
        with connect() as conn:
            _record_deviation(conn, "脏写偏了", f"{why}｜batch_key={body.batch_key}｜seat={seat}")
        raise HTTPException(409, f"偏离脏写切断卡：{why}")

    with connect() as conn:
        src = conn.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
        if not src:
            raise HTTPException(404, "未知源")

    delay = _fault_delay_seconds()
    if delay:
        time.sleep(delay)

    conn = connect()
    try:
        # 条件插入（幂等键）：已正式落库则拒绝第二次，保留首批正式行。
        if _batch_already_committed(conn, body.batch_key):
            owner = conn.execute("SELECT seat FROM batch_locks WHERE batch_key=?", (body.batch_key,)).fetchone()
            if owner and owner["seat"] != seat:
                _record_contention(conn, body.batch_key, seat, "批量脏写争用：异席重复提交已落库批次")
                conn.commit()
            raise HTTPException(409, "重复批次键")

        # 脏写锁：两席同时重试同一批，只有先到者获得写权，后交只记批量脏写争用。
        if not _acquire_dirty_lock(conn, body.batch_key, seat):
            _record_contention(conn, body.batch_key, seat, "批量脏写争用：脏写锁已被占用")
            conn.commit()
            raise HTTPException(409, "批量脏写争用")

        now = _now()
        fail_after = _fault_timeout_after()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO batches(batch_key,source,seat,status,row_count,created_at) VALUES (?,?,?,?,?,?)",
                     (body.batch_key, body.source, seat, "接收中", 0, now))
        total = 0
        worst_level = "正常"
        for idx, row in enumerate(body.rows, 1):
            lag = int((datetime.fromisoformat(row.ingest_clock) - datetime.fromisoformat(row.source_clock)).total_seconds() // 60)
            if lag < 0:
                lag = 0
            lv = level_of(lag, src["sla_minutes"])
            if _severity(lv) > _severity(worst_level):
                worst_level = lv
            conn.execute(
                "INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level,batch_key,batch_seq) VALUES (?,?,?,?,?,?,?)",
                (body.source, row.source_clock, row.ingest_clock, lag, lv, body.batch_key, idx))
            total += 1
            if fail_after is not None and total >= fail_after:
                # 超时：整批回退（禁止半批留库），同时释放脏写锁。
                conn.rollback()
                conn.execute("DELETE FROM batch_locks WHERE batch_key=?", (body.batch_key,))
                conn.commit()
                raise HTTPException(504, "批次超时")

        conn.execute("UPDATE batches SET status='完成', row_count=? WHERE batch_key=?", (total, body.batch_key))
        conn.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode_of(worst_level), body.source))
        conn.execute("INSERT INTO catchup_queue(batch_key,source,seat,status,created_at) VALUES (?,?,?,?,?)",
                     (body.batch_key, body.source, seat, "已补采", now))
        conn.execute("INSERT INTO reconciliation_entries(kind,ref_key,source,summary,created_at) VALUES ('批次',?,?,?,?)",
                     (body.batch_key, body.source, f"批次入账 {total} 行", now))
        conn.commit()
        return {"ok": True, "batch_key": body.batch_key, "accepted": total, "seat": seat}
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.get("/api/batches")
def list_batches(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM batches ORDER BY id DESC").fetchall()]}


@app.get("/api/batch_contentions")
def list_contentions(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM batch_contentions ORDER BY id DESC").fetchall()]}


@app.get("/api/catchup_queue")
def catchup_queue(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM catchup_queue ORDER BY id DESC").fetchall()]}


@app.get("/api/watermarks")
def watermarks(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""
            SELECT s.name AS source, s.sla_minutes, s.output_mode,
                   a.id AS arrival_id, a.lag_minutes, a.level, a.batch_key, a.ingest_clock
            FROM sources s LEFT JOIN arrivals a ON a.id = (
              SELECT id FROM arrivals WHERE source=s.name ORDER BY id DESC LIMIT 1)
            ORDER BY s.id
        """).fetchall()
        return {"items": [dict(r) for r in rows]}


@app.get("/api/reconciliation")
def reconciliation(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM reconciliation_entries ORDER BY id DESC").fetchall()]}


@app.get("/api/dirty_write_deviations")
def dirty_write_deviations(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM dirty_write_deviations ORDER BY id DESC").fetchall()]}


@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = _now()
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
        if not src:
            raise HTTPException(404, "未知源")
        c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
        c.execute("INSERT INTO catchups(source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?)",
                  (source, "成功", "可写", "补齐后恢复可写", now))
        return {"ok": True, "output_mode": "可写"}


@app.get("/api/catchups")
def catchups(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute("SELECT * FROM catchups ORDER BY id DESC").fetchall()]}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
