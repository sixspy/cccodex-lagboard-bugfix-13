
import hashlib, secrets, sqlite3
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 甲路径专用测试缝：抢冻闸机与逐节点钩子，生产环境保持默认值
_freeze_lock_gate = None
_cutoff_node_hook = staticmethod(lambda conn, row: None)


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _today():
    return datetime.now().date().isoformat()


class SealedCutoffError(Exception):
    """切断卡当天已封档，不许改口。"""


class ContentionError(Exception):
    """双席抢冻同一父源，后交只记争用。"""

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

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          sla_minutes INTEGER NOT NULL,
          output_mode TEXT NOT NULL,
          parent_id INTEGER REFERENCES sources(id)
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
        CREATE TABLE IF NOT EXISTS freeze_cutoffs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          root_id INTEGER NOT NULL,
          root_name TEXT NOT NULL,
          sealed_day TEXT NOT NULL,
          path TEXT NOT NULL,
          affected TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(root_id, sealed_day)
        );
        CREATE TABLE IF NOT EXISTS freeze_locks(
          root_id INTEGER PRIMARY KEY,
          holder TEXT NOT NULL,
          acquired_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS freeze_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          root_name TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 旧库迁移：补父子拓扑列（编排拓扑唯一来源，禁止第二套连线）
        cols = [r["name"] for r in conn.execute("PRAGMA table_info(sources)").fetchall()]
        if "parent_id" not in cols:
            conn.execute("ALTER TABLE sources ADD COLUMN parent_id INTEGER")
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode,parent_id) VALUES (?,?,?,?)", [
                ("订单源", 15, "可写", None),
                ("订单源-华东", 15, "可写", 1),
                ("订单源-华南", 15, "可写", 1),
                ("支付源", 20, "可写", None),
                ("支付源-直连", 20, "可写", 4),
                ("物流源", 30, "冻结", None),
                ("物流源-干线", 30, "冻结", 6),
            ])
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("支付源","2026-11-16T08:00:00","2026-11-16T08:35:00",35,"告警"))
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("物流源","2026-11-16T07:00:00","2026-11-16T09:10:00",130,"违约"))

def level_of(lag, sla):
    if lag <= sla: return "正常"
    if lag <= sla * 2: return "告警"
    return "违约"

def mode_of(level):
    return {"正常":"可写", "告警":"降级只读", "违约":"冻结"}[level]


def subtree_ids(conn, root_id):
    """递归 CTE 取一棵子树全部节点——父子拓扑的唯一读法。"""
    return [r["id"] for r in conn.execute(
        """WITH RECURSIVE tree(id) AS (
             SELECT id FROM sources WHERE id=?
             UNION ALL
             SELECT s.id FROM sources s JOIN tree t ON s.parent_id=t.id
           )
           SELECT id FROM tree ORDER BY id""", (root_id,)).fetchall()]


def record_event(kind, root_name, detail):
    """中断专册：只追加，永不改旧值。"""
    with connect() as c:
        c.execute("INSERT INTO freeze_events(kind,root_name,detail,created_at) VALUES (?,?,?,?)",
                  (kind, root_name, detail, datetime.now().isoformat(timespec="seconds")))
        c.commit()


def freeze_cutoff(conn, root, holder):
    """甲路径唯一口径：父子冻结捆成同一锁事务，遇挫则父子同撤。

    不混用乙路径（无影子表、无分段落库）。返回封档卡行；争用/已封档/传递失败分别抛异常。
    """
    root_id = root["id"]
    root_name = root["name"]
    today = _today()
    now = datetime.now().isoformat(timespec="seconds")

    card = conn.execute("SELECT id FROM freeze_cutoffs WHERE root_id=? AND sealed_day=?",
                        (root_id, today)).fetchone()
    if card:
        raise SealedCutoffError(f"{root_name} 的中断切断卡当日已封档，不许改口")

    if _freeze_lock_gate is not None:
        _freeze_lock_gate.wait()

    try:
        conn.execute("INSERT INTO freeze_locks(root_id,holder,acquired_at) VALUES (?,?,?)",
                     (root_id, holder, now))
    except sqlite3.IntegrityError:
        raise ContentionError(f"{root_name} 已被另一席位抢冻，后交只记传递中断争用")

    try:
        nodes = conn.execute(
            """WITH RECURSIVE tree(id) AS (
                 SELECT id FROM sources WHERE id=?
                 UNION ALL
                 SELECT s.id FROM sources s JOIN tree t ON s.parent_id=t.id
               )
               SELECT * FROM sources WHERE id IN tree ORDER BY id""", (root_id,)).fetchall()
        affected = [n["name"] for n in nodes]
        for row in nodes:
            _cutoff_node_hook(conn, row)  # 测试缝：模拟级联传递遇挫
            conn.execute("UPDATE sources SET output_mode='冻结' WHERE id=?", (row["id"],))
        cur = conn.execute(
            "INSERT INTO freeze_cutoffs(root_id,root_name,sealed_day,path,affected,status,created_at)"
            " VALUES (?,?,?,?,?,?,?)",
            (root_id, root_name, today, "甲", ",".join(affected), "已封档", now))
        conn.execute("INSERT INTO freeze_events(kind,root_name,detail,created_at) VALUES (?,?,?,?)",
                     ("封档", root_name, f"甲路径级联冻结 {len(affected)} 节点：{','.join(affected)}", now))
        card_row = conn.execute("SELECT * FROM freeze_cutoffs WHERE id=?", (cur.lastrowid,)).fetchone()
        conn.commit()
        return card_row
    except Exception:
        conn.rollback()  # 父子同撤：锁与所有部分冻结全部回滚，正式侧保持旧值
        record_event("传递失败", root_name, "甲路径事务遇挫，父子同撤，未留半树冻结")
        raise


def is_under_freeze(conn, source_name):
    row = conn.execute(
        """WITH RECURSIVE chain(id,parent_id,output_mode,name) AS (
             SELECT id,parent_id,output_mode,name FROM sources WHERE name=?
             UNION ALL
             SELECT s.id,s.parent_id,s.output_mode,s.name FROM sources s
             JOIN chain ch ON s.id=ch.parent_id
           )
           SELECT COUNT(*) c FROM chain WHERE output_mode='冻结'""", (source_name,)).fetchone()
    return row["c"] > 0

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/sources")
def sources(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM sources ORDER BY id").fetchall()]}

class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str

@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        # 中断切断先于写入：父源或本源已封冻，则写路径 409
        if is_under_freeze(c, body.source):
            record_event("偏卡", body.source, "冻结切断后仍试图写入到达，已拒绝（中断偏了）")
            raise HTTPException(409, "该源处于中断冻结切断中，只读不可写")
        # lag by minute rough parse
        def parse(t):
            return datetime.fromisoformat(t)
        lag = int((parse(body.ingest_clock) - parse(body.source_clock)).total_seconds() // 60)
        if lag < 0: lag = 0
        lv = level_of(lag, src["sla_minutes"])
        mode = mode_of(lv)
        c.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                  (body.source, body.source_clock, body.ingest_clock, lag, lv))
        if lv == "违约":
            # 父冻必须级联：走甲路径同一锁事务，禁止半树
            try:
                freeze_cutoff(c, src, holder="arrival")
                mode = "冻结"
            except SealedCutoffError as e:
                c.rollback()
                record_event("偏卡", body.source, str(e))
                raise HTTPException(409, str(e))
            except ContentionError as e:
                c.rollback()
                record_event("争用", body.source, str(e))
                raise HTTPException(409, str(e))
            except Exception:
                raise HTTPException(500, "中断级联传递失败，父子同撤，请重试")
        else:
            c.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode, body.source))
            c.commit()
        return {"lag_minutes": lag, "level": lv, "output_mode": mode}


@app.post("/api/cutoff/{source}")
def cutoff(source: str, authorization: str | None = Header(default=None)):
    """中断切断专卷入口（沿用本仓 API 前缀，不另起中断台）。"""
    require_user(authorization)
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        try:
            card = freeze_cutoff(c, src, holder="cutoff")
        except SealedCutoffError as e:
            c.rollback()
            record_event("偏卡", source, str(e))
            raise HTTPException(409, str(e))
        except ContentionError as e:
            c.rollback()
            record_event("争用", source, str(e))
            raise HTTPException(409, str(e))
        except Exception as e:
            raise HTTPException(500, f"中断级联传递失败，父子同撤：{e}")
        return {"ok": True, "card": dict(card)}

@app.get("/api/arrivals")
def arrivals(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM arrivals ORDER BY id DESC").fetchall()]}

@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        # 找该源所在树根；中断当天不许改口，次日方可补齐解冻
        root = c.execute(
            """WITH RECURSIVE chain(id,parent_id) AS (
                 SELECT id,parent_id FROM sources WHERE name=?
                 UNION ALL
                 SELECT s.id,s.parent_id FROM sources s JOIN chain ch ON s.id=ch.parent_id
               )
               SELECT * FROM sources WHERE id IN (SELECT id FROM chain WHERE parent_id IS NULL)""",
            (source,)).fetchone()
        card = c.execute("SELECT id FROM freeze_cutoffs WHERE root_id=? AND sealed_day=?",
                         (root["id"], _today())).fetchone()
        if card:
            c.rollback()
            record_event("偏卡", source, "封档当天试图补齐改口，已拒绝（中断偏了）")
            raise HTTPException(409, "中断切断卡当日已封档，当天不许改口；次日补齐后方可解冻")
        c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
        c.execute("INSERT INTO catchups(source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?)",
                  (source, "成功", "可写", "补齐后恢复可写", now))
        c.commit()
        return {"ok": True, "output_mode": "可写"}


@app.get("/api/freeze-events")
def freeze_events(authorization: str | None = Header(default=None)):
    """中断专册：封档/争用/传递失败/偏卡痕迹同看。"""
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute(
            "SELECT * FROM freeze_events ORDER BY id DESC").fetchall()]}

@app.get("/api/catchups")
def catchups(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM catchups ORDER BY id DESC").fetchall()]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
