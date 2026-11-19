"""中断切断（冻结级联）专卷测试。

口径：本仓只走甲路径——父子冻结捆成同一锁事务，遇挫则父子同撤。
红测先行：本文件在修复前必须全红，修复后全绿；禁止只改编排连线。
"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import app as A
from app import app, connect, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr(A, "_freeze_lock_gate", None)
    monkeypatch.setattr(A, "_cutoff_node_hook", staticmethod(lambda conn, row: None))
    init_db()
    yield


def _headers():
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return {"Authorization": "Bearer " + tok}


def make_tree(parent="测试父", children=("测试子1", "测试子2"), sla=15):
    """先造父冻子仍写夹具：一棵可写的父子源树。"""
    with connect() as c:
        c.execute(
            "INSERT INTO sources(name,sla_minutes,output_mode,parent_id) VALUES (?,?,?,?)",
            (parent, sla, "可写", None),
        )
        pid = c.execute("SELECT id FROM sources WHERE name=?", (parent,)).fetchone()["id"]
        for ch in children:
            c.execute(
                "INSERT INTO sources(name,sla_minutes,output_mode,parent_id) VALUES (?,?,?,?)",
                (ch, sla, "可写", pid),
            )


def modes():
    with connect() as c:
        return {r["name"]: r["output_mode"] for r in c.execute("SELECT name,output_mode FROM sources")}


def events(kind=None):
    with connect() as c:
        q = "SELECT * FROM freeze_events" + (" WHERE kind=?" if kind else "")
        rows = c.execute(q, (kind,) if kind else ()).fetchall()
        return [dict(r) for r in rows]


def cards():
    with connect() as c:
        return [dict(r) for r in c.execute("SELECT * FROM freeze_cutoffs")]


def test_parent_freeze_cascades_and_blocks_child_writes():
    """红测1的正面口径：父冻必须级联到整棵子树，且子源写入被切断。"""
    make_tree()
    h = _headers()
    c = TestClient(app)
    r = c.post(
        "/api/arrivals",
        headers=h,
        json={"source": "测试父", "source_clock": "2026-11-16T10:00:00", "ingest_clock": "2026-11-16T11:00:00"},
    )
    assert r.status_code == 200 and r.json()["output_mode"] == "冻结"

    got = modes()
    assert got["测试父"] == "冻结"
    assert got["测试子1"] == "冻结", "半树撕裂：子1未随父冻结"
    assert got["测试子2"] == "冻结", "半树撕裂：子2未随父冻结"

    cs = cards()
    assert len(cs) == 1 and cs[0]["path"] == "甲" and cs[0]["status"] == "已封档"
    assert any(e["kind"] == "封档" for e in events())

    before = len(c.get("/api/arrivals", headers=h).json()["items"])
    rw = c.post(
        "/api/arrivals",
        headers=h,
        json={"source": "测试子1", "source_clock": "2026-11-16T10:05:00", "ingest_clock": "2026-11-16T10:10:00"},
    )
    assert rw.status_code == 409, "父冻后子源仍可写：中断未切断"
    after = len(c.get("/api/arrivals", headers=h).json()["items"])
    assert before == after


def test_cutoff_transaction_rolls_back_together():
    """传递失败红测：任一子节点冻结遇挫，父子同撤，禁止半树冻结。"""
    make_tree()

    def boom(conn, row):
        if row["name"] == "测试子2":
            raise RuntimeError("注入的级联传递失败")

    A._cutoff_node_hook = boom
    h = _headers()
    r = TestClient(app).post("/api/cutoff/测试父", headers=h)
    assert r.status_code >= 400

    got = modes()
    assert got["测试父"] == "可写", "父节点未随事务同撤"
    assert got["测试子1"] == "可写", "半树失败：子1被冻而子2失败"
    assert got["测试子2"] == "可写"
    assert cards() == [], "回滚后不得留下封档卡"
    fails = events("传递失败")
    assert len(fails) == 1, "缺传递失败痕迹"
    assert not (got["测试子1"] == "冻结" and got["测试子2"] == "可写")


def test_dual_seat_freeze_contention():
    """双席抢冻红测：调度席与编排席同时抢冻同一父源，后交只记争用。"""
    make_tree(parent="争用父", children=("争用子",))
    gate = threading.Barrier(2, timeout=10)
    A._freeze_lock_gate = gate
    tokens = []
    cl = TestClient(app)
    for _ in range(2):
        tok = cl.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
        tokens.append({"Authorization": "Bearer " + tok})

    def fire(headers):
        client = TestClient(app)
        resp = client.post("/api/cutoff/争用父", headers=headers)
        return resp.status_code, resp.json()

    with ThreadPoolExecutor(max_workers=2) as ex:
        results = list(ex.map(fire, tokens))

    statuses = sorted(s for s, _ in results)
    assert statuses == [200, 409], f"抢冻结果应为一成一争，实为 {statuses}"
    loser = [j for s, j in results if s == 409][0]
    assert "争用" in loser["detail"]

    got = modes()
    assert got["争用父"] == "冻结" and got["争用子"] == "冻结"
    assert len(cards()) == 1, "争用败方不得写入第二张切断卡"
    contentions = events("争用")
    assert len(contentions) == 1, "缺中断争用痕迹"
    assert all(e["kind"] != "传递失败" for e in events())


def test_sealed_card_rejects_same_day_change(monkeypatch):
    """中断当天不许改口：封档后再切/补齐均拒绝并记偏卡。"""
    monkeypatch.setattr(A, "_today", lambda: "2026-11-16")
    make_tree(parent="封档父", children=("封档子",))
    h = _headers()
    c = TestClient(app)
    assert c.post("/api/cutoff/封档父", headers=h).status_code == 200
    again = c.post("/api/cutoff/封档父", headers=h)
    assert again.status_code == 409 and "争用" not in again.json()["detail"]
    assert len(cards()) == 1
    assert any(e["kind"] == "偏卡" for e in events()), "偏离切断卡须记下中断偏了"

    r = c.post("/api/catchup/封档父", headers=h)
    assert r.status_code == 409, "封档当天不许改口解冻"

    monkeypatch.setattr(A, "_today", lambda: "2026-11-17")
    assert c.post("/api/catchup/封档父", headers=h).status_code == 200


def test_sources_api_is_one_tree():
    """编排画布与源目录牌同树：单一接口带 parent_id，无两套连线。"""
    make_tree(parent="树父", children=("树子",))
    h = _headers()
    items = TestClient(app).get("/api/sources", headers=h).json()["items"]
    by_name = {r["name"]: r for r in items}
    assert "parent_id" in by_name["树子"]
    assert by_name["树子"]["parent_id"] == by_name["树父"]["id"]
    assert by_name["树父"]["parent_id"] is None
