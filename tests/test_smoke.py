
import pytest
from fastapi.testclient import TestClient
from app import app, init_db

@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path/"t.db"); init_db(); yield

def test_lag_freeze_and_catchup(monkeypatch):
    import app as A
    # 固定封档日与次日两个绝对日期，避免依赖真实时钟与跨用例桩残留
    monkeypatch.setattr(A, "_today", lambda: "2026-11-16")
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "源到达SLA"
    h = {"Authorization": "Bearer " + c.post("/api/login", json={"username":"admin","password":"123456"}).json()["token"]}
    r = c.post("/api/arrivals", headers=h, json={"source":"订单源","source_clock":"2026-11-16T10:00:00","ingest_clock":"2026-11-16T11:00:00"})
    assert r.status_code == 200 and r.json()["output_mode"] == "冻结"
    # 中断切断卡当天封档：不许改口
    assert c.post("/api/catchup/订单源", headers=h).status_code == 409
    # 次日补齐后方可解冻
    monkeypatch.setattr(A, "_today", lambda: "2026-11-17")
    assert c.post("/api/catchup/订单源", headers=h).json()["output_mode"] == "可写"
