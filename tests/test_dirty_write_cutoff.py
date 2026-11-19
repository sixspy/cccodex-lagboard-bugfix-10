import json
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app as app_module
from app import app, init_db

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    monkeypatch.delenv("LAGBOARD_TIMEOUT_AFTER", raising=False)
    monkeypatch.delenv("LAGBOARD_BATCH_DELAY", raising=False)
    init_db()
    yield


def login(client: TestClient) -> dict:
    r = client.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return {"Authorization": "Bearer " + r.json()["token"]}


def rows_for(client: TestClient, headers: dict, batch_key: str) -> list[dict]:
    items = client.get("/api/arrivals", headers=headers).json()["items"]
    return [x for x in items if x.get("batch_key") == batch_key]


# ---------------------------------------------------------------- 脏写失败测 1
def test_timeout_half_batch_must_roll_back_then_retry_single(monkeypatch):
    """夹具：超时半批。超时必须整批回退，重试后只能有一份正式行。"""
    case = load_fixture("batch_timeout_half.json")["request"]
    c = TestClient(app)
    h = login(c)

    monkeypatch.setenv("LAGBOARD_TIMEOUT_AFTER", "2")
    r1 = c.post("/api/batches", headers=h, json=case)
    assert r1.status_code == 504, r1.text
    # 关键断言：脏写半批不得留库
    assert rows_for(c, h, case["batch_key"]) == [], "超时仍留下半批，脏写未切断"

    monkeypatch.delenv("LAGBOARD_TIMEOUT_AFTER")
    r2 = c.post("/api/batches", headers=h, json=case)
    assert r2.status_code == 200, r2.text
    rows = rows_for(c, h, case["batch_key"])
    assert len(rows) == len(case["rows"]), "重试后到达卡片出现双份"
    assert sorted(x["batch_seq"] for x in rows) == list(range(1, len(case["rows"]) + 1))

    queue = [q for q in c.get("/api/catchup_queue", headers=h).json()["items"]
             if q["batch_key"] == case["batch_key"]]
    assert len(queue) == 1, "补采席队列出现双份"
    assert queue[0]["status"] == "已补采"


# ---------------------------------------------------------------- 脏写失败测 2
def test_retry_after_success_must_not_duplicate():
    """夹具：重试双份。成功后原样重试同一批次键，不得再落一份。"""
    case = load_fixture("batch_retry_duplicate.json")["request"]
    c = TestClient(app)
    h = login(c)

    r1 = c.post("/api/batches", headers=h, json=case)
    assert r1.status_code == 200, r1.text
    assert len(rows_for(c, h, case["batch_key"])) == len(case["rows"])

    r2 = c.post("/api/batches", headers=h, json=case)
    assert r2.status_code == 409, "第二次重试未拒绝，重试会写出双份"
    assert len(rows_for(c, h, case["batch_key"])) == len(case["rows"]), "重试写出双份到达"

    recon = [x for x in c.get("/api/reconciliation", headers=h).json()["items"]
             if x["ref_key"] == case["batch_key"]]
    assert len(recon) == 1, "对账附页出现双份"


# ---------------------------------------------------------------- 脏写失败测 3
def test_dual_seat_retry_requires_lock_and_records_contention(monkeypatch):
    """夹具：双席抢重。两席同时重试同一批，后交只记批量脏写争用，不落数据。"""
    case = load_fixture("batch_dual_seat_contention.json")["request"]
    monkeypatch.setenv("LAGBOARD_BATCH_DELAY", "0.6")

    c1 = TestClient(app)
    c2 = TestClient(app)
    h1 = login(c1)
    h2 = login(c2)
    barrier = threading.Barrier(2)
    results: list[int] = []
    lock = threading.Lock()

    def submit(client, headers):
        barrier.wait(timeout=5)
        r = client.post("/api/batches", headers=headers, json=case)
        with lock:
            results.append(r.status_code)

    t1 = threading.Thread(target=submit, args=(c1, h1))
    t2 = threading.Thread(target=submit, args=(c2, h2))
    t1.start(); t2.start(); t1.join(timeout=20); t2.join(timeout=20)

    assert sorted(results) == [200, 409], f"双席抢重未按锁裁决: {results}"
    c = TestClient(app)
    h = login(c)
    rows = rows_for(c, h, case["batch_key"])
    assert len(rows) == len(case["rows"]), "双席抢重落出双份到达"
    contentions = [x for x in c.get("/api/batch_contentions", headers=h).json()["items"]
                   if x["batch_key"] == case["batch_key"]]
    assert contentions, "缺脏写争用留痕，判红"


# ---------------------------------------------------------------- 脏写失败测 4
def test_deviation_from_cutoff_card_is_blocked_and_recorded(tmp_path, monkeypatch):
    """偏离脏写切断卡便拦下并记脏写偏了。"""
    case = load_fixture("batch_retry_duplicate.json")["request"]
    fake_card = tmp_path / "card.md"
    fake_card.write_text("# 脏写切断卡\n- 择定路径：甲\n- 封档状态：未封档\n", encoding="utf-8")
    monkeypatch.setattr(app_module, "CUTOFF_CARD", fake_card)

    c = TestClient(app)
    h = login(c)
    r = c.post("/api/batches", headers=h, json=case)
    assert r.status_code == 409, "偏离切断卡未被拦下"
    assert rows_for(c, h, case["batch_key"]) == [], "偏离切断卡仍发生脏写落库"
    deviations = c.get("/api/dirty_write_deviations", headers=h).json()["items"]
    assert any("脏写偏了" in d["reason"] for d in deviations), "偏离切断卡未记脏写偏了"


# ---------------------------------------------------------------- 验收：无双份
def test_acceptance_no_duplicates_in_ledgers():
    case = load_fixture("batch_retry_duplicate.json")["request"]
    c = TestClient(app)
    h = login(c)
    assert c.post("/api/batches", headers=h, json=case).status_code == 200
    c.post("/api/batches", headers=h, json=case)  # 重试须被拒

    arrivals = rows_for(c, h, case["batch_key"])
    pairs = [(x["batch_key"], x["batch_seq"]) for x in arrivals]
    assert len(pairs) == len(set(pairs)), "到达卡片双份"

    queue = [q["batch_key"] for q in c.get("/api/catchup_queue", headers=h).json()["items"]]
    assert len(queue) == len(set(queue)), "补采席队列双份"

    sources = [s["name"] for s in c.get("/api/sources", headers=h).json()["items"]]
    assert len(sources) == len(set(sources)), "源名录双份"

    wm = [w["source"] for w in c.get("/api/watermarks", headers=h).json()["items"]]
    assert len(wm) == len(set(wm)), "水位牌双份"

    recon = [(x["kind"], x["ref_key"]) for x in c.get("/api/reconciliation", headers=h).json()["items"]]
    assert len(recon) == len(set(recon)), "对账附页双份"
