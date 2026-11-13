"""容量试验的派发、处理租约与自动恢复。

关键约束：
- 每个运行当前的处理者由 `lease_token` 唯一标识。领取、续租、提交和失败上报都是条件更新，
  只有 token 仍匹配且状态仍是 running 时才生效。因此失去处理资格的旧进程既不能覆盖结果，
  也不能把产物文件写到最终路径。
- 计算期间由 worker 后台线程续租。进程退出后租约过期，恢复扫描把运行置为 `recovering`
  并重新入队；重新计算沿用数据库中的冻结快照、顾客数、暖机、重复数、根种子和置信水平。
- 入队失败（派发丢失）不会把已接收的试验判为失败，而是保持可领取状态等待扫描器重派。
- completed / failed 是终态，恢复扫描只处理 queued、recovering 和租约已过期的 running。
"""
import json
import os
import socket
import time
import traceback
import uuid
from pathlib import Path

from redis import Redis
from sqlalchemy import update

from ..config.settings import ARTIFACT_DIR, DISPATCH_GRACE_SECONDS, REDIS_URL, WORKER_LEASE_SECONDS
from ..constants.status import CLAIMABLE_STATUSES, RunStatus
from ..models import SimulationRun
from .audit_service import log

queue = Redis.from_url(REDIS_URL, decode_responses=True)
QUEUE_KEY = "queue:jobs"
STALE_TEMP_SECONDS = 3600


def now_seconds():
    return time.time()


def worker_identity():
    return f"{socket.gethostname()}-{os.getpid()}"


def heartbeat_path():
    return Path(ARTIFACT_DIR) / "heartbeat"


def touch_heartbeat():
    try:
        heartbeat_path().write_text(str(now_seconds()))
    except OSError:
        pass


def artifact_path(run_id):
    return Path(ARTIFACT_DIR) / f"run-{run_id}.json"


def _purge_temps(run_id):
    """清掉该运行遗留的私有临时文件；只有最终产物路径保存被接受的结果。"""
    directory = Path(ARTIFACT_DIR)
    if not directory.exists():
        return
    for path in directory.glob(".*run-{}-*.tmp".format(run_id)):
        _unlink(path)


def _unlink(path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _serialize_result(result):
    return json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def write_artifact_atomic(run_id, result):
    """原子写最终产物路径：先写同目录临时文件再 os.replace。"""
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    temp = Path(ARTIFACT_DIR) / f".run-{run_id}-{uuid.uuid4().hex}.tmp"
    temp.write_text(_serialize_result(result))
    os.replace(temp, artifact_path(run_id))


def _conditional(db, run_id, values, *conditions):
    """只更新仍满足条件的行；synchronize_session=False 避免 ORM 内存态掩盖并发结果。"""
    statement = (
        update(SimulationRun)
        .where(SimulationRun.id == run_id)
        .where(*conditions)
        .values(**values)
        .execution_options(synchronize_session=False)
    )
    result = db.execute(statement)
    db.commit()
    return result.rowcount == 1


def dispatch_run(db, run_id):
    """把运行放入 Redis 队列；失败时清空 dispatched_at 供恢复扫描重试。"""
    try:
        queue.rpush(QUEUE_KEY, run_id)
    except Exception:
        traceback.print_exc()
        db.execute(
            update(SimulationRun)
            .where(SimulationRun.id == run_id, SimulationRun.status.in_(CLAIMABLE_STATUSES))
            .values(dispatched_at=0.0)
            .execution_options(synchronize_session=False)
        )
        db.commit()
        return False
    db.execute(
        update(SimulationRun)
        .where(SimulationRun.id == run_id, SimulationRun.status.in_(CLAIMABLE_STATUSES))
        .values(dispatched_at=now_seconds())
        .execution_options(synchronize_session=False)
    )
    db.commit()
    return True


def _still_queued(run_id):
    """作业仍在 Redis 队列中等待被领取时不要重复派发。"""
    try:
        return queue.lpos(QUEUE_KEY, run_id) is not None
    except Exception:
        traceback.print_exc()
        return False


def claim_run(db, run_id, owner):
    """领取运行并取得租约。返回 lease_token；未领取到返回 None。"""
    token = uuid.uuid4().hex
    now_value = now_seconds()
    claimed = _conditional(
        db,
        run_id,
        {
            "status": RunStatus.RUNNING,
            "lease_owner": owner,
            "lease_token": token,
            "lease_until": now_value + WORKER_LEASE_SECONDS,
            "dispatched_at": now_value,
            "attempts": SimulationRun.attempts + 1,
            "error": "",
        },
        SimulationRun.status.in_(CLAIMABLE_STATUSES),
        SimulationRun.lease_until <= now_value,
    )
    return token if claimed else None


def renew_lease(db, run_id, token):
    """心跳续租；token 失效后返回 False，旧进程此后提交会被拒绝。"""
    return _conditional(
        db,
        run_id,
        {"lease_until": now_seconds() + WORKER_LEASE_SECONDS},
        SimulationRun.status == RunStatus.RUNNING,
        SimulationRun.lease_token == token,
    )


def finish_run(db, run_id, token, result, actor, action="simulation.completed"):
    """条件提交结果：数据库先确认 token 有效，再原子落盘。

    失去处理资格时不会创建任何产物文件，最终产物路径保持原样。返回是否被接受。
    """
    accepted = _conditional(
        db,
        run_id,
        {
            "status": RunStatus.COMPLETED,
            "result": result,
            "error": "",
            "lease_owner": "",
            "lease_token": "",
            "lease_until": 0.0,
            "artifact_ready": 0,
        },
        SimulationRun.status == RunStatus.RUNNING,
        SimulationRun.lease_token == token,
    )
    if not accepted:
        return False
    # 结果已入库但尚未落盘：扫描器会在进程中途退出时按数据库结果补写产物
    try:
        write_artifact_atomic(run_id, result)
    except Exception:
        traceback.print_exc()
    else:
        _conditional(db, run_id, {"artifact_ready": 1}, SimulationRun.status == RunStatus.COMPLETED)
    log(db, actor, action, run_id)
    db.commit()
    return True


def fail_run(db, run_id, token, error, actor):
    """仅当前处理者可把运行标记为失败；失去资格时静默丢弃。"""
    failed = _conditional(
        db,
        run_id,
        {
            "status": RunStatus.FAILED,
            "error": error,
            "lease_owner": "",
            "lease_token": "",
            "lease_until": 0.0,
        },
        SimulationRun.status == RunStatus.RUNNING,
        SimulationRun.lease_token == token,
    )
    if failed:
        log(db, actor, "simulation.failed", run_id)
        db.commit()
    return failed


def _cleanup_stale_temps():
    """清理崩溃时残留的私有临时文件；最终产物文件名不以点开头，不受影响。"""
    directory = Path(ARTIFACT_DIR)
    if not directory.exists():
        return
    cutoff = now_seconds() - STALE_TEMP_SECONDS
    for path in directory.glob(".*.tmp"):
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            pass


def _recover_expired_leases(db):
    count = 0
    expired = (
        db.query(SimulationRun.id, SimulationRun.lease_token)
        .filter(
            SimulationRun.status == RunStatus.RUNNING,
            SimulationRun.lease_until <= now_seconds(),
        )
        .all()
    )
    for run_id, token in expired:
        # 以旧 token 为条件，保证同一中断只被一个扫描者恢复一次
        recovered = _conditional(
            db,
            run_id,
            {
                "status": RunStatus.RECOVERING,
                "recoveries": SimulationRun.recoveries + 1,
                "lease_owner": "",
                "lease_token": "",
                "lease_until": 0.0,
                # 归零派发时间，使本次扫描立即重新入队，不必再等一个宽限期
                "dispatched_at": 0.0,
                "error": "计算进程中断，等待自动恢复",
            },
            SimulationRun.status == RunStatus.RUNNING,
            SimulationRun.lease_token == token,
            SimulationRun.lease_until <= now_seconds(),
        )
        if recovered:
            _purge_temps(run_id)
            log(db, "recovery", "simulation.recovered", run_id)
            db.commit()
            count += 1
    return count


def _redispatch_lost(db):
    count = 0
    cutoff = now_seconds() - DISPATCH_GRACE_SECONDS
    pending = (
        db.query(SimulationRun.id, SimulationRun.status, SimulationRun.dispatched_at)
        .filter(
            SimulationRun.status.in_(CLAIMABLE_STATUSES),
            SimulationRun.dispatched_at <= cutoff,
        )
        .all()
    )
    for run_id, status, dispatched_at in pending:
        if _still_queued(run_id):
            # 已被派发、尚未被领取，继续等待而不是重复入队
            _conditional(db, run_id, {"dispatched_at": now_seconds()}, SimulationRun.status == status)
            continue
        # 条件推进 dispatched_at，避免扫描器与创建者重复派发同一批作业
        bumped = _conditional(
            db,
            run_id,
            {"dispatched_at": now_seconds()},
            SimulationRun.status == status,
            SimulationRun.dispatched_at == dispatched_at,
        )
        if bumped and not dispatch_run(db, run_id):
            count += 1
    return count


def _repair_missing_artifacts(db):
    """completed 但产物缺失（进程在落盘前退出）时用数据库中的结果补写，不改状态与审批。"""
    rebuilt = 0
    rows = (
        db.query(SimulationRun.id, SimulationRun.result)
        .filter(
            SimulationRun.status == RunStatus.COMPLETED,
            SimulationRun.artifact_ready == 0,
        )
        .all()
    )
    for run_id, result in rows:
        if result:
            try:
                write_artifact_atomic(run_id, result)
            except Exception:
                traceback.print_exc()
                continue
        _conditional(db, run_id, {"artifact_ready": 1}, SimulationRun.status == RunStatus.COMPLETED)
        rebuilt += 1
    return rebuilt


def recover_stale(db):
    """恢复租约过期与派发丢失的试验；completed/failed 与审批数据不在扫描范围。"""
    _cleanup_stale_temps()
    _repair_missing_artifacts(db)
    return _recover_expired_leases(db), _redispatch_lost(db)
