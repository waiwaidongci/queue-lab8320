"""容量试验 worker：领取租约、续租计算、条件提交结果，并周期扫描恢复中断的运行。"""
import threading,time,traceback
from pathlib import Path
from .config.database import Base,engine,SessionLocal
from .config.settings import ARTIFACT_DIR,RECOVERY_SCAN_SECONDS,WORKER_RENEW_SECONDS
from .models import SimulationRun
from .services.comparison_service import calculate_run
from .services.recovery_service import (
    QUEUE_KEY,claim_run,fail_run,finish_run,queue,recover_stale,renew_lease,touch_heartbeat,worker_identity,
)

class LeaseHeartbeat:
    """计算期间后台续租并刷新心跳；一旦 token 失效就停止，旧进程随后无法提交结果。"""
    def __init__(self,run_id,token):
        self.run_id=run_id;self.token=token;self._stop=threading.Event()
        self._thread=threading.Thread(target=self._run,daemon=True)
    def __enter__(self):self._thread.start();return self
    def __exit__(self,*exc):self._stop.set();self._thread.join(timeout=WORKER_RENEW_SECONDS*2)
    def _run(self):
        while not self._stop.wait(WORKER_RENEW_SECONDS):
            touch_heartbeat()
            try:
                with SessionLocal() as db:
                    if not renew_lease(db,self.run_id,self.token):return
            except Exception:traceback.print_exc();return

def scan_recovery():
    try:
        with SessionLocal() as db:recover_stale(db)
    except Exception:traceback.print_exc()

def frozen_params(run):
    """重新计算只使用数据库中的冻结快照与根种子，不读取后续被改动的来源。"""
    return run.input_snapshot,run.customers,run.warmup,run.replications,run.seed,run.confidence

def process_job(job_id,actor):
    run_id=int(job_id)
    with SessionLocal() as db:
        run=db.get(SimulationRun,run_id)
        if not run:return
        # 领取前读取冻结参数；领取成功后不再依赖 ORM 缓存的状态
        params=frozen_params(run)
        token=claim_run(db,run_id,actor)
        if not token:return  # 已被其他处理者领取，或不再是可计算状态
    with LeaseHeartbeat(run_id,token):
        try:result=calculate_run(*params)
        except Exception as error:
            traceback.print_exc()
            with SessionLocal() as db:fail_run(db,run_id,token,str(error),actor)
            return
    # 结果只在 token 仍匹配且状态仍为 running 时落库落盘；失去资格的旧进程到此被静默丢弃
    with SessionLocal() as db:finish_run(db,run_id,token,result,actor)

def main():
    Base.metadata.create_all(engine)
    Path(ARTIFACT_DIR).mkdir(parents=True,exist_ok=True)
    actor=worker_identity()
    touch_heartbeat()
    last_scan=0.0
    while True:
        touch_heartbeat()
        if time.time()-last_scan>=RECOVERY_SCAN_SECONDS:scan_recovery();last_scan=time.time()
        try:job=queue.blpop(QUEUE_KEY,timeout=2)
        except Exception:job=None;traceback.print_exc();time.sleep(1)
        if not job:continue
        try:process_job(job[1],actor)
        except Exception:traceback.print_exc()

if __name__=="__main__":main()
