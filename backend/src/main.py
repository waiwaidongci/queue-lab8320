import logging
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlalchemy import text
from .config.database import Base,engine,SessionLocal
from .config.settings import RECOVERY_SCAN_SECONDS,SEED_DEMO
from . import models
from .routes import auth,studies,profiles,scenarios,runs,notes,audits
from .middlewares.errors import ErrorMiddleware
from .middlewares.rate_limit import RateLimitMiddleware
from .services.recovery_service import queue,recover_stale

def _recovery_loop(stop):
    while not stop.wait(RECOVERY_SCAN_SECONDS):
        try:
            with SessionLocal() as db:recover_stale(db)
        except Exception:logging.exception("run recovery scan failed")

@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(engine)
    if SEED_DEMO:
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:logging.exception("demo seed failed")
    # 即使 worker 进程不在，已接收的试验也不会一直停在 running：
    # 租约过期先转 recovering 并重新入队，派发丢失也会补发，等待处理者接手。
    stop=threading.Event();scanner=threading.Thread(target=_recovery_loop,args=(stop,),daemon=True);scanner.start()
    try:yield
    finally:stop.set();scanner.join(timeout=RECOVERY_SCAN_SECONDS*2)
app=FastAPI(title="栈桥窗口排队研究席",lifespan=lifespan)
app.add_middleware(ErrorMiddleware);app.add_middleware(RateLimitMiddleware)
for module in [auth,studies,profiles,scenarios,runs,notes,audits]:app.include_router(module.router)
@app.get("/health")
def health():
    with engine.connect() as c:c.execute(text("select 1"))
    queue.ping();return {"status":"healthy","service":"queue-lab"}
