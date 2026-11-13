import os
DATABASE_URL=os.environ.get("DATABASE_URL","postgresql+psycopg://queue:queue@db/queue")
REDIS_URL=os.environ.get("REDIS_URL","redis://redis:6379/0")
JWT_SECRET=os.environ.get("JWT_SECRET","local-queue-lab-change-this-key")
SEED_DEMO=os.environ.get("SEED_DEMO","true").lower()=="true"
# 处理租约与自动恢复：计算期间由 worker 续租；租约过期即视为计算中断
WORKER_LEASE_SECONDS=max(2.,float(os.environ.get("WORKER_LEASE_SECONDS","10")))
# 续租间隔必须小于租约，否则存活进程也会被判为中断
WORKER_RENEW_SECONDS=min(max(.5,float(os.environ.get("WORKER_RENEW_SECONDS","4"))),WORKER_LEASE_SECONDS/2)
RECOVERY_SCAN_SECONDS=max(.5,float(os.environ.get("RECOVERY_SCAN_SECONDS","2")))
# 派发宽限期：排队超过该时长仍未领取，视为派发丢失并重新入队
DISPATCH_GRACE_SECONDS=max(1.,float(os.environ.get("DISPATCH_GRACE_SECONDS","5")))
ARTIFACT_DIR=os.environ.get("ARTIFACT_DIR","/artifacts")
