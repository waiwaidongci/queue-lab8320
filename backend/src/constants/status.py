from enum import StrEnum
class RunStatus(StrEnum):
    QUEUED = "queued"
    RECOVERING = "recovering"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
# 可被派发/领取的状态：recovering 表示处理进程中断或派发丢失后等待重新计算
CLAIMABLE_STATUSES = (RunStatus.QUEUED, RunStatus.RECOVERING)
