from sqlalchemy import Float,ForeignKey,Integer,JSON
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now
class SimulationRun(Base):
    __tablename__="simulation_runs"
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]
    scenario_id:Mapped[int]=mapped_column(ForeignKey("service_scenarios.id"))
    customers:Mapped[int]
    warmup:Mapped[int]
    replications:Mapped[int]
    seed:Mapped[int]
    confidence:Mapped[float]
    input_snapshot:Mapped[dict]=mapped_column(JSON)
    status:Mapped[str]=mapped_column(default="queued")
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(default="")
    owner:Mapped[str]
    created_at:Mapped[str]=mapped_column(default=now)
    # 处理租约：只有持有当前 lease_token 的进程可以写入结果或产物文件
    lease_owner:Mapped[str]=mapped_column(default="")
    lease_token:Mapped[str]=mapped_column(default="")
    lease_until:Mapped[float]=mapped_column(Float,default=0.)
    attempts:Mapped[int]=mapped_column(Integer,default=0)
    recoveries:Mapped[int]=mapped_column(Integer,default=0)
    dispatched_at:Mapped[float]=mapped_column(Float,default=0.)
    # 产物文件是否已与数据库结果同步；恢复扫描只修复 completed 但文件缺失的运行
    artifact_ready:Mapped[int]=mapped_column(Integer,default=0)
