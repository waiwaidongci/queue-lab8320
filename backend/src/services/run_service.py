import csv,io
from fastapi import HTTPException
from ..models import SimulationRun,ServiceScenario,ArrivalProfile,QueueStudy,CapacityNote
from ..utils.serialize import record
from .erlang_algorithm import erlang_c
from .audit_service import log
from .recovery_service import dispatch_run

PUBLIC_RUN_FIELDS=("id","name","scenario_id","customers","warmup","replications","seed","confidence","input_snapshot","status","result","error","owner","created_at","lease_owner","attempts","recoveries")

def public_run(row):
    # 不外泄 lease_token：它只在数据库内用于校验处理资格
    return {k:getattr(row,k) for k in PUBLIC_RUN_FIELDS}

def list_runs(db):return [public_run(r) for r in db.query(SimulationRun).order_by(SimulationRun.id.desc())]
def get_run(db,rid):
    row=db.get(SimulationRun,rid)
    if not row:raise HTTPException(404,"运行不存在")
    return row

def create_snapshot(db,scenario):
    profile=db.get(ArrivalProfile,scenario.profile_id);study=db.get(QueueStudy,profile.study_id)
    return {"study":record(study),"profile":record(profile),"scenario":record(scenario),"time_unit":"minutes","rate_unit":"requests/minute"}
def create_run(db,data,user):
    scenario=db.get(ServiceScenario,data.scenario_id)
    if not scenario:raise HTTPException(404,"服务情景不存在")
    snapshot=create_snapshot(db,scenario)
    try:erlang_c(snapshot['profile']['arrival_rate'],1/scenario.mean_service_minutes,scenario.servers)
    except ValueError as e:raise HTTPException(422,str(e))
    if data.customers-data.warmup<100 or data.customers*data.replications>250000:raise HTTPException(422,"至少保留100顾客，且单任务总顾客不超过250000")
    row=SimulationRun(**data.model_dump(),input_snapshot=snapshot,owner=user.name)
    db.add(row);db.flush();log(db,user.email,"simulation.enqueue",row.id);db.commit()
    # 派发丢失不判失败：试验保持可领取状态，恢复扫描会补发，worker 也会按冻结参数重算
    dispatch_run(db,row.id)
    return public_run(row)
def export_run(db,rid):
    row=get_run(db,rid)
    if row.status!="completed" or not db.query(CapacityNote).filter_by(run_id=rid,status="approved").first():raise HTTPException(403,"仅获批的完成运行可导出")
    out=io.StringIO();w=csv.writer(out);w.writerow(['replication','mean_wait_minutes','mean_system_minutes','wait_probability','busy_fraction_window','window_minutes','retained_customers','arrival_spawn_key','service_spawn_key','analytic_wait_minutes','input_hash'])
    for x in row.result['replications']:w.writerow([x[k] for k in ['replication','mean_wait_minutes','mean_system_minutes','wait_probability','busy_fraction_window','window_minutes','retained_customers','arrival_spawn_key','service_spawn_key']]+[row.result['analytical']['mean_wait_minutes'],row.result['input_hash']])
    return out.getvalue()
