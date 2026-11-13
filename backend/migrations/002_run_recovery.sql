-- 容量试验自动恢复：处理租约、领取尝试次数、恢复次数与派发时间。
-- lease_token 是每次领取的唯一凭据；旧处理者失去该凭据后不得写入结果或产物文件。
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS lease_owner VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS lease_token VARCHAR NOT NULL DEFAULT '';
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS lease_until DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS recoveries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS dispatched_at DOUBLE PRECISION NOT NULL DEFAULT 0;
ALTER TABLE simulation_runs ADD COLUMN IF NOT EXISTS artifact_ready INTEGER NOT NULL DEFAULT 0;
-- 已有完成运行的产物在部署前已由旧 worker 写入，标记为已同步
UPDATE simulation_runs SET artifact_ready = 1 WHERE status = 'completed';
CREATE INDEX IF NOT EXISTS run_recovery_idx ON simulation_runs (status, lease_until);
CREATE INDEX IF NOT EXISTS run_dispatch_idx ON simulation_runs (status, dispatched_at);
