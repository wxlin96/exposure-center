-- 多机曝光任务中心 建表脚本（等价于 ORM create_all，生产环境可用）
CREATE TABLE IF NOT EXISTS machine (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    machine_id VARCHAR(64) NOT NULL UNIQUE COMMENT '机器唯一标识',
    name VARCHAR(128) NULL COMMENT '机器名称',
    base_url VARCHAR(256) NOT NULL COMMENT 'PS 服务 Base URL',
    status VARCHAR(16) DEFAULT 'offline' COMMENT 'online/offline',
    last_seen_at DATETIME NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='远程机器';

CREATE TABLE IF NOT EXISTS exposure_task (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL UNIQUE COMMENT '任务ID=机器侧job_id',
    machine_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending' COMMENT 'pending/configuring/split_issued/ric_uploading/starting/exposing/completed/failed/stopped',
    phase VARCHAR(32) NULL COMMENT '机器侧job phase',
    stripe_count INT DEFAULT 0,
    done_count INT DEFAULT 0,
    error_count INT DEFAULT 0,
    ric_required INT DEFAULT 0,
    ric_filename VARCHAR(256) NULL,
    error_msg TEXT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    KEY idx_task_machine (machine_id),
    KEY idx_task_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='曝光任务';

CREATE TABLE IF NOT EXISTS stripe_record (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    machine_id VARCHAR(64) NOT NULL,
    stripe_id VARCHAR(128) NOT NULL COMMENT 'job-001_stripe_0000',
    status VARCHAR(32) NOT NULL DEFAULT 'idle',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_task_stripe (task_id, stripe_id),
    KEY idx_stripe_task (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='条带记录';

CREATE TABLE IF NOT EXISTS task_event_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL,
    machine_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSON NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_event_task (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务事件流水';
