CREATE TABLE dq_rule_summary (
    summary_id       INT IDENTITY(1,1) PRIMARY KEY,
    run_id           INT NOT NULL,
    rule_id          INT NOT NULL,
    rule_name        VARCHAR(255) NOT NULL,
    field_name       VARCHAR(255) NOT NULL,
    failure_count    INT NOT NULL,
    created_at       DATETIME DEFAULT GETDATE()
);