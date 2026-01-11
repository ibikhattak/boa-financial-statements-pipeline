CREATE TABLE dq_rules (
    rule_id INT IDENTITY PRIMARY KEY,
    rule_name VARCHAR(200),
    field_name VARCHAR(100),
    rule_description VARCHAR(MAX),
    rule_sql VARCHAR(MAX),
    severity VARCHAR(20),   -- Error, Warning
    active_flag BIT
);

