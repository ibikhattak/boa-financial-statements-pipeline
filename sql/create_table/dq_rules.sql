use CMS_PDS_DATA
CREATE TABLE dbo.dq_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    field_name VARCHAR(100) NOT NULL,
    rule_description VARCHAR(MAX) NOT NULL,
    rule_sql VARCHAR(MAX) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    active_flag BIT NOT NULL,
    rule_name VARCHAR(200) NOT NULL
);




