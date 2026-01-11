USE CMS_PDS_DATA
CREATE TABLE dbo.dq_results (
    run_id INT NULL,
    rule_id INT NULL,
    providerCcn VARCHAR(20) NULL,
    field_name VARCHAR(100) NOT NULL,
    failure_detail VARCHAR(MAX) NOT NULL,
    run_timestamp DATETIME NULL,
    effectiveDate VARCHAR(20) NULL,
    nationalProviderIdentifier VARCHAR(20) NULL,
    invalid_value VARCHAR(MAX) NULL
);