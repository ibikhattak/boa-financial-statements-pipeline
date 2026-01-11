CREATE TABLE dq_results (
    run_id INT,
    rule_id INT,
    providerCcn VARCHAR(20),
    field_name VARCHAR(100),
    failure_detail VARCHAR(MAX),
    run_timestamp DATETIME DEFAULT GETDATE(),
	effectiveDate DATETIME,
	nationalProviderIdentifier varchar(20) NULL,
	invalid_value varchar(max) null
);