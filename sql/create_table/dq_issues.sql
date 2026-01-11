USE CMS_PDS_DATA;
GO

CREATE TABLE dq_issues (
    issue_id INT IDENTITY(1,1) PRIMARY KEY,
    run_id INT NOT NULL,
    provider_id VARCHAR(50) NULL,
    issue_type VARCHAR(100) NOT NULL,
    issue_details VARCHAR(500) NOT NULL,
    row_data NVARCHAR(MAX) NOT NULL,
    detected_at DATETIME2 NOT NULL,

    CONSTRAINT FK_dq_issues_etl_log
        FOREIGN KEY (run_id) REFERENCES etl_log(run_id)
);

GO