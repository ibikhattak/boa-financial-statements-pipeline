CREATE OR ALTER PROCEDURE run_dq_rules
AS
BEGIN
    SET NOCOUNT ON;

    -- Generate a new run_id for this DQ run
    DECLARE @run_id INT = (SELECT ISNULL(MAX(run_id), 0) + 1 FROM dq_results);

    DECLARE @rule_id INT;
    DECLARE @rule_sql NVARCHAR(MAX);
    DECLARE @sql_to_run NVARCHAR(MAX);

    -- Cursor over active rules
    DECLARE rule_cursor CURSOR FOR
        SELECT rule_id, rule_sql
        FROM dq_rules
        WHERE active_flag = 1
        ORDER BY rule_id;

    OPEN rule_cursor;
    FETCH NEXT FROM rule_cursor INTO @rule_id, @rule_sql;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Replace placeholders
        SET @sql_to_run = REPLACE(@rule_sql, '{run_id}', @run_id);
        SET @sql_to_run = REPLACE(@sql_to_run, '{rule_id}', @rule_id);

        -- Execute rule SQL
        EXEC(@sql_to_run);

        FETCH NEXT FROM rule_cursor INTO @rule_id, @rule_sql;
    END

    CLOSE rule_cursor;
    DEALLOCATE rule_cursor;
END;