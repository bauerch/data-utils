USE [master];
GO

/*
Drops the given external table, if it exists in the logical data warehouse.

@param name: External table name.
*/
CREATE OR ALTER PROCEDURE [usp_drop_external_table_if_exists]
    @name SYSNAME
AS BEGIN
    IF (0 <> (SELECT COUNT(*) FROM sys.external_tables WHERE [object_id] = OBJECT_ID(@name)))
    BEGIN
        PRINT '';
        PRINT '*** Dropping external table: ' + @name;
        DECLARE @drop_stmt VARCHAR(200) = N'DROP EXTERNAL TABLE ' + @name;
        EXEC sp_executesql @tsql = @drop_stmt;
    END
END
GO

/*
Drops the given external file format, if it exists in the logical data warehouse.

@param name: External file format name.
*/
CREATE OR ALTER PROCEDURE [usp_drop_external_file_format_if_exists]
    @name SYSNAME
AS BEGIN
    IF (0 <> (SELECT COUNT(*) FROM sys.external_file_formats WHERE name = @name))
    BEGIN
        PRINT '';
        PRINT '*** Dropping external file format: ' + @name;
        DECLARE @drop_stmt VARCHAR(200) = N'DROP EXTERNAL FILE FORMAT ' + @name;
        EXEC sp_executesql @tsql = @drop_stmt;
    END
END
GO

/*
Drops the given external data source, if it exists in the logical data warehouse.

@param name: External data source name.
*/
CREATE OR ALTER PROCEDURE [usp_drop_external_data_source_if_exists]
    @name SYSNAME
AS BEGIN
    IF (0 <> (SELECT COUNT(*) FROM sys.external_data_sources WHERE name = @name))
    BEGIN
        PRINT '';
        PRINT '*** Dropping external data source: ' + @name;
        DECLARE @drop_stmt VARCHAR(200) = N'DROP EXTERNAL DATA SOURCE ' + @name;
        EXEC sp_executesql @tsql = @drop_stmt;
    END
END
GO

/*
Drops the given view, if it exists in the logical data warehouse.

@param name: View name.
*/
CREATE OR ALTER PROCEDURE [usp_drop_view_if_exists]
    @name SYSNAME
AS BEGIN
    PRINT '';
    PRINT '*** Dropping view: ' + @name;
    DECLARE @drop_stmt VARCHAR(200) = N'DROP VIEW IF EXISTS ' + @name;
    EXEC sp_executesql @tsql = @drop_stmt;
END
GO
