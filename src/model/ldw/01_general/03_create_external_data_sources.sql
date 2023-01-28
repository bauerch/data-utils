USE [master];
GO

/*
Create external data sources.
*/
EXEC usp_drop_external_data_source_if_exists @name = 'ds_ldw';

PRINT '';
PRINT '*** Creating external data source: ds_ldw';

CREATE EXTERNAL DATA SOURCE ds_ldw
WITH (
    LOCATION = '<to be defined>'
);
GO
