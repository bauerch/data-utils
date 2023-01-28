USE [master];
GO

/*
Create external file format for semicolon separated CSV files.
*/
EXEC usp_drop_external_file_format_if_exists @name = 'ff_csv_sep_semicolon';

PRINT '';
PRINT '*** Creating external file format: ff_csv_sep_semicolon';

CREATE EXTERNAL FILE FORMAT ff_csv_sep_semicolon
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ";"
        , STRING_DELIMITER = '"'
        , First_Row = 2
        , USE_TYPE_DEFAULT = FALSE
        , Encoding = 'UTF-8'
        , PARSER_VERSION = '2.0'
    )
);
GO

/*
Create external file format for comma separated CSV files.
*/
EXEC usp_drop_external_file_format_if_exists @name = 'ff_csv_sep_comma';

PRINT '';
PRINT '*** Creating external file format: ff_csv_sep_comma';

CREATE EXTERNAL FILE FORMAT ff_csv_sep_comma
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ","
        , STRING_DELIMITER = '"'
        , First_Row = 2
        , USE_TYPE_DEFAULT = FALSE
        , Encoding = 'UTF-8'
        , PARSER_VERSION = '2.0'
    )
);
GO

/*
Create external file format for snappy compressed PARQUET files.
*/
EXEC usp_drop_external_file_format_if_exists @name = 'ff_parquet_snappy';

PRINT '';
PRINT '*** Creating external file format: ff_parquet_snappy'

CREATE EXTERNAL FILE FORMAT ff_parquet_snappy
WITH (
    FORMAT_TYPE = PARQUET
    , DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO
