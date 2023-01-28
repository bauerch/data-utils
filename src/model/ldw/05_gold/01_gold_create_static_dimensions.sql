USE [ldw];
GO

PRINT '';
PRINT '*** Creating table: [gold].[Dim_Kalendertag]';
GO

CREATE EXTERNAL TABLE [gold].[Dim_Kalendertag] (
    [Datum_Key]                         INT
    , [Jahr]                            SMALLINT
    , [Quartal]                         TINYINT
    , [Quartal_Name]                    CHAR(3)
    , [Quartal_Name_Jahr]               CHAR(7)
    , [Monat]                           TINYINT
    , [Monat_Text]                      CHAR(2)
    , [Monat_Name_Kurz]                 CHAR(3)
    , [Monat_Name_Lang]                 VARCHAR(10)
    , [Monat_Name_Lang_Jahr]            VARCHAR(20)
    , [Woche_Jahr]                      TINYINT
    , [Woche_Jahr_Text]                 VARCHAR(20)
    , [Tag_im_Monat]                    TINYINT
    , [Tag_im_Monat_Text]               CHAR(2)
    , [Tag_Name_Kurz]                   CHAR(2)         
    , [Tag_Name_Lang]                   VARCHAR(15)
    , [Feiertag_BW]                     BIT
    , [Feiertag_Name_BW]                VARCHAR(100)
)
WITH (
    LOCATION = 'gold/Dim_Kalendertag.parquet',
    DATA_SOURCE = ds_ldw,
    FILE_FORMAT = ff_parquet_snappy
);
GO

PRINT '';
PRINT '*** Creating table: [gold].[Dim_Zeit]';
GO

CREATE EXTERNAL TABLE [gold].[Dim_Zeit] (
    [Zeit_Key]                          INT
    , [Stunde_Format_24]                TINYINT
    , [Stunde_Format_24_Text]           CHAR(2)         
    , [Stunde_Format_24_Kurz_Text]      CHAR(5)
    , [Stunde_Format_24_Lang_Text]      CHAR(8)
    , [Minute_Key]                      INT
    , [Minute]                          TINYINT
    , [Minute_Text]                     CHAR(2)
    , [Minute_Kurz_Text]                CHAR(5)
    , [Minute_Lang_Text]                CHAR(8)
    , [Sekunde]                         TINYINT
    , [Sekunde_Text]                    CHAR(2)
    , [Zeit_Kurz_Text]                  CHAR(8)
    , [Zeit_Lang_Text]                  CHAR(12)
)
WITH (
    LOCATION = 'gold/Dim_Zeit.parquet',
    DATA_SOURCE = ds_ldw,
    FILE_FORMAT = ff_parquet_snappy
);
GO
