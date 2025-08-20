CREATE DATABASE SCOPED CREDENTIAL credential_vivek
WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE source_gold1
WITH(
    LOCATION='https://adestoragedatalakev2.blob.core.windows.net/gold-seving',
    CREDENTIAL=credential_vivek
);

CREATE EXTERNAL FILE FORMAT format_parquet
WITH(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL TABLE gold.gold_calendar
WITH(
    LOCATION='gold_calendar',
    DATA_SOURCE=source_gold1,
    FILE_FORMAT=format_parquet
)
AS
SELECT * FROM gold.calendar