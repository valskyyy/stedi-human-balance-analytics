CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    timestamp             BIGINT,
    user                  STRING,
    x                     FLOAT,
    y                     FLOAT,
    z                     FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://stedi-lakehouse-vl/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
