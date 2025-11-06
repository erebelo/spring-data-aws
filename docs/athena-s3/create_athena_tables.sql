CREATE EXTERNAL TABLE IF NOT EXISTS sd_aws_db.legacy_addresses (
    id string,
    address_type string,
    address_line_1 string,
    address_line_2 string,
    city string,
    state string,
    zip_code string,
    country string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'field.delim' = ','
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/legacy-addresses/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS sd_aws_db.addresses (
    address_id string,
    home_address_line_1 string,
    home_address_line_2 string,
    home_city string,
    home_state string,
    home_zip_code string,
    home_country string,
    work_address_line_1 string,
    work_address_line_2 string,
    work_city string,
    work_state string,
    work_zip_code string,
    work_country string,
    other_address_line_1 string,
    other_address_line_2 string,
    other_city string,
    other_state string,
    other_zip_code string,
    other_country string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'field.delim' = ','
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/addresses/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);