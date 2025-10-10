CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_hydration_db.hydration_runs (
    run_number string,
    created_at string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'field.delim' = ','
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/hydration/hydration-runs/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_hydration_db.advisor_contracts (
    id string,
    first_name string,
    last_name string,
    license_number string,
    start_date string,
    end_date string,
    run_number string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'field.delim' = ','
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/hydration/advisor-contracts/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_hydration_db.firm_contracts (
    id string,
    name string,
    registration_number string,
    tax_id string,
    start_date string,
    end_date string,
    run_number string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'field.delim' = ','
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/hydration/firm-contracts/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_db.legacy_addresses (
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
LOCATION 's3://spring-data-aws-bucket/primary/legacy-addresses/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_db.addresses (
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
LOCATION 's3://spring-data-aws-bucket/primary/addresses/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);