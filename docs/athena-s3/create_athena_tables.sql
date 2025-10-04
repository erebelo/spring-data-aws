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
LOCATION 's3://spring-data-aws-bucket/hydration/advisor_contracts/'
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
LOCATION 's3://spring-data-aws-bucket/hydration/firm_contracts/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_db.legacy_addresses (
    id string,
    address_type string,
    address_line1 string,
    address_line2 string,
    city string,
    country string,
    state string,
    zipcode string
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

CREATE EXTERNAL TABLE IF NOT EXISTS spring_data_aws_db.addresses (
    address_id string,
    home_city string,
    home_state string,
    home_country string,
    work_city string,
    work_state string,
    work_country string,
    other_city string,
    other_state string,
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