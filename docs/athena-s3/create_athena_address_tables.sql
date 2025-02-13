CREATE EXTERNAL TABLE IF NOT EXISTS `spring_data_aws_db`.`legacy_addresses` (
    'id' string,
    'addresstype' string,
    'addressline1' string,
    'addressline2' string,
    'city' string,
    'country' string,
    'state' string,
    'zipcode' string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delin' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.gl.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/legacy-addresses/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);

CREATE EXTERNAL TABLE IF NOT EXISTS `spring_data_aws_db`.`addresses` (
    'addressid' string,
    'home_city' string,
    'home_state' string,
    'home_country' string,
    'work_city' string,
    'work_state' string,
    'work_country' string,
	'other_city' string,
    'other_state' string,
    'other_country' string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delin' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.gl.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://spring-data-aws-bucket/addresses/'
TBLPROPERTIES (
    'classification' = 'csv',
    'skip.header.line.count' = '1'
);