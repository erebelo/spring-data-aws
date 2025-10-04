# Athena and S3 Setup

## [S3] Create S3 Bucket

- Open the Amazon S3 console
- Create the S3 Bucket
- Create a folder named `athena-result` inside the Bucket
- Create additional folders for each Athena table as needed

## [Athena] Create Workgroup

- In the Athena console, open the Workgroups section from the left-hand menu
- Click Add workgroup and enter a name for the new workgroup
- Under `Query result configuration`, select `Custom managed` and specify the S3 URI for the athena-result folder

## [Athena] Run the `create_athena_tables.sql` script

- Open Amazon Athena in the AWS Management Console
- Create a Database (if not already created):

```sql
CREATE DATABASE IF NOT EXISTS spring_data_aws_db;
CREATE DATABASE IF NOT EXISTS spring_data_aws_hydration_db;
```

- Run the `create_athena_tables.sql` script
  - Copy the contents of the `create_athena_tables.sql` file
  - Paste the SQL queries into the Athena query editor
  - Execute the queries to create tables registered in the Glue Data Catalog and store them in S3 Buckets

## [S3] Upload `.csv` files

- Open the Amazon S3 Bucket

- Go to the `spring-data-aws-bucket` bucket and open the desired folder

- Upload the `.csv` file
  - Click the **Upload** button.
  - Select the `.csv` file
  - Click **Upload** to complete the process

## Verify Data in Athena

Query the table:

```sql
SELECT * FROM spring_data_aws_db.<TABLE_NAME> LIMIT 10;
```
