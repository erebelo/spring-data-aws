# IAM, Athena and S3 Setup

## [AWS] Create IAM user and group

### 1. Create an IAM user and group (programmatic access)

- Open **IAM → Users** in the AWS Console and click **Add user**
- Enter a username (e.g. `sd-aws-user`)
- On **Permissions options**, choose **Add user to group** and click **Create group**
- Name the group (e.g. `sd-aws-group`) and attach these managed policies:

  - `AmazonS3FullAccess`
  - `AmazonAthenaFullAccess`  
    _(Skip extra policies to avoid unnecessary permissions; you can add them later if needed.)_

- Finish by clicking **Create user**

---

### 2. Create an Access Key

- Open the user previously created
- Click the **Security credentials** tab
- Under **Access keys**, click **Create access key**
- Choose **Use case**: _Application running outside AWS_
- Click **Create access key**
- Click **Download .csv file** — it contains your **Access key ID** and **Secret access key**.
  > ⚠️ **Important:** The secret access key is shown only once. Keep the `.csv` file in a safe place.

---

### 3. Reference environment variables in `application.properties`

- Add environment variables in IntelliJ (local development)
- Add the following properties to your Spring configuration file:
  ```properties
  aws.access-key=${AWS_ACCESS_KEY_ID}
  aws.secret-key=${AWS_SECRET_ACCESS_KEY}
  aws.region=${AWS_REGION:us-east-1}
  ```

## [S3] Create S3 Bucket

- Open the **Amazon S3** console
- Create the S3 Bucket
- Create a folder named `athena-result` inside the Bucket
- Create additional folders for each Athena table as needed

## [Athena] Create Workgroup

- In the **Athena** console, open the **Workgroups** section from the left-hand menu
- Click **Add workgroup** and enter a name for the new workgroup
- Under **Query result configuration**, select **Custom managed** and specify the S3 URI for the athena-result folder

## [Athena] Run the `create_athena_tables.sql` script

- Open **Amazon Athena → Query editor**
- Create a Database (if not already created):

  ```sql
  CREATE DATABASE IF NOT EXISTS spring_data_aws_db;
  CREATE DATABASE IF NOT EXISTS spring_data_aws_hydration_db;
  ```

- Run the `create_athena_tables.sql` script
  - Copy the contents of the `create_athena_tables.sql` file
  - Paste the SQL queries into the query editor
  - Execute the queries to create tables registered in the **Glue Data Catalog** and stored in **S3 Buckets**

## [S3] Upload `.csv` files

- Open the **Amazon S3 Bucket**

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
