# Spring Data AWS

REST API project developed in Java using Spring Boot 3 that orchestrates data extraction and transformation workflows on AWS by executing Amazon Athena queries, streaming and batching their results, and exporting the processed data to Amazon S3 in CSV format.

## Requirements

- Java 21
- Spring Boot 3.x.x
- Apache Maven 3.8.6

## Libraries

- [spring-common-parent](https://github.com/erebelo/spring-common-parent): Manages the Spring Boot version and provide common configurations for plugins and formatting.

## Configuring Maven for GitHub Dependencies

To pull the `spring-common-parent` dependency, follow these steps:

1. **Generate a Personal Access Token**:

   Go to your GitHub account -> **Settings** -> **Developer settings** -> **Personal access tokens** -> **Tokens (classic)** -> **Generate new token (classic)**:

   - Fill out the **Note** field: `Pull packages`.
   - Set the scope:
     - `read:packages` (to download packages)
   - Click **Generate token**.

2. **Set Up Maven Authentication**:

   In your local Maven `settings.xml`, define the GitHub repository authentication using the following structure:

   ```xml
   <servers>
     <server>
       <id>github-spring-common-parent</id>
       <username>USERNAME</username>
       <password>TOKEN</password>
     </server>
   </servers>
   ```

   **NOTE**: Replace `USERNAME` with your GitHub username and `TOKEN` with the personal access token you just generated.

## Run App

- Complete the required [Data Generator](#data-generator) and [AWS Setup](#aws-setup) steps.
- Set the following environment variables: `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.
- Run the `SpringDataAwsApplication` class as Java Application.

## Collection

[Project Collection](https://github.com/erebelo/spring-data-aws/tree/main/collection)

## Data Generator

[Address Data Generator](https://github.com/erebelo/spring-data-aws/blob/main/docs/data/address-data-generator.md)

## AWS Setup

[IAM, Athena and S3 Setup](https://github.com/erebelo/spring-data-aws/blob/main/docs/aws/aws-setup.md)
