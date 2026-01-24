
## Adapting GCP to AWS
Due to issues with the Google Cloud Platform (GCP), I have completed the task using **Amazon Web Services (AWS)** as the cloud provider.

### Equivalents used:
* **GCS Bucket** (Google Cloud Storage) -> **AWS S3 Bucket** (Simple Storage Service).
* **Region**: `us-east-1` (Virginia).

## Prerequisites
* [Terraform](https://www.terraform.io/) v1.14.3 or higher.
* [AWS CLI](https://aws.amazon.com/cli/) configured with Administrator permissions.

## Project Structure
* `main.tf`: Defines the AWS provider and the S3 Bucket resource that will act as the Data Lake.
* `variables.tf`: Contains the variables to parameterize the bucket name and region, avoiding hard-coded values in the main code.
* `.gitignore`: Configured to exclude Terraform state files and temporary folders.

## Instructions for use
1. Initialize Terraform:
```bash
  terraform init