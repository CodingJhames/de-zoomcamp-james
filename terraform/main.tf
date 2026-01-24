
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.s3_bucket_name 
  force_destroy = true
}