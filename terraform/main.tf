# This block configures Terraform's own settings, such as the required version
# and the backend where the state file will be stored.
terraform {
  required_version = ">= 1.0" # Specifies that we need Terraform version 1.0 or newer.

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # Use a version of the AWS provider compatible with 5.x.
    }
  }
}

# This block configures the AWS provider itself. We are telling it which
# region to create our resources in.
provider "aws" {
  region = "eu-west-3" # You can change this to your preferred AWS region.
}

# This is where we will start defining our AWS resources, like S3 buckets.
# For now, it is empty. We will add resources here in the next step.

# --- Random String for Unique Bucket Names ---
# This resource creates a random 8-character hex string. We'll use this
# to ensure our S3 bucket names are globally unique.
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}


# --- S3 Buckets for Data Lake Layers ---
# The foundation of our data lake storage. Each layer gets its own bucket.

# Bronze Layer: for raw, unaltered data from sources.
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = "data-lakehouse-bronze-${random_string.bucket_suffix.result}"

  # This setting is useful for development, as it allows Terraform to delete
  # the bucket even if it has objects in it. Do NOT use this in production.
  force_destroy = true
}

# Silver Layer: for cleaned, validated, and conformed data.
resource "aws_s3_bucket" "silver_bucket" {
  bucket = "data-lakehouse-silver-${random_string.bucket_suffix.result}"
  force_destroy = true
}

# Gold Layer: for aggregated, business-level data ready for analytics.
resource "aws_s3_bucket" "gold_bucket" {
  bucket = "data-lakehouse-gold-${random_string.bucket_suffix.result}"
  force_destroy = true
}