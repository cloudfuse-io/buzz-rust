terraform {
  backend "s3" {}
  required_version = ">=0.12"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

variable "profile" {}

variable "git_revision" {
  default = "unknown"
}

provider "aws" {
  profile = var.profile
  region  = module.env.region_name
}

module "env" {
  source = "./env"
}

data "http" "icanhazip" {
  url = "http://icanhazip.com"
}

