terraform {
  backend "s3" {}
  required_version = ">=0.12"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 2.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

variable "profile" {
  description = "The aws profile (from credentials file) to use to deploy/modify/destroy this infra"
}

variable "push_hcomb" {
  description = "Option whether to push or not the latest version of the hcomb container to the registry"
  default     = true
}

variable "git_revision" {
  description = "A tag that tracks the git hash of the source code for this infra"
  default     = "unknown"
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

