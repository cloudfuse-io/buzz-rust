data "aws_region" "current" {}

locals {
  envs = {
    dev = {
      tags = {
        module      = "buzz-rust"
        provisioner = "terraform"
        stage       = terraform.workspace
      },
      vpc_cidr     = "10.0.0.0/16"
      vpc_azs      = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
      subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
      region_name  = "eu-west-1"
    }
  }
  current_env = local.envs[terraform.workspace]
}

output "stage" {
  value = terraform.workspace
}

output "tags" {
  value = local.current_env.tags
}

output "region_name" {
  value = local.current_env["region_name"]
}

output "module_name" {
  value = local.current_env["tags"]["module"]
}

output "vpc_cidr" {
  value = local.current_env["vpc_cidr"]
}

output "vpc_azs" {
  value = local.current_env["vpc_azs"]
}

output "subnet_cidrs" {
  value = local.current_env["subnet_cidrs"]
}
