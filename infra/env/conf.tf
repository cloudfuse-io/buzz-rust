locals {
  envs = {
    dev = {
      tags = {
        module      = "buzz-rust"
        provisioner = "terraform"
        stage       = terraform.workspace
      },
      vpc_cidr     = "10.0.0.0/16"
      subnet_cidrs = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
    }
    prod = {
      tags = {
        module      = "buzz-rust"
        provisioner = "terraform"
        stage       = terraform.workspace
      },
      vpc_cidr     = "10.1.0.0/16"
      subnet_cidrs = ["10.1.1.0/24", "10.1.2.0/24", "10.1.3.0/24"]
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

output "module_name" {
  value = local.current_env["tags"]["module"]
}

output "vpc_cidr" {
  value = local.current_env["vpc_cidr"]
}

output "subnet_cidrs" {
  value = local.current_env["subnet_cidrs"]
}
