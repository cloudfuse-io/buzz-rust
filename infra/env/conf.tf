data "aws_region" "current" {}

locals {
  envs = {
    dev = {
      tags = {
        module      = "buzz-rust"
        provisioner = "terraform"
        stage       = terraform.workspace
      },
      region_name = "eu-west-1"
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
