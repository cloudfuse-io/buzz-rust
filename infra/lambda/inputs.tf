# global configuration
module "env" {
  source = "../env"
}

# function related configuration

variable "function_base_name" {}

variable "filename" {}

variable "handler" {}

variable "memory_size" {}

variable "timeout" {}

variable "runtime" {}

variable "additional_policies" {
  type    = list(any)
  default = []
}

variable "environment" {
  type = map(any)
}

# VPC -> if one is set all should be set

variable "vpc_id" {
  default = ""
}

variable "subnets" {
  default = []
}
