# global configuration

module "env" {
  source = "../env"
}

variable "region_name" {}

# function related configuration

variable "function_base_name" {}

variable "filename" {}

variable "handler" {
  default = "N/A" # handler not used with "provided" runtime
}

variable "memory_size" {}

variable "timeout" {}

variable "runtime" {
  default = "provided"
}

variable "additional_policies" {
  type    = list(any)
  default = []
}

variable "environment" {
  type = map(any)
}

# VPC 

variable "in_vpc" {
  description = "Set this to true if the lambda should be placed into a VPC. In that case vpc_id and subnets should also be specified"
  default     = false
}

variable "vpc_id" {
  default = ""
}

variable "subnets" {
  default = []
}
