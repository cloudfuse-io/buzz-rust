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

variable "vpc_id" {
  default = ""
}

variable "subnets" {
  default = []
}

variable "additional_policies" {
  type    = list
  default = []
}

variable "environment" {
  type = map
}
