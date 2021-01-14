resource "aws_security_group" "service_endpoint" {
  name        = "${module.env.module_name}_service_endpoint_${module.env.stage}"
  description = "allow inbound access to endpoint from local network"
  vpc_id      = module.vpc.vpc_id

  ingress {
    protocol    = "tcp"
    from_port   = 80
    to_port     = 80
    cidr_blocks = [module.env.vpc_cidr]
  }

  ingress {
    protocol    = "tcp"
    from_port   = 443
    to_port     = 443
    cidr_blocks = [module.env.vpc_cidr]
  }

  tags = module.env.tags
}

data "aws_availability_zones" "available" {}

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name           = "${module.env.module_name}-vpc-${module.env.stage}"
  cidr           = module.env.vpc_cidr
  azs            = slice(data.aws_availability_zones.available.names, 0, length(module.env.subnet_cidrs))
  public_subnets = module.env.subnet_cidrs

  enable_nat_gateway = false
  enable_vpn_gateway = false
  enable_s3_endpoint = true

  ## enable access to the ECS and Lambda APIs. This costs ~ $8 / month / endpoint
  # dns settings allow to access services transparently
  enable_dns_support   = true
  enable_dns_hostnames = true
  # activate ecs access
  enable_ecs_endpoint              = true
  ecs_endpoint_subnet_ids          = [module.vpc.public_subnets[0]]
  ecs_endpoint_security_group_ids  = [aws_security_group.service_endpoint.id]
  ecs_endpoint_private_dns_enabled = true
  # activate lambda access
  enable_lambda_endpoint              = true
  lambda_endpoint_subnet_ids          = [module.vpc.public_subnets[0]]
  lambda_endpoint_security_group_ids  = [aws_security_group.service_endpoint.id]
  lambda_endpoint_private_dns_enabled = true


  tags = module.env.tags
}

# TODO move all below this into ./fargate ?

resource "aws_ecs_cluster" "hcomb_cluster" {
  name               = "${module.env.module_name}-cluster-${module.env.stage}"
  capacity_providers = ["FARGATE"]
  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
  }
  setting {
    name  = "containerInsights"
    value = "disabled"
  }
  tags = module.env.tags
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name = "${module.env.module_name}_task_execution_${module.env.stage}_${var.region_name}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

  tags = module.env.tags
}

resource "aws_iam_role_policy" "ecs_task_execution_policy" {
  name = "${module.env.module_name}_task_execution_${module.env.stage}_${var.region_name}"
  role = aws_iam_role.ecs_task_execution_role.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ecr:GetAuthorizationToken",
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "ecs:StartTelemetrySession"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}
