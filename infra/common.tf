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

module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name           = "${module.env.module_name}-vpc-${module.env.stage}"
  cidr           = module.env.vpc_cidr
  azs            = module.env.vpc_azs
  public_subnets = module.env.subnet_cidrs

  enable_nat_gateway = false
  enable_vpn_gateway = false
  enable_s3_endpoint = true

  tags = module.env.tags
}

# TODO move this into ./fargate ?
resource "aws_iam_role" "ecs_task_execution_role" {
  name = "${module.env.module_name}_task_execution_${module.env.stage}_${module.env.region_name}"

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
  name = "${module.env.module_name}_task_execution_${module.env.stage}_${module.env.region_name}"
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

resource "aws_iam_policy" "s3-additional-policy" {
  name        = "${module.env.module_name}_s3_access_${module.env.region_name}_${module.env.stage}"
  description = "additional policy for s3 access"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:*"
      ],
      "Resource": "*",
      "Effect": "Allow"
    }
  ]
}
EOF
}
