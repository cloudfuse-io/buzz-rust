resource "aws_cloudwatch_log_group" "fargate_logging" {
  name = "/ecs/gateway/${module.env.module_name}-${var.name}-${module.env.stage}"

  tags = module.env.tags
}

resource "aws_security_group" "ecs_tasks" {
  name        = "${module.env.module_name}_${var.name}_${module.env.stage}"
  description = "allow inbound access from the ALB only"
  vpc_id      = var.vpc_id

  ingress {
    protocol    = "tcp"
    from_port   = 8080
    to_port     = 8080
    cidr_blocks = [module.env.vpc_cidr]
  }

  ingress {
    protocol    = "tcp"
    from_port   = 3333
    to_port     = 3333
    cidr_blocks = [module.env.vpc_cidr]
  }

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = module.env.tags
}

resource "aws_ecs_task_definition" "fargate_task_def" {
  family                   = "${module.env.module_name}-${var.name}-${module.env.stage}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  task_role_arn            = aws_iam_role.ecs_task_role.arn  // necessary to access other aws services
  execution_role_arn       = var.ecs_task_execution_role_arn // necessary to log and access ecr
  container_definitions    = jsonencode(local.container_definition)
  depends_on               = [aws_cloudwatch_log_group.fargate_logging] // make sure the first task does not fail because log group is not available yet
  tags                     = module.env.tags
}
