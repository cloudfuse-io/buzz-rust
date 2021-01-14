locals {
  container_definition = [
    {
      cpu         = var.task_cpu
      image       = var.docker_image
      memory      = var.task_memory
      name        = var.name
      essential   = true
      mountPoints = []
      portMappings = [
        {
          containerPort = 8080
          hostPort      = 8080
          protocol      = "tcp"
        },
        {
          containerPort = 3333
          hostPort      = 3333
          protocol      = "tcp"
        },
      ]
      volumesFrom = []
      environment = var.environment
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.fargate_logging.name
          awslogs-region        = var.region_name
          awslogs-stream-prefix = "ecs"
        }
      }
    },
  ]
}
