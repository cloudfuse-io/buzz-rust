locals {
  container_definition = [
    {
      cpu         = var.task_cpu
      image       = var.docker_image
      memory      = var.task_memory
      name        = "hcomb"
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
          awslogs-group         = aws_cloudwatch_log_group.hcomb_logging.name
          awslogs-region        = module.env.region_name
          awslogs-stream-prefix = "ecs"
        }
      }
    },
  ]
}
