module "hbee" {
  source = "./lambda"

  function_base_name = "hbee"
  filename           = "../code/target/docker/hbee_lambda.zip"
  handler            = "N/A"
  memory_size        = 2048
  timeout            = 10
  runtime            = "provided"

  additional_policies = [aws_iam_policy.s3-additional-policy.arn]
  environment = {
    GIT_REVISION = var.git_revision
  }
}

resource "aws_ecr_repository" "hcomb_repo" {
  name                 = "${module.env.module_name}-hcomb-${module.env.stage}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "null_resource" "hcomb_push" {
  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = <<EOT
      docker tag "cloudfuse/buzz-rust-hcomb:${var.git_revision}" "${aws_ecr_repository.hcomb_repo.repository_url}:${var.git_revision}"
      docker push "${aws_ecr_repository.hcomb_repo.repository_url}:${var.git_revision}"
    EOT
  }
}

module "hcomb" {
  source = "./fargate"

  vpc_id                      = module.vpc.vpc_id
  task_cpu                    = 2048
  task_memory                 = 4096
  ecs_cluster_id              = aws_ecs_cluster.hcomb_cluster.id
  ecs_cluster_name            = aws_ecs_cluster.hcomb_cluster.name
  ecs_task_execution_role_arn = aws_iam_role.ecs_task_execution_role.arn
  docker_image                = "${aws_ecr_repository.hcomb_repo.repository_url}:${var.git_revision}"
  subnets                     = module.vpc.public_subnets
  local_ip                    = "${chomp(data.http.icanhazip.body)}/32"

  environment = [{
    name  = "GIT_REVISION"
    value = var.git_revision
    }, {
    name  = "AWS_REGION"
    value = module.env.region_name
  }]
}


