module "hbee_tests" {
  source = "./lambda"

  function_base_name = "hbee_tests"
  filename           = "../code/target/docker/hbee_tests.zip"
  memory_size        = 2048
  timeout            = 10

  vpc_id  = module.vpc.vpc_id
  subnets = module.vpc.public_subnets

  additional_policies = [aws_iam_policy.s3-additional-policy.arn]
  environment = {
    GIT_REVISION = var.git_revision
  }
}
