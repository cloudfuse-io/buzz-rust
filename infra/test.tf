module "hbee_tests" {
  source = "./lambda"

  function_base_name = "hbee_tests"
  region_name        = var.region_name
  filename           = "../code/target/docker/hbee_tests.zip"
  memory_size        = 3008
  timeout            = 60

  in_vpc  = true
  vpc_id  = module.vpc.vpc_id
  subnets = module.vpc.public_subnets

  additional_policies = [aws_iam_policy.s3-additional-policy.arn]
  environment = {
    GIT_REVISION = var.git_revision
  }
}
