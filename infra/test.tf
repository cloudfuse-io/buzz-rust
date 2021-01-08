# module "test" {
#   source = "./lambda"

#   function_base_name = "test"
#   filename           = "../code/target/docker/lambda.zip"
#   handler            = "N/A"
#   memory_size        = 2048
#   timeout            = 10
#   runtime            = "provided"

#   additional_policies = [aws_iam_policy.s3-additional-policy.arn]
#   environment = {
#     GIT_REVISION = "${var.git_revision}"
#   }
# }
