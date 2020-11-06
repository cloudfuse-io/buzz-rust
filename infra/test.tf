resource "aws_iam_policy" "s3-additional-policy" {
  name        = "${module.env.tags["module"]}_s3_access_${module.env.region_name}_${module.env.stage}"
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

module "test" {
  source = "./lambda"

  function_base_name = "test"
  filename           = "../target/docker/lambda.zip"
  handler            = "N/A"
  memory_size        = 2048
  timeout            = 10
  runtime            = "provided"

  additional_policies = [aws_iam_policy.s3-additional-policy.arn]
  environment = {
    GIT_REVISION = "${var.git_revision}"
  }
}
