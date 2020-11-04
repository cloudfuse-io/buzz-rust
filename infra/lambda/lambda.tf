resource "aws_lambda_function" "lambda" {
  filename         = var.filename
  function_name    = "${module.env.tags["module"]}-${var.function_base_name}-${module.env.stage}"
  role             = aws_iam_role.lambda_role.arn
  handler          = var.handler
  memory_size      = var.memory_size
  timeout          = var.timeout
  source_code_hash = filebase64sha256(var.filename)
  runtime          = var.runtime

  environment {
    variables = merge(
      {
        STAGE = module.env.stage
      },
      var.environment
    )
  }

  dynamic "vpc_config" {
    for_each = var.vpc_id == "" ? [] : [1]
    content {
      security_group_ids = [aws_security_group.lambda_sg.id]
      subnet_ids         = var.subnets
    }
  }

  tags = module.env.tags
}

resource "aws_lambda_function_event_invoke_config" "lambda_conf" {
  function_name                = aws_lambda_function.lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
}


resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${aws_lambda_function.lambda.function_name}"
  retention_in_days = 14
  tags              = module.env.tags
}

resource "aws_security_group" "lambda_sg" {
  count = var.vpc_id == "" ? 0 : 1

  name        = "${module.env.tags["module"]}-${var.function_base_name}-${module.env.stage}"
  description = "allow outbound access"
  vpc_id      = var.vpc_id

  egress {
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = module.env.tags
}
