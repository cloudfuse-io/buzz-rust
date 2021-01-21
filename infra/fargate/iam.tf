resource "aws_iam_role" "ecs_task_role" {
  name = "${module.env.tags["module"]}_${var.name}_${module.env.stage}_${var.region_name}"

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

# resource "aws_iam_role_policy" "ecs_task_policy" {
#   name = "${module.env.tags["module"]}_${var.name}_${module.env.stage}_${var.region_name}"
#   role = aws_iam_role.ecs_task_role.id

#   policy = <<EOF
# {
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Effect": "Allow",
#             "Action": "*",
#             "Resource": "*"
#         }
#     ]
# }
# EOF

# }
