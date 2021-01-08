# resource group tracks git revision of the current tf state
resource "aws_resourcegroups_group" "resourcegroup" {
  name = "terraform_${module.env.module_name}"
  resource_query {
    query = <<JSON
{
  "ResourceTypeFilters": ["AWS::AllSupported"],
  "TagFilters": [
    {
      "Key": "module",
      "Values": ["${module.env.module_name}"]
    }
  ]
}
JSON
  }

  description = "git_revision ${var.git_revision}"
}
