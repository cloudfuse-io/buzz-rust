# resource group tracks git revision of the current tf state
resource "aws_resourcegroups_group" "resourcegroup" {
  name = "terraform_${module.env.tags["module"]}"
  resource_query {
    query = <<JSON
{
  "ResourceTypeFilters": ["AWS::AllSupported"],
  "TagFilters": [
    {
      "Key": "module",
      "Values": ["${module.env.tags["module"]}"]
    }
  ]
}
JSON
  }

  description = "git_revision ${var.git_revision}"
}
