use std::iter::IntoIterator;
use std::str::FromStr;
use std::sync::Arc;

use crate::error::{BuzzError, Result};
use rusoto_core::Region;
use rusoto_ecs::{
    AwsVpcConfiguration, DescribeTasksRequest, Ecs, EcsClient, NetworkConfiguration,
    StartTaskRequest,
};

pub struct FargateCreationClient {
    client: Arc<EcsClient>,
}

impl FargateCreationClient {
    pub fn new(region: &str) -> Self {
        Self {
            client: new_client(region),
        }
    }
}

impl FargateCreationClient {
    /// Create a new fargate task and returns private IP
    pub async fn create_new(&self) -> Result<String> {
        let input = StartTaskRequest {
            cluster: Some(String::new()), // TODO
            container_instances: vec![],
            enable_ecs_managed_tags: None,
            group: None,
            network_configuration: Some(NetworkConfiguration {
                awsvpc_configuration: Some(AwsVpcConfiguration {
                    assign_public_ip: Some("ENABLED".to_owned()),
                    subnets: vec![String::new()], // TODO
                    security_groups: Some(vec![String::new()]), // TODO
                }),
            }),
            overrides: None,
            propagate_tags: None,
            reference_id: None,
            started_by: None,
            tags: None,
            task_definition: String::new(), // TODO
        };
        let result = self
            .client
            .start_task(input)
            .await
            .map_err(|e| BuzzError::CloudClient(format!("{}", e)))?;
        if let Some(failures) = result.failures {
            return Err(BuzzError::CloudClient(format!(
                "An error occured with AWS Fargate task creation: {:?}",
                failures
            )));
        }

        let task_arn = result
            .tasks
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .task_arn
            .unwrap();

        println!("Task ARN: {}", &task_arn);

        loop {
            let input = DescribeTasksRequest {
                cluster: Some(String::new()), // TODO
                include: None,
                tasks: vec![task_arn.clone()],
            };
            let description = self
                .client
                .describe_tasks(input)
                .await
                .map_err(|e| BuzzError::CloudClient(format!("{}", e)))?;

            println!("{:?}", description);
        }
    }
}

//// Lambda Client ////

fn new_client(region: &str) -> Arc<EcsClient> {
    let region = Region::from_str(region).unwrap();
    Arc::new(EcsClient::new(region))
}
