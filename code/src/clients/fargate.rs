use std::iter::IntoIterator;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{BuzzError, Result};
use crate::models::env;
use rusoto_core::Region;
use rusoto_ecs::{
    AwsVpcConfiguration, DescribeTasksRequest, Ecs, EcsClient, NetworkConfiguration,
    RunTaskRequest,
};
use tokio::time::timeout;

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
        let config = env::get_fargate_config()?;
        let input = RunTaskRequest {
            task_definition: config.hcomb_task_def_arn,
            count: Some(1),
            cluster: Some(config.hcomb_cluster_name.clone()),
            group: None,
            network_configuration: Some(NetworkConfiguration {
                awsvpc_configuration: Some(AwsVpcConfiguration {
                    assign_public_ip: Some("ENABLED".to_owned()),
                    subnets: config.public_subnets,
                    security_groups: Some(vec![config.hcomb_task_sg_id]),
                }),
            }),
            enable_ecs_managed_tags: None,
            capacity_provider_strategy: None,
            placement_constraints: None,
            placement_strategy: None,
            platform_version: None,
            launch_type: None,
            overrides: None,
            propagate_tags: None,
            reference_id: None,
            started_by: None,
            tags: None,
        };
        let result = timeout(Duration::from_secs(5), self.client.run_task(input))
            .await
            .map_err(|e| BuzzError::CloudClient(format!("{}", e)))?
            .map_err(|e| BuzzError::CloudClient(format!("{}", e)))?;
        if let Some(failures) = result.failures {
            if failures.len() > 0 {
                return Err(BuzzError::CloudClient(format!(
                    "An error occured with AWS Fargate task creation: {:?}",
                    failures
                )));
            }
        }

        println!("[fuse] task started");

        let task_arn = result
            .tasks
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .task_arn
            .unwrap();

        tokio::time::delay_for(Duration::from_secs(1)).await;

        // TODO what is the best way to check whether it is ready
        // TODO better error management
        // TODO fargate container lifecycle
        loop {
            let input = DescribeTasksRequest {
                cluster: Some(config.hcomb_cluster_name.clone()),
                include: None,
                tasks: vec![task_arn.clone()],
            };
            let description = self
                .client
                .describe_tasks(input)
                .await
                .map_err(|e| BuzzError::CloudClient(format!("{}", e)))?
                .tasks
                .unwrap();

            println!(
                "[fuse] last_status({:?}) / stopped_reason({:?})",
                description[0].last_status, description[0].stopped_reason,
            );

            // println!(
            //     "{:?}",
            //     description[0].attachments.as_ref().unwrap()[0].details
            // );

            let attachment_props = description[0].attachments.as_ref().unwrap()[0]
                .details
                .as_ref()
                .unwrap();

            for prop in attachment_props {
                if let Some(ref key) = prop.name {
                    if key == "privateIPv4Address" && prop.value.as_ref().is_some() {
                        // make sure the container is truely available
                        tokio::time::delay_for(Duration::from_secs(5)).await;
                        return Ok(prop.value.as_ref().unwrap().clone());
                    }
                }
            }

            tokio::time::delay_for(std::time::Duration::new(0, 500_000_000)).await;
        }
    }
}

//// Lambda Client ////

fn new_client(region: &str) -> Arc<EcsClient> {
    let region = Region::from_str(region).unwrap();
    Arc::new(EcsClient::new(region))
}
