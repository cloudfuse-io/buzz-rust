use std::iter::IntoIterator;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{BuzzError, Result};
use crate::models::env;
use rusoto_core::Region;
use rusoto_ecs::{
    AwsVpcConfiguration, DescribeTasksRequest, Ecs, EcsClient, ListTasksRequest,
    ListTasksResponse, NetworkConfiguration, RunTaskRequest,
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
    /// Create a new fargate task and returns private IP.
    /// The task might not be ready to receive requests yet.
    pub async fn create_new(&self) -> Result<String> {
        let start = Instant::now();
        let config = env::get_fargate_config()?;

        let mut task_arn = self
            .get_existing_task(config.hcomb_cluster_name.clone())
            .await;

        if task_arn.is_none() {
            task_arn = Some(
                self.start_task(
                    config.hcomb_task_def_arn,
                    config.hcomb_cluster_name.clone(),
                    config.public_subnets,
                    config.hcomb_task_sg_id,
                )
                .await?,
            );
        }

        println!("[fuse] task started");

        tokio::time::delay_for(Duration::from_secs(1)).await;

        let result = self
            .wait_for_provisioning(task_arn.unwrap(), config.hcomb_cluster_name.clone())
            .await?;

        println!(
            "[fuse] took {}ms to create new task",
            start.elapsed().as_millis()
        );

        Ok(result)
    }

    /// Get existing task ARN if their is one
    /// /// TODO better error management
    async fn get_existing_task(&self, cluster_name: String) -> Option<String> {
        let request = ListTasksRequest {
            cluster: Some(cluster_name),
            container_instance: None,
            desired_status: Some("RUNNING".to_owned()),
            family: None,
            launch_type: None,
            max_results: Some(1),
            next_token: None,
            service_name: None,
            started_by: None,
        };

        let result =
            timeout(Duration::from_secs(2), self.client.list_tasks(request)).await;

        // if request for existing task failed for any reason, return None
        match result {
            Ok(Ok(ListTasksResponse {
                task_arns: Some(arns),
                ..
            })) if arns.len() > 0 => Some(arns[0].clone()),
            _ => None,
        }
    }

    /// Start new task and return its arn
    /// TODO better error management
    async fn start_task(
        &self,
        task_definition: String,
        cluster_name: String,
        subnets: Vec<String>,
        security_group: String,
    ) -> Result<String> {
        let input = RunTaskRequest {
            task_definition,
            count: Some(1),
            cluster: Some(cluster_name),
            group: None,
            network_configuration: Some(NetworkConfiguration {
                awsvpc_configuration: Some(AwsVpcConfiguration {
                    assign_public_ip: Some("ENABLED".to_owned()),
                    subnets,
                    security_groups: Some(vec![security_group]),
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

        Ok(result
            .tasks
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .task_arn
            .unwrap())
    }

    /// Wait for the given task to be provisioned and attributed a private IP
    /// TODO better error management
    /// TODO fargate container lifecycle
    async fn wait_for_provisioning(
        &self,
        task_arn: String,
        hcomb_cluster_name: String,
    ) -> Result<String> {
        loop {
            let input = DescribeTasksRequest {
                cluster: Some(hcomb_cluster_name.clone()),
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

            let attachment_props = description[0].attachments.as_ref().unwrap()[0]
                .details
                .as_ref()
                .unwrap();

            for prop in attachment_props {
                if let Some(ref key) = prop.name {
                    if key == "privateIPv4Address" && prop.value.as_ref().is_some() {
                        return Ok(prop.value.as_ref().unwrap().clone());
                    }
                }
            }

            tokio::time::delay_for(Duration::from_millis(200)).await;
        }
    }
}

//// Lambda Client ////

fn new_client(region: &str) -> Arc<EcsClient> {
    let region = Region::from_str(region).unwrap();
    Arc::new(EcsClient::new(region))
}
