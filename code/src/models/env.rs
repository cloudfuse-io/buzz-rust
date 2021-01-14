use crate::error::{BuzzError, Result};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct FargateConfig {
    pub hcomb_cluster_name: String,
    pub hcomb_task_sg_id: String,
    pub public_subnets: Vec<String>,
    pub hcomb_task_def_arn: String,
    pub aws_region: String,
}

pub fn get_fargate_config() -> Result<FargateConfig> {
    envy::from_env::<FargateConfig>().map_err(|e| BuzzError::Internal(format!("{}", e)))
}

#[derive(Deserialize, Debug)]
pub struct LambdaConfig {
    pub hbee_lambda_name: String,
    pub aws_region: String,
}

pub fn get_lambda_config() -> Result<LambdaConfig> {
    envy::from_env::<LambdaConfig>().map_err(|e| BuzzError::Internal(format!("{}", e)))
}
