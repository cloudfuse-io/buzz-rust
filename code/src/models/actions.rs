use serde::{Deserialize, Serialize};

pub enum ActionType {
    Fail,
    HealthCheck,
    Unknown,
}

impl ActionType {
    pub fn from_string(serialized: String) -> Self {
        match serialized.as_str() {
            "F" => ActionType::Fail,
            "H" => ActionType::HealthCheck,
            _ => ActionType::Unknown,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ActionType::Fail => "F".to_owned(),
            ActionType::HealthCheck => "H".to_owned(),
            ActionType::Unknown => "U".to_owned(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Fail {
    #[serde(rename = "qid")]
    pub query_id: String,
    #[serde(rename = "r")]
    pub reason: String,
}
