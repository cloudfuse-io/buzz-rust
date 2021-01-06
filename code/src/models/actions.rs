use serde::{Deserialize, Serialize};

pub enum ActionType {
    Fail,
    Unknown,
}

impl ActionType {
    pub fn from_string(serialized: String) -> Self {
        match serialized.as_str() {
            "FAIL" => ActionType::Fail,
            _ => ActionType::Unknown,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ActionType::Fail => "FAIL".to_owned(),
            ActionType::Unknown => "UNKNOWN".to_owned(),
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
