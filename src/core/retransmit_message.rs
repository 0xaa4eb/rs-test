use actix::Message;
use serde::{Deserialize, Serialize};

#[derive(Message, Serialize, Deserialize, PartialEq, Debug, Clone)]
#[rtype(result = "(usize)")]
pub struct RtxMessage {
    pub from_id: u64,
    pub to_id: u64,
    pub channel_id: u32,
}

impl RtxMessage {
    pub fn ser(&self) -> Vec<u8> {
        return bincode::serialize(&self).unwrap();
    }

    pub fn de(b: &[u8]) -> Self {
        return bincode::deserialize(b).unwrap();
    }
}