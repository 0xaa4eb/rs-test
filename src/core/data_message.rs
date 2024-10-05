use actix::Message;
use serde::{Deserialize, Serialize};
use zmq::{Sendable};

#[derive(Message, Serialize, Deserialize, PartialEq, Debug, Clone)]
#[rtype(result = "(usize)")]
pub struct DataMessage {
    pub id: u64,
    pub channel_id: u32,
    pub ts: u64,
    pub data: Vec<u8>
}

impl DataMessage {
    pub fn ser(&self) -> Vec<u8> {
        return bincode::serialize(&self).unwrap();
    }

    pub fn de(bytes: &[u8]) -> Self {
        return bincode::deserialize(bytes).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ser_de_slice() {
        let val = DataMessage {id: 6, ts: 0, channel_id: 42, data: vec![24,254,3,4,235,43,43,43,43,25,6,0,4,65,65]};
        let serialized = val.ser();
        let deserialized = DataMessage::de(serialized.as_slice());

        assert_eq!(deserialized, val);
    }
}