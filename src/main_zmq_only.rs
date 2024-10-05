/*use actix::{Actor};
use actix::prelude::*;
use crate::core::data_message::DataMessage;
use crate::core::zmq_receiver::ZMQReceiver;
use crate::core::zmq_sender::ZMQSender;

#[actix::main]
async fn main() {
    let zmq_rcvr_addr = ZMQReceiver::default().start();
    let zmq_send_addr = ZMQSender::default().start();


    let mut msg_id: u64 = 0;
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32];
    while true {
        msg_id += 1;
        let request = zmq_send_addr.send(DataMessage {id: msg_id, ts: 0, channel_id: msg_id as u32 & 15, data: data.clone()});
        request.await.expect("TODO: panic message");
    }
}*/