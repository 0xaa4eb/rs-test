use std::collections::VecDeque;
use std::sync::Arc;
use actix::{Actor, Addr, Context, Handler};
use crate::core::ack_message::AckMessage;
use crate::core::data_message::DataMessage;
use crate::core::metric_system::{Counter, MetricSystem};
use crate::core::zmq_sender::ZMQSender;

pub struct ChannelWriter {
    channel_id: u32,
    zmq_sender_addr: Addr<ZMQSender>,
    sent_msgs: VecDeque<DataMessage>,
    msgs_processed_ctr: Arc<Counter>,
    acks_processed_ctr: Arc<Counter>,
    in_flight_ctr: Arc<Counter>
}

impl ChannelWriter {
    pub fn new(channel_id: u32, metric_system: &'static MetricSystem, zmq_sender_addr: Addr<ZMQSender>) -> ChannelWriter {
        ChannelWriter {
            channel_id,
            zmq_sender_addr,
            sent_msgs: VecDeque::with_capacity(1024),
            msgs_processed_ctr: metric_system.get_counter(format!("ChannelWriter.{channel_id}.messages.data.processed")),
            acks_processed_ctr: metric_system.get_counter(format!("ChannelWriter.{channel_id}.messages.acks.processed")),
            in_flight_ctr: metric_system.get_counter(format!("ChannelWriter.{channel_id}.messages.in-flight"))
        }
    }
}

impl Actor for ChannelWriter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {

    }
}

impl Handler<DataMessage> for ChannelWriter {
    type Result = usize;

    fn handle(&mut self, msg: DataMessage, ctx: &mut Self::Context) -> Self::Result {
        self.sent_msgs.push_back(msg.clone());
        self.zmq_sender_addr.do_send(msg);
        self.msgs_processed_ctr.inc();
        self.in_flight_ctr.inc();
        return 0
    }
}

impl Handler<AckMessage> for ChannelWriter {
    type Result = usize;

    fn handle(&mut self, msg: AckMessage, ctx: &mut Self::Context) -> Self::Result {
        let eldest_msg = self.sent_msgs.front();
        if eldest_msg.is_none() {
            return 0;
        }
        if eldest_msg.is_some() {
            if eldest_msg.unwrap().id == msg.id {
                self.sent_msgs.pop_front();
                self.in_flight_ctr.dec(); // TODO replace counter with gauge
            } else {
                println!("out of order ack!");
            }
        }
        self.acks_processed_ctr.inc();
        return 0
    }
}