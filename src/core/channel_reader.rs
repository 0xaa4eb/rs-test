use std::sync::Arc;
use actix::{Actor, Addr, Context, Handler};
use crate::core::ack_message::AckMessage;
use crate::core::data_message::DataMessage;
use crate::core::metric_system::{Counter, MetricSystem};
use crate::core::zmq_sender::ZMQSender;

pub struct ChannelReader {
    channel_id: u32,
    zmq_sender: Addr<ZMQSender>,
    last_msg_id: u64,
    msgs_received_ctr: Arc<Counter>,
    nacks_sent_ctr: Arc<Counter>,
}

impl ChannelReader {
    pub fn new(channel_id: u32, metric_system: &'static MetricSystem, zmq_sender: Addr<ZMQSender>) -> ChannelReader {
        ChannelReader {
            channel_id,
            zmq_sender,
            last_msg_id: 0,
            msgs_received_ctr: metric_system.get_counter(format!("ChannelReader.{channel_id}.messages.data.processed")),
            nacks_sent_ctr: metric_system.get_counter(format!("ChannelReader.{channel_id}.messages.nack.sent")),
        }
    }
}

impl Actor for ChannelReader {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {

    }
}

impl Handler<DataMessage> for ChannelReader {
    type Result = usize;

    fn handle(&mut self, msg: DataMessage, ctx: &mut Self::Context) -> Self::Result {
        if (msg.id != self.last_msg_id) {
            // TODO this is not a problem, but should be handled with NACK
            println!("out of order message!");
        } else {
            self.last_msg_id += 1;
        }
        self.msgs_received_ctr.inc();
        self.zmq_sender.do_send(AckMessage {id: msg.id, channel_id: msg.channel_id});
        0
    }
}