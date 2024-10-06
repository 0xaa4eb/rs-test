use std::collections::{HashMap};
use std::sync::Arc;
use actix::{Actor, Addr, Context, Handler};
use crate::core::ack_message::AckMessage;
use crate::core::data_message::DataMessage;
use crate::core::metric_system::{Counter, MetricSystem};
use crate::core::zmq_sender::ZMQSender;

pub struct ChannelWriter {
    channel_id: u32,
    zmq_sender_addr: Addr<ZMQSender>,
    sent_msgs: HashMap<u64, DataMessage>, // TODO replace with chunked queue
    msgs_processed_ctr: Arc<Counter>,
    acks_processed_ctr: Arc<Counter>,
    in_flight_ctr: Arc<Counter>
}

impl ChannelWriter {
    pub fn new(channel_id: u32, metric_system: &'static MetricSystem, zmq_sender_addr: Addr<ZMQSender>) -> ChannelWriter {
        ChannelWriter {
            channel_id,
            zmq_sender_addr,
            sent_msgs: HashMap::new(),
            msgs_processed_ctr: metric_system.get_counter(format!("ChannelWriter.{channel_id}.messages.data.processed")),
            acks_processed_ctr: metric_system.get_counter(format!("ChannelWriter.{channel_id}.messages.acks.processed")),
            in_flight_ctr: metric_system.get_counter(format!("ChannelWriter.{channel_id}.messages.in-flight"))
        }
    }
}

impl Actor for ChannelWriter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // setting mailbox size to 8k may result in some channel devouring the whole bandwith and app goes OOM
        // ctx.set_mailbox_capacity(8192);
    }
}

impl Handler<DataMessage> for ChannelWriter {
    type Result = usize;

    fn handle(&mut self, msg: DataMessage, ctx: &mut Self::Context) -> Self::Result {
        // TODO check for dup
        self.sent_msgs.insert(msg.id, msg.clone());

        self.zmq_sender_addr.do_send(msg);
        self.msgs_processed_ctr.inc();
        self.in_flight_ctr.inc();
        return 0
    }
}

impl Handler<AckMessage> for ChannelWriter {
    type Result = usize;

    fn handle(&mut self, msg: AckMessage, ctx: &mut Self::Context) -> Self::Result {
        self.sent_msgs.remove(&msg.id);
        self.acks_processed_ctr.inc();
        self.in_flight_ctr.dec();
        return 0
    }
}