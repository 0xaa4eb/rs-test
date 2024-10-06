use std::sync::Arc;
use actix::{Actor, Context, Handler};
use zmq::Socket;
use crate::core::ack_message::AckMessage;
use crate::core::data_message::DataMessage;
use crate::core::metric_system::{Counter, MetricSystem};
use crate::core::retransmit_message::RtxMessage;

const WIRE_ID: u8 = 102;

pub struct ZMQSender {
    url: String,
    socket: Option<Socket>,
    zmq_ctx: Option<zmq::Context>,
    data_msgs_sent_ctr: Arc<Counter>,
    acks_sent_ctr: Arc<Counter>,
    rtx_sent_ctr: Arc<Counter>,
}

impl ZMQSender {
    pub fn new(url: &str, metric_system: &'static MetricSystem) -> ZMQSender {
        ZMQSender {
            url: url.to_owned(),
            socket: None,
            zmq_ctx: None,
            data_msgs_sent_ctr: metric_system.get_counter(format!("ZMQSender.{url}.messages.data.sent")),
            acks_sent_ctr: metric_system.get_counter(format!("ZMQSender.{url}.messages.acks.sent")),
            rtx_sent_ctr: metric_system.get_counter(format!("ZMQSender.{url}.messages.rtx.sent"))
        }
    }
}

impl Actor for ZMQSender {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        println!("ZMQ Sender started");
        _ctx.set_mailbox_capacity(8192);
        let mut ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PAIR).unwrap();
        socket.connect(self.url.as_str()).unwrap();
        self.socket = Some(socket);
        self.zmq_ctx = Some(ctx);
    }
}

impl Handler<DataMessage> for ZMQSender {
    type Result = usize;

    fn handle(&mut self, msg: DataMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let sock = self.socket.as_mut().unwrap();
        let mut bytes = msg.ser();
        bytes.push(101);
        sock.send(bytes, 0/*zmq::DONTWAIT*/).unwrap();
        self.data_msgs_sent_ctr.inc();
        0
    }
}

impl Handler<AckMessage> for ZMQSender {
    type Result = usize;

    fn handle(&mut self, msg: AckMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let sock = self.socket.as_mut().unwrap();
        let mut bytes = msg.ser();
        bytes.push(102);
        sock.send(bytes, 0).unwrap();
        self.acks_sent_ctr.inc();
        0
    }
}

impl Handler<RtxMessage> for ZMQSender {
    type Result = usize;

    fn handle(&mut self, msg: RtxMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let sock = self.socket.as_mut().unwrap();
        let mut bytes = msg.ser();
        bytes.push(103);
        sock.send(bytes, 0).unwrap();
        self.rtx_sent_ctr.inc();
        0
    }
}