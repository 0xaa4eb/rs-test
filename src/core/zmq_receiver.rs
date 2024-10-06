use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use actix::{Actor, Addr, AsyncContext, Context, Handler};
use crate::core::ack_message::AckMessage;
use crate::core::channel_reader::ChannelReader;
use crate::core::channel_writer::ChannelWriter;
use crate::core::data_message::DataMessage;
use crate::core::metric_system::{Counter, MetricSystem};
use crate::core::retransmit_message::RtxMessage;

pub struct ZMQReceiver {
    url: String,
    rcvr_thread_handle: Option<JoinHandle<()>>,
    channel_readers: Option<Vec<Addr<ChannelReader>>>,
    channel_writers: Option<Vec<Addr<ChannelWriter>>>,
    data_msgs_received: Arc<Counter>,
    ack_msgs_received: Arc<Counter>,
    rtx_msgs_received: Arc<Counter>,
}

impl ZMQReceiver {
    pub fn for_data_reader(url: &str, ch_readers: Vec<Addr<ChannelReader>>, metric_system: &'static MetricSystem) -> ZMQReceiver {
        ZMQReceiver {
            url: url.to_owned(),
            rcvr_thread_handle: None,
            channel_readers: Some(ch_readers),
            channel_writers: None,
            data_msgs_received: metric_system.get_counter(format!("ZMQReceiver.{url}.messages.data.processed")),
            ack_msgs_received: metric_system.get_counter(format!("ZMQReceiver.{url}.messages.ack.processed")),
            rtx_msgs_received: metric_system.get_counter(format!("ZMQReceiver.{url}.messages.rtx.processed")),
        }
    }

    pub fn for_data_writer(url: &str, ch_writers: Vec<Addr<ChannelWriter>>, metric_system: &'static MetricSystem) -> ZMQReceiver {
        ZMQReceiver {
            url: url.to_owned(),
            rcvr_thread_handle: None,
            channel_readers: None,
            channel_writers: Some(ch_writers),
            data_msgs_received: metric_system.get_counter(format!("ZMQReceiver.{url}.messages.data.processed")),
            ack_msgs_received: metric_system.get_counter(format!("ZMQReceiver.{url}.messages.acks.processed")),
            rtx_msgs_received: metric_system.get_counter(format!("ZMQReceiver.{url}.messages.rtx.processed")),
        }
    }
}

impl Actor for ZMQReceiver {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        _ctx.set_mailbox_capacity(8192);
        let addr = _ctx.address();
        println!("ZMQ Receiver started");
        let url = self.url.clone();

        self.rcvr_thread_handle = Some(
            thread::spawn(move || {
                let mut ctx = zmq::Context::new();
                let socket = ctx.socket(zmq::PAIR).unwrap();
                socket.bind(url.as_str()).unwrap();

                while true {
                    let mut zmq_msg = zmq::Message::new();
                    socket.recv(&mut zmq_msg, 0).unwrap();
                    let msg_bytes = zmq_msg.as_mut();

                    match msg_bytes[msg_bytes.len() - 1] {
                        101 => {
                            let msg = DataMessage::de(msg_bytes);
                            addr.do_send(msg);
                        },
                        102 => {
                            let msg = AckMessage::de(msg_bytes);
                            addr.do_send(msg);
                        },
                        103 => {
                            let msg = RtxMessage::de(msg_bytes);
                            addr.do_send(msg);
                        }
                        _ => {

                        }
                    }

                    // TODO handle failures somehow
                }
            })
        );
    }
}

impl Handler<DataMessage> for ZMQReceiver {
    type Result = usize;

    fn handle(&mut self, msg: DataMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.data_msgs_received.inc();
        if let Some(channel_readers) = self.channel_readers.as_ref() {
            let ch_reader = &channel_readers[msg.channel_id as usize];
            ch_reader.do_send(msg);
        }
        return 0;
    }
}

impl Handler<AckMessage> for ZMQReceiver {
    type Result = usize;

    fn handle(&mut self, msg: AckMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.ack_msgs_received.inc();
        if let Some(ch_writers) = self.channel_writers.as_ref() {
            let ch_writer = &ch_writers[msg.channel_id as usize];
            ch_writer.do_send(msg);
        }
        return 0;
    }
}

impl Handler<RtxMessage> for ZMQReceiver {
    type Result = usize;

    fn handle(&mut self, msg: RtxMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.rtx_msgs_received.inc();
        if let Some(ch_writers) = self.channel_writers.as_ref() {
            let ch_writer = &ch_writers[msg.channel_id as usize];
            //ch_writer.do_send(msg);
        }
        return 0;
    }
}