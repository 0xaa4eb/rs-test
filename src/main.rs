mod main_zmq_only;
mod core;
mod msg_spamer;

use actix::Actor;
use actix::prelude::*;
use core::channel_reader::ChannelReader;
use core::channel_writer::ChannelWriter;
use core::zmq_receiver::ZMQReceiver;
use core::zmq_sender::ZMQSender;
use crate::core::metric_system::MetricSystem;
use crate::msg_spamer::start_spamer;

const CHANNELS_COUNT: u32 = 8;

fn setup_sending_side(system: &SystemRunner, metric_system: &'static MetricSystem) -> Vec<Addr<ChannelWriter>> {
    let zmq_arbiter = Arbiter::new();
    let channel_writer_arbiter = Arbiter::new();

    let zmq_sender = ZMQSender::new("tcp://127.0.0.1:3456", metric_system);
    let zmq_sender_addr = ZMQSender::start_in_arbiter(&zmq_arbiter.handle(), move |ctx| zmq_sender);

    let mut channel_writers_addrs = Vec::new();
    for ch_id in 0..CHANNELS_COUNT {
        let ch_writer = ChannelWriter::new(ch_id, metric_system, zmq_sender_addr.clone());
        let ch_writer_addr = ChannelWriter::start_in_arbiter(&channel_writer_arbiter.handle(), move |ctx| ch_writer);
        channel_writers_addrs.push(ch_writer_addr);
    }

    let zmq_receiver = ZMQReceiver::for_data_writer("tcp://127.0.0.1:3457", channel_writers_addrs.clone(), metric_system);
    ZMQReceiver::start_in_arbiter(&zmq_arbiter.handle(), move |ctx| zmq_receiver);

    channel_writers_addrs
}

fn setup_receiver_side(system: &SystemRunner, metric_system: &'static MetricSystem) {
    let zmq_arbiter = Arbiter::new();
    let channel_reader_arbiter = Arbiter::new();

    let zmq_sender = ZMQSender::new("tcp://127.0.0.1:3457", metric_system);
    let zmq_sender_addr = ZMQSender::start_in_arbiter(&zmq_arbiter.handle(), move |ctx| zmq_sender);

    let mut channel_readers = Vec::new();
    for ch_id in 0..CHANNELS_COUNT {
        let channel_reader = ChannelReader::new(ch_id, metric_system, zmq_sender_addr.clone());
        let ch_writer_addr = ChannelReader::start_in_arbiter(&channel_reader_arbiter.handle(), move |ctx| channel_reader);
        channel_readers.push(ch_writer_addr);
    }

    let zmq_receiver = ZMQReceiver::for_data_reader("tcp://127.0.0.1:3456", channel_readers, metric_system);
    ZMQReceiver::start_in_arbiter(&zmq_arbiter.handle(), move |ctx| zmq_receiver);
}

fn main() {
    let system = System::new();

    let metric_system: &'static mut MetricSystem = Box::leak(Box::new(MetricSystem::new()));
    metric_system.init();

    let channel_writers = setup_sending_side(&system, metric_system);

    setup_receiver_side(&system, metric_system);

    let mut handles = Vec::new();
    for ch_id in 0..CHANNELS_COUNT {
        handles.push(
            start_spamer(format!("msg-spamer-{ch_id}"), ch_id, channel_writers[ch_id as usize].clone())
        );
    }

    system.run().unwrap();
}