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

fn setup_receiver_side(system: &SystemRunner, metric_system: &'static MetricSystem) {

    let zmq_sender = system.block_on(async { ZMQSender::new("tcp://127.0.0.1:3457", metric_system).start()});

    let mut channel_readers = Vec::new();
    for ch_id in 0..CHANNELS_COUNT {
        let channel_reader = system.block_on(async { ChannelReader::new(ch_id, zmq_sender.clone()).start()});
        channel_readers.push(channel_reader);
    }

    let zmq_receiver = system.block_on(
        async { ZMQReceiver::for_data_reader("tcp://127.0.0.1:3456", channel_readers, metric_system).start()}
    );
}

fn main() {
    let system = System::new();

    let metric_system: &'static mut MetricSystem = Box::leak(Box::new(MetricSystem::new()));
    metric_system.init();

    let zmq_sender = system.block_on(async { ZMQSender::new("tcp://127.0.0.1:3456", metric_system).start() });
    let mut channel_writers = Vec::new();
    for ch_id in 0..CHANNELS_COUNT {
        let ch_writer = system.block_on(async { ChannelWriter::new(ch_id, metric_system, zmq_sender.clone()).start() });
        channel_writers.push(ch_writer);
    }
    let zmq_receiver = system.block_on(
        async { ZMQReceiver::for_data_writer("tcp://127.0.0.1:3457", channel_writers.clone(), metric_system).start() }
    );

    setup_receiver_side(&system, metric_system);

    let mut handles = Vec::new();
    for ch_id in 0..CHANNELS_COUNT {
        handles.push(
            start_spamer(format!("msg-spamer-{ch_id}"), ch_id, channel_writers[ch_id as usize].clone())
        );
    }

    system.run().unwrap();
}