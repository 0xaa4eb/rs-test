use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use actix::Addr;
use crate::core::channel_writer::ChannelWriter;
use crate::core::data_message::DataMessage;
use crate::core::utils::current_time_millis;

pub fn start_spamer(name: String, channel_id: u32, address: Addr<ChannelWriter>) -> JoinHandle<()> {
    thread::Builder::new().name(name).spawn(
        move || {
            do_spam_messages(channel_id, address);
        }
    ).unwrap()
}

pub fn do_spam_messages(channel_id: u32, address: Addr<ChannelWriter>) {
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32];
    let mut msg_id = 0;
    while true {
        let msg = DataMessage {id: msg_id, channel_id, ts: 0, data: data.clone()};
        match address.try_send(msg) {
            Ok(_) => {
                msg_id += 1;
                if (msg_id & 1048575 == 0) {
                    println!("{} ch-{} Sent messages to systems: {}", current_time_millis(), channel_id, msg_id);
                }
            }
            Err(_) => {
                thread::sleep(Duration::from_micros(50));
            }
        };
    }
}