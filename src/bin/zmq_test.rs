use std::f64;
use std::thread;
use std::time::{Duration, Instant};

fn seconds(d: &Duration) -> f64 {
    d.as_secs() as f64 + (f64::from(d.subsec_nanos()) / 1e9)
}

fn alloc_msg() -> String {
    return String::from("ABJASUASDHASGDAGJSDGHASDGJHASDGA");
}

fn main() {
    let producer_msg_count = 10000000;
    let start = Instant::now();

    let receiver_thread = thread::spawn(move || {
        let mut ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PAIR).unwrap();
        socket.bind("tcp://127.0.0.1:3456").unwrap();

        let mut msg = zmq::Message::new();
        let mut total_bytes_rcvd = 0;
        for msg_num in 0..producer_msg_count {
            socket.recv(&mut msg, 0).unwrap();
            total_bytes_rcvd += msg.len();
        }
        let avg_msg_byts = (total_bytes_rcvd as f64) / producer_msg_count as f64;
        println!("Avg msg len: {} bytes", avg_msg_byts);
    });

    let t1 = thread::spawn(move || {
        let mut ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PAIR).unwrap();
        socket.connect("tcp://127.0.0.1:3456").unwrap();

        for msg_num in 0..producer_msg_count {
            socket.send(&alloc_msg(), 0).unwrap()
        }
    });

    t1.join();
    receiver_thread.join();

    let elapsed = seconds(&start.elapsed());
    println!("Test took {} seconds", elapsed);
    let thruput = ((producer_msg_count / 1) as f64) / elapsed;
    println!("Throughput={} per sec", thruput);
}
