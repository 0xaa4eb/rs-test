use std::collections::HashMap;
use std::sync::atomic::{AtomicI64};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use crate::core::utils::current_time_millis;

pub struct Counter {
    name: String,
    counter: AtomicI64
}

impl Counter {
    pub fn new(name: String) -> Counter {
        Counter {
            name,
            counter: AtomicI64::new(0)
        }
    }

    pub fn inc(&self) {
        self.counter.fetch_add(1, Relaxed);
    }

    pub fn dec(&self) {
        self.counter.fetch_add(-1, Relaxed);
    }

    fn print(&self) {
        println!("{} Counter {} value: {}", current_time_millis(), self.name, self.counter.load(Relaxed));
    }
}

pub struct MetricSystem {
    counters: Arc<Mutex<HashMap<String, Arc<Counter>>>>,
    thread_handle: Option<JoinHandle<()>>
}

impl MetricSystem {

    pub fn new() -> MetricSystem {
        MetricSystem {
            counters: Arc::new(Mutex::new(HashMap::new())),
            thread_handle: None
        }
    }

    pub fn init(&mut self) {
        let counters = self.counters.clone();
        self.thread_handle = Some(
            thread::Builder::new().name("metric-system".to_owned()).spawn(
                move || {
                    while true {
                        {
                            let mut ctrs = counters.lock().unwrap();
                            for (key, value) in ctrs.iter() {
                                value.print();
                            }
                        }
                        thread::sleep(Duration::from_secs(5));
                    }
                }
        ).unwrap());
    }

    fn insert_counter(&self, name: &str) {
        let mut counters = self.counters.lock().unwrap();
        if !counters.contains_key(name) {
            counters.insert(String::from(name), Arc::new(Counter::new(name.to_owned())));
        }
    }

    pub fn get_counter(&self, name: String) -> Arc<Counter> {
        self.insert_counter(name.as_str());
        let mut counters = self.counters.lock().unwrap();
        let counter = counters.get(name.as_str()).unwrap();
        return counter.clone();
    }
}