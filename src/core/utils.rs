pub fn current_time_millis() -> u128 {
    std::time::UNIX_EPOCH.elapsed().unwrap().as_millis()
}