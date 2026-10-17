use std::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let pipe = nefe::pipe_tokio::new().await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let l = pipe.EnableLogController.0.lock().unwrap();
    eprintln!("l: {}", l.count);
    let mnt = pipe.make_number_time.load(std::sync::atomic::Ordering::Relaxed);
    let mvt = pipe.make_value_time.load(std::sync::atomic::Ordering::Relaxed);
    let lot = pipe.log_out_time.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("mnt: {:?}", Duration::from_nanos(mnt));
    eprintln!("mvt: {:?}", Duration::from_nanos(mvt));
    eprintln!("lot: {:?}", Duration::from_nanos(lot));
}

fn _main() {
    let pipe = nefe::pipe::new();
    std::thread::sleep(Duration::from_secs(10));
    let l = pipe.EnableLogController.0.lock().unwrap();
    eprintln!("{}", l.count);
}
