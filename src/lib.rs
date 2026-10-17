/*
(counter) -> MakeNumberInput
    | make_value -> Value
    | log_out(_, (EnableLog))
*/

pub mod ipt {
    // nefe can add values to a Generate Queue
    // Consume Queue will distribute one message to one actor. Message may be taken
    //#[nefe(queue(consume), onqueue(timer(1s)))]
    pub struct MakeNumberInput {
        //#[nefe(generate = counter)]
        pub call_count: u64,
    }

    #[derive(Debug)]
    // Share Queue will distribute every message to every actor. Message must be used as a reference
    //#[nefe(queue(consume), onqueue(make_value))]
    pub struct Value {
        pub v: u64,
    }

    //#[nefe(dequeue(MakeNumberInput), onqueue(Value))]
    pub fn make_value(input: MakeNumberInput) -> Value {
        Value {
            v: (input.call_count * 2),
        }
    }

    // TODO: shared
    //#[nefe(queue(consume), onqueue(on_demand), user)]
    #[derive(Debug, Default)]
    pub struct EnableLog {
        // Value is defined by the executor of the pipeline
        //#[nefe(user_value)]
        pub enable: bool,
        pub count: u64,
    }

    //#[nefe(dequeue(Value, EnableLog))]
    pub fn log_out<'l>(value: Value, log: &'l mut EnableLog) {
        if log.enable {
            eprintln!("{value:?}");
        }
        log.count += 1;
    }
}

#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub mod pipe {
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    pub struct Controller {
        pub EnableLogController: EnableLogQueue,
        pub MakeNumberThread: JoinHandle<()>,
        pub make_valueThread: JoinHandle<()>,
        pub log_outThread_: JoinHandle<()>,
    }

    pub fn new() -> Controller {
        // pipes
        let (MakeNumberInput_send, MakeNumberInput_recv) =
            mpsc::channel::<super::ipt::MakeNumberInput>();
        let MakeNumberInput_gen_timer = Duration::from_secs(1);
        let (Value_send, Value_recv) = mpsc::channel::<super::ipt::Value>();
        let EnableLog = Arc::new(Mutex::new(super::ipt::EnableLog::default()));

        // handlers
        let mut MakeNumberInputQueue_ = MakeNumberInputQueue {
            counter: 0,
            MakeNumberInput_send,
        };
        let _ValueQueue_ = ValueQueue {};
        let EnableLogQueue_ = EnableLogQueue(EnableLog);
        let mut make_valueActor_ = make_valueActor {
            MakeNumberInput_recv,
            Value_send,
        };
        let mut log_outActor_ = log_outActor {
            Value_recv,
            EnableLogQueue: EnableLogQueue_.clone(),
        };

        // workers
        let MakeNumberThread_ = std::thread::spawn(move || {
            loop {
                //std::thread::sleep(MakeNumberInput_gen_timer);
                MakeNumberInputQueue_.tick();
            }
        });
        let make_valueThread_ = std::thread::spawn(move || {
            loop {
                make_valueActor_.tick();
            }
        });
        let log_outThread_ = std::thread::spawn(move || {
            loop {
                log_outActor_.tick();
            }
        });

        Controller {
            EnableLogController: EnableLogQueue_,
            MakeNumberThread: MakeNumberThread_,
            make_valueThread: make_valueThread_,
            log_outThread_: log_outThread_,
        }
    }

    /// On timer
    pub struct MakeNumberInputQueue {
        pub counter: u64,
        pub MakeNumberInput_send: Sender<super::ipt::MakeNumberInput>,
    }

    impl MakeNumberInputQueue {
        pub fn tick(&mut self) {
            self.MakeNumberInput_send
                .send(crate::ipt::MakeNumberInput {
                    call_count: self.counter,
                })
                .unwrap();
            self.counter += 1;
        }
    }

    // MakeNumberInput -> make_value -> Value
    pub struct make_valueActor {
        pub MakeNumberInput_recv: Receiver<super::ipt::MakeNumberInput>,
        pub Value_send: Sender<super::ipt::Value>,
    }

    impl make_valueActor {
        pub fn tick(&mut self) {
            let i0 = self.MakeNumberInput_recv.recv().unwrap();
            let o0 = super::ipt::make_value(i0);
            self.Value_send.send(o0).unwrap();
        }
    }

    /// On push
    pub struct ValueQueue {}

    /// On demand
    #[derive(Clone)]
    pub struct EnableLogQueue(pub Arc<Mutex<super::ipt::EnableLog>>);

    impl EnableLogQueue {
        pub fn set(&self, v: super::ipt::EnableLog) {
            *self.0.lock().unwrap() = v;
        }
        pub fn pull(&self) -> std::sync::MutexGuard<'_, super::ipt::EnableLog> {
            self.0.lock().unwrap()
        }
    }

    pub struct log_outActor {
        pub Value_recv: Receiver<super::ipt::Value>,
        pub EnableLogQueue: EnableLogQueue,
    }

    impl log_outActor {
        pub fn tick(&mut self) {
            let i0 = self.Value_recv.recv().unwrap();
            let mut i1 = self.EnableLogQueue.pull();
            super::ipt::log_out(i0, &mut *i1);
        }
    }
}

#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
pub mod pipe_tokio {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::task::JoinHandle;

    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

    pub struct Controller {
        pub EnableLogController: EnableLogQueue,
        pub MakeNumberThread: JoinHandle<()>,
        pub make_valueThread: JoinHandle<()>,
        pub log_outThread_: JoinHandle<()>,
        pub make_number_time: Arc<AtomicU64>,
        pub make_value_time: Arc<AtomicU64>,
        pub log_out_time: Arc<AtomicU64>,
    }

    pub async fn new() -> Controller {
        // pipes
        let (MakeNumberInput_send, MakeNumberInput_recv) =
            tokio::sync::mpsc::unbounded_channel::<super::ipt::MakeNumberInput>();
        let MakeNumberInput_gen_timer = Duration::from_secs(1);
        let (Value_send, Value_recv) = tokio::sync::mpsc::unbounded_channel::<super::ipt::Value>();
        let EnableLog = Arc::new(Mutex::new(super::ipt::EnableLog::default()));

        // handlers
        let mut MakeNumberInputQueue_ = MakeNumberInputQueue {
            counter: 0,
            MakeNumberInput_send,
        };
        let _ValueQueue_ = ValueQueue {};
        let EnableLogQueue_ = EnableLogQueue(EnableLog);
        let mut make_valueActor_ = make_valueActor {
            MakeNumberInput_recv,
            Value_send,
        };
        let mut log_outActor_ = log_outActor {
            Value_recv,
            EnableLogQueue: EnableLogQueue_.clone(),
        };

        let mnt = Arc::new(AtomicU64::new(0));
        let mnt_ = Arc::clone(&mnt);
        let mvt = Arc::new(AtomicU64::new(0));
        let mvt_ = Arc::clone(&mvt);
        let lot = Arc::new(AtomicU64::new(0));
        let lot_ = Arc::clone(&lot);
        // workers
        let MakeNumberThread_ = tokio::task::spawn(async move {
            loop {
                //tokio::time::sleep(MakeNumberInput_gen_timer).await;
                let s = std::time::Instant::now();
                MakeNumberInputQueue_.tick().await;
                mnt_.fetch_add(s.elapsed().as_nanos() as u64, Relaxed);
            }
        });
        let make_valueThread_ = tokio::task::spawn(async move {
            loop {
                let s = std::time::Instant::now();
                make_valueActor_.tick().await;
                mvt_.fetch_add(s.elapsed().as_nanos() as u64, Relaxed);
            }
        });
        let log_outThread_ = tokio::task::spawn(async move {
            loop {
                let s = std::time::Instant::now();
                log_outActor_.tick().await;
                lot_.fetch_add(s.elapsed().as_nanos() as u64, Relaxed);
            }
        });

        Controller {
            EnableLogController: EnableLogQueue_,
            MakeNumberThread: MakeNumberThread_,
            make_valueThread: make_valueThread_,
            log_outThread_: log_outThread_,
            make_number_time: mnt,
            make_value_time: mvt,
            log_out_time: lot,
        }
    }

    /// On timer
    pub struct MakeNumberInputQueue {
        pub counter: u64,
        pub MakeNumberInput_send: UnboundedSender<super::ipt::MakeNumberInput>,
    }

    impl MakeNumberInputQueue {
        pub async fn tick(&mut self) {
            let Ok(_) = self.MakeNumberInput_send.send(crate::ipt::MakeNumberInput {
                call_count: self.counter,
            }) else {
                panic!("got to: {}", self.counter);
            };
            self.counter += 1;
        }
    }

    // MakeNumberInput -> make_value -> Value
    pub struct make_valueActor {
        pub MakeNumberInput_recv: UnboundedReceiver<super::ipt::MakeNumberInput>,
        pub Value_send: UnboundedSender<super::ipt::Value>,
    }

    impl make_valueActor {
        pub async fn tick(&mut self) {
            let i0 = self.MakeNumberInput_recv.recv().await.unwrap();
            let o0 = super::ipt::make_value(i0);
            self.Value_send.send(o0).unwrap();
        }
    }

    /// On push
    pub struct ValueQueue {}

    /// On demand
    #[derive(Clone)]
    pub struct EnableLogQueue(pub Arc<Mutex<super::ipt::EnableLog>>);

    impl EnableLogQueue {
        pub fn set(&self, v: super::ipt::EnableLog) {
            *self.0.lock().unwrap() = v;
        }
        pub fn pull(&self) -> std::sync::MutexGuard<'_, super::ipt::EnableLog> {
            self.0.lock().unwrap()
        }
    }

    pub struct log_outActor {
        pub Value_recv: UnboundedReceiver<super::ipt::Value>,
        pub EnableLogQueue: EnableLogQueue,
    }

    impl log_outActor {
        pub async fn tick(&mut self) {
            let i0 = self.Value_recv.recv().await.unwrap();
            let mut i1 = self.EnableLogQueue.pull();
            super::ipt::log_out(i0, &mut *i1);
        }
    }
}
