fn main() {
    println!("Hello, world!");
}

// nefe can add values to a Generate Queue
// Consume Queue will distribute one message to one actor. Message may be taken
//#[nefe(queue(consume, generate), onqueue_on_timer(1s))]
pub struct MakeNumberInput {
    //#[nefe(value_generator = counter)]
    pub call_count: u64,
}

#[derive(Debug)]
// Share Queue will distribute every message to every actor. Message must be used as a reference
//#[nefe(queue(share))]
pub struct Value {
    pub v: String,
}

//#[nefe(onqueue(Value))]
pub fn make_value(input: MakeNumberInput) -> Value {
    Value {
        v: (input.call_count * 2).to_string(),
    }
}

//#[nefe(queue(share, generate), onqueue_on_demand)]
pub struct EnableLog {
    // Value is defined by the executor of the pipeline
    //#[nefe(user_value)]
    pub enable: bool,
}

//#[nefe(dequeue(&Value), dequeue(&EnableLog))]
pub fn log_out(value: Value) {
    eprintln!("{value:?}");
}
