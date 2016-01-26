use std::sync::mpsc::channel;
use std::thread;
use std::time;

fn main() {
    let nthreads = 1;
    let nmsgs = 10000000;
    let (qp, qc) = channel();
    for _ in 0..nthreads {
        let qp = qp.clone();
        thread::spawn(move|| {
            for i in 0..nmsgs {
                qp.send(());
            }
	println!("Done!");
            qp.send(());
        });
    }

    for i in 0..(nthreads * nmsgs) {
        qc.recv().unwrap();
    }
}
