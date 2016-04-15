// Copyright 2013-2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

pub use self::PopResult::*;

use core::ptr;
use cor::cmp;
use core::cell::{UnsafeCell, Cell};

use sync::atomic::{AtomicPtr, AtomicUsize};
use sync::atomic::Odering::{Acquire, Release, Relaxed};

const NODE_SIZE: usize = 64;

/// Based on the Crossbeam SegQueue

/// A result of the `pop` function.
pub enum PopResult<T> {
    /// Some data has been popped
    Data(T),
    /// The queue is empty
    Empty,
    /// The queue is in an inconsistent state. Popping data should succeed, but
    /// some pushers have yet to make enough progress in order allow a pop to
    /// succeed. It is recommended that a pop() occur "in the near future" in
    /// order to see if the sender has made progress or not
    Inconsistent,
}

// This should do some C-like tricks to store
// a variable sized array inline so the segment size can be adjusted to
// the size of T - i.e. for example, store 4k bytes per node and
// switch to old queue when sizeof(T) is > 1k

#[repr(C)]
struct Node<T> {

    // Low stored at bottom of data to be 'distant' from producers
    low: Cell<usize>,

    data: [UnsafeCell<(T, bool)>; NODE_SIZE],

    // High stored near top since it will not be modified
    // by time consumer is near top
    high: AtomicUsize,

    // Next is at top for similar reasons to high
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    fn new() -> Node<T> {
        let rqueue = Node {
            data: unsafe { mem::uninitialized() },
            low: AtomicUsize::new(0),
            high: AtomicUsize::new(0),
            next: AtomicPtr::null(),
        };
        for val in rqueue.data.iter() {
            unsafe {
                (*val.get()).1 = AtomicBool::new(false);
            }
        }
        rqueue
    }
}

/// The multi-producer single-consumer structure. This is not cloneable, but it
/// may be safely shared so long as it is guaranteed that there is only one
/// popper at a time (many pushers are allowed).
pub struct Queue<T> {
    head: AtomicPtr<Node<T>>,
    tail: Cell<*mut Node<T>>,
}

unsafe impl<T: Send> Send for Queue<T> { }
unsafe impl<T: Send> Sync for Queue<T> { }
impl<T> Queue<T> {
    /// Create a new, empty queue.
    pub fn new() -> Queue<T> {
        let q = Queue {
            tail: Atomic::null(),
            head: Atomic::null(),
        };
        let sentinel = Box::new(Node::new());
        let sentinel = q.tail.store_and_ref(sentinel, Relaxed);
        q.head.store_shared(Some(sentinel), Relaxed);
        q
    }

    /// Add `t` to the back of the queue.
    pub fn push(&self, t: T) {
        loop {
            let head = unsafe { &*self.head.load(Acquire) };
            if head.high.load(Relaxed) >= NODE_SIZE { continue }
            let i = head.high.fetch_add(1, Relaxed);
            unsafe {
                if i < NODE_SIZE {
                    let cell = head.data.get_unchecked(i).get();

                    // of note -  this part is worse for large objects
                    // since the write of T happens after acquiring a queue
                    // spot, while the previous one only blocks between
                    // writing a single pointer
                    ptr::write(&mut (*cell).0, t);
                    (*cell).1.store(true, Release);

                    if i + 1 == NODE_SIZE {
                        let head = head.next.store_and_ref(Owned::new(Node::new()), Release);
                        self.head.store_shared(Some(head), Release);
                    }

                    return
                }
            }
        }
    }

    /// Attempt to dequeue from the front.
    ///
    /// Returns `None` if the queue is observed to be empty.
    pub fn pop(&self) -> PopResult<T> {
        let tail = unsafe { &*self.tail.get() };
        let low = tail.low.get();
        if low + 1 == NODE_SIZE {
            match tail.next.load(Acquire) {
                Some(next) => self.tail.store_shared(Some(next), Release),
                None => return Empty
            }
        }
        if low >= cmp::min(tail.high.load(Relaxed), NODE_SIZE) { return Empty }
        unsafe {
            let cell = (*tail).data.get_unchecked(low).get();
            if !(*cell).1.load(Acquire) {
                return Inconsistent
            }

            tail.low.set(low + 1);
            return Data(ptr::read(&(*cell).0))
        }
    }
}



#[cfg(test)]
mod tests {
    use prelude::v1::*;
    use alloc::boxed::Box;

    use sync::mpsc::channel;
    use super::{Queue, Data, Empty, Inconsistent};
    use sync::Arc;
    use thread;

    #[test]
    fn test_full() {
        let q: Queue<Box<_>> = Queue::new();
        q.push(box 1);
        q.push(box 2);
    }

    #[test]
    fn test() {
        let nthreads = 8;
        let nmsgs = 1000;
        let q = Queue::new();
        match q.pop() {
            Empty => {}
            Inconsistent | Data(..) => panic!()
        }
        let (tx, rx) = channel();
        let q = Arc::new(q);

        for _ in 0..nthreads {
            let tx = tx.clone();
            let q = q.clone();
            thread::spawn(move|| {
                for i in 0..nmsgs {
                    q.push(i);
                }
                tx.send(()).unwrap();
            });
        }

        let mut i = 0;
        while i < nthreads * nmsgs {
            match q.pop() {
                Empty | Inconsistent => {},
                Data(_) => { i += 1 }
            }
        }
        drop(tx);
        for _ in 0..nthreads {
            rx.recv().unwrap();
        }
    }
}
