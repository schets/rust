/* Copyright (c) 2010-2011 Dmitry Vyukov. All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY DMITRY VYUKOV "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL DMITRY VYUKOV OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Dmitry Vyukov.
 */

//! A mostly lock-free multi-producer, single consumer queue.
//!
//! This module contains an implementation of a concurrent MPSC queue. This
//! queue can be used to share data between threads, and is also used as the
//! building block of channels in rust.
//!
//! Note that the current implementation of this queue has a caveat of the `pop`
//! method, and see the method for more information about it. Due to this
//! caveat, this queue may not be appropriate for all use-cases.

// http://www.1024cores.net/home/lock-free-algorithms
//                         /queues/non-intrusive-mpsc-node-based-queue

pub use self::PopResult::*;

use alloc::boxed::Box;
use core::ptr;
use vec::Vec;
use core::cell::UnsafeCell;

use sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

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

struct Node<T> {
    next: AtomicPtr<Node<T>>,
    value: Option<T>,
}

// A linked spmc or stack for the cache seems better,
// but both of those suffer from the aba problem.
// This has very comparable performance - practically identical
// This allows a bounded cache without maintaining a count,
// which means inserts iinvolve 0 atomics, meaning queue pops
// involve 0 atomics.

/// A SpMc cache for mpsc nodes
struct CacheBufferSpMc<T> {
    buffer_head: AtomicUsize,
    buffer_tail: AtomicUsize,
    buffer_mask: usize,
    cache_buffer: Vec<AtomicPtr<T>>,
    // FIXME:
    // If a cache barrier were present,
    // a tail local head cache would be useful
    // Once a good way for that is in the stdlib
    // it should be added here
}


impl<T> CacheBufferSpMc<T> {
    pub fn new (buffer: usize) -> CacheBufferSpMc<T> {
        debug_assert!(buffer > 0);
        debug_assert!(buffer % 2 == 0);
        let mut buff_vec = Vec::with_capacity(buffer + 1);
        for _ in 0..(buffer + 1) {
            buff_vec.push(AtomicPtr::new(ptr::null_mut()));
        }
        CacheBufferSpMc {
            cache_buffer: buff_vec,
            buffer_mask: buffer - 1,
            buffer_head: AtomicUsize::new(0),
            buffer_tail: AtomicUsize::new(0),
        }
    }


    #[cfg(not(target = "x86_64"))]
    pub fn add_or_delete(&self, ptr: *mut T) {
        let cur_tail = self.buffer_tail.load(Ordering::Relaxed);
        let next_tail = cur_tail.wrapping_add(1);
        let cur_tail_mask = cur_tail & self.buffer_mask;
        let cur_head = self.buffer_head.load(Ordering::Acquire);

        if next_tail - cur_head > (self.buffer_mask + 1) {
            unsafe {
                let _ = Box::from_raw(ptr);
                return
            }
        }

        self.cache_buffer[cur_tail_mask].store(ptr, Ordering::Relaxed);
        self.buffer_tail.store(next_tail, Ordering::Release);
    }

    #[cfg(target = "x86_64")]
    pub fn add_or_delete(&self, ptr: *mut T) {
        if ptr == ptr::null_mut() {
            return;
        }
        let cur_tail = self.buffer_tail.load(Ordering::Relaxed);
        let next_tail = cur_tail.wrapping_add(1);
        let cur_tail_mask = cur_tail & self.buffer_mask;

        let cur_val = &self.cache_buffer[cur_tail_mask];
        if cur_val.load(Ordering::Acquire) != ptr::null_mut() {
            unsafe {
                let _ = Box::from_raw(ptr);
                return
            }
        }
        cur_val.store(ptr, Ordering::Relaxed);

        self.buffer_tail.store(next_tail, Ordering::Release);
    }

    #[cfg(not(target = "x86_64"))]
    pub fn try_retrieve(&self) -> Option<*mut T> {
        let mut cur_head = self.buffer_head.load(Ordering::Acquire);
        let mut ctail = self.buffer_tail.load(Ordering::Acquire);

        if cur_head == ctail {
            return None
        }

        for _ in 0..3 {
            let cur_head_mask = cur_head & self.buffer_mask;

            let cur_ptr = &self.cache_buffer[cur_head_mask];
            let rval = cur_ptr.load(Ordering::Relaxed);
            let old_head = cur_head;
            cur_head = self.buffer_head.compare_and_swap(cur_head,
                                                         cur_head.wrapping_add(1),
                                                         Ordering::Release);
            if cur_head == old_head {
                return Some(rval)
            }
            let mut ctail = self.buffer_tail.load(Ordering::Acquire);
            if cur_head == ctail {
                return None
            }
        }
        None
    }


    // This is far better under x86, since multiple consumers can
    // act at the same time. I don't know about arm, since there's
    // ll/sc contention on the add.
    #[cfg(target = "x86_64")]
     pub fn try_retrieve(&self) -> Option<*mut T> {
        //let cur_head_guess = self.buffer_head.load(Ordering::A);
        let ctail = self.buffer_tail.load(Ordering::Acquire);



        let head_slot = self.buffer_head.fetch_add(1, Ordering::Relaxed);
        let cur_head_mask = head_slot & self.buffer_mask;
        let cur_ptr = &self.cache_buffer[cur_head_mask];
        // This doesn't need to be acquire, since we have synchronized
        // with the tail value
        let cur_val = cur_ptr.load(Ordering::Relaxed);
        if head_slot >= ctail || cur_val == ptr::null_mut() {
            self.buffer_head.fetch_sub(1, Ordering::Relaxed);
            return None
        }



        let rval = Some(cur_val);
        cur_val.store(ptr::null_mut(), Ordering::Release);
        rval
    }
}

/// The multi-producer single-consumer structure. This is not cloneable, but it
/// may be safely shared so long as it is guaranteed that there is only one
/// popper at a time (many pushers are allowed).
pub struct Queue<T> {
    head: AtomicPtr<Node<T>>,
    tail: UnsafeCell<*mut Node<T>>,
    cache: CacheBufferSpMc<Node<T>>,
}

unsafe impl<T: Send> Send for Queue<T> { }
unsafe impl<T: Send> Sync for Queue<T> { }

impl<T> Node<T> {
    unsafe fn new(v: Option<T>) -> *mut Node<T> {
        Box::into_raw(box Node {
            next: AtomicPtr::new(ptr::null_mut()),
            value: v,
        })
    }
    unsafe fn from_ptr(v: Option<T>, ptr: *mut Node<T>) -> *mut Node<T> {
        *ptr = Node {
            next: AtomicPtr::new(ptr::null_mut()),
            value: v,
        };
        ptr
    }
}

impl<T> Queue<T> {
    /// Creates a new queue that is safe to share among multiple producers and
    /// one consumer.
    pub fn new(buffer: usize) -> Queue<T> {
        let stub = unsafe { Node::new(None) };
        Queue {
            head: AtomicPtr::new(stub),
            tail: UnsafeCell::new(stub),
            cache: CacheBufferSpMc::new(buffer),
        }
    }

    /// Pushes a new value onto this queue.
    pub fn push(&self, t: T) {
        unsafe {
            //self.cache.try_retrieve().map(|p| Box::from_raw(p));
            let n = match self.cache.try_retrieve() {
                Some(ptr) => Node::from_ptr(Some(t), ptr),
                None => Node::new(Some(t)),
            };
            let prev = self.head.swap(n, Ordering::AcqRel);
            (*prev).next.store(n, Ordering::Release);
        }
    }

    /// Pops some data from this queue.
    ///
    /// Note that the current implementation means that this function cannot
    /// return `Option<T>`. It is possible for this queue to be in an
    /// inconsistent state where many pushes have succeeded and completely
    /// finished, but pops cannot return `Some(t)`. This inconsistent state
    /// happens when a pusher is pre-empted at an inopportune moment.
    ///
    /// This inconsistent state means that this queue does indeed have data, but
    /// it does not currently have access to it at this time.
    pub fn pop(&self) -> PopResult<T> {
        unsafe {
            let tail = *self.tail.get();
            let next = (*tail).next.load(Ordering::Acquire);

            if !next.is_null() {
                *self.tail.get() = next;
                assert!((*tail).value.is_none());
                assert!((*next).value.is_some());
                let ret = (*next).value.take().unwrap();
                self.cache.add_or_delete(tail);
                return Data(ret);
            }

            if self.head.load(Ordering::Acquire) == tail {Empty} else {Inconsistent}
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        unsafe {
            let mut cur = *self.tail.get();
            while !cur.is_null() {
                let next = (*cur).next.load(Ordering::Relaxed);
                let _: Box<Node<T>> = Box::from_raw(cur);
                cur = next;
            }
            loop {
                match self.cache.try_retrieve() {
                    Some(ptr) => {let _ = Box::from_raw(ptr);},
                    None => break,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use prelude::v1::*;

    use sync::mpsc::channel;
    use super::{Queue, Data, Empty, Inconsistent};
    use sync::Arc;
    use thread;

    #[test]
    fn test_full() {
        let q: Queue<Box<_>> = Queue::new(128);
        q.push(box 1);
        q.push(box 2);
    }

    #[test]
    fn test() {
        let nthreads = 8;
        let nmsgs = 1000;
        let q = Queue::new(128);
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
