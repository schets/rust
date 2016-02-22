//! A fast unbounded spsc_queue. Uses a linked list of arrays

use sync::atomic::Ordering::{Acquire, Release, Relaxed};
use sync::atomic::{AtomicUsize, AtomicPtr};
use core::ptr;
use core::mem;
use core::cell::UnsafeCell;
use alloc::boxed::Box;

const SEG_SIZE: usize = 64;

struct Segment<T> {
    data: [UnsafeCell<T>; SEG_SIZE],
    next: AtomicPtr<Segment<T>>,
}

impl<T> Segment<T> {
    pub fn new() -> Segment<T> {
        Segment {
            data: unsafe { mem::uninitialized() },
            next: AtomicPtr::new(ptr::null_mut()),
        }
    }
}

/// A single-producer, single consumer queue
pub struct Queue<T> {
    cache_stack: AtomicPtr<Segment<T>>,
    cache_size: AtomicUsize,

    // These dummies result in a tremendous performance improvement, ~300%+
    _dummy_1: [u64; 8],
    // data for the consumer
    head: AtomicUsize,
    head_block: AtomicPtr<Segment<T>>,
    tail_cache: AtomicUsize,

    _dummy_2: [u64; 8],
    // data for the producer
    tail: AtomicUsize,
    tail_block: AtomicPtr<Segment<T>>,
}

unsafe impl<T: Send> Send for Queue<T> {}
unsafe impl<T: Send> Sync for Queue<T> {}

impl<T> Queue<T> {
    pub fn new() -> Queue<T> {
        let first_block = Box::into_raw(Box::new(Segment::new()));
        Queue {
            cache_stack: AtomicPtr::new(ptr::null_mut()),
            cache_size: AtomicUsize::new(0),

            _dummy_1: unsafe { mem::uninitialized() },
            head: AtomicUsize::new(1),
            head_block: AtomicPtr::new(first_block),
            tail_cache: AtomicUsize::new(1),

            _dummy_2: unsafe { mem::uninitialized() },
            tail: AtomicUsize::new(1),
            tail_block: AtomicPtr::new(first_block),
        }
    }

    //#[inline(always)]
    fn acquire_segment(&self) -> *mut Segment<T> {
        let mut chead = self.cache_stack.load(Acquire);
        loop {
            if chead == ptr::null_mut() {
                return Box::into_raw(Box::new(Segment::new()));
            }
            let next = unsafe { (*chead).next.load(Relaxed) };
            let cas = self.cache_stack.compare_and_swap(chead, next, Acquire);
            if cas == chead {
                self.cache_size.fetch_sub(1, Relaxed);
                unsafe { (*chead).next.store(ptr::null_mut(), Relaxed); }
                return chead
            }
            chead = cas;
        }
    }

    //#[inline(always)]
    fn release_segment(&self, seg: *mut Segment<T>) {
        let mut chead = self.cache_stack.load(Acquire);
        loop {
            unsafe { (*seg).next.store(chead, Relaxed); }
            if chead == ptr::null_mut() {
                self.cache_stack.store(seg, Release);
                return;
            }
            let cas = self.cache_stack.compare_and_swap(chead, seg, Release);
            if cas == chead {
                self.cache_size.fetch_add(1, Relaxed);
                return;
            }
            chead = cas;
        }
    }

    /// Tries constructing the element and inserts into the queue
    ///
    /// Returns the closure if there isn't space
    //#[inline(always)]
    pub fn push(&self, val: T) {
        let ctail = self.tail.load(Relaxed);
        let next_tail = ctail.wrapping_add(1);
        //SEG_SIZE is a power of 2, so this is cheap
        let write_ind = ctail % SEG_SIZE;
        let mut tail_block = self.tail_block.load(Relaxed);
        if write_ind == 0 {
            let next = self.acquire_segment();
            unsafe { (*tail_block).next.store(next, Relaxed); }
            tail_block = next;
            self.tail_block.store(next, Relaxed);
        }
        unsafe {
            let data_pos = (*tail_block).data[write_ind].get();
            ptr::write(data_pos, val);
        }
        self.tail.store(next_tail, Release);
    }

    pub fn pop(&self) -> Option<T> {
        let chead = self.head.load(Relaxed);
        if chead == self.tail_cache.load(Relaxed) {
            let cur_tail = self.tail.load(Acquire);
            self.tail_cache.store(cur_tail, Relaxed);
            if chead == cur_tail {
                return None;
            }
        }

        let next_head = chead + 1;
        let read_ind = chead % SEG_SIZE;
        let mut head_block = self.head_block.load(Relaxed);
        if read_ind == 0 {
            // Acquire is not needed because this can only happen
            // once the head/tail have moved appropriately (and synchronized)
            let next = unsafe{ (*head_block).next.load(Relaxed) };
            if next == ptr::null_mut() {
                return None;
            }
            self.release_segment(head_block);
            head_block = next;
            self.head_block.store(next, Relaxed);
        }
        unsafe {
            let data_pos = (*head_block).data[read_ind].get();
            let rval = Some(ptr::read(data_pos));
            // Nothing synchronizes with the head! so the store can be relaxed
            // A benefit of this is that the common case
            self.head.store(next_head, Relaxed);
            rval
        }
    }

    pub fn peek(&self) -> Option<&mut T> {
        let chead = self.head.load(Relaxed);
        if chead == self.tail_cache.load(Relaxed) {
            let cur_tail = self.tail.load(Acquire);
            self.tail_cache.store(cur_tail, Relaxed);
            if chead == cur_tail {
                return None;
            }
        }

        let read_ind = chead % SEG_SIZE;
        let head_block = self.head_block.load(Relaxed);
        if read_ind == 0 {
            // Acquire is not needed because this can only happen
            // once the head/tail have moved appropriately (and synchronized)
            let next = unsafe{ (*head_block).next.load(Relaxed) };
            if next == ptr::null_mut() {
                return None;
            }
            return unsafe { Some(&mut *(*next).data[read_ind].get()) }
        }
        unsafe {
            let data_pos = (*head_block).data[read_ind].get();
            Some(&mut *data_pos)
        }
    }
}


impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        loop {
            if let None = self.pop() {
                break;
            }
        }
        let mut cache_head = self.cache_stack.load(Relaxed);
        while cache_head != ptr::null_mut() {
            unsafe {
                let next = (*cache_head).next.load(Relaxed);
                Box::from_raw(cache_head);
                cache_head = next;
            }
        }
        let head_block = self.head_block.load(Relaxed);
        let tail_block = self.tail_block.load(Relaxed);
        unsafe { Box::from_raw(head_block); }
        if tail_block != head_block { unsafe { Box::from_raw(tail_block); } }
    }
}

/*
#[allow(unused_must_use)]
#[cfg(test)]
mod test {

    use scope;
    use super::*;
    use std::sync::atomic::Ordering::{Relaxed};
    use std::sync::atomic::AtomicUsize;
    const CONC_COUNT: i64 = 1000000;

    #[test]
    fn push_pop_1_b() {
        let (prod, cons) = Queue::<i64>::new(1);
        assert_eq!(prod.try_push(37), Ok(()));
        assert_eq!(cons.try_pop(), Some(37));
        assert_eq!(cons.try_pop(), None)
    }


    #[test]
    fn push_pop_2_b() {
        let (prod, cons) = Queue::<i64>::new(1);
        assert_eq!(prod.try_push(37).is_ok(), true);
        assert_eq!(prod.try_construct(|| 48).is_ok(), true);
        assert_eq!(cons.try_pop(), Some(37));
        assert_eq!(cons.try_pop(), Some(48));
        assert_eq!(cons.try_pop(), None)
    }

    #[test]
    fn push_pop_many_seq() {
        let (prod, cons) = Queue::<i64>::new(5);
        for i in 0..200 {
            assert_eq!(prod.try_push(i).is_ok(), true);
        }
        for i in 0..200 {
            assert_eq!(cons.try_pop(), Some(i));
        }
    }

    #[test]
    fn push_bounded() {
        //this is strange but a side effect of starting...
        let msize = 63;
        let (prod, cons) = Queue::<i64>::new(1);
        for _ in 0..msize {
            assert_eq!(prod.try_push(1).is_ok(), true);
        }
        assert_eq!(prod.try_push(2), Err(2));
        assert_eq!(cons.try_pop(), Some(1));
        assert_eq!(prod.try_push(2).is_ok(), true);
        for _ in 0..(msize-1) {
            assert_eq!(cons.try_pop(), Some(1));
        }
        assert_eq!(cons.try_pop(), Some(2));

    }

    struct Dropper<'a> {
        aref: &'a AtomicUsize,
    }

    impl<'a> Drop for Dropper<'a> {
        fn drop(& mut self) {
            self.aref.fetch_add(1, Relaxed);
        }
    }

    #[test]
    fn drop_on_dtor() {
        let msize = 100;
        let drop_count = AtomicUsize::new(0);
        {
            let (prod, _) = Queue::new(msize);
            for _ in 0..msize {
                prod.try_push(Dropper{aref: &drop_count});
            };
        }
        assert_eq!(drop_count.load(Relaxed), msize);
    }

    #[test]
    fn push_pop_many_spsc() {
        return;
        let qsize = 100;
        let (prod, cons) = Queue::<i64>::new(100);

        scope(|scope| {
            scope.spawn(move || {
                let mut next = 0;

                while next < CONC_COUNT {
                    if let Some(elem) = cons.try_pop() {
                        assert_eq!(elem, next);
                        next += 1;
                    }
                }
            });

            let mut i = 0;
            while i < CONC_COUNT {
                match prod.try_push(i) {
                    Err(_) => continue,
                    Ok(_) => {i += 1;},
                }
            }
        });
    }
/*
    #[test]
    fn test_capacity() {
        let qsize = 100;
        let (prod, cons) = Queue::<i64>::new(qsize);
        assert_eq!(prod.capacity(), qsize);
        assert_eq!(cons.capacity(), qsize);
        for _ in 0..(qsize/2) {
            prod.try_push(1);
        }
        assert_eq!(prod.capacity(), qsize);
        assert_eq!(cons.capacity(), qsize);
    }*/
/*
    #[test]
    fn test_life_queries() {
        let (prod, cons) = Queue::<i64>::new();
        assert_eq!(prod.is_consumer_alive(), true);
        assert_eq!(cons.is_producer_alive(), true);
        assert_eq!(prod.try_push(1), Ok(()));
        {
            let _x = cons;
            assert_eq!(prod.is_consumer_alive(), true);
            assert_eq!(prod.create_consumer().is_none(), true);
        }
        assert_eq!(prod.is_consumer_alive(), false);
        assert_eq!(prod.try_push(1), Err(1));
        let new_cons_o = prod.create_consumer();
        assert_eq!(prod.is_consumer_alive(), true);
        assert_eq!(new_cons_o.is_some(), true);
        assert_eq!(prod.create_consumer().is_none(), true);
        let new_cons = new_cons_o.unwrap();

        {
            let _x = prod;
            assert_eq!(new_cons.is_producer_alive(), true);
            assert_eq!(new_cons.create_producer().is_none(), true);
        }
        assert_eq!(new_cons.is_producer_alive(), false);
        assert_eq!(new_cons.try_pop(), Some(1));
        let new_prod = new_cons.create_producer();
        assert_eq!(new_prod.is_some(), true);
        assert_eq!(new_cons.create_producer().is_none(), true);
    }*/
}*/
