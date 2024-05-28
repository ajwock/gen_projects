#![allow(dead_code)]
use std::sync::mpsc::Sender;
use std::sync::mpsc;
use std::fmt::Debug;
use std::collections::VecDeque;

fn insertion_sort<T: Ord>(slice: &mut [T]) {
    for i in 1..slice.len() {
        let mut j = i;
        while j >= 1 && slice[j - 1] > slice[j] {
            slice.swap(j, j - 1);
            j -= 1;
        }
    }
}

// left -> mid -> right ->
fn triangular_rotate<T: Debug>(slice: &mut [T], left: &mut usize, mid: &mut usize, right: &mut usize) {
    slice.swap(*left, *mid);
    std::mem::swap(left, mid);
    // Note: the index mid originally pointed to is now in left
    slice.swap(*mid, *right);
    std::mem::swap(mid, right);
}

fn swap_update<T>(slice: &mut [T], left: &mut usize, right: &mut usize) {
    slice.swap(*left, *right);
    std::mem::swap(left, right);
}

fn compare_and_swap_indices<T: Ord + Debug>(slice: &mut [T], left: &mut usize, right: &mut usize) -> bool {
    if &slice[*left] > &slice[*right] {
        let mut after_left = *left + 1;
        if after_left == *right {
            swap_update(slice, left, right);
        } else {
            triangular_rotate(slice, left, &mut after_left, right);
        }
        true
    } else {
        false
    }
}

fn select_pivot<T: Ord + Debug>(_slice: &mut [T]) -> usize {
    0
}

fn partition<T: Ord + Debug>(slice: &mut [T]) -> (&mut [T], &mut [T]) {
    assert!(slice.len() >= 1);
    let mut pivot = select_pivot(slice);
    slice.swap(0, pivot);
    for mut i in 1..slice.len() {
        compare_and_swap_indices(slice, &mut pivot, &mut i);
    }
    let (left, remainder) = slice.split_at_mut(pivot);
    (left, &mut remainder[1..])
}

fn quicksort<T: Ord + Debug>(slice: &mut [T]) {
    if slice.len() <= 64 {
        insertion_sort(slice);
    }
    if slice.len() > 1 {
        let (left, right) = partition(slice);
        quicksort(left);
        quicksort(right);
    }
}

#[derive(Debug)]
enum SubordinateMsg<'a, T> {
    Job(usize, &'a mut [T]),
    // Sorted is used to count how much of the slice has been sorted in total.
    // It also indicates to the manager thread that this is the last in a series
    // of 1 or more messages from the sending thread, and that it is ready for a new job.
    Sorted(usize, usize),
    Finished,
}

#[derive(Debug)]
enum ManagerMsg<'a, T> {
    Job(&'a mut [T]),
    Finish,
}

fn quicksort_job<'b, T: Ord + Debug + Send>(tid: usize, tx: &mut Sender<SubordinateMsg<'b, T>>, slice: &'b mut[T]) {
    if slice.len() <= 0x8000 {
        quicksort(slice);
        tx.send(SubordinateMsg::Sorted(tid, slice.len())).unwrap();
    } else {
        let (left, right) = partition(slice);
        tx.send(SubordinateMsg::Job(tid, left)).unwrap();
        tx.send(SubordinateMsg::Job(tid, right)).unwrap();
        tx.send(SubordinateMsg::Sorted(tid, 1)).unwrap();
    }
}

fn threaded_quicksort<T: Ord + Debug + Send>(slice: &mut [T], n_threads: usize) {
    // Don't thread when it's unnecessary
    if slice.len() <= 0x10000 {
        quicksort(slice);
        return
    }

    // Determine the number of threads to use.  If the n_threads argument is 0,
    // determine this value from the system.
    let num_threads = if n_threads == 0 {
        if let Ok(n) = std::thread::available_parallelism() {
            n.get()
        } else {
            1
        }
    } else { n_threads };

    std::thread::scope(|s| {
        let (sub_tx, man_rx) = mpsc::channel();

        let mut man_txers = Vec::new();
        // Workers
        for i in 0..num_threads {
            let sub_tx = sub_tx.clone();
            let (man_tx, sub_rx) = mpsc::channel();
            man_txers.push(man_tx);
            s.spawn(move || {
                let thread_id = i;
                let rx = sub_rx;
                let mut tx = sub_tx;
                loop {
                    let msg = rx.recv().unwrap();
                    //println!("thread {}: Got msg: {:?}", thread_id, msg);
                    match msg {
                        ManagerMsg::Job(slice) => {
                            //println!("thread {}: got len: {}", thread_id, slice.len());
                            quicksort_job(thread_id, &mut tx, slice);
                        }
                        ManagerMsg::Finish => {
                            tx.send(SubordinateMsg::Finished).unwrap();
                            return
                        }
                    }
                }

            });
        }

        let slots_to_sort = slice.len();
        let mut threads_finished = 0;
        let mut slots_sorted = 0;
        let mut job_queue = VecDeque::new();
        let mut waiting_for_jobs = VecDeque::new();
        for i in 1..num_threads {
            waiting_for_jobs.push_back(i);
        }
        man_txers[0].send(ManagerMsg::Job(slice)).unwrap();
        let mut finished_msg_sent = false;

        'finish : loop {
            let msg = man_rx.recv().unwrap();
            //println!("Manager: Got msg: {:?}", msg);
            match msg {
                SubordinateMsg::Job(_, slice) => {
                    if slice.len() > 1 {
                        job_queue.push_back(slice)
                    } else {
                        slots_sorted += slice.len();
                    }
                }
                SubordinateMsg::Sorted(tid, size) => {
                    waiting_for_jobs.push_back(tid);
                    slots_sorted += size;
                    if slots_sorted == slots_to_sort && !finished_msg_sent {
                        assert_eq!(job_queue.len(), 0);
                        for tx in man_txers.iter_mut() {
                            tx.send(ManagerMsg::Finish).unwrap();
                        }
                        finished_msg_sent = true;
                    }
                    while !job_queue.is_empty() && !waiting_for_jobs.is_empty() {
                        // FIFO job feed strategy ensures that usually the larger remaining portions
                        // are sent off to threads.
                        // A heap based strategy might be even better.
                        let (Some(slice), Some(tid)) = (job_queue.pop_front(), waiting_for_jobs.pop_front()) else {
                            unreachable!() // We looked before we lept.
                        };
                        man_txers[tid].send(ManagerMsg::Job(slice)).unwrap();
                    }

                }
                SubordinateMsg::Finished => {
                    threads_finished += 1;
                    // println!("{} / {} threads finished", threads_finished, num_threads);
                    if threads_finished == num_threads {
                        assert_eq!(job_queue.len(), 0);
                        break 'finish;
                    }
                }
            }
        }

    });
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn test_insertion_sort() {
        let mut unsorted_guy = [0, 7, 3, 6, 1, 2, 4, 5];
        insertion_sort(&mut unsorted_guy);
        assert_eq!(unsorted_guy, [0, 1, 2, 3, 4, 5, 6, 7]);

        let mut empty: [u32; 0] = [];
        insertion_sort(&mut empty);
        assert_eq!(empty, []);

        let mut singular = [-1];
        insertion_sort(&mut singular);
        assert_eq!(singular, [-1]);
    }

    #[test]
    fn test_quicksort() {
        let mut unsorted_guy = [0, 7, 3, 6, 1, 2, 4, 5];
        quicksort(&mut unsorted_guy);
        assert_eq!(unsorted_guy, [0, 1, 2, 3, 4, 5, 6, 7]);

        let mut empty: [u32; 0] = [];
        quicksort(&mut empty);
        assert_eq!(empty, []);

        let mut singular = [-1];
        quicksort(&mut singular);
        assert_eq!(singular, [-1]);
    }

    #[test]
    fn test_threaded_quicksort() {
        let mut unsorted_guy = [0, 7, 3, 6, 1, 2, 4, 5];
        threaded_quicksort(&mut unsorted_guy, 4);
        assert_eq!(unsorted_guy, [0, 1, 2, 3, 4, 5, 6, 7]);

        let mut empty: [u32; 0] = [];
        threaded_quicksort(&mut empty, 4);
        assert_eq!(empty, []);

        let mut singular = [-1];
        threaded_quicksort(&mut singular, 4);
        assert_eq!(singular, [-1]);
    }

    use std::time::{Duration, Instant};
    fn execution_time<F: FnOnce() -> ()>(f: F) -> Duration {
        let start = Instant::now();
        f();
        let end= Instant::now();
        end.duration_since(start)
    }

    fn generate_test_vec(cap: usize) -> Vec<u64> {
        let mut rng = StdRng::from_seed([8; 32]);
        let mut unsorted_vec = Vec::<u64>::with_capacity(cap);
        for _ in 0..cap {
            unsorted_vec.push(rng.gen());
        }
        unsorted_vec
    }

    fn cool_processor(seconds: usize) {
         for i in (0..seconds).rev() {
            println!("Letting processor cool... {}s", i + 1);
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn run_timed_test<F: FnOnce(&mut [u64]) -> ()>(f: F, name: &str, size: usize) {
        let mut unsorted_vec = generate_test_vec(size);
        println!("Kicking off performance test of {}", name);
        let dur = execution_time(|| f(&mut unsorted_vec));
        println!("{}: {:?} (Validating)", name, dur);
        // monotonicity
        assert!(unsorted_vec.windows(2).all(|window| window[0] <= window[1]));
        println!("{}: validated", name);
    }

    #[test]
    fn test_threaded_quicksort_large() {
        let test_size = 0x1000000;
        run_timed_test(|x| threaded_quicksort(x, 32), "threaded_quicksort", test_size);
        cool_processor(3);
        run_timed_test(|x| quicksort(x), "quicksort", test_size);
    }


    #[test]
    fn test_partition() {
        let mut unsorted_guy = [4, 0, 6, 3, 1, 2, 7, 5];
        let (left, right) = partition(&mut unsorted_guy);
        assert_eq!(left.len(), 4);
        assert_eq!(right.len(), 3);
        assert!(left.iter().all(|x| *x < 4));
        assert!(right.iter().all(|x| *x > 4));
    }
}
