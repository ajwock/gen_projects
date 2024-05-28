#![allow(dead_code)]
use std::fmt::Debug;
use rayon::ThreadPool;

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
fn triangular_rotate<T>(slice: &mut [T], left: &mut usize, mid: &mut usize, right: &mut usize) {
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

fn compare_and_swap_indices<T: Ord>(slice: &mut [T], left: &mut usize, right: &mut usize) -> bool {
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

fn select_pivot<T: Ord>(_slice: &mut [T]) -> usize {
    0
}

fn partition<T: Ord>(slice: &mut [T]) -> (&mut [T], &mut [T]) {
    assert!(slice.len() >= 1);
    let mut pivot = select_pivot(slice);
    slice.swap(0, pivot);
    for mut i in 1..slice.len() {
        compare_and_swap_indices(slice, &mut pivot, &mut i);
    }
    let (left, remainder) = slice.split_at_mut(pivot);
    (left, &mut remainder[1..])
}

fn quicksort<T: Ord>(slice: &mut [T]) {
    if slice.len() <= 64 {
        insertion_sort(slice);
    }
    if slice.len() > 1 {
        let (left, right) = partition(slice);
        quicksort(left);
        quicksort(right);
    }
}

use rayon::Scope;

fn quicksort_job_scoped<'s, 'env: 's, T: Ord + Send>(s: &Scope<'s>, slice: &'env mut [T]) {
    if slice.len() <= 0x8000 {
        quicksort(slice);
    } else {
        let (left, right) = partition(slice);
        s.spawn(|s| quicksort_job_scoped(s, left));
        s.spawn(|s| quicksort_job_scoped(s, right));
    }
}

fn threaded_quicksort<T: Ord + Send>(slice: &mut [T], tp: &ThreadPool) {
    tp.scope(|s| {
        s.spawn(|s| {
            quicksort_job_scoped(s, slice);
        });
    });
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::prelude::*;
    use rayon::prelude::*;

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
        let tp = rayon::ThreadPoolBuilder::new().num_threads(4).build().unwrap();
        let mut unsorted_guy = [0, 7, 3, 6, 1, 2, 4, 5];
        threaded_quicksort(&mut unsorted_guy, &tp);
        assert_eq!(unsorted_guy, [0, 1, 2, 3, 4, 5, 6, 7]);

        let mut empty: [u32; 0] = [];
        threaded_quicksort(&mut empty, &tp);
        assert_eq!(empty, []);

        let mut singular = [-1];
        threaded_quicksort(&mut singular, &tp);
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
        let tp = rayon::ThreadPoolBuilder::new().num_threads(33).build().unwrap();
        let test_size = 0x1000000;
        run_timed_test(|x| threaded_quicksort(x, &tp), "threaded_quicksort", test_size);
        std::mem::drop(tp);
        cool_processor(3);
         
        run_timed_test(|x| x.par_sort_unstable(), "par_sort_unstable", test_size);
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
