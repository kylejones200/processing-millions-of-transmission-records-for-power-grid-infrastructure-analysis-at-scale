//! Group sum aggregation by integer keys.

pub fn group_sum_by_key(keys: &[i64], values: &[f64], n_groups: usize) -> Vec<f64> {
    assert_eq!(keys.len(), values.len());
    let mut sums = vec![0.0; n_groups];
    for (&k, &v) in keys.iter().zip(values) {
        let idx = k as usize;
        if idx < n_groups {
            sums[idx] += v;
        }
    }
    sums
}
