use processing_millions_of_transmission_records_for_power_grid_infrastructure_analysis_at_scale_core::group_sum_by_key;

fn main() {
    let n = 10000usize;
    let keys: Vec<i64> = (0..n).map(|i| (i % 50) as i64).collect();
    let values: Vec<f64> = (0..n).map(|i| (i as f64 * 0.01).sin() + 1.0).collect();
    for _ in 0..5000 {
        let _ = group_sum_by_key(&keys, &values, 50);
    }
}
