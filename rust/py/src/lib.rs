use processing_millions_of_transmission_records_for_power_grid_infrastructure_analysis_at_scale_core::group_sum_by_key;
use numpy::{PyArray1, PyReadonlyArray1, IntoPyArray};
use pyo3::prelude::*;

#[pyfunction]
fn group_sum_by_key_py<'py>(
    py: Python<'py>,
    keys: PyReadonlyArray1<i64>,
    values: PyReadonlyArray1<f64>,
    n_groups: usize,
) -> PyResult<Bound<'py, PyArray1<f64>>> {
    Ok(group_sum_by_key(keys.as_slice()?, values.as_slice()?, n_groups).into_pyarray(py))
}

#[pyfunction]
#[pyo3(signature = (keys, values, n_groups, iterations=5_000))]
fn bench_kernel_py(
    keys: PyReadonlyArray1<i64>,
    values: PyReadonlyArray1<f64>,
    n_groups: usize,
    iterations: usize,
) -> PyResult<f64> {
    let k = keys.as_slice()?.to_vec();
    let v = values.as_slice()?.to_vec();
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        let _ = group_sum_by_key(&k, &v, n_groups);
    }
    Ok(start.elapsed().as_secs_f64())
}

#[pymodule]
fn processing_millions_of_transmission_records_for_power_grid_infrastructure_analysis_at_scale_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(group_sum_by_key_py, m)?)?;
    m.add_function(wrap_pyfunction!(bench_kernel_py, m)?)?;
    Ok(())
}
