import pytest
from data_container import config

@pytest.mark.parametrize(
             'os, numpy_size, data_type, dtype_x, dtype_y',
    (
            ('Windows', 'default', 'heart_rate', 'float64', 'uint64'),
            ('Windows', 'default', 'ppg_ir', 'float64', 'uint64'),
            ('Windows', 'default', 'eeg_1', 'float64', 'int64'),
            ('Windows', 'default', 'acc_x', 'float64', 'float64'),

            ('Windows', 'maximize', 'heart_rate', 'float64', 'uint64'),
            ('Windows', 'default', 'ppg_ir', 'float64', 'uint64'),
            ('Windows', 'default', 'eeg_1', 'float64', 'int64'),
            ('Windows', 'maximize', 'acc_x', 'float64', 'float64'),

            ('Linux', 'default', 'heart_rate', 'float32', 'uint8'),
            ('Linux', 'default', 'ppg_ir', 'float64', 'uint32'),
            ('Linux', 'default', 'eeg_1', 'float64', 'int32'),
            ('Linux', 'default', 'acc_x', 'float64', 'float16'),

            ('Linux', 'maximize', 'heart_rate', 'float64', 'uint64'),
            ('Linux', 'maximize', 'ppg_ir', 'float64', 'uint64'),
            ('Linux', 'maximize', 'eeg_1', 'float64', 'int64'),
            ('Linux', 'maximize', 'acc_x', 'float64', 'float64'),
    )
)
def test_dtype_of_x_and_y_depending_on_operating_system_and_config_setting(
        fixture_empty_df, monkeypatch,
        os, numpy_size, data_type, dtype_x, dtype_y
):

    monkeypatch.setattr(config, '_operating_system', os)
    monkeypatch.setattr(config, '_numpy_size', numpy_size)

    df = fixture_empty_df
    df.append_value(data_type, 1, 0.1)

    # assert

    assert df.cols[data_type].x.dtype == dtype_x
    assert df.cols[data_type].y.dtype == dtype_y

