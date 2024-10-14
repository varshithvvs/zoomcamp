import pytest
import pandas as pd
from datetime import datetime
from batch import prepare_data


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

@pytest.fixture
def sample_data():
   data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]   
   columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
   df = pd.DataFrame(data, columns=columns)
   return df

def test_prepare_data(sample_data: pd.DataFrame):
    categorical = ['PULocationID', 'DOLocationID']
    df_prepared = prepare_data(sample_data, categorical)
    
    prepared_data = df_prepared.values.tolist()
    expected_data = [
        ['-1', '-1', dt(1, 1), dt(1, 10), 9.0],
        ['1', '1', dt(1, 2), dt(1, 10), 8.0]
    ]
    
    # df_expected = pd.DataFrame(expected_data, columns=['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration'])
    
    assert len(df_prepared)==2
    assert prepared_data == expected_data