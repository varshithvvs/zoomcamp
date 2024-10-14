import os
import pandas as pd
import boto3
from datetime import datetime

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

# Create the sample DataFrame
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]
columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df = pd.DataFrame(data, columns=columns)

# Save the DataFrame to a Parquet file locally
input_file = 'input.parquet'
df.to_parquet(input_file, engine='pyarrow', index=False)

# Set environment variables for Localstack
os.environ['S3_ENDPOINT_URL'] = 'http://localhost:4566'
os.environ['INPUT_FILE_PATTERN'] = 's3://nyc-duration/input.parquet'
os.environ['OUTPUT_FILE_PATTERN'] = 's3://nyc-duration/output.parquet'

# Configure the S3 client to use Localstack
s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

bucket_name = 'nyc-duration'

# Upload the input file to S3
s3.upload_file(input_file, bucket_name, 'input.parquet')

# Run the batch.py script for January 2023
os.system('cd module_6 && python batch.py 2023 1')


# Download the output file from S3
s3.download_file(bucket_name, 'output.parquet', 'output.parquet')

# Read the output file and verify the result
df_result = pd.read_parquet('output.parquet')
print('Sum of predicted durations:', df_result['predicted_duration'].sum())