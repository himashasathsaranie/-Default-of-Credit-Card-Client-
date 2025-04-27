import pandas as pd
import os
import time

# Directory to simulate incoming data
input_dir = "stream_input"
os.makedirs(input_dir, exist_ok=True)

# Read the full dataset
df = pd.read_csv("credit_card.csv")

# Split into chunks of 1000 rows each
chunk_size = 1000
for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    chunk_file = f"{input_dir}/credit_chunk_{i // chunk_size}.csv"
    chunk.to_csv(chunk_file, index=False)
    print(f"Created {chunk_file}")
    time.sleep(1)  # Simulate delay in data arrival

print("Data preparation complete.")
