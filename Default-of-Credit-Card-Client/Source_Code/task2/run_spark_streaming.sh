#!/bin/bash

# Script to automate Spark Streaming tutorial for credit_card.csv
# Uses current directory and a temp directory for chunk files

# Variables
WORKING_DIR="$(pwd)"  # Use current directory
PREPARE_SCRIPT="prepare_stream.py"
STREAM_SCRIPT="stream_processing.py"
INPUT_DIR="stream_input"
TEMP_DIR="stream_temp"  # Temporary directory for chunk files
CSV_FILE="credit_card.csv"

# Check if required files exist in the current directory
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: $CSV_FILE not found in $WORKING_DIR."
    exit 1
fi
if [ ! -f "$PREPARE_SCRIPT" ]; then
    echo "Error: $PREPARE_SCRIPT not found in $WORKING_DIR."
    exit 1
fi
if [ ! -f "$STREAM_SCRIPT" ]; then
    echo "Error: $STREAM_SCRIPT not found in $WORKING_DIR."
    exit 1
fi

# Step 1: Prepare the data by running prepare_stream.py into a temp directory
echo "Step 1: Preparing data chunks in $TEMP_DIR..."
rm -rf "$TEMP_DIR"  # Clear temp directory if it exists
mkdir -p "$TEMP_DIR"
sed -i "s|$INPUT_DIR|$TEMP_DIR|g" "$PREPARE_SCRIPT"  # Temporarily modify prepare_stream.py to use TEMP_DIR
python "$PREPARE_SCRIPT"
if [ $? -ne 0 ]; then
    echo "Error: Data preparation failed."
    exit 1
fi
sed -i "s|$TEMP_DIR|$INPUT_DIR|g" "$PREPARE_SCRIPT"  # Revert prepare_stream.py back to INPUT_DIR
echo "Data preparation complete."

# Step 2: Clear the stream_input directory for fresh simulation
echo "Step 2: Clearing $INPUT_DIR for streaming simulation..."
rm -rf "$INPUT_DIR"/*
if [ $? -ne 0 ]; then
    echo "Error: Failed to clear $INPUT_DIR."
    exit 1
fi
mkdir -p "$INPUT_DIR"

# Step 3: Start Spark Streaming in the background
echo "Step 3: Starting Spark Streaming process..."
python "$STREAM_SCRIPT" > streaming_output.log 2>&1 &
STREAM_PID=$!
echo "Spark Streaming started with PID $STREAM_PID. Output redirected to streaming_output.log."

# Wait a moment to ensure Spark is ready
sleep 5

# Step 4: Simulate real-time data arrival from temp directory
echo "Step 4: Simulating real-time data arrival..."
for i in {0..29}; do
    cp "$TEMP_DIR/credit_chunk_$i.csv" "$INPUT_DIR/" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "Moved credit_chunk_$i.csv to $INPUT_DIR"
    else
        echo "Warning: Could not move credit_chunk_$i.csv"
    fi
    sleep 5  # Wait 5 seconds between files
done

# Step 5: Wait for processing to complete and stop Spark
echo "Step 5: Waiting for streaming to process batches (30 seconds)..."
sleep 30  # Give Spark time to process the last few batches

echo "Stopping Spark Streaming process (PID: $STREAM_PID)..."
kill -SIGINT $STREAM_PID
if [ $? -eq 0 ]; then
    echo "Spark Streaming stopped successfully."
else
    echo "Error: Failed to stop Spark Streaming."
    exit 1
fi

# Step 6: Display results
echo "Step 6: Displaying results from streaming_output.log..."
cat streaming_output.log

echo "Automation complete. Check streaming_output.log for detailed output."
