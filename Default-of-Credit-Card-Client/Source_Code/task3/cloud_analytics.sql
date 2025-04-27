-- Cloud Analytics with Snowflake for Default of Credit Card Clients Dataset
-- Author: [Your Name]
-- Date: March 22, 2025

-- Step 1: Setup Database and Schema
-- Create database and schema if they don't exist
CREATE DATABASE IF NOT EXISTS credit_db;
USE DATABASE credit_db;
CREATE SCHEMA IF NOT EXISTS analytics;
USE SCHEMA analytics;

-- Step 2: Create Raw Table
-- Define table structure matching credit_card.csv (30,000 rows, 24 columns)
CREATE OR REPLACE TABLE credit_card_raw (
    ID INT,
    LIMIT_BAL INT,
    SEX INT,
    EDUCATION INT,
    MARRIAGE INT,
    AGE INT,
    PAY_0 INT,
    PAY_2 INT,
    PAY_3 INT,
    PAY_4 INT,
    PAY_5 INT,
    PAY_6 INT,
    BILL_AMT1 INT,
    BILL_AMT2 INT,
    BILL_AMT3 INT,
    BILL_AMT4 INT,
    BILL_AMT5 INT,
    BILL_AMT6 INT,
    PAY_AMT1 INT,
    PAY_AMT2 INT,
    PAY_AMT3 INT,
    PAY_AMT4 INT,
    PAY_AMT5 INT,
    PAY_AMT6 INT,
    default_payment_next_month INT
);

-- Step 3: Create Stage for S3
-- Point to your S3 bucket (replace with your bucket URL and credentials)
CREATE OR REPLACE STAGE credit_stage
    URL = 's3://your-snowflake-bucket/'
    CREDENTIALS = (AWS_KEY_ID = 'YOUR_ACCESS_KEY' AWS_SECRET_KEY = 'YOUR_SECRET_KEY');

-- Step 4: Load Data from S3
-- Load credit_card.csv into the raw table
COPY INTO credit_card_raw
    FROM @credit_stage/credit_card.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- Verify loaded data
SELECT * FROM credit_card_raw LIMIT 5;

-- Step 5: Data Integration
-- Create a cleaned and enriched table
CREATE OR REPLACE TABLE credit_card_clean AS
SELECT
    ID,
    LIMIT_BAL AS credit_limit,
    SEX,
    EDUCATION,
    MARRIAGE,
    AGE,
    PAY_0 AS pay_status_sept,
    PAY_2 AS pay_status_aug,
    PAY_3 AS pay_status_july,
    PAY_4,
    PAY_5,
    PAY_6,
    BILL_AMT1,
    BILL_AMT2,
    BILL_AMT3,
    BILL_AMT4,
    BILL_AMT5,
    BILL_AMT6,
    PAY_AMT1,
    PAY_AMT2,
    PAY_AMT3,
    PAY_AMT4,
    PAY_AMT5,
    PAY_AMT6,
    default_payment_next_month AS default_next_month,
    CASE
        WHEN LIMIT_BAL <= 50000 THEN 'Low'
        WHEN LIMIT_BAL <= 200000 THEN 'Medium'
        ELSE 'High'
    END AS credit_limit_category
FROM credit_card_raw;

-- Verify integrated data
SELECT * FROM credit_card_clean LIMIT 5;

-- Step 6: SQL Queries for Insights
-- Trend: Average bill amounts over months
SELECT
    AVG(BILL_AMT1) AS avg_bill_sept,
    AVG(BILL_AMT2) AS avg_bill_aug,
    AVG(BILL_AMT3) AS avg_bill_july
FROM credit_card_clean;

-- Pattern: Default rate by education level
SELECT
    EDUCATION,
    COUNT(*) AS total_clients,
    SUM(default_next_month) AS default_count,
    ROUND((SUM(default_next_month) * 100.0 / COUNT(*)), 2) AS default_rate
FROM credit_card_clean
GROUP BY EDUCATION
ORDER BY default_rate DESC;

-- Summary: Total payments and defaults
SELECT
    SUM(PAY_AMT1 + PAY_AMT2 + PAY_AMT3 + PAY_AMT4 + PAY_AMT5 + PAY_AMT6) AS total_payments,
    SUM(default_next_month) AS total_defaults
FROM credit_card_clean;

-- Step 7: Showcase Snowflake Features
-- Time Travel Demonstration
-- Check current timestamp for reference
SELECT CURRENT_TIMESTAMP;

-- Record initial state of PAY_AMT1 for first 5 rows
SELECT ID, PAY_AMT1
FROM credit_card_clean
WHERE ID <= 5;

-- Update PAY_AMT1 (increase by 10%)
UPDATE credit_card_clean
SET PAY_AMT1 = PAY_AMT1 * 1.10;

-- Wait ~10 seconds manually (pause execution briefly), then query state before update
SELECT ID, PAY_AMT1
FROM credit_card_clean
AT (OFFSET => -10)  -- 10 seconds ago
WHERE ID <= 5;

-- Compare with current state
SELECT ID, PAY_AMT1
FROM credit_card_clean
WHERE ID <= 5;

-- Query Optimization Demonstration
-- Complex aggregation to show Snowflake's optimization
SELECT
    credit_limit_category,
    AVG(BILL_AMT1 + BILL_AMT2 + BILL_AMT3 + BILL_AMT4 + BILL_AMT5 + BILL_AMT6) AS avg_total_bill
FROM credit_card_clean
GROUP BY credit_limit_category
ORDER BY avg_total_bill DESC;

-- End of Script
