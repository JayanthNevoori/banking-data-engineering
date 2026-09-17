USE banking_project;

CREATE TABLE IF NOT EXISTS customer (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    phone STRING,
    email STRING,
    city STRING,
    state STRING,
    customer_type STRING,
    created_date DATE,
    modified_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS branch (
    branch_id STRING,
    branch_name STRING,
    city STRING,
    state STRING,
    branch_type STRING
);

CREATE TABLE IF NOT EXISTS account (
    account_id STRING,
    customer_id STRING,
    branch_id STRING,
    account_type STRING,
    account_status STRING,
    opening_date DATE,
    closing_date DATE,
    balance DECIMAL(18,2),
    modified_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_data (
    transaction_id STRING,
    account_id STRING,
    transaction_date DATE,
    transaction_type STRING,
    transaction_amount DECIMAL(18,2),
    transaction_status STRING,
    channel STRING,
    merchant STRING,
    modified_date TIMESTAMP
);