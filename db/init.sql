CREATE DATABASE IF NOT EXISTS sales;
USE sales;

CREATE TABLE transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    product_id INT,
    customer_id VARCHAR(255),
    transaction_amount DECIMAL(10, 2),
    transaction_date DATETIME
);

INSERT INTO transactions (transaction_id, product_id, customer_id, transaction_amount, transaction_date) VALUES
('txn_001', 101, 'cust_a', 29.99, '2025-08-15 09:15:00'),
('txn_002', 102, 'cust_b', 15.50, '2025-08-15 09:20:00'),
('txn_003', 101, 'cust_a', 29.99, '2025-08-15 10:05:00'),
('txn_004', 104, 'cust_c', 89.99, '2025-08-16 11:30:00'),
('txn_005', 105, 'cust_b', 5.75, '2025-08-16 12:10:00'),
('txn_006', 106, 'cust_d', 32.45, '2025-08-16 14:00:00');