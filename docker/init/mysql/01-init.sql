-- MySQL initialization script for goth-datastore testing
-- This script sets up the test database and user with proper permissions

-- Create test database if it doesn't exist
CREATE DATABASE IF NOT EXISTS test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create test user with necessary permissions
-- Note: The MYSQL_USER and MYSQL_PASSWORD from docker-compose.yml will create the basic user
-- This script ensures additional permissions and configurations

-- Grant all privileges on test database to test user
GRANT ALL PRIVILEGES ON test.* TO 'test'@'%';

-- Grant basic privileges for connection testing
GRANT SELECT ON mysql.user TO 'test'@'%';

-- Ensure changes take effect
FLUSH PRIVILEGES;

-- Create some basic tables for testing if needed
USE test;

-- Basic test table for database connectivity testing
CREATE TABLE IF NOT EXISTS connection_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT IGNORE INTO connection_test (name) VALUES ('test_connection');

-- Show databases and tables for verification
SHOW DATABASES;
SHOW TABLES FROM test;
