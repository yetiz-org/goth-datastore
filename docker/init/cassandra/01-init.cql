-- Cassandra initialization script for goth-datastore testing
-- This script sets up the test keyspace and basic configurations

-- Create keyspace for testing
CREATE KEYSPACE IF NOT EXISTS cassandra
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Use the keyspace
USE cassandra;

-- Create a basic test table for connectivity testing
CREATE TABLE IF NOT EXISTS connection_test (
    id UUID PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMP
);

-- Insert test data
INSERT INTO connection_test (id, name, created_at) 
VALUES (now(), 'test_connection', toTimestamp(now()));

-- Describe keyspace for verification
DESCRIBE KEYSPACE cassandra;
