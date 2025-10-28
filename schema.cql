-- Cassandra schema for Chainlit Data Layer
-- This schema supports the BaseDataLayer interface for persisting users, threads, steps, elements, and feedback

-- Create keyspace (if not exists)
CREATE KEYSPACE IF NOT EXISTS chainlit
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE chainlit;

-- Drop existing tables to recreate with UUID types
DROP TABLE IF EXISTS elements_by_step_id;
DROP TABLE IF EXISTS elements_by_thread_id;
DROP TABLE IF EXISTS elements;
DROP TABLE IF EXISTS steps_by_thread_id;
DROP TABLE IF EXISTS steps;
DROP TABLE IF EXISTS threads;
DROP TABLE IF EXISTS users_by_identifier;
DROP TABLE IF EXISTS users;

-- Drop obsolete secondary index if it exists
DROP INDEX IF EXISTS users_identifier_idx;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    identifier TEXT,
    created_at TIMESTAMP,
    metadata BLOB  -- MessagePack encoded metadata
);

-- Users by identifier lookup table for efficient identifier-based queries
CREATE TABLE IF NOT EXISTS users_by_identifier (
    identifier TEXT PRIMARY KEY,
    id UUID,
    created_at TIMESTAMP,
    metadata BLOB  -- MessagePack encoded metadata
);

-- Threads table
CREATE TABLE IF NOT EXISTS threads (
    id UUID PRIMARY KEY,
    user_id UUID,
    user_identifier TEXT,
    name TEXT,
    created_at TIMESTAMP,
    last_activity_at TIMESTAMP,  -- Timestamp of most recent step/activity
    metadata BLOB,  -- MessagePack encoded metadata
    tags LIST<TEXT>,
    deleted BOOLEAN  -- Soft-delete flag for two-phase deletion
);

-- Threads by user activity: optimized for list_threads query
-- Ordered by most recent activity (descending) for efficient pagination
CREATE TABLE IF NOT EXISTS threads_by_user_activity (
    user_id UUID,
    last_activity_at TIMESTAMP,
    thread_id UUID,
    thread_name TEXT,
    thread_created_at TIMESTAMP,
    PRIMARY KEY (user_id, last_activity_at, thread_id)
) WITH CLUSTERING ORDER BY (last_activity_at DESC, thread_id ASC);

-- No SAI index needed! Efficient single-partition reads with pre-sorted results.

-- Steps by thread table: optimized for get_thread() access pattern
-- Partition key is thread_id, clustering column is id
-- Steps are sorted in Python by created_at when returned to maintain chronological order
--
-- Write strategy: Single INSERT into steps_by_thread_id
-- Delete strategy: Use context.session.thread_id (following DynamoDB pattern) to delete by thread_id and id
CREATE TABLE IF NOT EXISTS steps_by_thread_id (
    thread_id UUID,
    id UUID,
    parent_id UUID,
    name TEXT,
    type TEXT,
    streaming BOOLEAN,
    wait_for_answer BOOLEAN,
    is_error BOOLEAN,
    metadata BLOB,  -- MessagePack encoded metadata
    tags LIST<TEXT>,
    input TEXT,
    output TEXT,
    created_at TIMESTAMP,
    start TIMESTAMP,
    end TIMESTAMP,
    generation BLOB,  -- MessagePack encoded generation data
    show_input TEXT,
    language TEXT,
    feedback_value INT,  -- Feedback value: 0 (thumbs down) or 1 (thumbs up)
    feedback_comment TEXT,  -- Optional feedback comment
    PRIMARY KEY (thread_id, id)
);

-- Elements by thread table: optimized for get_thread() access pattern
-- Partition key is thread_id, clustering column is id
-- Elements can be grouped by for_id (step) in Python when needed
--
-- Write strategy: Single INSERT into elements_by_thread_id
-- Delete strategy: Use context.session.thread_id (following DynamoDB pattern) to delete by thread_id and id
CREATE TABLE IF NOT EXISTS elements_by_thread_id (
    thread_id UUID,
    id UUID,
    for_id UUID,       -- Step ID this element belongs to (stored as regular column, not clustering)
    mime TEXT,
    name TEXT,
    object_key TEXT,   -- Key in storage_client where file is stored
    url TEXT,          -- Direct URL if provided (external links)
    chainlit_key TEXT,
    display TEXT,
    size BIGINT,
    language TEXT,
    page INT,
    props TEXT,        -- JSON string
    created_at TIMESTAMP,  -- Timestamp when element was created
    PRIMARY KEY (thread_id, id)
);

-- No SAI indexes needed! Single partition reads are fast.
