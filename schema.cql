-- Cassandra schema for Chainlit Data Layer
-- This schema supports the BaseDataLayer interface for persisting users, threads, steps, elements, and feedback

-- Create keyspace (if not exists)
CREATE KEYSPACE IF NOT EXISTS chainlit
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE chainlit;

-- Drop existing tables to recreate with UUID types
DROP TABLE IF EXISTS feedbacks;
DROP TABLE IF EXISTS elements;
DROP TABLE IF EXISTS steps_by_thread_id;
DROP TABLE IF EXISTS steps;
DROP TABLE IF EXISTS threads;
DROP TABLE IF EXISTS users;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    identifier TEXT,
    created_at TIMESTAMP,
    metadata BLOB  -- MessagePack encoded metadata
);

-- SAI index on identifier for user lookups
CREATE CUSTOM INDEX IF NOT EXISTS users_identifier_idx ON users (identifier) USING 'StorageAttachedIndex';

-- Threads table
CREATE TABLE IF NOT EXISTS threads (
    id UUID PRIMARY KEY,
    user_id UUID,
    user_identifier TEXT,
    name TEXT,
    created_at TIMESTAMP,
    last_activity_at TIMESTAMP,  -- Timestamp of most recent step/activity
    metadata BLOB,  -- MessagePack encoded metadata
    tags LIST<TEXT>
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

-- Steps lookup table: maps step_id to thread_id for delete operations
-- This table allows us to find which thread a step belongs to when we only have the step_id
CREATE TABLE IF NOT EXISTS steps (
    id UUID PRIMARY KEY,
    thread_id UUID
);

-- Steps by thread table: optimized for get_thread() access pattern
-- Partition key is thread_id, clustering columns are (created_at, id)
-- This allows efficient retrieval of all steps in a thread in chronological order
-- The id is included in clustering to ensure uniqueness if created_at values collide
--
-- Write strategy: Use LOGGED BATCH to insert into both steps and steps_by_thread_id atomically
-- Delete strategy: Query steps table by id to get thread_id, then delete from both tables
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
    PRIMARY KEY (thread_id, created_at, id)
) WITH CLUSTERING ORDER BY (created_at ASC, id ASC);

-- Elements lookup table: maps element_id to thread_id for delete operations
-- Similar to the steps lookup table pattern
CREATE TABLE IF NOT EXISTS elements (
    id UUID PRIMARY KEY,
    thread_id UUID,
    for_id UUID
);

-- Elements by thread table: optimized for get_thread() access pattern
-- Partition key is thread_id, clustering key is id
-- This allows efficient retrieval of all elements in a thread without ALLOW FILTERING
CREATE TABLE IF NOT EXISTS elements_by_thread_id (
    thread_id UUID,
    id UUID,
    for_id UUID,
    mime TEXT,
    name TEXT,
    object_key TEXT,  -- Key in storage_client where file is stored
    url TEXT,  -- Direct URL if provided (external links)
    chainlit_key TEXT,
    display TEXT,
    size BIGINT,
    language TEXT,
    page INT,
    props TEXT,  -- JSON string
    created_at TIMESTAMP,  -- Timestamp when element was created
    PRIMARY KEY (thread_id, id)
);

-- Elements by step table: optimized for delete_step() access pattern
-- Partition key is for_id (step_id), clustering columns order elements chronologically
-- This allows efficient retrieval and deletion of all elements attached to a step
CREATE TABLE IF NOT EXISTS elements_by_step_id (
    for_id UUID,              -- step_id (partition key)
    created_at TIMESTAMP,     -- clustering column for chronological ordering
    id UUID,                  -- element_id (clustering column for uniqueness)
    thread_id UUID,           -- needed for delete_element()
    object_key TEXT,          -- for visibility during cleanup
    PRIMARY KEY (for_id, created_at, id)
) WITH CLUSTERING ORDER BY (created_at ASC, id ASC);

-- No SAI indexes needed! Single partition reads are fast.

-- Feedback table
CREATE TABLE IF NOT EXISTS feedbacks (
    id UUID PRIMARY KEY,
    for_id UUID,
    thread_id UUID,
    value INT,
    comment TEXT
);

-- SAI index on for_id (step_id) for querying feedback by step
CREATE CUSTOM INDEX IF NOT EXISTS feedbacks_for_id_idx ON feedbacks (for_id) USING 'StorageAttachedIndex';
-- SAI index on thread_id for cleanup operations (delete_thread cascade)
CREATE CUSTOM INDEX IF NOT EXISTS feedbacks_thread_id_idx ON feedbacks (thread_id) USING 'StorageAttachedIndex';
