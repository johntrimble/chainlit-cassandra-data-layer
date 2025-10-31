"""Pytest configuration for Chainlit Cassandra Data Layer tests."""

import uuid
from unittest.mock import MagicMock

import pytest
from cassandra_asyncio.cluster import Cluster
from chainlit.context import ChainlitContext, context_var

from chainlit_cassandra_data_layer.data import CassandraDataLayer

TEST_KEYSPACE = "chainlit_test"


@pytest.fixture(autouse=True)
def mock_chainlit_context():
    """Mock Chainlit context for testing."""
    mock_context = MagicMock(spec=ChainlitContext)
    mock_session = MagicMock()
    mock_session.has_first_interaction = True  # Bypass queue_until_user_message
    mock_context.session = mock_session

    # Set the context in context_var
    token = context_var.set(mock_context)

    yield mock_context

    # Reset the context
    context_var.reset(token)


@pytest.fixture
def cassandra_session():
    """Provide a Cassandra session for tests."""
    cluster = Cluster(contact_points=["cassandra"])
    session = cluster.connect()
    try:
        yield session
    finally:
        session.shutdown()
        cluster.shutdown()


@pytest.fixture
def data_layer(cassandra_session):
    """Create a CassandraDataLayer instance for testing."""
    cassandra_session.execute(f"DROP KEYSPACE IF EXISTS {TEST_KEYSPACE}")
    data_layer = CassandraDataLayer(session=cassandra_session, keyspace=TEST_KEYSPACE)
    data_layer.setup(replication_factor=1)
    try:
        yield data_layer
    finally:
        if not getattr(cassandra_session, "is_shutdown", False):
            cassandra_session.execute(f"DROP KEYSPACE IF EXISTS {TEST_KEYSPACE}")


@pytest.fixture
def test_user_id():
    """Generate a unique user ID for testing."""
    return str(uuid.uuid4())


@pytest.fixture
def test_thread_id():
    """Generate a unique thread ID for testing."""
    return str(uuid.uuid4())
