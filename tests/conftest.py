"""Pytest configuration for Chainlit Cassandra Data Layer tests."""

from unittest.mock import MagicMock

import pytest
from chainlit.context import ChainlitContext, context_var


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
