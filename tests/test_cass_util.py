"""Tests for async ResultSet wrapper utilities in cass_util.py.

This test module is independent of other tests and focuses on testing the
AsyncResultSetWrapper class for async iteration over Cassandra query results.
"""

import asyncio
import uuid
from typing import NamedTuple
from unittest.mock import AsyncMock, MagicMock

import pytest

from chainlit_cassandra_data_layer.cass_util import AsyncResultSetWrapper, aexecute


class MockRow(NamedTuple):
    """Mock row type for testing."""

    id: uuid.UUID
    name: str
    value: int


class MockResponseFuture:
    """Mock ResponseFuture that simulates cassandra-driver behavior.

    This mock simulates the behavior of ResponseFuture from cassandra-driver,
    which is what execute_async() returns. It supports callbacks and errbacks
    that receive the raw result (list from row_factory).
    """

    def __init__(self, pages: list[list[MockRow]], page_index: int = 0):
        """Initialize mock future with pages data.

        Args:
            pages: All available pages of data
            page_index: Current page index
        """
        self._pages = pages
        self._page_index = page_index
        self._callbacks = []
        self._errbacks = []
        # These attributes are needed for ResultSet construction
        self._col_names = ["id", "name", "value"]
        self._col_types = None
        self._paging_state = (
            f"page_{page_index}" if page_index < len(pages) - 1 else None
        )
        self.has_more_pages = page_index < len(pages) - 1

    def add_callback(self, fn, *args, **kwargs):
        """Register callback to be called with result.

        Callbacks receive the raw result (list of rows from row_factory),
        not a ResultSet object. This matches real ResponseFuture behavior.
        """
        self._callbacks.append((fn, args, kwargs))
        # Immediately trigger callback in async context
        import asyncio

        # Get current page rows (simulating row_factory output)
        result = (
            self._pages[self._page_index] if self._page_index < len(self._pages) else []
        )
        # Schedule callback
        loop = asyncio.get_event_loop()
        loop.call_soon(lambda: fn(result, *args, **kwargs))
        return self

    def add_errback(self, fn, *args, **kwargs):
        """Register errback to be called with exception."""
        self._errbacks.append((fn, args, kwargs))
        return self


class MockSession:
    """Mock Cassandra session that simulates execute_async() with pagination.

    This mock returns MockResponseFuture objects to simulate the real
    cassandra-driver Session.execute_async() method.
    """

    def __init__(self, pages: list[list[MockRow]]):
        """Initialize with pages of data.

        Args:
            pages: List of pages to return sequentially
        """
        self._pages = pages
        self._call_count = 0

    def execute_async(self, query, params=None, paging_state=None, **kwargs):
        """Mock execute_async that returns ResponseFuture.

        Args:
            query: Query (ignored in mock)
            params: Parameters (ignored in mock)
            paging_state: If provided, advances to next page
            **kwargs: Additional args (ignored)

        Returns:
            MockResponseFuture configured for the appropriate page
        """
        self._call_count += 1

        if paging_state is None:
            # First call - return first page
            page_index = 0
        else:
            # Extract page index from paging_state
            page_index = int(paging_state.split("_")[1]) + 1

        # Return ResponseFuture for this page
        return MockResponseFuture(self._pages, page_index)


@pytest.mark.asyncio
class TestAsyncResultSetWrapper:
    """Unit tests for AsyncResultSetWrapper with mocks."""

    async def test_single_page_iteration(self):
        """Test async iteration over a single page of results."""
        rows = [
            MockRow(uuid.uuid4(), "Alice", 100),
            MockRow(uuid.uuid4(), "Bob", 200),
            MockRow(uuid.uuid4(), "Charlie", 300),
        ]
        mock_session = MockSession([rows])
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        result = []
        async for row in wrapper:
            result.append(row)

        assert result == rows
        assert len(result) == 3
        assert result[0].name == "Alice"
        assert result[1].name == "Bob"
        assert result[2].name == "Charlie"

    async def test_multi_page_iteration(self):
        """Test async iteration with multiple pages (tests pagination)."""
        page1 = [
            MockRow(uuid.uuid4(), "Page1_Row1", 1),
            MockRow(uuid.uuid4(), "Page1_Row2", 2),
        ]
        page2 = [
            MockRow(uuid.uuid4(), "Page2_Row1", 3),
            MockRow(uuid.uuid4(), "Page2_Row2", 4),
        ]
        page3 = [
            MockRow(uuid.uuid4(), "Page3_Row1", 5),
        ]

        mock_session = MockSession([page1, page2, page3])
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        result = []
        async for row in wrapper:
            result.append(row)

        expected = page1 + page2 + page3
        assert result == expected
        assert len(result) == 5
        # Verify all pages were fetched
        assert result[0].name == "Page1_Row1"
        assert result[2].name == "Page2_Row1"
        assert result[4].name == "Page3_Row1"

        # Verify session.aexecute was called for each page
        assert mock_session._call_count == 3  # Initial + 2 pagination calls

    async def test_empty_result_set(self):
        """Test async iteration over empty result set."""
        mock_session = MockSession([[]])
        wrapper = await aexecute(mock_session, "SELECT * FROM empty")

        result = []
        async for row in wrapper:
            result.append(row)

        assert result == []

    async def test_column_names_property(self):
        """Test that column_names property is exposed."""
        rows = [MockRow(uuid.uuid4(), "Test", 1)]
        mock_session = MockSession([rows])
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        assert wrapper.column_names == ["id", "name", "value"]

    async def test_has_more_pages_property(self):
        """Test that has_more_pages reflects pagination state."""
        page1 = [MockRow(uuid.uuid4(), "Row1", 1)]
        page2 = [MockRow(uuid.uuid4(), "Row2", 2)]

        mock_session = MockSession([page1, page2])
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        # Before iteration starts
        assert wrapper.has_more_pages is True

        # Complete iteration in one go
        result = []
        async for row in wrapper:
            result.append(row)

        # After complete iteration
        assert len(result) == 2
        assert wrapper.has_more_pages is False

    async def test_current_rows_property(self):
        """Test that current_rows exposes the current page."""
        page1 = [MockRow(uuid.uuid4(), "Row1", 1), MockRow(uuid.uuid4(), "Row2", 2)]
        page2 = [MockRow(uuid.uuid4(), "Row3", 3)]

        mock_session = MockSession([page1, page2])
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        # Before iteration
        assert len(wrapper.current_rows) == 2

        # Iterate through all rows
        result = []
        async for row in wrapper:
            result.append(row)

        # After complete iteration
        assert len(result) == 3
        # After iteration, we should be on page 2 which has 1 row
        assert len(wrapper.current_rows) == 1

    async def test_concurrent_iteration(self):
        """Test that multiple wrappers can iterate concurrently without interference."""
        rows1 = [MockRow(uuid.uuid4(), f"Set1_Row{i}", i) for i in range(5)]
        rows2 = [MockRow(uuid.uuid4(), f"Set2_Row{i}", i) for i in range(5)]

        mock_session1 = MockSession([rows1])
        mock_session2 = MockSession([rows2])

        wrapper1 = await aexecute(mock_session1, "SELECT * FROM test1")
        wrapper2 = await aexecute(mock_session2, "SELECT * FROM test2")

        async def collect_rows(wrapper):
            result = []
            async for row in wrapper:
                await asyncio.sleep(0.001)  # Simulate some async work
                result.append(row)
            return result

        # Run both concurrently
        results = await asyncio.gather(collect_rows(wrapper1), collect_rows(wrapper2))

        assert results[0] == rows1
        assert results[1] == rows2

    async def test_with_query_parameters(self):
        """Test that query parameters are preserved across pagination."""
        page1 = [MockRow(uuid.uuid4(), "Alice", 1)]
        page2 = [MockRow(uuid.uuid4(), "Bob", 2)]

        mock_session = MockSession([page1, page2])
        test_params = (uuid.uuid4(), "test_value")

        wrapper = await aexecute(
            mock_session, "SELECT * FROM test WHERE id = %s AND name = %s", test_params
        )

        result = []
        async for row in wrapper:
            result.append(row)

        assert len(result) == 2
        # Mock session should have been called with pagination
        assert mock_session._call_count == 2

    async def test_execute_kwargs_preserved(self):
        """Test that execute kwargs are preserved across pagination calls."""
        page1 = [MockRow(uuid.uuid4(), "Page1", 1)]
        page2 = [MockRow(uuid.uuid4(), "Page2", 2)]

        # Track what kwargs were passed to execute_async
        captured_kwargs = []

        class MockSessionWithKwargs:
            def __init__(self, pages):
                self._pages = pages
                self._call_count = 0

            def execute_async(self, query, params=None, **kwargs):
                captured_kwargs.append(kwargs.copy())
                self._call_count += 1

                # Determine page based on paging_state
                if "paging_state" not in kwargs or kwargs["paging_state"] is None:
                    page_index = 0
                else:
                    page_index = int(kwargs["paging_state"].split("_")[1]) + 1

                return MockResponseFuture(self._pages, page_index)

        mock_session = MockSessionWithKwargs([page1, page2])

        # Call aexecute helper with specific kwargs
        wrapper = await aexecute(
            mock_session,
            "SELECT * FROM test",
            None,
            timeout=30,
            trace=True,
            execution_profile="custom_profile",
        )

        result = []
        async for row in wrapper:
            result.append(row)

        # Verify all rows retrieved
        assert len(result) == 2

        # Verify kwargs were preserved in pagination call
        assert len(captured_kwargs) == 2  # Initial + pagination

        # Check initial call
        assert captured_kwargs[0]["timeout"] == 30
        assert captured_kwargs[0]["trace"] is True
        assert captured_kwargs[0]["execution_profile"] == "custom_profile"

        # Check pagination call - should have same kwargs plus paging_state
        assert captured_kwargs[1]["timeout"] == 30
        assert captured_kwargs[1]["trace"] is True
        assert captured_kwargs[1]["execution_profile"] == "custom_profile"
        assert "paging_state" in captured_kwargs[1]

    async def test_paging_state_not_overridden_by_kwargs(self):
        """Test that paging_state parameter takes precedence over execute_kwargs.

        If someone accidentally passes paging_state in execute_kwargs, our
        internally managed paging_state should take precedence.
        """
        page1 = [MockRow(uuid.uuid4(), "Page1", 1)]
        page2 = [MockRow(uuid.uuid4(), "Page2", 2)]

        captured_paging_states = []

        class MockSessionTrackingPagingState:
            def __init__(self, pages):
                self._pages = pages

            def execute_async(self, query, params=None, **kwargs):
                # Track the paging_state that was actually used
                captured_paging_states.append(kwargs.get("paging_state"))

                # Determine page
                paging_state = kwargs.get("paging_state")
                if paging_state is None:
                    page_index = 0
                elif paging_state.startswith("page_"):
                    # Our correct paging state format
                    page_index = int(paging_state.split("_")[1]) + 1
                else:
                    # Invalid/wrong paging state - just return first page
                    page_index = 0

                return MockResponseFuture(self._pages, page_index)

        mock_session = MockSessionTrackingPagingState([page1, page2])

        # Initial call with a paging_state in kwargs using aexecute helper
        wrapper = await aexecute(
            mock_session,
            "SELECT * FROM test",
            None,
            paging_state="wrong_state",  # This should be overridden during pagination
        )

        result = []
        async for row in wrapper:
            result.append(row)

        # Verify correct pagination happened
        assert len(result) == 2

        # Check captured paging states
        # First call had "wrong_state"
        assert captured_paging_states[0] == "wrong_state"
        # Second call should have the CORRECT paging_state from the ResultSet
        # (not "wrong_state" from execute_kwargs)
        assert captured_paging_states[1] == "page_0"  # Correct state from MockResultSet

    async def test_paging_state_property(self):
        """Test that paging_state property exposes the underlying ResultSet's paging state."""
        page1 = [MockRow(uuid.uuid4(), "Page1", 1)]
        page2 = [MockRow(uuid.uuid4(), "Page2", 2)]

        mock_session = MockSession([page1, page2])
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        # Before iteration - should have paging state since there are more pages
        assert wrapper.paging_state == "page_0"
        assert wrapper.has_more_pages is True

        # Initialize the iterator (required before calling __anext__)
        wrapper.__aiter__()

        # Consume first page
        first_row = await wrapper.__anext__()
        assert first_row.name == "Page1"

        # Still on first page, paging state should be same
        assert wrapper.paging_state == "page_0"

        # Consume second page
        second_row = await wrapper.__anext__()
        assert second_row.name == "Page2"

        # Now on second page - paging state should be None (no more pages)
        assert wrapper.paging_state is None
        assert wrapper.has_more_pages is False

    async def test_manual_pagination_use_case(self):
        """Test manual pagination: get first page, extract paging_state, resume later.

        This tests the use case where we want to:
        1. Asynchronously fetch the first page
        2. Extract current_rows and paging_state
        3. Return those to a caller (e.g., send to client)
        4. Later, use the paging_state to fetch the next page
        """
        page1 = [
            MockRow(uuid.uuid4(), "Page1_Row1", 1),
            MockRow(uuid.uuid4(), "Page1_Row2", 2),
        ]
        page2 = [
            MockRow(uuid.uuid4(), "Page2_Row1", 3),
            MockRow(uuid.uuid4(), "Page2_Row2", 4),
        ]
        page3 = [MockRow(uuid.uuid4(), "Page3_Row1", 5)]

        mock_session = MockSession([page1, page2, page3])

        # Step 1: Get first page using our wrapper
        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        # Step 2: Extract current page data and paging state
        first_page_rows = wrapper.current_rows
        first_paging_state = wrapper.paging_state

        assert len(first_page_rows) == 2
        assert first_page_rows[0].name == "Page1_Row1"
        assert first_page_rows[1].name == "Page1_Row2"
        assert first_paging_state == "page_0"
        assert wrapper.has_more_pages is True

        # Step 3: Simulate sending first_page_rows and first_paging_state to client
        # ... (this would happen over network) ...

        # Step 4: Later, client requests next page with the paging_state
        wrapper2 = await aexecute(
            mock_session, "SELECT * FROM test", None, paging_state=first_paging_state
        )

        # Extract second page data
        second_page_rows = wrapper2.current_rows
        second_paging_state = wrapper2.paging_state

        assert len(second_page_rows) == 2
        assert second_page_rows[0].name == "Page2_Row1"
        assert second_page_rows[1].name == "Page2_Row2"
        assert second_paging_state == "page_1"
        assert wrapper2.has_more_pages is True

        # Step 5: Get third page
        wrapper3 = await aexecute(
            mock_session, "SELECT * FROM test", None, paging_state=second_paging_state
        )

        third_page_rows = wrapper3.current_rows
        third_paging_state = wrapper3.paging_state

        assert len(third_page_rows) == 1
        assert third_page_rows[0].name == "Page3_Row1"
        assert third_paging_state is None
        assert wrapper3.has_more_pages is False


@pytest.mark.asyncio
class TestAexecuteHelperFunction:
    """Tests for the aexecute() helper function."""

    async def test_aexecute_returns_wrapper(self):
        """Test that aexecute() returns an AsyncResultSetWrapper."""
        rows = [MockRow(uuid.uuid4(), "Test", 1)]
        mock_session = MockSession([rows])

        wrapper = await aexecute(mock_session, "SELECT * FROM test")

        assert isinstance(wrapper, AsyncResultSetWrapper)

        result = []
        async for row in wrapper:
            result.append(row)

        assert result == rows

    async def test_aexecute_with_params(self):
        """Test aexecute() with query parameters."""
        rows = [MockRow(uuid.uuid4(), "Alice", 100)]
        mock_session = MockSession([rows])
        test_id = uuid.uuid4()

        wrapper = await aexecute(
            mock_session, "SELECT * FROM test WHERE id = %s", (test_id,)
        )

        result = []
        async for row in wrapper:
            result.append(row)

        assert len(result) == 1
        assert result[0].name == "Alice"


@pytest.fixture
def cassandra_integration_session():
    """Create Cassandra session for integration tests."""
    from cassandra.cluster import Cluster

    cluster = Cluster(contact_points=["cassandra"])
    session = cluster.connect()

    # Drop and create test keyspace
    session.execute("DROP KEYSPACE IF EXISTS cass_util_integration_test")
    session.execute("""
        CREATE KEYSPACE cass_util_integration_test
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("cass_util_integration_test")

    # Create test table
    session.execute("""
        CREATE TABLE test_data (
            id UUID PRIMARY KEY,
            name TEXT,
            value INT
        )
    """)

    yield session

    # Cleanup
    session.execute("DROP KEYSPACE IF EXISTS cass_util_integration_test")
    session.shutdown()
    cluster.shutdown()


@pytest.mark.asyncio
class TestCassUtilIntegration:
    """Integration tests against real Cassandra database."""

    async def test_pagination_with_real_cassandra(self, cassandra_integration_session):
        """Test pagination with real Cassandra cluster."""
        session = cassandra_integration_session

        # Insert 20 rows
        for i in range(20):
            test_id = uuid.uuid4()
            session.execute(
                "INSERT INTO test_data (id, name, value) VALUES (%s, %s, %s)",
                (test_id, f"Row_{i}", i),
            )

        # Create a prepared statement with small fetch_size to force pagination
        from cassandra.query import SimpleStatement

        query = SimpleStatement("SELECT * FROM test_data", fetch_size=5)

        # Use aexecute with the query
        wrapper = await aexecute(session, query)

        # Verify we can iterate and get all rows
        result = []
        async for row in wrapper:
            result.append(row)

        assert len(result) == 20
        # Verify session was called multiple times for pagination
        # (20 rows / 5 fetch_size = 4 pages)

    async def test_resultset_properties_with_real_data(
        self, cassandra_integration_session
    ):
        """Test ResultSet properties with real Cassandra data."""
        session = cassandra_integration_session

        # Insert 10 rows
        for i in range(10):
            test_id = uuid.uuid4()
            session.execute(
                "INSERT INTO test_data (id, name, value) VALUES (%s, %s, %s)",
                (test_id, f"TestRow_{i}", i * 10),
            )

        # Query with small fetch_size
        from cassandra.query import SimpleStatement

        query = SimpleStatement("SELECT * FROM test_data", fetch_size=3)

        wrapper = await aexecute(session, query)

        # Verify column_names
        assert "id" in wrapper.column_names
        assert "name" in wrapper.column_names
        assert "value" in wrapper.column_names

        # Verify has_more_pages is True initially (10 rows > 3 fetch_size)
        assert wrapper.has_more_pages is True

        # Verify paging_state is not None
        assert wrapper.paging_state is not None

        # Verify current_rows contains fetch_size rows
        assert len(wrapper.current_rows) == 3

        # Iterate through all rows
        result = []
        async for row in wrapper:
            result.append(row)

        assert len(result) == 10

    async def test_empty_result_set_from_real_query(
        self, cassandra_integration_session
    ):
        """Test empty result set from real Cassandra query."""
        session = cassandra_integration_session

        # Query that returns no results (use ALLOW FILTERING for non-indexed column)
        wrapper = await aexecute(
            session,
            "SELECT * FROM test_data WHERE name = %s ALLOW FILTERING",
            ("NonexistentName",),
        )

        # Verify iteration over empty result set works
        result = []
        async for row in wrapper:
            result.append(row)

        assert result == []
        assert len(result) == 0

    async def test_query_parameters_with_real_data(self, cassandra_integration_session):
        """Test parameterized queries with real Cassandra."""
        session = cassandra_integration_session

        # Insert specific test data
        test_id = uuid.uuid4()
        session.execute(
            "INSERT INTO test_data (id, name, value) VALUES (%s, %s, %s)",
            (test_id, "SpecificName", 999),
        )

        # Query with parameters
        wrapper = await aexecute(
            session, "SELECT * FROM test_data WHERE id = %s", (test_id,)
        )

        # Collect results
        result = []
        async for row in wrapper:
            result.append(row)

        assert len(result) == 1
        assert result[0].id == test_id
        assert result[0].name == "SpecificName"
        assert result[0].value == 999
