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


class MockResultSet:
    """Mock ResultSet that simulates cassandra-driver pagination behavior.

    This mock allows us to test pagination without a real Cassandra instance.
    It tracks paging state and supports multiple pages.
    """

    def __init__(self, pages: list[list[MockRow]], column_names: list[str] | None = None):
        """Initialize with list of pages (each page is a list of rows).

        Args:
            pages: List of pages, where each page is a list of rows
            column_names: Optional column names (defaults to ['id', 'name', 'value'])
        """
        self._all_pages = pages
        self._page_index = 0
        self.column_names = column_names or ['id', 'name', 'value']
        self.has_more_pages = len(pages) > 1
        self.current_rows = pages[0] if pages else []
        self.paging_state = f"page_{self._page_index}" if self.has_more_pages else None

    def _advance_to_page(self, page_index: int):
        """Simulate advancing to a specific page (used by mock session)."""
        self._page_index = page_index
        self.current_rows = self._all_pages[page_index] if page_index < len(self._all_pages) else []
        self.has_more_pages = page_index < len(self._all_pages) - 1
        self.paging_state = f"page_{page_index}" if self.has_more_pages else None


class MockSession:
    """Mock Cassandra session that simulates aexecute() with pagination."""

    def __init__(self, pages: list[list[MockRow]]):
        """Initialize with pages of data.

        Args:
            pages: List of pages to return sequentially
        """
        self._pages = pages
        self._result_set = MockResultSet(pages)
        self._call_count = 0

    async def aexecute(self, query, params=None, paging_state=None, **kwargs):
        """Mock aexecute that returns pages based on paging_state.

        Args:
            query: Query (ignored in mock)
            params: Parameters (ignored in mock)
            paging_state: If provided, advances to next page
            **kwargs: Additional args (ignored)

        Returns:
            MockResultSet configured for the appropriate page
        """
        self._call_count += 1

        if paging_state is None:
            # First call - return first page
            page_index = 0
        else:
            # Extract page index from paging_state
            page_index = int(paging_state.split('_')[1]) + 1

        # Create result set for this page
        result = MockResultSet(self._pages, self._result_set.column_names)
        result._advance_to_page(page_index)
        return result


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
        initial_result = await mock_session.aexecute("SELECT * FROM test")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM test", None, initial_result)

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
        initial_result = await mock_session.aexecute("SELECT * FROM test")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM test", None, initial_result)

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
        initial_result = await mock_session.aexecute("SELECT * FROM empty")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM empty", None, initial_result)

        result = []
        async for row in wrapper:
            result.append(row)

        assert result == []

    async def test_column_names_property(self):
        """Test that column_names property is exposed."""
        rows = [MockRow(uuid.uuid4(), "Test", 1)]
        mock_session = MockSession([rows])
        initial_result = await mock_session.aexecute("SELECT * FROM test")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM test", None, initial_result)

        assert wrapper.column_names == ['id', 'name', 'value']

    async def test_has_more_pages_property(self):
        """Test that has_more_pages reflects pagination state."""
        page1 = [MockRow(uuid.uuid4(), "Row1", 1)]
        page2 = [MockRow(uuid.uuid4(), "Row2", 2)]

        mock_session = MockSession([page1, page2])
        initial_result = await mock_session.aexecute("SELECT * FROM test")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM test", None, initial_result)

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
        initial_result = await mock_session.aexecute("SELECT * FROM test")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM test", None, initial_result)

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

        initial_result1 = await mock_session1.aexecute("SELECT * FROM test1")
        initial_result2 = await mock_session2.aexecute("SELECT * FROM test2")

        wrapper1 = AsyncResultSetWrapper(mock_session1, "SELECT * FROM test1", None, initial_result1)
        wrapper2 = AsyncResultSetWrapper(mock_session2, "SELECT * FROM test2", None, initial_result2)

        async def collect_rows(wrapper):
            result = []
            async for row in wrapper:
                await asyncio.sleep(0.001)  # Simulate some async work
                result.append(row)
            return result

        # Run both concurrently
        results = await asyncio.gather(
            collect_rows(wrapper1),
            collect_rows(wrapper2)
        )

        assert results[0] == rows1
        assert results[1] == rows2

    async def test_with_query_parameters(self):
        """Test that query parameters are preserved across pagination."""
        page1 = [MockRow(uuid.uuid4(), "Alice", 1)]
        page2 = [MockRow(uuid.uuid4(), "Bob", 2)]

        mock_session = MockSession([page1, page2])
        test_params = (uuid.uuid4(), "test_value")

        initial_result = await mock_session.aexecute("SELECT * FROM test WHERE id = %s AND name = %s", test_params)
        wrapper = AsyncResultSetWrapper(
            mock_session,
            "SELECT * FROM test WHERE id = %s AND name = %s",
            test_params,
            initial_result
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

        # Track what kwargs were passed to aexecute
        captured_kwargs = []

        class MockSessionWithKwargs:
            def __init__(self, pages):
                self._pages = pages
                self._call_count = 0

            async def aexecute(self, query, params=None, **kwargs):
                captured_kwargs.append(kwargs.copy())
                self._call_count += 1

                # Determine page based on paging_state
                if 'paging_state' not in kwargs or kwargs['paging_state'] is None:
                    page_index = 0
                else:
                    page_index = int(kwargs['paging_state'].split('_')[1]) + 1

                result = MockResultSet(self._pages)
                result._advance_to_page(page_index)
                return result

        mock_session = MockSessionWithKwargs([page1, page2])

        # Call with specific kwargs
        initial_result = await mock_session.aexecute(
            "SELECT * FROM test",
            None,
            timeout=30,
            trace=True,
            execution_profile="custom_profile"
        )

        wrapper = AsyncResultSetWrapper(
            mock_session,
            "SELECT * FROM test",
            None,
            initial_result,
            timeout=30,
            trace=True,
            execution_profile="custom_profile"
        )

        result = []
        async for row in wrapper:
            result.append(row)

        # Verify all rows retrieved
        assert len(result) == 2

        # Verify kwargs were preserved in pagination call
        assert len(captured_kwargs) == 2  # Initial + pagination

        # Check initial call
        assert captured_kwargs[0]['timeout'] == 30
        assert captured_kwargs[0]['trace'] is True
        assert captured_kwargs[0]['execution_profile'] == "custom_profile"

        # Check pagination call - should have same kwargs plus paging_state
        assert captured_kwargs[1]['timeout'] == 30
        assert captured_kwargs[1]['trace'] is True
        assert captured_kwargs[1]['execution_profile'] == "custom_profile"
        assert 'paging_state' in captured_kwargs[1]

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

            async def aexecute(self, query, params=None, **kwargs):
                # Track the paging_state that was actually used
                captured_paging_states.append(kwargs.get('paging_state'))

                # Determine page
                paging_state = kwargs.get('paging_state')
                if paging_state is None:
                    page_index = 0
                elif paging_state.startswith('page_'):
                    # Our correct paging state format
                    page_index = int(paging_state.split('_')[1]) + 1
                else:
                    # Invalid/wrong paging state - just return first page
                    page_index = 0

                result = MockResultSet(self._pages)
                result._advance_to_page(page_index)
                return result

        mock_session = MockSessionTrackingPagingState([page1, page2])

        # Initial call with a paging_state in kwargs (this should be ignored for initial call)
        initial_result = await mock_session.aexecute(
            "SELECT * FROM test",
            None,
            paging_state="wrong_state"  # This shouldn't affect pagination
        )

        # Create wrapper - it should manage its own paging_state
        wrapper = AsyncResultSetWrapper(
            mock_session,
            "SELECT * FROM test",
            None,
            initial_result,
            paging_state="wrong_state"  # This should be overridden during pagination
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
        initial_result = await mock_session.aexecute("SELECT * FROM test")

        wrapper = AsyncResultSetWrapper(mock_session, "SELECT * FROM test", None, initial_result)

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
        page1 = [MockRow(uuid.uuid4(), "Page1_Row1", 1), MockRow(uuid.uuid4(), "Page1_Row2", 2)]
        page2 = [MockRow(uuid.uuid4(), "Page2_Row1", 3), MockRow(uuid.uuid4(), "Page2_Row2", 4)]
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
        # We can create a new wrapper with the paging_state
        second_page_result = await mock_session.aexecute(
            "SELECT * FROM test",
            None,
            paging_state=first_paging_state
        )
        wrapper2 = AsyncResultSetWrapper(
            mock_session,
            "SELECT * FROM test",
            None,
            second_page_result
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
        third_page_result = await mock_session.aexecute(
            "SELECT * FROM test",
            None,
            paging_state=second_paging_state
        )
        wrapper3 = AsyncResultSetWrapper(
            mock_session,
            "SELECT * FROM test",
            None,
            third_page_result
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

        wrapper = await aexecute(mock_session, "SELECT * FROM test WHERE id = %s", (test_id,))

        result = []
        async for row in wrapper:
            result.append(row)

        assert len(result) == 1
        assert result[0].name == "Alice"
