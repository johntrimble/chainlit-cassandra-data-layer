from typing import Any, Generic, Protocol, TypeVar

from cassandra.cluster import ResultSet

# Generic type for row (namedtuple by default, but configurable via row_factory)
RowT = TypeVar("RowT")


class AsyncResultSet(Protocol, Generic[RowT]):
    """Protocol for async iteration over Cassandra query results.

    This protocol defines the interface for async iteration without
    inheriting from ResultSet (composition over inheritance).

    Supports both automatic async iteration and manual pagination via
    current_rows and paging_state properties.
    """

    def __aiter__(self) -> "AsyncResultSet[RowT]": ...

    async def __anext__(self) -> RowT: ...

    def one(self) -> RowT:
        """Fetch a single row asynchronously."""
        ...

    @property
    def column_names(self) -> list[str]:
        """Column names from the query result."""
        ...

    @property
    def has_more_pages(self) -> bool:
        """True if more pages are available to fetch."""
        ...

    @property
    def current_rows(self) -> list[RowT]:
        """Current page rows."""
        ...

    @property
    def paging_state(self) -> Any:
        """Paging state for manual pagination."""
        ...


class AsyncResultSetWrapper(Generic[RowT]):
    """Async iterator wrapper for cassandra-driver ResultSet.

    Prevents event loop blocking during pagination by calling session.aexecute()
    again with the paging_state parameter to fetch subsequent pages asynchronously.

    Usage:
        result_set = await session.aexecute(query, params)
        async_result = AsyncResultSetWrapper(session, query, params, result_set)

        async for row in async_result:
            process(row)

    The wrapper iterates over current_rows (synchronous, no blocking), and when
    a page is exhausted, it calls session.aexecute() with the paging_state to
    fetch the next page asynchronously.
    """

    def __init__(
        self,
        session: Any,
        query: Any,
        params: Any,
        initial_result_set: ResultSet,
        **execute_kwargs,
    ):
        """Initialize wrapper with session, query info, and initial ResultSet.

        Args:
            session: Cassandra session with aexecute() method
            query: Query string or Statement
            params: Query parameters (can be None)
            initial_result_set: The ResultSet from the initial aexecute() call
            **execute_kwargs: Additional keyword arguments to preserve for subsequent
                            aexecute() calls (e.g., execution_profile, timeout, trace,
                            custom_payload, host, execute_as)
        """
        self._session = session
        self._query = query
        self._params = params
        self._execute_kwargs = execute_kwargs
        self._current_result_set = initial_result_set
        self._current_page_iter: Any = None
        self._page_index = 0

    def __aiter__(self) -> "AsyncResultSetWrapper[RowT]":
        """Initialize async iteration by creating iterator over current page.

        Returns:
            self for async iteration protocol
        """
        self._current_page_iter = iter(self._current_result_set.current_rows)
        self._page_index = 0
        return self

    async def __anext__(self) -> RowT:
        """Fetch the next row, fetching new pages via execute_async as needed.

        Iterates over the current page's rows (synchronous, non-blocking). When
        the page is exhausted, calls session.execute_async() with paging_state to
        fetch the next page asynchronously (no blocking).

        Returns:
            The next row from the result set

        Raises:
            StopAsyncIteration: When no more rows are available
        """
        # Try to get next row from current page
        try:
            return next(self._current_page_iter)  # type: ignore[no-any-return]
        except StopIteration:
            # Current page exhausted - check if more pages available
            if not self._current_result_set.has_more_pages:
                raise StopAsyncIteration

            # Fetch next page using paging_state (fully async!)
            # Preserve all kwargs from initial call (execution_profile, timeout, etc.)
            # but remove paging_state if present - we manage it internally
            paging_state = self._current_result_set.paging_state
            fetch_kwargs = {
                k: v for k, v in self._execute_kwargs.items() if k != "paging_state"
            }

            # Use same logic as aexecute() to get ResultSet from execute_async
            import asyncio

            from cassandra.cluster import ResultSet

            future = self._session.execute_async(
                self._query, self._params, paging_state=paging_state, **fetch_kwargs
            )

            loop = asyncio.get_running_loop()
            async_future = loop.create_future()

            def handle_result(result):
                """Callback that wraps result in ResultSet."""
                if not async_future.cancelled():
                    result_set = ResultSet(future, result)
                    loop.call_soon_threadsafe(
                        lambda: async_future.set_result(result_set)
                    )

            def handle_error(exc):
                """Errback that sets exception."""
                if not async_future.cancelled():
                    loop.call_soon_threadsafe(lambda: async_future.set_exception(exc))

            future.add_callback(handle_result)
            future.add_errback(handle_error)

            self._current_result_set = await async_future

            # Update iterator for new page
            self._current_page_iter = iter(self._current_result_set.current_rows)
            self._page_index += 1

            # Return first row from new page
            try:
                return next(self._current_page_iter)  # type: ignore[no-any-return]
            except StopIteration:
                # Empty page (shouldn't happen, but handle gracefully)
                raise StopAsyncIteration

    def one(self) -> RowT:
        """Return a single row from the current page of results."""
        return self._current_result_set.one()  # type: ignore[no-any-return]

    # Expose useful ResultSet properties for inspection and debugging

    @property
    def column_names(self) -> list[str]:
        """Column names from the underlying ResultSet."""
        return self._current_result_set.column_names or []

    @property
    def has_more_pages(self) -> bool:
        """Check if more pages are available from the underlying ResultSet."""
        return self._current_result_set.has_more_pages  # type: ignore[no-any-return]

    @property
    def current_rows(self) -> list[Any]:
        """Current page rows from the underlying ResultSet."""
        return self._current_result_set.current_rows  # type: ignore[no-any-return]

    @property
    def paging_state(self) -> Any:
        """Paging state from the underlying ResultSet for manual pagination.

        This allows extracting the paging state to send to a client, which can
        later be used to resume pagination from where we left off.

        Returns:
            The opaque paging state token, or None if no more pages
        """
        return self._current_result_set.paging_state


class SessionWithAsyncExecute(Protocol):
    """Protocol for Cassandra Session with async execute capability."""

    async def aexecute(self, *args, **kwargs) -> ResultSet:
        """Execute a query asynchronously and return ResultSet."""
        ...


async def aexecute(session: Any, query, params=None, **kwargs) -> AsyncResultSet[Any]:
    """Execute a query and return an async iterator over results.

    Bypasses session.aexecute() to properly create ResultSet objects from
    ResponseFuture callbacks. This is necessary because cassandra-asyncio-driver's
    aexecute() returns a raw list from row_factory, not a ResultSet object.

    Args:
        session: Cassandra session with execute_async() method
        query: Query string or Statement
        params: Query parameters (optional)
        **kwargs: Additional arguments passed to session.execute_async() and preserved
                 for subsequent page fetches (e.g., execution_profile, timeout,
                 trace, custom_payload, host, execute_as)

    Returns:
        AsyncResultSetWrapper for async iteration over query results

    Example:
        async for row in aexecute(session, "SELECT * FROM users"):
            print(row.name)
    """
    import asyncio

    from cassandra.cluster import ResultSet

    # Get ResponseFuture from execute_async (not aexecute)
    future = session.execute_async(query, params, **kwargs)

    # Create asyncio Future for result
    loop = asyncio.get_running_loop()
    async_future = loop.create_future()

    def handle_result(result):
        """Callback that wraps result in ResultSet like .result() does."""
        if not async_future.cancelled():
            # Wrap the raw list in ResultSet just like ResponseFuture.result() does
            result_set = ResultSet(future, result)
            loop.call_soon_threadsafe(lambda: async_future.set_result(result_set))

    def handle_error(exc):
        """Errback that sets exception."""
        if not async_future.cancelled():
            loop.call_soon_threadsafe(lambda: async_future.set_exception(exc))

    # Attach callbacks
    future.add_callback(handle_result)
    future.add_errback(handle_error)

    # Wait for result
    result_set = await async_future

    # Wrap in AsyncResultSetWrapper for async iteration
    return AsyncResultSetWrapper(session, query, params, result_set, **kwargs)
