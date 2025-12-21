import asyncio
import json
import logging
import re
import uuid
from collections.abc import (
    AsyncIterator,
    Collection,
    Sequence,
)
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import (
    Any,
    NotRequired,
    Tuple,
    TypedDict,
    cast,
    overload,
)

import aiofiles
import base62  # type: ignore[import-untyped]
import msgpack
import uuid_utils
import uuid_utils.compat
from cassandra.cluster import EXEC_PROFILE_DEFAULT, PreparedStatement, Session
from cassandra.query import BatchStatement, BatchType
from chainlit.context import ChainlitContextException, context
from chainlit.data.base import BaseDataLayer
from chainlit.data.storage_clients.base import BaseStorageClient
from chainlit.data.utils import queue_until_user_message
from chainlit.element import Element, ElementDict
from chainlit.step import StepDict
from chainlit.types import (
    Feedback,
    FeedbackDict,
    PageInfo,
    PaginatedResponse,
    Pagination,
    ThreadDict,
    ThreadFilter,
)
from chainlit.user import PersistedUser, User

from chainlit_cassandra_data_layer.cass_util import AsyncResultSet, aexecute
from chainlit_cassandra_data_layer.migration import MigrationManager
from chainlit_cassandra_data_layer.util import (
    achain,
    achain_from,
    aempty,
    afilter,
    amap,
    apushback,
    dedupe_iterable,
)

# Import for runtime usage (isinstance checks)
try:
    from chainlit.data.storage_clients.gcs import GCSStorageClient
except ImportError:
    GCSStorageClient = None  # type: ignore[assignment,misc]


# Maximum number of threads to retrieve per page
# This prevents excessive memory usage and ensures reasonable query performance
DEFAULT_THREADS_PER_PAGE = 200

FEEDBACK_ID_REGEX: re.Pattern = re.compile(
    r"^THREAD#([0-9a-fA-F-]{36})::STEP#([0-9a-fA-F-]{36})$"
)


def first_exc(items: Collection) -> Exception | None:
    for item in items:
        if isinstance(item, Exception):
            return item
    return None


def select_exc(items: Sequence) -> Sequence[Exception]:
    return [item for item in items if isinstance(item, Exception)]


def raise_first_exc[T: Sequence](items: T) -> T:
    exc = first_exc(items)
    if exc:
        raise exc
    return items


def _pack_metadata(metadata: dict[str, Any] | None) -> bytes | None:
    """Serialize metadata dict to MessagePack bytes."""
    if metadata is None or not metadata:
        return None
    packed = msgpack.packb(metadata, use_bin_type=True)
    return cast(bytes, packed)


def _unpack_metadata(data: bytes | None) -> dict[str, Any]:
    """Deserialize MessagePack bytes to metadata dict."""
    if data is None or data == b"":
        return {}
    unpacked = msgpack.unpackb(data, raw=False)
    return cast(dict[str, Any], unpacked)


def _thread_step_id_to_feedback_id(thread_id: uuid.UUID, step_id: uuid.UUID) -> str:
    """Convert step_id to feedback_id format: step#<uuid>.

    This creates a deterministic feedback ID from the step ID, allowing us to
    avoid storing feedback_id separately in the database while maintaining
    distinct identifiers for feedback and steps.
    """
    return f"THREAD#{str(thread_id)}::STEP#{str(step_id)}"


def _feedback_id_to_thread_step_id(feedback_id: str) -> Tuple[uuid.UUID, uuid.UUID]:
    """Extract step_id from feedback_id format: step#<uuid> -> <uuid>.

    Reverses the _step_id_to_feedback_id conversion. If the feedback_id doesn't
    match the expected format, it's returned as-is for backward compatibility.
    """
    m = FEEDBACK_ID_REGEX.match(feedback_id)
    if m is None:
        raise ValueError(f"Invalid feedback_id format: {feedback_id}")

    thread_id, step_id = m.groups()
    thread_id_uuid = to_uuid(thread_id)
    step_id_uuid = to_uuid(step_id)

    assert thread_id_uuid is not None
    assert step_id_uuid is not None

    return (thread_id_uuid, step_id_uuid)


class _ThreadActivity(TypedDict):
    user_id: uuid.UUID
    thread_id: uuid.UUID
    activity_at: uuid.UUID
    created_at: uuid.UUID | None
    name: str | None


def uuid7(*, time_ms: int | None = None, datetime: datetime | None = None) -> uuid.UUID:
    # Do not allow both time_ms and datetime to be provided
    if time_ms is not None and datetime is not None:
        raise ValueError("Provide only one of time_ms or datetime")

    if time_ms is None and datetime is not None:
        time_ms = int(datetime.timestamp() * 1000)

    if time_ms is not None:
        # uuid_utils.uuid7 expects timestamp in seconds, not milliseconds
        # Convert milliseconds to seconds and nanoseconds
        timestamp_s = time_ms // 1000
        nanos = (time_ms % 1000) * 1_000_000
        return uuid_utils.compat.uuid7(timestamp=timestamp_s, nanos=nanos)

    # No timestamp provided, use uuid_utils default
    return uuid_utils.compat.uuid7()


def smallest_uuid7_for_datetime(dt: datetime) -> uuid.UUID:
    """Get the smallest UUIDv7 for the given datetime."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    epoch_millis = int(dt.timestamp() * 1000)

    # Construct the smallest UUID7 for this timestamp
    # First 48 bits: timestamp in milliseconds
    timestamp_ms = epoch_millis & 0xFFFFFFFFFFFF
    uuid_int = timestamp_ms << 80

    # Next 16 bits: version 7 (0x7000) with all other bits as 0
    uuid_int |= 0x7000 << 64

    # Next 64 bits: variant (0b10) in high 2 bits, all other bits as 0
    uuid_int |= 0b10 << 62

    return uuid.UUID(int=uuid_int)


def largest_uuid7_for_datetime(dt: datetime) -> uuid.UUID:
    """Get the largest UUIDv7 for the given datetime."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    epoch_millis = int(dt.timestamp() * 1000)

    # Construct the largest UUID7 for this timestamp
    # First 48 bits: timestamp in milliseconds
    timestamp_ms = epoch_millis & 0xFFFFFFFFFFFF
    uuid_int = timestamp_ms << 80

    # Next 16 bits: version 7 (0x7000) with all other bits as 1
    uuid_int |= 0x7000 << 64
    uuid_int |= 0x0FFF << 64  # Set lower 12 bits to 1

    # Next 64 bits: variant (0b10) in high 2 bits, all other bits as 1
    uuid_int |= 0b10 << 62
    uuid_int |= (1 << 62) - 1  # Set lower 62 bits to 1

    return uuid.UUID(int=uuid_int)


def to_uuid_utils_uuid(u: str | uuid.UUID | uuid_utils.UUID) -> uuid_utils.UUID:
    """Convert a UUID input to a uuid_utils.UUID object."""
    if isinstance(u, str):
        return uuid_utils.UUID(u)
    if isinstance(u, uuid.UUID):
        return uuid_utils.UUID(int=u.int)
    if isinstance(u, uuid_utils.UUID):
        return u
    else:
        raise ValueError(f"Cannot convert to uuid_utils.UUID of type {type(u)}")


@overload
def to_uuid(u: str | uuid.UUID | uuid_utils.UUID) -> uuid.UUID:
    ...

@overload
def to_uuid(u: str | uuid.UUID | uuid_utils.UUID | None) -> uuid.UUID | None:
    ...

def to_uuid(u: str | uuid.UUID | uuid_utils.UUID | None) -> uuid.UUID | None:
    """Convert a UUID input to a standard library uuid.UUID object."""
    if u is None:
        return None
    if isinstance(u, str):
        return uuid.UUID(u)
    if isinstance(u, uuid.UUID):
        return u
    if isinstance(u, uuid_utils.UUID):
        return uuid.UUID(int=u.int)
    else:
        raise ValueError(f"Cannot convert to uuid.UUID of type {type(u)}")


def uuid7_millis(u: uuid.UUID | uuid_utils.UUID | str) -> int:
    u = to_uuid_utils_uuid(u)
    if u.version != 7:
        raise ValueError("UUID is not version 7")
    return u.timestamp


def uuid7_to_datetime(u: uuid.UUID | uuid_utils.UUID | str) -> datetime:
    """Convert a UUIDv7 to a timezone-aware UTC datetime."""
    millis = uuid7_millis(u)
    time_epoch_seconds = millis / 1000
    dt = datetime.fromtimestamp(time_epoch_seconds, tz=UTC)
    return dt


def uuid7_isoformat(u: uuid.UUID) -> str:
    """Convert a UUIDv7 to an ISO formatted datetime string (UTC)."""
    millis = uuid7_millis(u)
    time_epoch_seconds = millis / 1000
    dt = datetime.fromtimestamp(time_epoch_seconds, tz=UTC)
    return dt.isoformat()


def isoformat_to_uuid7(iso_str: str) -> uuid.UUID:
    """Convert an ISO formatted datetime string (UTC) to a UUIDv7."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    epoch_millis = int(dt.timestamp() * 1000)
    return uuid7(time_ms=epoch_millis)


def isoformat_to_datetime(iso_str: str) -> datetime:
    """Parse ISO formatted datetime string into timezone-aware UTC datetime."""
    normalized = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def datetime_to_isoformat(dt: datetime) -> str:
    """Convert a datetime object to an ISO formatted datetime string (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.isoformat()


def add_timezone_if_missing(dt: datetime) -> datetime:
    """Add UTC timezone to naive datetime objects."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class TimeBucketStrategy:
    def get_bucket(self, dt: datetime | uuid.UUID) -> tuple[datetime, datetime]:
        raise NotImplementedError("Subclasses must implement get_bucket")


def _calc_bucket_segment_start(
    segment_size: timedelta, dt: datetime, start_time: datetime
) -> datetime:
    """
    Returns the datetime of where a bucket segments starts for a given datetime.
    """
    if start_time is None:
        start_time = datetime(1970, 1, 1, tzinfo=UTC)
    else:
        start_time = add_timezone_if_missing(start_time)

    dt = add_timezone_if_missing(dt)
    delta = dt - start_time
    total_seconds = delta.total_seconds()
    segment_seconds = segment_size.total_seconds()
    segment_index = int(total_seconds // segment_seconds)
    segment_start_seconds = segment_index * segment_seconds
    segment_start = start_time + timedelta(seconds=segment_start_seconds)
    return segment_start


class BasicActivityBucketStrategy(TimeBucketStrategy):
    def __init__(
        self,
        partition_bucket_size: timedelta,
        clustering_bucket_size: timedelta,
        start_time: datetime | None = None,
    ):
        self.partition_bucket_size = partition_bucket_size
        self.clustering_bucket_size = clustering_bucket_size

        if start_time is None:
            start_time = datetime(1970, 1, 1, tzinfo=UTC)
        self.start_time = add_timezone_if_missing(start_time)

        assert self.partition_bucket_size >= self.clustering_bucket_size, (
            "Partition bucket size must be >= clustering bucket size"
        )

    def get_bucket(self, dt: datetime | uuid.UUID) -> tuple[datetime, datetime]:
        if isinstance(dt, uuid.UUID):
            dt = uuid7_to_datetime(dt)

        partition_bucket = _calc_bucket_segment_start(
            self.partition_bucket_size, dt, start_time=self.start_time
        )

        clustering_bucket = _calc_bucket_segment_start(
            self.clustering_bucket_size, dt, start_time=partition_bucket
        )

        return (partition_bucket, clustering_bucket)


class ThreadCursor(TypedDict):
    start: uuid.UUID
    thread_start: NotRequired[uuid.UUID]


@dataclass
class CollectThreadListResult:
    selected_rows: Sequence[Any]
    next_cursor: ThreadCursor | None = None
    duplicate_rows: Sequence[Any] | None = None


async def consume_next_clustering_bucket(
    row_iterator: AsyncIterator[Any],
    max_rows: int = 1_000,
) -> tuple[list[Any], AsyncIterator[Any] | None, bool]:
    """Consume rows from the iterator until the clustering bucket changes.

    Returns when the clustering bucket changes or the iterator is exhausted.
    """
    first_row = True
    current_clustering_bucket = None
    bucket_rows: list[Any] = []

    async for row in row_iterator:
        if first_row:
            current_clustering_bucket = row.clustering_bucket_start
            first_row = False

        # Clustering bucket changed, return the bucket contents
        if row.clustering_bucket_start != current_clustering_bucket:
            row_iterator = apushback(row, row_iterator)
            return (bucket_rows, row_iterator, True)

        # We exceeded our budget for this bucket, so return early
        if len(bucket_rows) >= max_rows:
            row_iterator = apushback(row, row_iterator)
            return (bucket_rows, row_iterator, False)

        bucket_rows.append(row)

    # Iterator exhausted
    return (bucket_rows, None, True)


async def collect_thread_list_results(
    row_iterator: AsyncIterator[Any],
    page_size: int,
    cursor: ThreadCursor | None = None,
    max_clustering_size: int = 1_000,
    search: str | None = None,
) -> CollectThreadListResult:
    # Get rid of any duplicates the iterator might return and track them
    duplicate_rows: list[Any] = []
    row_iterator = dedupe_iterable(
        row_iterator, key_func=lambda r: r.thread_id, duplicates=duplicate_rows
    )

    # Apply search filter
    if search is not None:
        row_iterator = afilter(
            lambda r: search.lower() in (r.thread_name or "").lower(),
            row_iterator,
        )

    # start indicates the activity_at UUID at which we want to start collecting
    # rows (inclusive). Rows with activity_at > start are skipped.
    start: uuid.UUID
    if cursor and cursor.get("start"):
        start = cursor["start"]
    else:
        # Make largest possible UUID
        start = uuid.UUID("ffffffff-ffff-7fff-bfff-ffffffffffff")

    # thread_start is used when a page was completed without reading the entire
    # clustering bucket. In that case, the first bucket we read from now should
    # only include rows with thread_created_at <= thread_start.
    thread_start: uuid.UUID | None = cursor.get("thread_start") if cursor else None

    empty_iter: AsyncIterator[Any] = aempty()

    collected: list[Any] = []
    first_bucket = True

    # Track the current iterator which may be None or AsyncIterator
    current_iterator: AsyncIterator[Any] | None = row_iterator

    while current_iterator is not None and current_iterator is not empty_iter:
        # If we accumulate a full page of results _and_ exhaust max_clustering_size,
        # that's when we return early. Set max_rows accordingly.
        needed_to_fill_page = page_size - len(collected)
        max_rows = max(needed_to_fill_page, max_clustering_size)

        (
            bucket_rows,
            current_iterator,
            completed_bucket,
        ) = await consume_next_clustering_bucket(
            current_iterator,
            max_rows=max_rows,
        )

        # This will simplify some uses of anext() below
        if current_iterator is None:
            current_iterator = empty_iter

        # Remove prefix of rows that are more recent than our cursor
        if first_bucket and thread_start is not None:
            while bucket_rows and bucket_rows[0].thread_created_at <= thread_start:
                bucket_rows.pop(0)

        # Even if we were able to read a full clustering bucket on the previous
        # page, we may not have returned all the results from it yet, so we may
        # see rows that are more recent than our start cursor. Drop all such
        # rows.
        if not first_bucket or thread_start is None:
            bucket_rows = [row for row in bucket_rows if row.activity_at <= start]
        first_bucket = False

        if completed_bucket:
            bucket_rows.sort(key=lambda r: r.activity_at, reverse=True)
            collected.extend(bucket_rows)
            if len(collected) >= page_size:
                # We have enough results for the page, but we need the next row
                # for the cursor
                next_row = None
                if len(collected) > page_size:
                    next_row = collected[page_size]
                else:
                    next_row = await anext(current_iterator, None)

                next_cursor: ThreadCursor | None = None
                if next_row is not None:
                    next_cursor = {
                        "start": next_row.activity_at,
                    }
                return CollectThreadListResult(
                    duplicate_rows=duplicate_rows,
                    selected_rows=collected[:page_size],
                    next_cursor=next_cursor,
                )
        else:
            # We did not complete the bucket, so we do have enough results for the
            # page, but we do need to return a special cursor to continue from here
            number_to_add = page_size - len(collected)
            rows_to_add = bucket_rows[:number_to_add]
            collected.extend(
                sorted(rows_to_add, key=lambda r: r.activity_at, reverse=True)
            )

            # Need to find the next row to construct the cursor
            next_row = None
            if number_to_add < len(bucket_rows):
                next_row = bucket_rows[number_to_add]
            else:
                # We need to get one more row from the iterator
                next_row = await anext(current_iterator, None)

            next_cursor = None
            if next_row is not None:
                next_cursor = {
                    "start": next_row.clustering_bucket_start,
                    "thread_start": next_row.thread_created_at,
                }
            return CollectThreadListResult(
                duplicate_rows=duplicate_rows,
                selected_rows=collected,
                next_cursor=next_cursor,
            )

    # Exhausted iterator without filling the page
    return CollectThreadListResult(
        duplicate_rows=duplicate_rows,
        selected_rows=collected,
        next_cursor=None,
    )


class CassandraDataLayer(BaseDataLayer):
    """Cassandra-backed data layer for Chainlit."""

    def __init__(
        self,
        session: Session,
        storage_client: BaseStorageClient | None = None,
        *,
        keyspace: str | None = None,
        default_consistency_level: int | None = None,
        activity_bucket_strategy: TimeBucketStrategy | None = None,
        max_clustering_bucket_size: int = 1_000,
        log: logging.Logger | None = None,
    ):
        """Initialize the Cassandra data layer.

        Args:
            session: A connected Cassandra `Session` instance. The caller is
                responsible for configuring authentication, load balancing,
                and execution profiles before passing it in. The data layer does
                not mutate the session (no keyspace switching or profile changes).
            storage_client: Optional storage backend for element file uploads.
            keyspace: Optional keyspace name to qualify table references. If not
                provided, `session.keyspace` must already be set.
            default_consistency_level: Optional consistency level override. When
                omitted, the value from the session's default execution profile is
                used.
            activity_bucket_strategy: Strategy for bucketing thread activity by time.
                If not provided, defaults to `BasicActivityBucketStrategy` with
                60-day partitions and 10-day clustering buckets. Controls how thread
                activity data is distributed across Cassandra partitions for efficient
                querying.
            max_clustering_bucket_size: Number of rows to scan from a clustering
                bucket before potentially returning a page of results. This value
                should be larger than the typical number of rows in a clustering
                bucket. Defaults to 1000.
            log: Optional logger instance for data layer operations. If not provided,
                a logger named after this module will be created.
        """
        self.session: Session = session
        self.cluster = session.cluster

        # Setup logging
        if log is not None:
            self.log = log
        else:
            self.log = logging.getLogger(__name__)

        # Setup bucketing for the activity table
        if activity_bucket_strategy is None:
            activity_bucket_strategy = BasicActivityBucketStrategy(
                partition_bucket_size=timedelta(days=60),
                clustering_bucket_size=timedelta(days=10),
            )
        self.activity_bucket_strategy = activity_bucket_strategy

        # Save the max clustering bucket size. This is used to limit how many
        # rows of a bucket we will scan before returning a page of results.
        self.max_clustering_bucket_size = max_clustering_bucket_size

        # Determine the consistency level
        ep = session.cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
        self.default_consistency_level = (
            default_consistency_level
            if default_consistency_level is not None
            else ep.consistency_level
        )

        # Determine the keyspace
        self.keyspace = keyspace if keyspace is not None else session.keyspace
        if not self.keyspace:
            raise ValueError(
                "CassandraDataLayer requires a keyspace. Provide one explicitly or "
                "use a session already bound to a keyspace."
            )

        self._table_users = self._qualified_table("users")
        self._table_users_by_identifier = self._qualified_table("users_by_identifier")
        self._table_threads = self._qualified_table("threads")
        self._table_threads_by_user_activity = self._qualified_table(
            "threads_by_user_activity"
        )
        self._table_steps_by_thread = self._qualified_table("steps_by_thread_id")
        self._table_elements_by_thread = self._qualified_table("elements_by_thread_id")
        self._table_user_activity_by_thread = self._qualified_table(
            "user_activity_by_thread"
        )
        self.storage_client = storage_client
        self._prepared_statements: dict[str, PreparedStatement] = {}

    def setup(self, replication_factor: int = 3) -> None:
        mm = MigrationManager(
            self.session, self.keyspace, replication_factor=replication_factor
        )
        mm.migrate()

    def _qualified_table(self, table: str) -> str:
        """Return the fully qualified table name for the configured keyspace."""
        return f"{self.keyspace}.{table}"

    def _get_prepared_statement(self, query: str) -> PreparedStatement:
        """Return a prepared statement for the given query, preparing it lazily."""
        statement = self._prepared_statements.get(query)
        if statement is None:
            prepared_query = query.replace("%s", "?")
            statement = self.session.prepare(prepared_query)
            if self.default_consistency_level is not None:
                statement.consistency_level = self.default_consistency_level
            self._prepared_statements[query] = statement
        return statement

    async def _aexecute_prepared(
        self,
        query: str,
        parameters: tuple[Any, ...] = (),
        fetch_size: int | None = None,
    ) -> AsyncResultSet:
        """Execute a prepared statement asynchronously with the provided parameters."""
        statement = self._get_prepared_statement(query)
        statement = statement.bind(parameters)
        if fetch_size is not None:
            statement.fetch_size = fetch_size
        return await aexecute(self.session, statement, None)

    def _batch_add_prepared(
        self, batch: BatchStatement, query: str, parameters: tuple[Any, ...]
    ) -> None:
        """Add a prepared statement with parameters to a batch."""
        statement = self._get_prepared_statement(query)
        batch.add(statement, parameters)

    async def _row_iterator_prepared(
        self,
        query: str,
        params: tuple[Any, ...],
        fetch_size: int,
    ) -> AsyncIterator[Any]:
        rs = await self._aexecute_prepared(
            query,
            params,
            fetch_size=fetch_size,
        )
        async for row in rs:
            yield row

    def _to_utc_datetime(self, dt: datetime) -> datetime:
        """Convert a naive datetime (assumed UTC) to timezone-aware UTC datetime.

        Cassandra returns naive datetime objects, but we need timezone-aware ones
        for proper ISO format serialization.
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt

    async def _get_activities_for_thread(
        self,
        user_id: uuid.UUID,
        thread_id: uuid.UUID,
        limit: int,
    ) -> Sequence[_ThreadActivity]:
        """Get old activity_at UUIDs for a given user and thread.

        Args:
            user_id: User UUID
            thread_id: Thread UUID
        Returns:
            List of old activity_at UUIDs
        """

        query = f"""
        SELECT thread_name, thread_created_at, activity_at FROM {self._table_user_activity_by_thread}
        WHERE thread_id = %s
        LIMIT {limit}
        """
        rows = await self._aexecute_prepared(query, (thread_id,))
        return [
            _ThreadActivity(
                user_id=user_id,
                thread_id=thread_id,
                activity_at=row.activity_at,
                created_at=row.thread_created_at,
                name=row.thread_name,
            )
            for row in rows.current_rows
            if row.activity_at is not None
        ]

    async def _update_activity_by_thread(
        self,
        thread_id: uuid.UUID,
        user_id: uuid.UUID,
        activity_at: uuid.UUID | None,
    ) -> uuid.UUID:
        """Update thread activity in user_activity_by_thread table.

        Args:
            thread_id: Thread UUID
            user_id: User UUID (required - threads without users are not tracked)
            activity_at: New activity timestamp (created_at of new step)
        """

        # Create our activity_at if not provided
        if activity_at is None:
            activity_at = uuid7()

        _, clustering_bucket = self.activity_bucket_strategy.get_bucket(activity_at)

        insert_query = f"""
        INSERT INTO {self._table_user_activity_by_thread}
        (thread_id, clustering_bucket_start, activity_at, user_id)
        VALUES (%s, %s, %s, %s)
        """

        await self._aexecute_prepared(
            insert_query, (thread_id, clustering_bucket, activity_at, user_id)
        )

        return activity_at

    async def _sync_activity_by_user_with_activity_by_thread(
        self,
        thread_id: uuid.UUID,
        user_id: uuid.UUID,
    ):
        # Step 1: Get latest activity from user_activity_by_thread
        activities = await self._get_activities_for_thread(user_id, thread_id, limit=20)

        # If there are no activities, nothing to sync
        if not activities:
            return

        # Step 2: Add to batch delete old activities from threads_by_user_activity
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        old_activities = activities[1:]  # Exclude latest activity
        delete_user_activity_query = f"""
        DELETE FROM {self._table_threads_by_user_activity}
        WHERE user_id = %s AND partition_bucket_start = %s AND clustering_bucket_start = %s AND thread_created_at = %s
        """
        for old_activity in old_activities:
            partition_bucket, clustering_bucket = (
                self.activity_bucket_strategy.get_bucket(old_activity["activity_at"])
            )
            self._batch_add_prepared(
                batch,
                delete_user_activity_query,
                (
                    user_id,
                    partition_bucket,
                    clustering_bucket,
                    old_activity["created_at"],
                ),
            )

        # Step 3: Add to batch insert latest activity into threads_by_user_activity
        latest_activity = activities[0]
        partition_bucket, clustering_bucket = self.activity_bucket_strategy.get_bucket(
            latest_activity["activity_at"]
        )
        insert_query = f"""
        INSERT INTO {self._table_threads_by_user_activity}
        (user_id, partition_bucket_start, clustering_bucket_start, thread_created_at, activity_at, thread_id, thread_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self._batch_add_prepared(
            batch,
            insert_query,
            (
                user_id,
                partition_bucket,
                clustering_bucket,
                latest_activity["created_at"],
                latest_activity["activity_at"],
                thread_id,
                latest_activity["name"],
            ),
        )

        # Step 4: Execute batch for 2 and 3
        #
        # Bundling all of this into a single batch reduces the chance of
        # seeing duplicate entries in threads_by_user_activity.
        await aexecute(self.session, batch)

        # Step 5: Delete old activities from user_activity_by_thread
        #
        # We could include this in the logged batch above, but we are deleting
        # from a different partition in this table, so performance might be
        # worse if we do it that way.
        delete_by_thread_query = f"""
        DELETE FROM {self._table_user_activity_by_thread}
        WHERE thread_id = %s AND clustering_bucket_start = %s
        """
        tasks = []
        for activity in old_activities:
            _, clustering_bucket = self.activity_bucket_strategy.get_bucket(
                activity["activity_at"]
            )
            tasks.append(
                self._aexecute_prepared(
                    delete_by_thread_query,
                    (
                        activity["thread_id"],
                        clustering_bucket,
                    ),
                )
            )
        await asyncio.gather(*tasks)

    # _ThreadActivity
    async def _update_activity(
        self,
        thread_id: uuid.UUID,
        user_id: uuid.UUID | None,
        activity_at: uuid.UUID | None,
    ):
        if not user_id:
            # Threads without users cannot be listed, so don't track activity
            self.log.debug(
                f"Skipping activity update for thread {thread_id} - no user_id"
            )
            return

        # Insert a new activity entry for this thread
        await self._update_activity_by_thread(
            thread_id,
            user_id,
            activity_at,
        )

        # Sync up threads_by_user_activity table for this thread
        await self._sync_activity_by_user_with_activity_by_thread(
            thread_id,
            user_id,
        )

    # User methods

    async def _get_user_identifier_for_id(self, user_id: str | uuid.UUID) -> str | None:
        """Get user identifier by user ID."""
        query = f"""
            SELECT identifier
            FROM {self._table_users}
            WHERE id = %s
        """
        rs = await self._aexecute_prepared(query, (to_uuid(user_id),))
        row = rs.one()

        if not row:
            return None

        return row.identifier  # type: ignore[no-any-return]

    async def _get_user_row(self, identifier: str):
        """Get a user by identifier."""
        query = f"""
            SELECT id, identifier, created_at, metadata
            FROM {self._table_users_by_identifier}
            WHERE identifier = %s
        """
        rs = await self._aexecute_prepared(query, (identifier,))
        row = rs.one()

        if not row or row.created_at is None:
            return None

        return row

    async def _get_user_created_at(self, user_id: uuid.UUID | str) -> datetime | None:
        """Get user from context or database by user ID."""
        # Try to get user from context
        try:
            if (
                context.session
                and context.session.user
                and hasattr(context.session.user, "createdAt")
            ):
                user_obj = context.session.user
                if hasattr(user_obj, "id") and str(
                    getattr(user_obj, "id", None)
                ) == str(user_id):
                    created_at = getattr(user_obj, "createdAt", None)

                    if created_at:
                        if isinstance(created_at, str):
                            return isoformat_to_datetime(created_at)
                        return created_at  # type: ignore[no-any-return]
        except ChainlitContextException:
            # No context available, so we will look it up from the database
            pass

        # Fallback to database lookup
        identifier = await self._get_user_identifier_for_id(user_id)
        if identifier is None:
            return None
        user = await self.get_user(identifier)
        if user is None:
            return None
        return isoformat_to_datetime(user.createdAt)

    async def get_user(self, identifier: str) -> PersistedUser | None:
        """Get a user by identifier."""
        row = await self._get_user_row(identifier)

        if row is None:
            return None

        metadata = _unpack_metadata(row.metadata)
        return PersistedUser(
            id=str(row.id),  # Convert UUID to string for Chainlit
            identifier=row.identifier,
            createdAt=uuid7_isoformat(row.created_at),
            metadata=metadata,
        )

    async def create_user(self, user: User) -> PersistedUser | None:
        """Create or update a user."""
        existing_user = await self.get_user(user.identifier)

        query_params: dict[str, Any] = {}

        # Always include the metadata
        query_params["metadata"] = _pack_metadata(user.metadata or {})

        if not existing_user:
            query_params["id"] = uuid.uuid4()
            query_params["created_at"] = uuid7()
            query_params["identifier"] = user.identifier
        else:
            query_params["id"] = uuid.UUID(existing_user.id)
            # NOTE: we do not allow the identifier to be changed, but we still
            # need the value to update the metadata in the users_by_identifier
            # table
            query_params["identifier"] = existing_user.identifier

        # Create INSERT queries
        keys = list(query_params.keys())
        param_values = list(query_params.values())
        column_names = ", ".join(keys)
        placeholders = ", ".join(["%s"] * len(query_params))

        insert_user_query = f"""
        INSERT INTO {self._table_users} ({column_names})
        VALUES ({placeholders})
        """

        insert_user_by_identifier_query = f"""
        INSERT INTO {self._table_users_by_identifier} ({column_names}) 
        VALUES ({placeholders})
        """

        batch = BatchStatement(batch_type=BatchType.LOGGED)
        self._batch_add_prepared(
            batch,
            insert_user_query,
            tuple(param_values),
        )
        self._batch_add_prepared(
            batch,
            insert_user_by_identifier_query,
            tuple(param_values),
        )

        await aexecute(self.session, batch)
        return await self.get_user(user.identifier)

    # Feedback methods

    def _thread_id_from_context(self) -> uuid.UUID | None:
        """Get thread_id from context, if available."""
        try:
            thread_id = context.session.thread_id if context.session else None
            if thread_id is None:
                return None
            elif isinstance(thread_id, uuid.UUID):
                return thread_id
            elif isinstance(thread_id, str):
                return uuid.UUID(thread_id)
            else:
                raise ValueError("Invalid thread_id type in context")
        except ChainlitContextException:
            # No context available, just return None
            return None

    async def delete_feedback(self, feedback_id: str) -> bool:
        """Delete feedback by ID."""

        # Extract step_id from feedback_id format
        thread_id, step_id = _feedback_id_to_thread_step_id(feedback_id)

        # Clear feedback columns (no ALLOW FILTERING needed with full primary key)
        update_query = f"""
        UPDATE {self._table_steps_by_thread}
        SET feedback_value = null, feedback_comment = null
        WHERE thread_id = %s AND id = %s
        """
        await self._aexecute_prepared(
            update_query,
            (thread_id, step_id),
        )

        return True

    async def upsert_feedback(self, feedback: Feedback) -> str:
        """Create or update feedback for a step.

        Feedback is stored inline with the step in steps_by_thread_id table.
        The feedback ID is derived from the step ID using the format "step#<step_uuid>".

        Uses context.session.thread_id to determine which thread the step belongs to,
        following the DynamoDB implementation pattern.
        """
        # Extract thread_id and step_id from the feedback.id
        thread_id: uuid.UUID | None = None
        if feedback.id:
            thread_id, step_id = _feedback_id_to_thread_step_id(feedback.id)
        elif feedback.threadId:
            thread_id = uuid.UUID(feedback.threadId)
            step_id = uuid.UUID(feedback.forId)

        if thread_id is None:
            raise ValueError("Thread ID could not be determined from feedback ID")

        # Update feedback columns in the step row (no ALLOW FILTERING needed with full primary key)
        update_query = f"""
            UPDATE {self._table_steps_by_thread}
            SET feedback_value = %s, feedback_comment = %s
            WHERE thread_id = %s AND id = %s
        """
        await self._aexecute_prepared(
            update_query,
            (
                feedback.value,
                feedback.comment,
                thread_id,
                step_id,
            ),
        )

        # Return derived feedback ID
        return _thread_step_id_to_feedback_id(thread_id, step_id)

    # Element methods

    async def _delete_element_storage(self, object_key: str):
        """Helper to delete element file from storage.

        Args:
            object_key: The storage key for the file to delete

        This is idempotent and safe to call even if the file doesn't exist.
        """
        if self.storage_client is not None and object_key:
            await self.storage_client.delete_file(object_key=object_key)

    @queue_until_user_message()
    async def create_element(self, element: Element):
        """Create an element."""
        if not element.for_id:
            return

        # Determine thread_id - use element.thread_id if provided, otherwise fallback to context
        thread_id_str: str | None = element.thread_id
        if not thread_id_str:
            # Fallback to getting thread_id from context
            thread_id_uuid = self._thread_id_from_context()
            if thread_id_uuid:
                thread_id_str = str(thread_id_uuid)

        if not thread_id_str:
            raise ValueError(
                "Element thread_id must be provided or available in context"
            )

        # Handle file uploads only if storage_client is configured
        object_key = None
        if self.storage_client:
            content: bytes | str | None = None

            if element.path:
                async with aiofiles.open(element.path, "rb") as f:
                    content = await f.read()
            elif element.content:
                content = element.content
            elif not element.url:
                raise ValueError("Element url, path or content must be provided")

            if content is not None:
                object_key = f"threads/{thread_id_str}/files/{element.id}"

                content_disposition = (
                    f'attachment; filename="{element.name}"'
                    if not (
                        GCSStorageClient is not None
                        and isinstance(self.storage_client, GCSStorageClient)
                    )
                    else None
                )
                await self.storage_client.upload_file(
                    object_key=object_key,
                    data=content,
                    mime=element.mime or "application/octet-stream",
                    overwrite=True,
                    content_disposition=content_disposition,
                )

        else:
            # Log warning only if element has file content that needs uploading
            if element.path or element.content:
                self.log.warning(
                    "Data Layer: No storage client configured. "
                    "File will not be uploaded."
                )

        # Convert UUIDs
        element_id = uuid.UUID(element.id)
        thread_id_uuid = uuid.UUID(thread_id_str)
        for_id = uuid.UUID(element.for_id) if element.for_id else None

        # Generate created_at timestamp
        created_at = datetime.now()

        # Single INSERT to elements_by_thread_id (no batch needed, no lookup table)
        query = f"""INSERT INTO {self._table_elements_by_thread} (
            thread_id, id, for_id, mime, name, object_key, url,
            chainlit_key, display, size, language, page, props, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        await self._aexecute_prepared(
            query,
            (
                thread_id_uuid,
                element_id,
                for_id,
                element.mime,
                element.name,
                object_key,
                element.url,
                element.chainlit_key,
                element.display,
                element.size,
                element.language,
                getattr(element, "page", None),
                json.dumps(getattr(element, "props", {})),
                created_at,
            ),
        )

    async def get_element(self, thread_id: str, element_id: str) -> ElementDict | None:
        """Get an element by ID and thread ID.

        Queries elements_by_thread_id directly with thread_id and id (no lookup needed).
        """
        # Query elements_by_thread_id with thread_id and id (no for_id needed in key)
        query = f"""
            SELECT thread_id, for_id, id, mime, name, object_key, url,
                   chainlit_key, display, size, language, page, props, created_at
            FROM {self._table_elements_by_thread}
            WHERE thread_id = %s AND id = %s
        """
        rs = await self._aexecute_prepared(
            query, (uuid.UUID(thread_id), uuid.UUID(element_id))
        )
        row = rs.one()

        if not row:
            return None

        # Generate signed URL if storage_client is configured and no direct URL exists
        url = row.url
        if self.storage_client is not None and not url and row.object_key:
            url = await self.storage_client.get_read_url(object_key=row.object_key)

        return ElementDict(
            id=str(row.id),
            threadId=str(row.thread_id),
            type="file",
            chainlitKey=row.chainlit_key,
            url=url,
            objectKey=row.object_key,
            name=row.name,
            display=row.display,
            size=row.size,
            language=row.language,
            page=row.page,
            forId=str(row.for_id) if row.for_id else None,
            mime=row.mime,
            props=json.loads(row.props) if row.props else None,
        )

    @queue_until_user_message()
    async def delete_element(self, element_id: str, thread_id: str | None = None):
        """Delete an element by ID.

        Deletion order:
        1. Delete file from storage (idempotent)
        2. Delete from elements_by_thread_id

        Uses context.session.thread_id if thread_id not provided (DynamoDB pattern).
        """
        try:
            element_uuid = uuid.UUID(element_id)

            # Get thread_id from parameter or context (DynamoDB pattern)
            thread_id_uuid: uuid.UUID | None = to_uuid(thread_id)
            if thread_id_uuid is None:
                thread_id_uuid = self._thread_id_from_context()

            if not thread_id_uuid:
                self.log.warning(
                    f"Cannot delete element {element_id} - thread_id not available"
                )
                return

            # Get element details to find object_key before deleting
            query = f"SELECT object_key FROM {self._table_elements_by_thread} WHERE thread_id = %s AND id = %s"
            rs = await self._aexecute_prepared(query, (thread_id_uuid, element_uuid))
            row = rs.one()

            # Phase 1: Delete file from storage if it exists (idempotent)
            if row and row.object_key:
                await self._delete_element_storage(row.object_key)

            # Phase 2: Delete from elements_by_thread_id (simple DELETE, no batch needed)
            delete_query = f"DELETE FROM {self._table_elements_by_thread} WHERE thread_id = %s AND id = %s"
            await self._aexecute_prepared(delete_query, (thread_id_uuid, element_uuid))
        except:
            self.log.exception(
                "Error deleting element",
                extra={"element_id": element_id, "thread_id": thread_id},
            )
            raise

    # Step methods

    async def _create_step(self, step_dict: StepDict, is_update: bool = False):
        """Internal helper to create or update a step with partial update semantics.

        Args:
            step_dict: Step data to insert/update
            is_update: If True, this is an update operation (never modifies createdAt).
                      If False, this is a create operation (createdAt is required).

        Only fields present in step_dict are written. Omitted fields preserve
        their existing values in the database (partial update semantics).
        This matches the reference implementation behavior.
        """
        thread_id = step_dict["threadId"]
        thread_id_uuid = uuid.UUID(thread_id)

        # Update thread timestamp
        await self.update_thread(thread_id)

        # Update thread activity - get user_id, name, created_at from threads table
        # Also check if thread is deleted
        thread_query = f"SELECT user_id, deleted_at, name, created_at FROM {self._table_threads} WHERE id = %s"
        thread_rs = await self._aexecute_prepared(thread_query, (thread_id_uuid,))
        thread_row = thread_rs.one()

        # Reject adding steps to deleted threads
        if thread_row and thread_row.deleted_at:
            raise ValueError(f"Cannot add step to deleted thread {thread_id}")

        # Build db_params dict with only non-None values
        # Maps camelCase StepDict keys to snake_case database columns
        db_params: dict[str, Any] = {}

        # Required fields
        if "id" in step_dict and step_dict["id"] is not None:
            db_params["id"] = uuid.UUID(step_dict["id"])
        if "threadId" in step_dict and step_dict["threadId"] is not None:
            db_params["thread_id"] = uuid.UUID(step_dict["threadId"])

        # Optional fields - only include if present and non-None
        if "parentId" in step_dict and step_dict["parentId"]:
            db_params["parent_id"] = uuid.UUID(step_dict["parentId"])

        if "name" in step_dict and step_dict["name"] is not None:
            db_params["name"] = step_dict["name"]

        if "type" in step_dict and step_dict["type"] is not None:
            db_params["type"] = step_dict["type"]

        if "streaming" in step_dict and step_dict["streaming"] is not None:
            db_params["streaming"] = step_dict["streaming"]

        if "waitForAnswer" in step_dict and step_dict["waitForAnswer"] is not None:
            db_params["wait_for_answer"] = step_dict["waitForAnswer"]

        if "isError" in step_dict and step_dict["isError"] is not None:
            db_params["is_error"] = step_dict["isError"]

        # metadata: always include if present, even if empty dict (clears existing value)
        if "metadata" in step_dict:
            db_params["metadata"] = _pack_metadata(step_dict["metadata"])

        if "tags" in step_dict and step_dict["tags"] is not None:
            db_params["tags"] = step_dict["tags"]

        if "input" in step_dict and step_dict["input"] is not None:
            db_params["input"] = step_dict["input"]

        if "output" in step_dict and step_dict["output"] is not None:
            db_params["output"] = step_dict["output"]

        # Handle createdAt based on whether this is an update or create
        if not is_update:
            # Chainlit gives us createdAt, but uuidv7 is more convenient for
            # ordering and lookups later, so we generate our own.
            created_at_uuid = uuid7()
            db_params["created_at"] = created_at_uuid

            if step_dict.get("createdAt"):
                # Use provided createdAt
                created_at_raw = step_dict["createdAt"]
                if created_at_raw:
                    db_params["created_at"] = isoformat_to_uuid7(created_at_raw)
            else:
                db_params["created_at"] = uuid7()
        # else: For updates, never include created_at (preserve existing value)

        # Other timestamp fields
        if "start" in step_dict and step_dict["start"]:
            start_raw = step_dict["start"]
            db_params["start"] = isoformat_to_datetime(start_raw)

        if "end" in step_dict and step_dict["end"]:
            end_raw = step_dict["end"]
            db_params["end"] = isoformat_to_datetime(end_raw)

        # generation: always include if present, even if empty dict (clears existing value)
        if "generation" in step_dict:
            db_params["generation"] = _pack_metadata(step_dict["generation"])

        if "showInput" in step_dict and step_dict["showInput"] is not None:
            db_params["show_input"] = step_dict["showInput"]

        if "language" in step_dict and step_dict["language"] is not None:
            db_params["language"] = step_dict["language"]

        # Build dynamic query with only the columns we have values for
        columns = ", ".join(f'"{c}"' if c == "end" else c for c in db_params.keys())
        placeholders = ", ".join(["%s"] * len(db_params))

        query = f"""
        INSERT INTO {self._table_steps_by_thread} ({columns})
        VALUES ({placeholders})
        """

        upsert_step_task = asyncio.create_task(
            self._aexecute_prepared(query, tuple(db_params.values()))
        )

        # Update thread activity
        if not is_update and thread_row and thread_row.user_id:
            await self._update_activity(
                thread_id=thread_id_uuid,
                user_id=thread_row.user_id,
                activity_at=db_params["created_at"],
            )

        await upsert_step_task

    @queue_until_user_message()
    async def create_step(self, step_dict: StepDict):
        """Create a new step.

        The createdAt field is required (defaults to current time if not provided).
        This creates a new activity entry for thread ordering.
        """
        await self._create_step(step_dict, is_update=False)

    @queue_until_user_message()
    async def update_step(self, step_dict: StepDict):
        """Update an existing step.

        The createdAt field is never modified (preserved from original creation).
        This prevents creating duplicate activity entries.
        """
        await self._create_step(step_dict, is_update=True)

    @queue_until_user_message()
    async def delete_step(self, step_id: str):
        """Delete a step and related elements with retry-friendly semantics.

        Deletion order:
        1. Delete all elements (async gather) - can retry if fails
        2. Delete step from steps_by_thread_id

        Note: Feedback is stored inline with the step, so it's automatically
        deleted when the step row is deleted.

        Uses context.session.thread_id to determine which thread the step belongs to,
        following the DynamoDB implementation pattern.
        """
        thread_id = self._thread_id_from_context()
        assert thread_id is not None, "Thread ID must be available in context"
        return await self._delete_step(thread_id, to_uuid(step_id))

    async def _delete_step(self, thread_id: uuid.UUID, step_id: uuid.UUID):
        # Step 1: Perform soft-delete
        deleted_at = uuid7()

        soft_delete_steps_by_thread_query = f"""
        UPDATE {self._table_steps_by_thread}
        SET deleted_at = %s
        WHERE thread_id = %s AND id = %s
        """

        # Execute soft-delete in logged batch
        await self._aexecute_prepared(
            soft_delete_steps_by_thread_query,
            (deleted_at, thread_id, step_id),
        )

        # At this point, the step is marked as deleted, now proceed with
        # cleaning up the step.
        async def cleanup_step():
            # Step 2: Delete all elements
            elements_query = f"""
            SELECT id, for_id
            FROM {self._table_elements_by_thread}
            WHERE thread_id = %s
            """
            elements_rs = await self._aexecute_prepared(elements_query, (thread_id,))

            # Delete all elements in parallel using existing delete_element
            # This properly cleans up storage AND deletes from both tables
            delete_elements_tasks = []
            element_ids_to_delete = [
                row.id async for row in elements_rs if row.for_id == step_id
            ]
            for element_id in element_ids_to_delete:
                delete_elements_tasks.append(
                    self.delete_element(str(element_id), str(thread_id))
                )

            # Step 3: Delete step (simple DELETE, no batch needed)
            delete_query = f"DELETE FROM {self._table_steps_by_thread} WHERE thread_id = %s AND id = %s"
            delete_step_row_task = asyncio.create_task(
                self._aexecute_prepared(delete_query, (thread_id, step_id))
            )

            # Step 4: Await all deletions
            results = await asyncio.gather(
                *delete_elements_tasks, delete_step_row_task, return_exceptions=True
            )
            exceptions = select_exc(results)
            if exceptions:
                raise ExceptionGroup("Failed to cleanup step", list(exceptions))

        await cleanup_step()

    # Thread methods

    async def get_thread_author(self, thread_id: str) -> str:
        """Get the author (user identifier) of a thread.

        Raises ValueError if thread doesn't exist or has been deleted.
        """
        query = f"SELECT user_identifier, deleted_at FROM {self._table_threads} WHERE id = %s"
        rs = await self._aexecute_prepared(
            query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        row = rs.one()

        if not row or not row.user_identifier or row.deleted_at:
            raise ValueError(f"Author not found for thread_id {thread_id}")

        return cast(str, row.user_identifier)

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        """Get a thread with all its steps and elements.

        Returns None if thread doesn't exist or has been deleted.
        If thread is marked deleted but still exists, triggers cleanup in background.
        """
        # Get thread metadata
        thread_query = f"SELECT id, user_id, user_identifier, name, created_at, metadata, tags, deleted_at FROM {self._table_threads} WHERE id = %s"
        thread_rs = await self._aexecute_prepared(
            thread_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        thread_row = thread_rs.one()

        if not thread_row:
            return None

        # If thread is marked deleted, trigger cleanup and return None
        if thread_row.deleted_at:
            # This thread was accessed even though it was deleted. Trigger
            # cleanup to ensure the thread is removed.
            asyncio.create_task(self.delete_thread(thread_id))
            return None

        # Get all steps for this thread (including feedback columns)
        steps_query = f"""
            SELECT id, thread_id, parent_id, name, type, streaming,
                   wait_for_answer, is_error, metadata, tags, input, output,
                   created_at, start, "end", generation, show_input, language,
                   feedback_value, feedback_comment
            FROM {self._table_steps_by_thread} WHERE thread_id = %s
        """
        step_rows = await self._aexecute_prepared(
            steps_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID

        steps = []
        step_ids = []
        async for row in step_rows:
            step_ids.append(row.id)

            # Build feedback dict from inline columns, generating feedback_id from step_id
            feedback = None
            if row.feedback_value is not None:
                feedback = FeedbackDict(
                    id=_thread_step_id_to_feedback_id(
                        thread_row.id, row.id
                    ),  # Generate feedback_id
                    forId=str(row.id),  # Convert UUID to string
                    value=row.feedback_value,
                    comment=row.feedback_comment,
                )

            step_dict = StepDict(
                id=str(row.id),  # Convert UUID to string
                threadId=str(row.thread_id),  # Convert UUID to string
                parentId=str(row.parent_id)
                if row.parent_id
                else None,  # Convert UUID to string
                name=row.name,
                type=row.type,
                streaming=row.streaming,
                waitForAnswer=row.wait_for_answer,
                isError=row.is_error,
                metadata=_unpack_metadata(row.metadata),
                tags=row.tags,
                input=row.input or "",
                output=row.output or "",
                createdAt=uuid7_isoformat(row.created_at) if row.created_at else None,
                start=datetime_to_isoformat(row.start) if row.start else None,
                end=datetime_to_isoformat(row.end) if row.end else None,
                generation=_unpack_metadata(row.generation) if row.generation else None,
                showInput=row.show_input,
                language=row.language,
                feedback=feedback,
            )
            steps.append(step_dict)

        # Sort steps by created_at in Python (since Cassandra no longer orders by it)
        # ISO format timestamps sort correctly as strings
        steps.sort(key=lambda s: s.get("createdAt") or "")

        # Get all elements for this thread - single partition read!
        elements_query = f"""
            SELECT thread_id, id, for_id, mime, name, object_key, url,
                   chainlit_key, display, size, language, page, props, created_at
            FROM {self._table_elements_by_thread} WHERE thread_id = %s
        """
        element_rows = await self._aexecute_prepared(
            elements_query, (uuid.UUID(thread_id),)
        )

        elements = []
        async for row in element_rows:
            # Generate signed URL if storage_client is configured and no direct URL exists
            url = row.url
            if self.storage_client is not None and not url and row.object_key:
                url = await self.storage_client.get_read_url(object_key=row.object_key)

            element_dict = ElementDict(
                id=str(row.id),
                threadId=str(row.thread_id),
                type="file",
                chainlitKey=row.chainlit_key,
                url=url,
                objectKey=row.object_key,
                name=row.name,
                display=row.display,
                size=row.size,
                language=row.language,
                page=row.page,
                forId=str(row.for_id) if row.for_id else None,
                mime=row.mime,
                props=json.loads(row.props) if row.props else None,
            )
            elements.append(element_dict)

        created_at_value: str | None = (
            uuid7_isoformat(thread_row.created_at) if thread_row.created_at else None
        )
        metadata_value = (
            _unpack_metadata(thread_row.metadata) if thread_row.metadata else None
        )

        return ThreadDict(
            id=str(thread_row.id),  # Convert UUID to string
            createdAt=created_at_value or "",
            name=thread_row.name,
            userId=str(thread_row.user_id)
            if thread_row.user_id
            else None,  # Convert UUID to string
            userIdentifier=thread_row.user_identifier,
            tags=thread_row.tags,
            metadata=metadata_value,
            steps=steps,
            elements=elements,
        )

    async def update_thread(
        self,
        thread_id: str,
        name: str | None = None,
        user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ):
        """Create or update a thread."""

        # Check if thread exists and is not deleted
        existing_query = f"""SELECT created_at, metadata, deleted_at,
                        user_id, user_identifier, name, tags
                 FROM {self._table_threads} WHERE id = %s"""
        existing_rs = await self._aexecute_prepared(
            existing_query, (uuid.UUID(thread_id),)
        )
        existing_row = existing_rs.one()
        existing_metadata = None
        existing_user_id = None
        existing_created_at = None
        existing_deleted_at = None
        if existing_row is not None:
            if existing_row.metadata:
                existing_metadata = _unpack_metadata(existing_row.metadata)
            existing_user_id = existing_row.user_id
            existing_created_at = existing_row.created_at
            existing_deleted_at = existing_row.deleted_at

        # Reject updates to deleted threads
        if existing_deleted_at:
            # This thread was accessed even though it was deleted. Trigger
            # cleanup to ensure the thread is removed.
            try:
                await self.delete_thread(thread_id)
            except Exception:
                self.log.exception(
                    "Error cleaning up deleted thread", extra={"thread_id": thread_id}
                )
            raise ValueError(f"Cannot update deleted thread {thread_id}")

        # Get user ID
        final_user_id: uuid.UUID | None = to_uuid(user_id) or existing_user_id

        # Build db_params dict with only non-None values
        db_params: dict[str, Any] = {"id": uuid.UUID(thread_id)}

        # Only add the created_at if creating a new thread
        #
        # NOTE: Possible race here if two concurrent updates to a not-yet-created
        # thread which could lead to a mutation of the created_at value.
        if not existing_created_at:
            db_params["created_at"] = uuid7()

        # Only add user_id, if provided, if one is not already set
        if not existing_user_id and user_id:
            user_identifier = await self._get_user_identifier_for_id(user_id)
            if not user_identifier:
                raise ValueError(f"User with id {user_id} does not exist")
            db_params["user_id"] = to_uuid(user_id)
            db_params["user_identifier"] = user_identifier

        # Merge metadata if provided
        metadata_provided_name = None
        if metadata:
            new_metadata = {**(existing_metadata or {}), **metadata}
            db_params["metadata"] = _pack_metadata(new_metadata)

            # Extract name from metadata if present, this will be used later
            # to update the thread name
            metadata_provided_name = new_metadata.get("name")

        # Update name if specified either directly or via metadata
        if metadata_provided_name is not None or name is not None:
            db_params["name"] = name or metadata_provided_name

        # Add tags if provided (allow clearing by passing empty list)
        if tags is not None:
            db_params["tags"] = tags

        insert_thread_param_names = list(db_params.keys())
        insert_thread_values = [db_params[k] for k in insert_thread_param_names]
        insert_thread_columns = ", ".join(insert_thread_param_names)
        insert_thread_placeholders = ", ".join(["%s"] * len(insert_thread_param_names))

        insert_thread_query = f"""
        INSERT INTO {self._table_threads} ({insert_thread_columns})
        VALUES ({insert_thread_placeholders})
        """

        queries_and_params = [(insert_thread_query, tuple(insert_thread_values))]

        # Properties that map from the threads table to the user_activity_by_thread table
        #
        # threads table columns -> user_activity_by_thread table columns
        activity_view_keys = {
            "name": "thread_name",
            "created_at": "thread_created_at",
            "user_id": "user_id",
        }

        # Values to update in the user_activity_by_thread table
        activity_view_params = {
            activity_view_keys[k]: db_params[k]
            for k in activity_view_keys.keys()
            if k in db_params
        }

        # If we have a user and some of the list view fields have changed,
        # update the user_activity_by_thread table
        user_is_known = final_user_id is not None
        should_update_activity_view = len(activity_view_params) > 0 and user_is_known
        if should_update_activity_view:
            activity_view_params["thread_id"] = uuid.UUID(thread_id)

            activity_param_names = list(activity_view_params.keys())
            activity_values = [activity_view_params[k] for k in activity_param_names]
            activity_columns = ", ".join(activity_param_names)
            activity_placeholders = ", ".join(["%s"] * len(activity_param_names))

            # NOTE: For the view update, we are only modifying the static
            # columns so we do not need the activity_at timestamp here.
            update_activity_query = f"""
            INSERT INTO {self._table_user_activity_by_thread} ({activity_columns})
            VALUES ({activity_placeholders})
            """
            queries_and_params.append((update_activity_query, tuple(activity_values)))

        # Execute queries
        if len(queries_and_params) > 1:
            # All changes are to the same partition so we use an unlogged batch
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for q, params in queries_and_params:
                self._batch_add_prepared(batch, q, params)
            await self.session.aexecute(batch)
        elif len(queries_and_params) == 1:
            q, params = queries_and_params[0]
            await self._aexecute_prepared(q, params)
        else:
            raise ValueError("No queries to execute for update_thread")

        # Create an activity entry for the thread which will add a new row to
        # both user_activity_by_thread and threads_by_user_activity tables and
        # cleanup old entries as needed.
        #
        # NOTE: We have to wait for the above insert to complete to ensure we
        # we have the latest thread name and created_at in the
        # user_activity_by_thread table
        if should_update_activity_view and final_user_id:
            await self._update_activity(
                thread_id=uuid.UUID(thread_id),
                user_id=final_user_id,
                activity_at=uuid7(),
            )

    async def _delete_activity_entry(
        self,
        thread_id: uuid.UUID | str,
        thread_created_at: uuid.UUID | str,
        user_id: uuid.UUID | str,
        activity_at: uuid.UUID | str,
    ):
        thread_id_uuid = to_uuid(thread_id)
        thread_created_at_uuid = to_uuid(thread_created_at)
        user_id_uuid = to_uuid(user_id)
        activity_at_uuid = to_uuid(activity_at)

        if not thread_id_uuid or not user_id_uuid or not activity_at_uuid:
            raise ValueError("Invalid UUID provided")

        partition_bucket_start, clustering_bucket_start = (
            self.activity_bucket_strategy.get_bucket(activity_at_uuid)
        )

        delete_by_user_activity_query = f"""
        DELETE FROM {self._table_threads_by_user_activity}
        WHERE user_id = %s AND partition_bucket_start = %s AND clustering_bucket_start = %s AND thread_created_at = %s
        """

        delete_user_activity_by_thread_query = f"""
        DELETE FROM {self._table_user_activity_by_thread}
        WHERE thread_id = %s AND clustering_bucket_start = %s
        """

        # First delete from the main activity table
        await aexecute(
            self.session,
            delete_by_user_activity_query,
            (
                user_id_uuid,
                partition_bucket_start,
                clustering_bucket_start,
                thread_created_at_uuid,
            ),
        )

        # Second delete from the activity by thread table only if the first
        # succeeded. We do it this way to avoid orphaned entries as we can get
        # from a thread_id to values in the user_activity_by_thread table, but
        # we can only, in a performant manner, find entries in the
        # threads_by_user_activity table via the activity_at values in the
        # user_activity_by_thread table.
        await aexecute(
            self.session,
            delete_user_activity_by_thread_query,
            (
                thread_id_uuid,
                clustering_bucket_start,
            ),
        )

    async def _delete_thread_activity_entries(
        self, thread_id: uuid.UUID | str, thread_created_at: uuid.UUID | str
    ):
        thread_id_uuid = to_uuid(thread_id)
        assert thread_id_uuid is not None
        activity_by_thread_query = f"""
        SELECT user_id, activity_at FROM {self._table_user_activity_by_thread}
        WHERE thread_id = %s
        """
        rs = await aexecute(
            self.session,
            activity_by_thread_query,
            (thread_id_uuid,),
        )
        activity_at_and_user_ids = [
            (row.activity_at, row.user_id)
            async for row in rs
            if row.activity_at and row.user_id
        ]
        delete_activity_tasks = []
        for activity_at, user_id in activity_at_and_user_ids:
            delete_activity_tasks.append(
                self._delete_activity_entry(
                    thread_id_uuid,
                    thread_created_at,
                    user_id,
                    activity_at,
                )
            )
        result_activity_deletions = await asyncio.gather(
            *delete_activity_tasks, return_exceptions=True
        )
        for index, result in enumerate(result_activity_deletions):
            activity_at, user_id = activity_at_and_user_ids[index]
            if isinstance(result, BaseException):
                self.log.error(
                    "Error deleting activity",
                    extra={
                        "thread_id": str(thread_id_uuid),
                        "user_id": str(user_id),
                        "activity_at": str(activity_at),
                    },
                    exc_info=result,
                )
        exceptions = select_exc(result_activity_deletions)
        if exceptions:
            raise ExceptionGroup(
                f"Failed to delete some activity entries for thread {str(thread_id_uuid)}",
                list(exceptions),
            )

    async def delete_thread(self, thread_id: str):
        """Delete a thread.

        Performs a soft delete in the threads table by setting the `deleted`
        column to true, then cleans up all related data. This ensure that any
        in-flight operations do no resurrect the thread.

        Subsequent calls to delete_thread for the same thread_id will continue
        the cleanup process, allowing for retry on partial failures.
        """
        thread_id_uuid = to_uuid(thread_id)
        if not thread_id_uuid:
            raise ValueError(f"Invalid thread_id: {thread_id}")

        # Get thread info for activity cleanup
        thread_query = f"SELECT user_id, deleted_at, created_at FROM {self._table_threads} WHERE id = %s"
        thread_rs = await self._aexecute_prepared(thread_query, (thread_id_uuid,))
        thread_row = thread_rs.one()

        if not thread_row:
            # We only do a soft-delete in the threads table, so if the thread
            # row doesn't exist, then the thread never did.
            return

        # Step 1: Mark as deleted if not already
        if not thread_row.deleted_at:
            soft_delete_query = (
                f"UPDATE {self._table_threads} SET deleted_at = %s WHERE id = %s"
            )
            await self._aexecute_prepared(soft_delete_query, (uuid7(), thread_id_uuid))

        # At this point, the thread is marked deleted which should stop any
        # further use of it. We can run the remaining cleanup steps in parallel.

        async def cleanup_thread(thread_id: uuid.UUID, thread_created_at: uuid.UUID):
            # Step 2: Delete all steps
            select_step_ids_query = (
                f"SELECT id FROM {self._table_steps_by_thread} WHERE thread_id = %s"
            )
            rs = await aexecute(
                self.session,
                select_step_ids_query,
                (thread_id,),
            )
            step_ids = [row.id async for row in rs]
            delete_step_tasks = []
            for step_id in step_ids:
                delete_step_tasks.append(self._delete_step(thread_id, step_id))

            # Step 3: Delete all activity
            delete_activity_task = asyncio.create_task(
                self._delete_thread_activity_entries(thread_id, thread_created_at)
            )

            result_step_deletions = await asyncio.gather(
                *delete_step_tasks, return_exceptions=True
            )
            for index, result in enumerate(result_step_deletions):
                if isinstance(result, BaseException):
                    self.log.error(
                        "Error deleting step during thread deletion",
                        extra={
                            "thread_id": str(thread_id),
                            "step_id": str(step_ids[index]),
                        },
                        exc_info=result,
                    )

            result_activity_deletions = await asyncio.gather(
                delete_activity_task, return_exceptions=True
            )

            exceptions = select_exc(list(result_step_deletions) + list(result_activity_deletions))
            if exceptions:
                raise ExceptionGroup(
                    f"Failed to delete some data for thread {str(thread_id)}",
                    list(exceptions),
                )

        await cleanup_thread(thread_id_uuid, thread_row.created_at)

    async def _find_partitions_for_user_descending(
        self, user_id: uuid.UUID, start: datetime | None = None
    ) -> AsyncIterator[datetime]:
        if start is None:
            start = datetime.now(tz=UTC)

        first_partition, _ = self.activity_bucket_strategy.get_bucket(start)

        # There cannot be any partitions before the user's creation date (since
        # a user cannot have activity before they exist).)
        created_at = await self._get_user_created_at(user_id)
        if not created_at:
            raise ValueError(f"User with id {str(user_id)} does not exist")

        last_bucket, _ = self.activity_bucket_strategy.get_bucket(created_at)

        current_partition = first_partition
        while current_partition >= last_bucket:
            yield current_partition
            current_partition, _ = self.activity_bucket_strategy.get_bucket(
                current_partition - timedelta(milliseconds=1)
            )

    def decode_thread_cursor(self, cursor: str) -> ThreadCursor:
        # Base62 decode
        cursor_bytes = base62.decodebytes(cursor)
        cursor_unpacked = _unpack_metadata(cursor_bytes)

        # Start with required fields
        if "start" not in cursor_unpacked:
            raise ValueError("Invalid cursor: missing 'start' field")

        cursor_dict: ThreadCursor = {"start": uuid.UUID(bytes=cursor_unpacked["start"])}

        # Add optional fields
        if "thread_start" in cursor_unpacked:
            cursor_dict["thread_start"] = uuid.UUID(
                bytes=cursor_unpacked["thread_start"]
            )

        return cursor_dict

    def encode_thread_cursor(self, cursor: ThreadCursor) -> str:
        # Base62 encode
        dict_to_encode: dict[str, bytes] = {}
        if cursor.get("start"):
            dict_to_encode["start"] = cursor["start"].bytes
        if cursor.get("thread_start"):
            dict_to_encode["thread_start"] = cursor["thread_start"].bytes
        cursor_bytes = _pack_metadata(dict_to_encode)
        if cursor_bytes is None:
            cursor_bytes = b""
        encoded_cursor: str = base62.encodebytes(cursor_bytes)
        return encoded_cursor

    async def list_threads(self, pagination: Pagination, filters: ThreadFilter):
        if not filters.userId:
            raise ValueError("userId is required")

        if filters.feedback is not None:
            self.log.warning(
                "Cassandra: filters on feedback not supported. "
                "Feedback filtering requires full thread data with steps."
            )

        # Decode the cursor if we have one
        cursor_dict: ThreadCursor | None = None
        if pagination.cursor:
            cursor_dict = self.decode_thread_cursor(pagination.cursor)

        start: uuid.UUID = uuid7(datetime=datetime.now(tz=UTC))
        if cursor_dict and cursor_dict.get("start"):
            start = cursor_dict["start"]

        thread_start: uuid.UUID = uuid7(datetime=add_timezone_if_missing(datetime.max))
        if cursor_dict and cursor_dict.get("thread_start"):
            thread_start = cursor_dict["thread_start"]

        # Figure out how many items to fetch
        page_size = pagination.first if pagination.first else DEFAULT_THREADS_PER_PAGE

        # Get 20% extra to account for duplicate results and consuming entire
        # clustering buckets
        fetch_size = int(page_size * 1.20)

        # If filtering with a search string, double the fetch size again as most
        # results will be filtered out
        if filters.search:
            fetch_size = min(fetch_size * 2, DEFAULT_THREADS_PER_PAGE)

        # Get the user ID as a UUID to include in the query
        user_id = to_uuid(filters.userId)
        if not user_id:
            # User ID is required
            raise ValueError(f"Invalid userId: {filters.userId}")

        select_threads_query = f"""
        SELECT partition_bucket_start,clustering_bucket_start, thread_id, thread_name, thread_created_at, activity_at
        FROM {self._table_threads_by_user_activity}
        WHERE user_id = %s AND partition_bucket_start = %s
        AND clustering_bucket_start < %s
        """

        # If we have a thread_start from the cursor, then we need to add an
        # additional condition to the query to only get threads before that ID
        # in the first clustering bucket.
        select_threads_with_thread_start_query = f"""
        SELECT partition_bucket_start, clustering_bucket_start, thread_id, thread_name, thread_created_at, activity_at
        FROM {self._table_threads_by_user_activity}
        WHERE user_id = %s AND partition_bucket_start = %s
        AND clustering_bucket_start = %s AND thread_id < %s
        """

        # Build the row iterator
        if cursor_dict and "thread_start" in cursor_dict:
            # Edge case
            #
            # Previously, we were unable to fully consume a clustering bucket
            # and had to stop in the middle. Now we are resuming from that point
            # so the first clustering bucket needs to be handled specially.
            partition_bucket_start, clustering_bucket_start = (
                self.activity_bucket_strategy.get_bucket(start)
            )
            first_iterator = self._row_iterator_prepared(
                select_threads_with_thread_start_query,
                (
                    user_id,
                    partition_bucket_start,
                    clustering_bucket_start,
                    thread_start,
                ),
                fetch_size=fetch_size,
            )

            # After the first clustering bucket, the remaining can be handled
            # normally
            next_partition_bucket_start, _ = self.activity_bucket_strategy.get_bucket(
                clustering_bucket_start - timedelta(milliseconds=1)
            )
            partitions = self._find_partitions_for_user_descending(
                user_id, next_partition_bucket_start
            )
            row_iterator = achain_from(
                amap(
                    lambda partition: self._row_iterator_prepared(
                        select_threads_query,
                        (
                            user_id,
                            partition,
                            clustering_bucket_start,
                        ),
                        fetch_size=fetch_size,
                    ),
                    partitions,
                )
            )
            row_iterator = achain(first_iterator, row_iterator)
        else:
            # Normal case
            iterators: AsyncIterator[AsyncIterator[Any]] = amap(
                lambda partition: self._row_iterator_prepared(
                    select_threads_query,
                    (
                        user_id,
                        partition,
                        uuid7_to_datetime(start),
                    ),
                    fetch_size=fetch_size,
                ),
                self._find_partitions_for_user_descending(
                    user_id, uuid7_to_datetime(start)
                ),
            )

            row_iterator = achain_from(iterators)

        result = await collect_thread_list_results(
            row_iterator,
            page_size=page_size,
            cursor=cursor_dict,
            search=filters.search if filters else None,
            max_clustering_size=self.max_clustering_bucket_size,
        )

        # Cleanup any duplicates we found
        if result.duplicate_rows and user_id:
            delete_tasks = []
            unique_duplicate_ids = set()
            for row in result.duplicate_rows:
                unique_duplicate_ids.add(row.thread_id)
            for thread_id in unique_duplicate_ids:
                delete_tasks.append(
                    asyncio.create_task(
                        self._sync_activity_by_user_with_activity_by_thread(
                            thread_id=thread_id,
                            user_id=user_id,
                        )
                    )
                )
            # TODO: Should we await these tasks or just fire and forget?

        # Create our thread list
        threads: list[ThreadDict] = []
        for row in result.selected_rows:
            thread_dict = ThreadDict(
                id=str(row.thread_id),
                createdAt=uuid7_isoformat(row.thread_created_at),
                name=row.thread_name,
                userId=str(user_id),  # Available from query context (partition key)
                userIdentifier=None,
                tags=None,
                metadata=None,
                steps=[],  # Empty array - UI will call get_thread when needed
                elements=[],  # Empty array
                # Optional fields default to None
            )
            threads.append(thread_dict)

        # Create our cursor for the next page
        next_cursor_str = None
        if result.next_cursor:
            next_cursor_str = self.encode_thread_cursor(result.next_cursor)

        response = PaginatedResponse(
            pageInfo=PageInfo(
                hasNextPage=next_cursor_str is not None,
                startCursor=pagination.cursor,
                endCursor=next_cursor_str,
            ),
            data=threads,
        )

        return response

    async def build_debug_url(self) -> str:
        """Build a debug URL (not implemented for Cassandra)."""
        return ""

    async def close(self) -> None:
        """Close the Cassandra connection and storage client."""
        if self.storage_client:
            await self.storage_client.close()
        if not self.session.is_shutdown:
            self.session.shutdown()
        if self.cluster and not self.cluster.is_shutdown:
            self.cluster.shutdown()
