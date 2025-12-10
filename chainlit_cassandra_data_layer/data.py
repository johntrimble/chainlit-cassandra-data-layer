import asyncio
import json
import uuid
import uuid6
import uuid_utils
import uuid_utils.compat
from datetime import UTC, datetime
from typing import Any, Optional, Sequence, TypedDict, cast

import aiofiles
import msgpack
from cassandra.cluster import EXEC_PROFILE_DEFAULT, PreparedStatement, Session
from cassandra.query import BatchStatement, BatchType
from chainlit.context import context
from chainlit.data.base import BaseDataLayer
from chainlit.data.storage_clients.base import BaseStorageClient
from chainlit.data.utils import queue_until_user_message
from chainlit.element import Element, ElementDict
from chainlit.logger import logger
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

from chainlit_cassandra_data_layer.migration import MigrationManager

# Import for runtime usage (isinstance checks)
try:
    from chainlit.data.storage_clients.gcs import GCSStorageClient
except ImportError:
    GCSStorageClient = None  # type: ignore[assignment,misc]


# Maximum number of threads to retrieve per page
# This prevents excessive memory usage and ensures reasonable query performance
MAX_THREADS_PER_PAGE = 50


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


def _step_id_to_feedback_id(step_id: str) -> str:
    """Convert step_id to feedback_id format: step#<uuid>.

    This creates a deterministic feedback ID from the step ID, allowing us to
    avoid storing feedback_id separately in the database while maintaining
    distinct identifiers for feedback and steps.
    """
    return f"step#{step_id}"


def _feedback_id_to_step_id(feedback_id: str) -> str:
    """Extract step_id from feedback_id format: step#<uuid> -> <uuid>.

    Reverses the _step_id_to_feedback_id conversion. If the feedback_id doesn't
    match the expected format, it's returned as-is for backward compatibility.
    """
    if feedback_id.startswith("step#"):
        return feedback_id[5:]  # Remove "step#" prefix
    # Fallback: treat as plain step_id if format is unexpected
    return feedback_id


class _ThreadActivity(TypedDict):
    user_id: uuid.UUID
    thread_id: uuid.UUID
    activity_at: uuid.UUID
    created_at: Optional[datetime]
    name: Optional[str]


def uuid7(*, time_ms:int|None=None, datetime:datetime|None=None) -> uuid.UUID:
    # Do not allow both time_ms and datetime to be provided
    if time_ms is not None and datetime is not None:
        raise ValueError("Provide only one of time_ms or datetime")
    
    if time_ms is not None:
        return uuid_utils.compat.uuid7(timestamp=time_ms)

    if datetime is not None:
        epoch_millis = int(datetime.timestamp() * 1000)
        return uuid_utils.compat.uuid7(timestamp=epoch_millis)

    return uuid_utils.compat.uuid7()


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


def uuid7_millis(u: uuid.UUID | uuid_utils.UUID | str) -> int:
    u = to_uuid_utils_uuid(u)
    if u.version != 7:
        raise ValueError("UUID is not version 7")
    return u.timestamp


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

class CassandraDataLayer(BaseDataLayer):
    """Cassandra-backed data layer for Chainlit."""

    def __init__(
        self,
        session: Session,
        storage_client: BaseStorageClient | None = None,
        *,
        keyspace: str | None = None,
        default_consistency_level: int | None = None,
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
        """
        self.session: Session = session
        self.cluster = session.cluster

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
        self._table_user_activity_by_thread = self._qualified_table("user_activity_by_thread")
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

    async def _aexecute_prepared(self, query: str, parameters: tuple[Any, ...] = ()):
        """Execute a prepared statement asynchronously with the provided parameters."""
        statement = self._get_prepared_statement(query)
        return await self.session.aexecute(statement, parameters)

    def _batch_add_prepared(
        self, batch: BatchStatement, query: str, parameters: tuple[Any, ...]
    ) -> None:
        """Add a prepared statement with parameters to a batch."""
        statement = self._get_prepared_statement(query)
        batch.add(statement, parameters)

    def _to_utc_datetime(self, dt: datetime) -> datetime:
        """Convert a naive datetime (assumed UTC) to timezone-aware UTC datetime.

        Cassandra returns naive datetime objects, but we need timezone-aware ones
        for proper ISO format serialization.
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt

    def _truncate_to_millis(self, dt: datetime) -> datetime:
        """Truncate datetime to millisecond precision (Cassandra's precision).

        This prevents bugs where microsecond precision datetimes don't match
        the values returned from Cassandra queries.
        """
        return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)

    def _utc_now_millis(self) -> datetime:
        """Return current UTC time truncated to millisecond precision."""
        return self._truncate_to_millis(datetime.now(UTC))

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
        SELECT activity_at FROM {self._table_threads_by_user_activity}
        WHERE user_id = %s AND thread_id = %s
        LIMIT {limit}
        """
        rows = await self._aexecute_prepared(query, (user_id, thread_id))
        return [
            _ThreadActivity(
                user_id=user_id,
                thread_id=thread_id,
                activity_at=row.activity_at,
                name='', # TODO: populate name
            )
            for row in rows
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

        insert_query = f"""
        INSERT INTO {self._table_user_activity_by_thread}
        (user_id, thread_id, activity_at)
        VALUES (%s, %s, %s)
        """

        await self._aexecute_prepared(
            insert_query, (user_id, thread_id, activity_at)
        )

        return activity_at
    
    async def _sync_activity_by_user_with_activity_by_thread(
        self,
        thread_id: uuid.UUID,
        user_id: uuid.UUID,
    ):
        # TODO: Grab lock for thread_id. This prevents the possibility of
        # duplicates being produced by a single chainlit instance.


        # Step 1: Get latest activity from user_activity_by_thread
        activities = await self._get_activities_for_thread(
            user_id,
            thread_id,
            limit=20
        )

        # If there are no activities, nothing to sync
        if not activities:
            return

        # Step 2: Add to batch delete old activities from threads_by_user_activity
        batch = BatchStatement(batch_type=BatchType.LOGGED)
        old_activities = activities[1:]  # Exclude latest activity
        for old_activity in old_activities:
            delete_user_activity_query = f"""
            DELETE FROM {self._table_threads_by_user_activity}
            WHERE user_id = %s AND activity_at = %s AND thread_id = %s
            """
            self._batch_add_prepared(
                batch,
                delete_user_activity_query,
                (
                    user_id,
                    old_activity['activity_at'],
                    thread_id,
                )
            )

        # Step 3: Add to batch insert latest activity into threads_by_user_activity
        latest_activity = activities[0] if activities else None
        insert_query = f"""
        INSERT INTO {self._table_threads_by_user_activity}
        (user_id, activity_at, thread_id, thread_name, thread_created_at)
        """
        self._batch_add_prepared(
            batch,
            insert_query,
            (
                user_id,
                latest_activity['activity_at'],
                thread_id,
                latest_activity['name'],
                latest_activity['created_at'],
            )
        )

        # Step 4: Execute batch for 2 and 3
        #
        # Bundling all of this into a single logged batch reduces the chance of
        # seeing duplicate entries in threads_by_user_activity in case of
        await self._aexecute_prepared(batch)

        # Step 5: Delete old activities from user_activity_by_thread
        #
        # We could include this in the logged batch above, but we are deleting
        # from a different partition in this table, so performance might be
        # worse if we do it that way.
        delete_by_thread_query = f"""
        DELETE FROM {self._table_user_activity_by_thread}
        WHERE user_id = %s AND thread_id = %s AND activity_at = %s
        """
        tasks = []
        for activity in activities:
            tasks.append(
                self._aexecute_prepared(
                    delete_by_thread_query,
                    (
                        activity['user_id'],
                        activity['thread_id'],
                        activity['activity_at'],
                    )
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
            logger.debug(
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

    async def _get_user_identifier_for_id(self, user_id: str) -> str | None:
        """Get user identifier by user ID."""
        query = f"""
            SELECT identifier
            FROM {self._table_users}
            WHERE id = %s
        """
        rows = await self._aexecute_prepared(query, (uuid.UUID(user_id),))
        row = rows[0] if rows else None

        if not row:
            return None

        return row.identifier

    async def _get_user_row(self, identifier: str):
        """Get a user by identifier."""
        query = f"""
            SELECT id, identifier, created_at, metadata
            FROM {self._table_users_by_identifier}
            WHERE identifier = %s
        """
        rows = await self._aexecute_prepared(query, (identifier,))
        row = rows[0] if rows else None

        if not row or row.created_at is None:
            return None

    async def get_user(self, identifier: str) -> PersistedUser | None:
        """Get a user by identifier."""
        row = await self._get_user_row(identifier)

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
            param_values,
        )
        self._batch_add_prepared(
            batch,
            insert_user_by_identifier_query,
            param_values,
        )

        await self.session.aexecute(batch)
        return await self.get_user(user.identifier)

    # Feedback methods

    async def delete_feedback(self, feedback_id: str) -> bool:
        """Delete feedback by ID.

        Since feedback is stored inline with steps and the feedback_id has format
        "step#<step_uuid>", we can extract the step_id and directly locate the step.

        Uses context.session.thread_id to determine which thread the step belongs to,
        following the DynamoDB implementation pattern.
        """
        # Extract step_id from feedback_id format
        step_id_str = _feedback_id_to_step_id(feedback_id)
        step_id = uuid.UUID(step_id_str)

        # Get thread_id from context (DynamoDB pattern)
        # TODO: The session may not actually exist for delete operations
        thread_id = context.session.thread_id if context.session else None
        if not thread_id or not isinstance(thread_id, str):
            logger.warning(
                f"Cannot delete feedback {feedback_id} - thread_id not available in context"
            )
            return False

        thread_id_uuid = uuid.UUID(thread_id)

        # Clear feedback columns (no ALLOW FILTERING needed with full primary key)
        update_query = f"""
            UPDATE {self._table_steps_by_thread}
            SET feedback_value = null, feedback_comment = null
            WHERE thread_id = %s AND id = %s
        """
        await self._aexecute_prepared(
            update_query,
            (thread_id_uuid, step_id),
        )

        return True

    async def upsert_feedback(self, feedback: Feedback) -> str:
        """Create or update feedback for a step.

        Feedback is stored inline with the step in steps_by_thread_id table.
        The feedback ID is derived from the step ID using the format "step#<step_uuid>".

        Uses context.session.thread_id to determine which thread the step belongs to,
        following the DynamoDB implementation pattern.
        """
        step_id = uuid.UUID(feedback.forId)

        # Get thread_id from context (DynamoDB pattern)
        thread_id = context.session.thread_id if context.session else None
        if not thread_id or not isinstance(thread_id, str):
            raise ValueError(
                f"Cannot upsert feedback for step {feedback.forId} - thread_id not available in context"
            )

        thread_id_uuid = uuid.UUID(thread_id)

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
                thread_id_uuid,
                step_id,
            ),
        )

        # Return derived feedback ID
        return _step_id_to_feedback_id(feedback.forId)

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
        thread_id: str | None = element.thread_id
        if not thread_id:
            # Fallback to context.session.thread_id (DynamoDB pattern)
            context_thread_id = context.session.thread_id if context.session else None
            if not context_thread_id or not isinstance(context_thread_id, str):
                logger.warning(
                    f"Data Layer: Cannot create element {element.id} - "
                    f"thread_id not available in element or context"
                )
                return
            thread_id = context_thread_id

        assert isinstance(thread_id, str)

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
                object_key = f"threads/{thread_id}/files/{element.id}"

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
                logger.warning(
                    "Data Layer: No storage client configured. "
                    "File will not be uploaded."
                )

        # Convert UUIDs
        element_id = uuid.UUID(element.id)
        thread_id_uuid = uuid.UUID(thread_id)
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
        rows = await self._aexecute_prepared(
            query, (uuid.UUID(thread_id), uuid.UUID(element_id))
        )
        row = rows[0] if rows else None

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
        element_uuid = uuid.UUID(element_id)

        # Get thread_id from parameter or context (DynamoDB pattern)
        if not thread_id:
            thread_id = context.session.thread_id if context.session else None
            if not thread_id or not isinstance(thread_id, str):
                logger.warning(
                    f"Cannot delete element {element_id} - thread_id not available"
                )
                return

        thread_id_uuid = uuid.UUID(thread_id)

        # Get element details to find object_key before deleting
        query = f"SELECT object_key FROM {self._table_elements_by_thread} WHERE thread_id = %s AND id = %s"
        rows = await self._aexecute_prepared(query, (thread_id_uuid, element_uuid))
        row = rows[0] if rows else None

        # Phase 1: Delete file from storage if it exists (idempotent)
        if row and row.object_key:
            await self._delete_element_storage(row.object_key)

        # Phase 2: Delete from elements_by_thread_id (simple DELETE, no batch needed)
        delete_query = f"DELETE FROM {self._table_elements_by_thread} WHERE thread_id = %s AND id = %s"
        await self._aexecute_prepared(delete_query, (thread_id_uuid, element_uuid))

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
        thread_query = f"SELECT user_id, deleted, name, created_at FROM {self._table_threads} WHERE id = %s"
        thread_rows = await self._aexecute_prepared(thread_query, (thread_id_uuid,))
        thread_row = thread_rows[0] if thread_rows else None

        # Reject adding steps to deleted threads
        if thread_row and thread_row.deleted:
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
            db_params["created_at_uuid"] = created_at_uuid
            
            if step_dict.get("createdAt"):
                # Use provided createdAt
                created_at_raw = step_dict["createdAt"]
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

        queries_and_params = [
            (query, tuple(db_params.values()))
        ]

        if not is_update:
            # Populate the steps table as well for creates. This is so we can
            # map back to the thread from the step ID later if needed.
            steps_table_query = f"""
            INSERT INTO {self._table_all_steps} (id, thread_id, created_at)
            VALUES (%s, %s, %s)
            """
            queries_and_params.append(
                (
                    steps_table_query,
                    (
                        db_params["id"],
                        db_params["thread_id"],
                        db_params["created_at"],
                    ),
                )
            )
        
        tasks = []

        # Execute queries
        if len(queries_and_params) > 1:
            # logged batch here due to steps table and steps_by_thread_id table
            # having different partition keys
            batch = BatchStatement(batch_type=BatchType.LOGGED)
            for q, params in queries_and_params:
                self._batch_add_prepared(batch, q, params)
            tasks.append(
                asyncio.create_task(self.session.aexecute(batch))
            )
        elif len(queries_and_params) == 1:
            q, params = queries_and_params[0]
            tasks.append(
                asyncio.create_task(
                    self._aexecute_prepared(q, params)
                )
            )
        else:
            raise ValueError("No queries to execute for create_step/update_step")

        if not is_update and thread_row and thread_row.user_id:
            tasks.append(
                asyncio.create_task(
                    self._update_activity(
                        thread_id=thread_id_uuid,
                        user_id=thread_row.user_id,
                        activity_at=db_params["created_at"],
                    )
                )
            )

        await asyncio.gather(*tasks)

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
        step_uuid = uuid.UUID(step_id)

        # Get thread_id from context (DynamoDB pattern)
        thread_id = context.session.thread_id if context.session else None
        if not thread_id or not isinstance(thread_id, str):
            logger.warning(
                f"Cannot delete step {step_id} - thread_id not available in context"
            )
            return

        thread_id_uuid = uuid.UUID(thread_id)

        # Phase 1: Delete all elements first (with error handling)
        # Query all elements for this thread, then filter by for_id in Python
        # (for_id is no longer a clustering column, so we can't use it in WHERE clause efficiently)
        elements_query = f"""
            SELECT id, for_id
            FROM {self._table_elements_by_thread}
            WHERE thread_id = %s
        """
        all_element_rows = await self._aexecute_prepared(
            elements_query, (thread_id_uuid,)
        )

        # Filter to elements belonging to this step
        element_rows = [row for row in all_element_rows if row.for_id == step_uuid]

        # Delete all elements in parallel using existing delete_element
        # This properly cleans up storage AND deletes from both tables
        if element_rows:
            element_delete_tasks = [
                self.delete_element(str(row.id), thread_id) for row in element_rows
            ]
            try:
                await asyncio.gather(*element_delete_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error deleting elements for step {step_id}: {e}")
                # Continue with step deletion even if element cleanup fails

        # Phase 2: Delete step (simple DELETE, no batch needed)
        delete_query = f"DELETE FROM {self._table_steps_by_thread} WHERE thread_id = %s AND id = %s"
        await self._aexecute_prepared(delete_query, (thread_id_uuid, step_uuid))

    # Thread methods

    async def get_thread_author(self, thread_id: str) -> str:
        """Get the author (user identifier) of a thread.

        Raises ValueError if thread doesn't exist or has been deleted.
        """
        query = (
            f"SELECT user_identifier, deleted FROM {self._table_threads} WHERE id = %s"
        )
        rows = await self._aexecute_prepared(
            query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        row = rows[0] if rows else None

        if not row or not row.user_identifier or row.deleted:
            raise ValueError(f"Author not found for thread_id {thread_id}")

        return cast(str, row.user_identifier)

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        """Get a thread with all its steps and elements.

        Returns None if thread doesn't exist or has been deleted.
        If thread is marked deleted but still exists, triggers cleanup in background.
        """
        # Get thread metadata
        thread_query = f"SELECT id, user_id, user_identifier, name, created_at, metadata, tags, deleted FROM {self._table_threads} WHERE id = %s"
        thread_rows = await self._aexecute_prepared(
            thread_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        thread_row = thread_rows[0] if thread_rows else None

        if not thread_row:
            return None

        # If thread is marked deleted, trigger cleanup and return None
        if thread_row.deleted:
            # Trigger cleanup asynchronously (don't wait for it)
            # This handles cases where deletion failed partway through
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
        for row in step_rows:
            step_ids.append(row.id)

            # Build feedback dict from inline columns, generating feedback_id from step_id
            feedback = None
            if row.feedback_value is not None:
                feedback = FeedbackDict(
                    id=_step_id_to_feedback_id(str(row.id)),  # Generate feedback_id
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
                createdAt=uuid7_isoformat(row.created_at)
                if row.created_at
                else None,
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
        for row in element_rows:
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

        created_at_value = (
            uuid7_isoformat(thread_row.created_at) 
            if thread_row.created_at else None
        )
        metadata_value = (
            _unpack_metadata(thread_row.metadata) if thread_row.metadata else None
        )

        return ThreadDict(
            id=str(thread_row.id),  # Convert UUID to string
            createdAt=created_at_value,
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
        existing_query = f"""SELECT created_at, metadata, deleted,
                        user_id, user_identifier, name, tags
                 FROM {self._table_threads} WHERE id = %s"""
        existing_rows = await self._aexecute_prepared(
            existing_query, (uuid.UUID(thread_id),)
        )
        existing_row = existing_rows[0] if existing_rows else None
        existing_metadata = (
            _unpack_metadata(existing_row.metadata)
            if existing_row and existing_row.metadata
            else None
        )
        existing_user_id = (
            existing_row.user_id
            if existing_row and existing_row.user_id
            else None
        )
        existing_created_at = (
            existing_row.created_at
            if existing_row and existing_row.created_at
            else None
        )

        # Reject updates to deleted threads
        if existing_row and existing_row.deleted:
            raise ValueError(f"Cannot update deleted thread {thread_id}")
        
        # Do we have a user?
        final_user_id: uuid.UUID | None = uuid.UUID(user_id) if user_id else None
        if final_user_id is None:
            final_user_id = (
                existing_row.user_id
                if existing_row and existing_row.user_id
                else None
            )
        
        # Build db_params dict with only non-None values
        db_params: dict[str, Any] = {
            "id": uuid.UUID(thread_id)
        }

        # Only add the created_at if creating a new thread
        #
        # NOTE: Possible race here if two concurrent updates to a not-yet-created
        # thread which could lead to a mutation of the created_at value.
        if not existing_created_at:
            db_params["created_at"] = uuid7()

        # Only add user_id, if provided, if one is not already set
        if not existing_user_id and user_id:
            user_identifier = await self._get_user_identifier(user_id)
            if not user_identifier:
                raise ValueError(f"User with id {user_id} does not exist")
            db_params["user_id"] = uuid.UUID(user_id)
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

        queries_and_params = [
            (insert_thread_query, tuple(insert_thread_values))
        ]

        # Properties that map from the threads table to the user_activity_by_thread table
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
        should_update_activity_view = (
            len(activity_view_params) > 0
            and user_is_known
        )
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
            queries_and_params.append(
                (update_activity_query, tuple(activity_values))
            )
        
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
        if should_update_activity_view:
            await self._update_activity(
                thread_id=uuid.UUID(thread_id),
                user_id=final_user_id,
                activity_at=uuid7(),
            )


    async def delete_thread(self, thread_id: str):
        """Delete a thread.

        Performs a soft delete in the threads table by setting the `deleted`
        column to true, then cleans up all related data. This ensure that any
        in-flight operations do no resurrect the thread.

        Subsequent calls to delete_thread for the same thread_id will continue
        the cleanup process, allowing for retry on partial failures.
        """
        thread_id_uuid = uuid.UUID(thread_id)

        # Get thread info for activity cleanup
        thread_query = f"SELECT user_id, deleted FROM {self._table_threads} WHERE id = %s"
        thread_rows = await self._aexecute_prepared(thread_query, (thread_id_uuid,))
        thread_row = thread_rows[0] if thread_rows else None

        if not thread_row:
            # We only do a soft-delete in the threads table, so if the thread
            # row doesn't exist, then the thread never did.
            return
        
        # Step 1: Mark as deleted (skip if already deleted - allow retry)
        if not thread_row.deleted:
            soft_delete_query = (
                f"UPDATE {self._table_threads} SET deleted = true WHERE id = %s"
            )
            await self._aexecute_prepared(soft_delete_query, (thread_id_uuid,))

        # At this point, the thread is marked deleted which should stop any
        # further use of it. We can run the remaining cleanup steps in parallel.

        tasks = []

        # Step 2: Delete all element storage
        async def delete_all_elements():
            try:
                # Query all elements for this thread to get their object_keys
                elements_query = f"SELECT object_key FROM {self._table_elements_by_thread} WHERE thread_id = %s"
                element_rows = list(
                    await self._aexecute_prepared(elements_query, (thread_id_uuid,))
                )

                if element_rows:
                    # Delete all element files from storage in parallel
                    storage_delete_tasks = [
                        self._delete_element_storage(row.object_key)
                        for row in element_rows
                        if row.object_key
                    ]
                    await asyncio.gather(*storage_delete_tasks)

                    # Once storage is cleaned up, we can delete all the elements
                    #
                    # NOTE: We want to do this _after_ storage deletion so that
                    # we don't leave orphaned files if storage deletion fails.
                    #
                    # It is still possible to have orphaned files if a new
                    # element is added after the thread has been marked for
                    # deletion, but there is no perfect solution to that. We
                    # _could_ delete the elements one at a time to deal with
                    # that, but that will generate a lot of tombstones whereas
                    # deleting the entire partition at once will not.
                    #
                    delete_elements_query = f"DELETE FROM {self._table_elements_by_thread} WHERE thread_id = %s"
                    await self._aexecute_prepared(
                        delete_elements_query, (thread_id_uuid,)
                    )

            except Exception:
                # Don't propagate errors from element deletion/storage cleanup
                # to allow thread deletion to continue
                logger.exception(
                    f"Error while removing elements for thread: {thread_id}"
                )
        tasks.append(asyncio.create_task(delete_all_elements()))

        # Step 3: Delete all steps and feedback
        async def delete_all_steps():
            pass



        # Get latest activity timestamp from step_activity_by_thread
        # This is used to delete the corresponding entry in threads_by_user_activity
        activity_query = f"""
            SELECT created_at FROM {self._table_step_activity}
            WHERE thread_id = %s
            LIMIT 1
        """
        activity_rows = await self._aexecute_prepared(activity_query, (thread_id_uuid,))
        activity_timestamp = (

            self._to_utc_datetime(activity_rows[0].created_at)
            if activity_rows
            else None
        )


        # Phase 2: Delete all element storage in parallel (scatter-gather)

        
            

        # Phase 3: Final deletion - delete entire partitions + thread rows in batch
        # Delete entire partitions (all steps, elements, and feedback in one operation)
        # Plus thread metadata from both tables
        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Delete entire partition from steps_by_thread_id
        self._batch_add_prepared(
            batch,
            f"DELETE FROM {self._table_steps_by_thread} WHERE thread_id = %s",
            (thread_id_uuid,),
        )

        # Delete entire partition from elements_by_thread_id
        self._batch_add_prepared(
            batch,
            f"DELETE FROM {self._table_elements_by_thread} WHERE thread_id = %s",
            (thread_id_uuid,),
        )

        # Delete from threads table
        self._batch_add_prepared(
            batch,
            f"DELETE FROM {self._table_threads} WHERE id = %s",
            (thread_id_uuid,),
        )

        # Delete from threads_by_user_activity if it exists
        if thread_row.user_id and activity_timestamp:
            self._batch_add_prepared(
                batch,
                f"DELETE FROM {self._table_threads_by_user_activity} WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s",
                (thread_row.user_id, activity_timestamp, thread_id_uuid),
            )

        await self.session.aexecute(batch)

    async def list_threads(
        self, pagination: Pagination, filters: ThreadFilter
    ) -> PaginatedResponse[ThreadDict]:
        """List threads for a user with efficient cursor-based pagination.

        Returns minimal ThreadDict objects without steps/elements (following DynamoDB approach).
        Supports search filtering on thread names. Feedback filtering is not supported.

        Uses composite cursor (timestamp,thread_id) for efficient range queries
        on threads_by_user_activity clustering keys. Deduplicates and actively
        cleans up any duplicate entries found.

        Cursor format: "2025-10-27T10:00:00.123456+00:00,thread-uuid"
        """
        if not filters.userId:
            raise ValueError("userId is required")

        # Log warning for unsupported feedback filter (like DynamoDB)
        if filters.feedback is not None:
            logger.warning(
                "Cassandra: filters on feedback not supported. "
                "Feedback filtering requires full thread data with steps."
            )

        user_id = uuid.UUID(filters.userId)

        # Enforce pagination limit: clamp to MAX_THREADS_PER_PAGE
        # If no limit provided (first=0 or None), use max
        requested_count = pagination.first if pagination.first else MAX_THREADS_PER_PAGE
        limit = min(requested_count, MAX_THREADS_PER_PAGE)

        # Adaptive fetch size: double when search filter is active (most results will be filtered out)
        # Clamp to MAX_THREADS_PER_PAGE to prevent excessive fetching
        if filters.search:
            fetch_limit = min(limit * 2, MAX_THREADS_PER_PAGE)
        else:
            fetch_limit = limit

        # Iteratively fetch until we have enough matching results or exhaust data
        # This handles cases where filtering removes most results
        # To correctly determine hasNextPage, we fetch one extra batch after getting
        # enough results to check if more matches exist
        matching_rows: list[Any] = []
        current_cursor_timestamp: datetime | None = None
        current_cursor_thread_id: uuid.UUID | None = None
        all_seen_threads: dict[str, Any] = {}
        all_duplicates_to_delete: list[Any] = []
        reached_end = False
        found_extra_match = (
            False  # Tracks if we found matches beyond the requested limit
        )

        # Parse initial cursor if provided
        if pagination.cursor:
            try:
                cursor_parts = pagination.cursor.split(",", 1)
                current_cursor_timestamp = datetime.fromisoformat(cursor_parts[0])
                if current_cursor_timestamp.tzinfo is not None:
                    current_cursor_timestamp = current_cursor_timestamp.replace(
                        tzinfo=None
                    )
                current_cursor_thread_id = uuid.UUID(cursor_parts[1])
            except (ValueError, IndexError) as e:
                logger.warning(
                    f"Invalid cursor format: {pagination.cursor}, error: {e}"
                )
                current_cursor_timestamp = None
                current_cursor_thread_id = None

        # Keep fetching until we have enough results or run out of data
        # After getting enough results, fetch one more batch to determine hasNextPage
        while not reached_end:
            # Stop if we have enough results AND we've checked for more matches
            if len(matching_rows) >= limit and found_extra_match:
                break
            # Also stop if we already have more than limit (found extra matches)
            if len(matching_rows) > limit:
                found_extra_match = True
                break
            # Build query based on whether we have a cursor
            if current_cursor_timestamp and current_cursor_thread_id:
                query = f"""
                    SELECT thread_id, thread_name, thread_created_at, last_activity_at
                    FROM {self._table_threads_by_user_activity}
                    WHERE user_id = %s
                    AND (last_activity_at, thread_id) < (%s, %s)
                    LIMIT %s
                """
                rows = await self._aexecute_prepared(
                    query,
                    (
                        user_id,
                        current_cursor_timestamp,
                        current_cursor_thread_id,
                        fetch_limit + 3,
                    ),
                )
            else:
                query = f"""
                    SELECT thread_id, thread_name, thread_created_at, last_activity_at
                    FROM {self._table_threads_by_user_activity}
                    WHERE user_id = %s
                    LIMIT %s
                """
                rows = await self._aexecute_prepared(query, (user_id, fetch_limit + 3))

            # If we got no rows, we've reached the end
            if not rows:
                reached_end = True
                break

            # Deduplicate within this batch
            seen_threads = {}
            duplicates_to_delete = []

            for row in rows:
                thread_id_str = str(row.thread_id)

                # Check against all previously seen threads across all fetches
                if thread_id_str not in all_seen_threads:
                    if thread_id_str not in seen_threads:
                        seen_threads[thread_id_str] = row
                        all_seen_threads[thread_id_str] = row
                    else:
                        existing = seen_threads[thread_id_str]
                        if row.last_activity_at > existing.last_activity_at:
                            duplicates_to_delete.append(existing)
                            seen_threads[thread_id_str] = row
                            all_seen_threads[thread_id_str] = row
                        else:
                            duplicates_to_delete.append(row)

            all_duplicates_to_delete.extend(duplicates_to_delete)

            # Get unique rows from this batch
            unique_rows = list(seen_threads.values())

            # Apply search filter if provided
            if filters.search:
                search_lower = filters.search.lower()
                batch_matching = [
                    row
                    for row in unique_rows
                    if search_lower in (row.thread_name or "").lower()
                ]
            else:
                batch_matching = unique_rows

            # Add to our accumulated results
            matching_rows.extend(batch_matching)

            # If we now have more than limit matches, we know there's a next page
            if len(matching_rows) > limit:
                found_extra_match = True

            # Update cursor to last row for next iteration
            if rows:
                last_row = rows[-1]
                current_cursor_timestamp = last_row.last_activity_at
                current_cursor_thread_id = last_row.thread_id

            # If we got fewer rows than requested, we've reached the end
            if len(rows) < fetch_limit:
                reached_end = True
                # Mark that we've checked for extra matches (even if we didn't find any)
                if len(matching_rows) >= limit:
                    found_extra_match = True

        # Clean up all duplicates found across all fetches in background
        if all_duplicates_to_delete:
            logger.info(
                f"Cleaning up {len(all_duplicates_to_delete)} duplicate entries "
                f"for user {filters.userId}"
            )
            for dup in all_duplicates_to_delete:
                try:
                    delete_query = f"""
                        DELETE FROM {self._table_threads_by_user_activity}
                        WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s
                    """
                    await self._aexecute_prepared(
                        delete_query, (user_id, dup.last_activity_at, dup.thread_id)
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to delete duplicate entry for thread {dup.thread_id}: {e}"
                    )

        # Take only the requested limit number after filtering
        paginated_rows = matching_rows[:limit]

        # Build minimal ThreadDict objects directly from query results
        # Following DynamoDB approach: return only metadata without steps/elements
        threads: list[ThreadDict] = []
        for row in paginated_rows:
            thread_dict = ThreadDict(
                id=str(row.thread_id),
                createdAt=self._to_utc_datetime(row.thread_created_at).isoformat(),
                name=row.thread_name,
                userId=str(user_id),  # Available from query context (partition key)
                userIdentifier=None,
                tags=None,
                metadata=None,
                steps=[],  # Empty array - UI will call get_thread when needed
                elements=[],  # Empty array
                # Optional fields default to None when not available from index query
            )
            threads.append(thread_dict)

        # Build composite cursor for last row: "timestamp,thread_id"
        end_cursor = None
        start_cursor = None
        if paginated_rows:
            # For start cursor, use the first row we're returning
            first_row = paginated_rows[0]
            start_cursor = f"{self._to_utc_datetime(first_row.last_activity_at).isoformat()},{first_row.thread_id}"

            # For end cursor, we want to position right before the first match on the next page
            # This ensures the next page doesn't rescan rows we've already processed
            if len(matching_rows) > limit:
                # There is a next page - use the first match of next page
                next_page_first_match = matching_rows[
                    limit
                ]  # The first match for next page

                # Add 1ms to its timestamp to position cursor right before it
                # Query uses (last_activity_at, thread_id) < (cursor) in DESC order
                # So cursor with timestamp + 1ms will position us right before this match
                from datetime import timedelta

                cursor_timestamp = next_page_first_match.last_activity_at + timedelta(
                    milliseconds=1
                )
                end_cursor = f"{self._to_utc_datetime(cursor_timestamp).isoformat()},{next_page_first_match.thread_id}"
            else:
                # No next page - use the last row we're returning
                last_row = paginated_rows[-1]
                end_cursor = f"{self._to_utc_datetime(last_row.last_activity_at).isoformat()},{last_row.thread_id}"

        # Determine if there are more pages
        # hasNextPage is true only if we found more matching results than requested
        # This is determined by finding matches beyond the limit during our fetch loop
        has_next_page = len(matching_rows) > limit

        return PaginatedResponse(
            pageInfo=PageInfo(
                hasNextPage=has_next_page,
                startCursor=start_cursor,
                endCursor=end_cursor,
            ),
            data=threads,
        )

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
