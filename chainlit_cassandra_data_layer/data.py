import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any, cast

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

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format (UTC)."""
        return datetime.now(UTC).isoformat()

    def _to_utc_datetime(self, dt: datetime) -> datetime:
        """Convert a naive datetime (assumed UTC) to timezone-aware UTC datetime.

        Cassandra returns naive datetime objects, but we need timezone-aware ones
        for proper ISO format serialization.
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt

    def _parse_iso_datetime(self, iso_str: str) -> datetime:
        """Parse ISO formatted datetime string into timezone-aware UTC datetime."""
        normalized = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    async def _update_thread_activity(
        self,
        thread_id: str,
        user_id: str | None,
        activity_timestamp: datetime,
        thread_name: str | None = None,
        thread_created_at: datetime | None = None,
    ):
        """Update thread activity in threads_by_user_activity table.

        Uses delete+insert pattern to handle clustering key changes.
        Accepts potential race conditions (duplicates handled at read time).

        Args:
            thread_id: Thread UUID
            user_id: User UUID (required - threads without users are not tracked)
            activity_timestamp: New activity timestamp
            thread_name: Thread name (optional, will fetch if not provided)
            thread_created_at: Thread creation time (optional, will fetch if not provided)
        """
        if not user_id:
            # Threads without users cannot be listed, so don't track activity
            logger.debug(
                f"Skipping activity update for thread {thread_id} - no user_id"
            )
            return

        thread_id_uuid = uuid.UUID(thread_id)
        user_id_uuid = uuid.UUID(user_id)

        # Fetch thread details if not provided
        if thread_name is None or thread_created_at is None:
            thread_query = f"""
                SELECT name, created_at, last_activity_at, deleted
                FROM {self._table_threads}
                WHERE id = %s
            """
            thread_rows = await self._aexecute_prepared(thread_query, (thread_id_uuid,))
            thread_row = thread_rows[0] if thread_rows else None
            if not thread_row:
                logger.warning(
                    f"Cannot update activity for non-existent thread {thread_id}"
                )
                return

            # Skip updates for deleted threads
            if thread_row.deleted:
                logger.debug(f"Skipping activity update for deleted thread {thread_id}")
                return

            if thread_name is None:
                thread_name = thread_row.name
            if thread_created_at is None:
                thread_created_at = thread_row.created_at
            old_activity_at = thread_row.last_activity_at
        else:
            # If provided, still need to fetch old activity timestamp
            old_activity_query = (
                f"SELECT last_activity_at FROM {self._table_threads} WHERE id = %s"
            )
            old_activity_rows = await self._aexecute_prepared(
                old_activity_query, (thread_id_uuid,)
            )
            old_activity_row = old_activity_rows[0] if old_activity_rows else None
            old_activity_at = (
                old_activity_row.last_activity_at if old_activity_row else None
            )

        # Use batch for atomicity (delete old + insert new + update threads)
        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Delete old activity entry (if exists)
        if old_activity_at:
            self._batch_add_prepared(
                batch,
                f"""DELETE FROM {self._table_threads_by_user_activity}
                   WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s""",
                (user_id_uuid, old_activity_at, thread_id_uuid),
            )

        # Insert new activity entry
        self._batch_add_prepared(
            batch,
            f"""INSERT INTO {self._table_threads_by_user_activity}
               (user_id, last_activity_at, thread_id, thread_name, thread_created_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                user_id_uuid,
                activity_timestamp,
                thread_id_uuid,
                thread_name,
                thread_created_at,
            ),
        )

        # Update threads table with new last_activity_at
        self._batch_add_prepared(
            batch,
            f"UPDATE {self._table_threads} SET last_activity_at = %s WHERE id = %s",
            (activity_timestamp, thread_id_uuid),
        )

        await self.session.aexecute(batch)

    # User methods

    async def get_user(self, identifier: str) -> PersistedUser | None:
        """Get a user by identifier."""
        query = f"""
            SELECT id, identifier, created_at, metadata
            FROM {self._table_users_by_identifier}
            WHERE identifier = %s
        """
        rows = await self._aexecute_prepared(query, (identifier,))
        row = rows[0] if rows else None

        if not row:
            return None

        metadata = _unpack_metadata(row.metadata)
        return PersistedUser(
            id=str(row.id),  # Convert UUID to string for Chainlit
            identifier=row.identifier,
            createdAt=self._to_utc_datetime(row.created_at).isoformat()
            if row.created_at
            else self._get_current_timestamp(),
            metadata=metadata,
        )

    async def create_user(self, user: User) -> PersistedUser | None:
        """Create or update a user."""
        existing_user = await self.get_user(user.identifier)

        metadata_blob = _pack_metadata(user.metadata)

        if existing_user:
            user_id = uuid.UUID(existing_user.id)
            created_at = self._parse_iso_datetime(existing_user.createdAt)
        else:
            user_id = uuid.uuid4()
            created_at = datetime.now(UTC)

        batch = BatchStatement(batch_type=BatchType.LOGGED)
        self._batch_add_prepared(
            batch,
            f"""
            INSERT INTO {self._table_users} (id, identifier, created_at, metadata)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, user.identifier, created_at, metadata_blob),
        )
        self._batch_add_prepared(
            batch,
            f"""
            INSERT INTO {self._table_users_by_identifier} (identifier, id, created_at, metadata)
            VALUES (%s, %s, %s, %s)
            """,
            (user.identifier, user_id, created_at, metadata_blob),
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

        # Update thread timestamp
        await self.update_thread(thread_id)

        # Update thread activity - get user_id from threads table
        # Also check if thread is deleted
        thread_query = (
            f"SELECT user_id, deleted FROM {self._table_threads} WHERE id = %s"
        )
        thread_rows = await self._aexecute_prepared(
            thread_query, (uuid.UUID(thread_id),)
        )
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
            # For create: createdAt is always included (required)
            # Default to current time if not provided
            if "createdAt" in step_dict and step_dict["createdAt"]:
                created_at_raw = step_dict["createdAt"]
                db_params["created_at"] = (
                    self._parse_iso_datetime(created_at_raw)
                    if isinstance(created_at_raw, str)
                    else created_at_raw
                )
            else:
                # Default to current time for new steps
                db_params["created_at"] = datetime.now(UTC)
        # else: For updates, never include created_at (preserve existing value)

        # Other timestamp fields
        if "start" in step_dict and step_dict["start"]:
            start_raw = step_dict["start"]
            db_params["start"] = (
                self._parse_iso_datetime(start_raw)
                if isinstance(start_raw, str)
                else start_raw
            )

        if "end" in step_dict and step_dict["end"]:
            end_raw = step_dict["end"]
            db_params["end"] = (
                self._parse_iso_datetime(end_raw)
                if isinstance(end_raw, str)
                else end_raw
            )

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

        await self._aexecute_prepared(query, tuple(db_params.values()))

        # Update thread activity if we have a created_at timestamp
        if "created_at" in db_params and thread_row and thread_row.user_id:
            await self._update_thread_activity(
                thread_id=thread_id,
                user_id=str(thread_row.user_id),
                activity_timestamp=db_params["created_at"],
            )

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
                createdAt=self._to_utc_datetime(row.created_at).isoformat()
                if row.created_at
                else None,
                start=self._to_utc_datetime(row.start).isoformat()
                if row.start
                else None,
                end=self._to_utc_datetime(row.end).isoformat() if row.end else None,
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
            self._to_utc_datetime(thread_row.created_at).isoformat()
            if thread_row.created_at
            else self._get_current_timestamp()
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
        """Create or update a thread.

        Raises ValueError if attempting to update a deleted thread.
        """
        # Check if thread exists and is not deleted
        existing_query = f"""SELECT created_at, last_activity_at, metadata, deleted,
                        user_id, user_identifier, name, tags
                 FROM {self._table_threads} WHERE id = %s"""
        existing_rows = await self._aexecute_prepared(
            existing_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        existing_row = existing_rows[0] if existing_rows else None

        # Reject updates to deleted threads
        if existing_row and existing_row.deleted:
            raise ValueError(f"Cannot update deleted thread {thread_id}")

        now_utc = datetime.now(UTC)
        existing_created_at = (
            self._to_utc_datetime(existing_row.created_at)
            if existing_row and existing_row.created_at
            else None
        )
        created_at = existing_created_at if existing_created_at else now_utc

        # Merge metadata if updating
        existing_metadata: dict[str, Any] | None = (
            _unpack_metadata(existing_row.metadata)
            if existing_row and existing_row.metadata
            else None
        )
        metadata_provided = metadata is not None
        metadata_dict: dict[str, Any] | None
        if metadata is not None:
            base_dict = (
                existing_metadata.copy() if existing_metadata is not None else {}
            )
            incoming = {k: v for k, v in metadata.items() if v is not None}
            metadata_dict = {**base_dict, **incoming}
        else:
            metadata_dict = (
                existing_metadata.copy() if existing_metadata is not None else None
            )

        # Get user_identifier if user_id is provided
        user_identifier = None
        if user_id:
            user_query = f"SELECT identifier FROM {self._table_users} WHERE id = %s"
            user_rows = await self._aexecute_prepared(
                user_query, (uuid.UUID(user_id),)
            )  # Convert to UUID
            user_row = user_rows[0] if user_rows else None
            if user_row:
                user_identifier = user_row.identifier
        elif existing_row:
            user_identifier = existing_row.user_identifier

        # Determine final values
        if user_id:
            final_user_id = uuid.UUID(user_id)
        elif existing_row and existing_row.user_id:
            final_user_id = existing_row.user_id
        else:
            final_user_id = None

        if name is not None:
            final_name = name
        elif (
            metadata_provided and metadata_dict is not None and "name" in metadata_dict
        ):
            final_name = metadata_dict["name"]
        elif existing_row:
            final_name = existing_row.name
        else:
            final_name = None

        if final_name is not None and not isinstance(final_name, str):
            final_name = str(final_name)
        final_tags = (
            tags
            if tags is not None
            else (
                list(existing_row.tags) if existing_row and existing_row.tags else None
            )
        )

        # Build the INSERT statement
        query = f"""
            INSERT INTO {self._table_threads} (id, user_id, user_identifier, name, created_at, metadata, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        await self._aexecute_prepared(
            query,
            (
                uuid.UUID(thread_id),  # Convert to UUID
                final_user_id,
                user_identifier
                if user_identifier
                else (existing_row and getattr(existing_row, "user_identifier", None)),
                final_name,
                created_at,
                _pack_metadata(metadata_dict) if metadata_dict else None,
                final_tags,
            ),
        )

        # Update activity tracking if thread has a user
        if final_user_id:
            existing_last_activity = (
                self._to_utc_datetime(existing_row.last_activity_at)
                if existing_row and existing_row.last_activity_at
                else None
            )
            activity_timestamp = existing_last_activity or created_at
            thread_created_at = (
                created_at if not existing_created_at else existing_created_at
            )

            await self._update_thread_activity(
                thread_id=thread_id,
                user_id=str(final_user_id),
                activity_timestamp=activity_timestamp,
                thread_name=final_name,
                thread_created_at=thread_created_at,
            )

    async def delete_thread(self, thread_id: str):
        """Delete a thread using efficient partition-level deletion.

        Phase 1: Mark thread as deleted (soft-delete) - users immediately stop seeing it
        Phase 2: Delete all elements (scatter-gather with storage cleanup)
        Phase 3: Delete entire steps partition + thread rows (batch)

        If thread is already deleted, continues with cleanup (handles retry on partial failures).
        """
        thread_id_uuid = uuid.UUID(thread_id)

        # Get thread info for activity cleanup
        thread_query = f"SELECT user_id, last_activity_at, deleted FROM {self._table_threads} WHERE id = %s"
        thread_rows = await self._aexecute_prepared(thread_query, (thread_id_uuid,))
        thread_row = thread_rows[0] if thread_rows else None

        if not thread_row:
            return  # Thread doesn't exist

        # Phase 1: Mark as deleted (skip if already deleted - allow retry)
        if not thread_row.deleted:
            soft_delete_query = (
                f"UPDATE {self._table_threads} SET deleted = true WHERE id = %s"
            )
            await self._aexecute_prepared(soft_delete_query, (thread_id_uuid,))

        # Phase 2: Delete all element storage in parallel (scatter-gather)
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
            if storage_delete_tasks:
                try:
                    await asyncio.gather(*storage_delete_tasks, return_exceptions=True)
                except Exception as e:
                    logger.error(
                        f"Error during storage cleanup for thread {thread_id}: {e}"
                    )
                    # Continue with thread deletion even if storage cleanup partially fails

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
        if thread_row.user_id and thread_row.last_activity_at:
            self._batch_add_prepared(
                batch,
                f"DELETE FROM {self._table_threads_by_user_activity} WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s",
                (thread_row.user_id, thread_row.last_activity_at, thread_id_uuid),
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
