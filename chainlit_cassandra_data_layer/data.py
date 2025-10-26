import json
import uuid
from datetime import datetime, timezone
from typing import Any

import aiofiles
import msgpack
from cassandra.cluster import Cluster, Session
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

# Import for runtime usage (isinstance checks)
try:
    from chainlit.data.storage_clients.gcs import GCSStorageClient
except ImportError:
    GCSStorageClient = None  # type: ignore[assignment,misc]


def _pack_metadata(metadata: dict[str, Any] | None) -> bytes | None:
    """Serialize metadata dict to MessagePack bytes."""
    if metadata is None or not metadata:
        return None
    return msgpack.packb(metadata, use_bin_type=True)


def _unpack_metadata(data: bytes | None) -> dict[str, Any]:
    """Deserialize MessagePack bytes to metadata dict."""
    if data is None or data == b"":
        return {}
    return msgpack.unpackb(data, raw=False)


class CassandraDataLayer(BaseDataLayer):
    """Cassandra-backed data layer for Chainlit."""

    def __init__(
        self,
        contact_points: list[str] = None,
        keyspace: str = "chainlit",
        user_thread_limit: int = 1000,
        storage_client: BaseStorageClient | None = None,
    ):
        """Initialize the Cassandra data layer.

        Args:
            contact_points: List of Cassandra contact points (default: ['cassandra'])
            keyspace: Cassandra keyspace to use (default: 'chainlit')
            user_thread_limit: Maximum number of threads to fetch per user (default: 1000)
            storage_client: Optional storage client for file uploads (S3, GCS, Azure, etc.)
        """
        if contact_points is None:
            contact_points = ["cassandra"]

        self.cluster = Cluster(contact_points)
        self.session: Session = self.cluster.connect(keyspace)
        self.keyspace = keyspace
        self.user_thread_limit = user_thread_limit
        self.storage_client = storage_client

    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format (UTC)."""
        return datetime.now(timezone.utc).isoformat()

    def _to_utc_datetime(self, dt: datetime) -> datetime:
        """Convert a naive datetime (assumed UTC) to timezone-aware UTC datetime.

        Cassandra returns naive datetime objects, but we need timezone-aware ones
        for proper ISO format serialization.
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _update_thread_activity(
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
            thread_query = """
                SELECT name, created_at, last_activity_at
                FROM threads
                WHERE id = %s
            """
            thread_row = self.session.execute(thread_query, (thread_id_uuid,)).one()
            if not thread_row:
                logger.warning(
                    f"Cannot update activity for non-existent thread {thread_id}"
                )
                return

            if thread_name is None:
                thread_name = thread_row.name
            if thread_created_at is None:
                thread_created_at = thread_row.created_at
            old_activity_at = thread_row.last_activity_at
        else:
            # If provided, still need to fetch old activity timestamp
            old_activity_row = self.session.execute(
                "SELECT last_activity_at FROM threads WHERE id = %s", (thread_id_uuid,)
            ).one()
            old_activity_at = (
                old_activity_row.last_activity_at if old_activity_row else None
            )

        # Use batch for atomicity (delete old + insert new + update threads)
        from cassandra.query import BatchStatement, BatchType

        batch = BatchStatement(batch_type=BatchType.LOGGED)

        # Delete old activity entry (if exists)
        if old_activity_at:
            batch.add(
                """DELETE FROM threads_by_user_activity
                   WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s""",
                (user_id_uuid, old_activity_at, thread_id_uuid),
            )

        # Insert new activity entry
        batch.add(
            """INSERT INTO threads_by_user_activity
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
        batch.add(
            "UPDATE threads SET last_activity_at = %s WHERE id = %s",
            (activity_timestamp, thread_id_uuid),
        )

        self.session.execute(batch)

    # User methods

    async def get_user(self, identifier: str) -> PersistedUser | None:
        """Get a user by identifier."""
        query = "SELECT id, identifier, created_at, metadata FROM users WHERE identifier = %s ALLOW FILTERING"
        rows = self.session.execute(query, (identifier,))
        row = rows.one()

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

        if existing_user:
            # Update metadata - need to use INSERT to upsert in Cassandra
            metadata_blob = _pack_metadata(user.metadata)
            query = """
                INSERT INTO users (id, identifier, created_at, metadata)
                VALUES (%s, %s, %s, %s)
            """
            self.session.execute(
                query,
                (
                    uuid.UUID(existing_user.id),  # Convert string ID to UUID
                    user.identifier,
                    datetime.fromisoformat(existing_user.createdAt.replace("Z", "")),
                    metadata_blob,
                ),
            )
            return await self.get_user(user.identifier)
        else:
            # Create new user
            user_id = uuid.uuid4()  # Generate UUID directly
            created_at = datetime.now()
            metadata_blob = _pack_metadata(user.metadata)

            query = """
                INSERT INTO users (id, identifier, created_at, metadata)
                VALUES (%s, %s, %s, %s)
            """
            self.session.execute(
                query, (user_id, user.identifier, created_at, metadata_blob)
            )
            return await self.get_user(user.identifier)

    # Feedback methods

    async def delete_feedback(self, feedback_id: str) -> bool:
        """Delete feedback by ID."""
        query = "DELETE FROM feedbacks WHERE id = %s"
        self.session.execute(query, (uuid.UUID(feedback_id),))  # Convert to UUID
        return True

    async def upsert_feedback(self, feedback: Feedback) -> str:
        """Create or update feedback."""
        # Generate or convert feedback ID
        if feedback.id:
            feedback_id = uuid.UUID(feedback.id)
        else:
            feedback_id = uuid.uuid4()

        query = """
            INSERT INTO feedbacks (id, for_id, thread_id, value, comment)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.session.execute(
            query,
            (
                feedback_id,
                uuid.UUID(feedback.forId),  # Convert to UUID
                uuid.UUID(feedback.threadId)
                if feedback.threadId
                else None,  # Convert to UUID
                feedback.value,
                feedback.comment,
            ),
        )
        return str(feedback_id)  # Return as string for Chainlit

    # Element methods

    @queue_until_user_message()
    async def create_element(self, element: Element):
        """Create an element."""
        if not element.for_id:
            return

        # Determine thread_id - look it up from the step if not provided
        thread_id = element.thread_id
        if not thread_id and element.for_id:
            lookup_query = "SELECT thread_id FROM steps WHERE id = %s"
            rows = self.session.execute(lookup_query, (uuid.UUID(element.for_id),))
            row = rows.one()
            if row:
                thread_id = str(row.thread_id)

        if not thread_id:
            logger.warning(
                f"Data Layer: Cannot create element {element.id} - "
                f"thread_id could not be determined"
            )
            return

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

        # Write to lookup table
        lookup_query = (
            "INSERT INTO elements (id, thread_id, for_id) VALUES (%s, %s, %s)"
        )
        self.session.execute(lookup_query, (element_id, thread_id_uuid, for_id))

        # Write to main elements_by_thread_id table
        main_query = """
            INSERT INTO elements_by_thread_id (
                thread_id, id, for_id, mime, name, object_key, url,
                chainlit_key, display, size, language, page, props
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.session.execute(
            main_query,
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
            ),
        )

    async def get_element(self, thread_id: str, element_id: str) -> ElementDict | None:
        """Get an element by ID and thread ID."""
        query = """
            SELECT thread_id, id, for_id, mime, name, object_key, url,
                   chainlit_key, display, size, language, page, props
            FROM elements_by_thread_id
            WHERE thread_id = %s AND id = %s
        """
        rows = self.session.execute(
            query, (uuid.UUID(thread_id), uuid.UUID(element_id))
        )
        row = rows.one()

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
        """Delete an element by ID."""
        # Get thread_id from lookup table if not provided
        if not thread_id:
            lookup_query = "SELECT thread_id FROM elements WHERE id = %s"
            lookup_rows = self.session.execute(lookup_query, (uuid.UUID(element_id),))
            lookup_row = lookup_rows.one()
            if not lookup_row:
                return  # Element doesn't exist
            thread_id = str(lookup_row.thread_id)

        # Get element details to find object_key before deleting
        query = "SELECT object_key FROM elements_by_thread_id WHERE thread_id = %s AND id = %s"
        rows = self.session.execute(
            query, (uuid.UUID(thread_id), uuid.UUID(element_id))
        )
        row = rows.one()

        # Delete file from storage if it exists
        if self.storage_client is not None and row and row.object_key:
            await self.storage_client.delete_file(object_key=row.object_key)

        # Delete from lookup table
        lookup_delete_query = "DELETE FROM elements WHERE id = %s"
        self.session.execute(lookup_delete_query, (uuid.UUID(element_id),))

        # Delete from main table
        main_delete_query = (
            "DELETE FROM elements_by_thread_id WHERE thread_id = %s AND id = %s"
        )
        self.session.execute(
            main_delete_query, (uuid.UUID(thread_id), uuid.UUID(element_id))
        )

    # Step methods

    @queue_until_user_message()
    async def create_step(self, step_dict: StepDict):
        """Create a step."""
        thread_id = step_dict["threadId"]

        # Update thread timestamp
        await self.update_thread(thread_id)

        created_at = (
            datetime.fromisoformat(step_dict["createdAt"].replace("Z", ""))
            if step_dict.get("createdAt")
            else datetime.now()
        )

        # Update thread activity - get user_id from threads table
        thread_query = "SELECT user_id FROM threads WHERE id = %s"
        thread_row = self.session.execute(thread_query, (uuid.UUID(thread_id),)).one()
        if thread_row and thread_row.user_id:
            self._update_thread_activity(
                thread_id=thread_id,
                user_id=str(thread_row.user_id),
                activity_timestamp=created_at,
            )
        start_time = (
            datetime.fromisoformat(step_dict["start"].replace("Z", ""))
            if step_dict.get("start")
            else None
        )
        end_time = (
            datetime.fromisoformat(step_dict["end"].replace("Z", ""))
            if step_dict.get("end")
            else None
        )

        # Insert into lookup table
        lookup_query = "INSERT INTO steps (id, thread_id) VALUES (%s, %s)"
        self.session.execute(
            lookup_query,
            (
                uuid.UUID(step_dict["id"]),
                uuid.UUID(step_dict["threadId"]),
            ),
        )

        # Insert into main steps_by_thread_id table
        query = """
            INSERT INTO steps_by_thread_id (
                id, thread_id, parent_id, name, type, streaming,
                wait_for_answer, is_error, metadata, tags, input, output,
                created_at, start, "end", generation, show_input, language
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.session.execute(
            query,
            (
                uuid.UUID(step_dict["id"]),  # Convert to UUID
                uuid.UUID(step_dict["threadId"]),  # Convert to UUID
                uuid.UUID(step_dict.get("parentId"))
                if step_dict.get("parentId")
                else None,  # Convert to UUID
                step_dict.get("name"),
                step_dict.get("type"),
                step_dict.get("streaming", False),
                step_dict.get("waitForAnswer"),
                step_dict.get("isError", False),
                _pack_metadata(step_dict.get("metadata", {})),
                step_dict.get("tags"),
                step_dict.get("input"),
                step_dict.get("output"),
                created_at,
                start_time,
                end_time,
                _pack_metadata(step_dict.get("generation"))
                if step_dict.get("generation")
                else None,
                step_dict.get("showInput"),
                step_dict.get("language"),
            ),
        )

    @queue_until_user_message()
    async def update_step(self, step_dict: StepDict):
        """Update a step.

        Note: In practice, createdAt is never modified after initial creation,
        so we can safely use the same upsert logic as create_step.
        """
        # Update the step (which includes updating thread activity)
        await self.create_step(step_dict)

    @queue_until_user_message()
    async def delete_step(self, step_id: str):
        """Delete a step and related feedback/elements."""
        # First, get the thread_id from the lookup table
        lookup_query = "SELECT thread_id FROM steps WHERE id = %s"
        lookup_rows = self.session.execute(lookup_query, (uuid.UUID(step_id),))
        lookup_row = lookup_rows.one()

        if not lookup_row:
            return  # Step doesn't exist

        thread_id = lookup_row.thread_id

        # Get the created_at timestamp from steps_by_thread_id
        step_select_query = """
            SELECT created_at FROM steps_by_thread_id
            WHERE thread_id = %s AND id = %s
            ALLOW FILTERING
        """
        step_rows = self.session.execute(
            step_select_query, (thread_id, uuid.UUID(step_id))
        )
        step_row = step_rows.one()

        if not step_row:
            return  # Step doesn't exist in main table

        created_at = step_row.created_at

        # Delete feedback for this step - get IDs first, then delete individually
        feedback_select_query = (
            "SELECT id FROM feedbacks WHERE for_id = %s ALLOW FILTERING"
        )
        feedback_rows = self.session.execute(
            feedback_select_query, (uuid.UUID(step_id),)
        )  # Convert to UUID
        for feedback_row in feedback_rows:
            feedback_delete_query = "DELETE FROM feedbacks WHERE id = %s"
            self.session.execute(feedback_delete_query, (feedback_row.id,))

        # Delete elements for this step - get IDs first, then delete individually
        elements_select_query = (
            "SELECT id FROM elements WHERE for_id = %s ALLOW FILTERING"
        )
        element_rows = self.session.execute(
            elements_select_query, (uuid.UUID(step_id),)
        )  # Convert to UUID
        for element_row in element_rows:
            elements_delete_query = "DELETE FROM elements WHERE id = %s"
            self.session.execute(elements_delete_query, (element_row.id,))

        # Delete from steps_by_thread_id table (need full primary key)
        step_query = """
            DELETE FROM steps_by_thread_id
            WHERE thread_id = %s AND created_at = %s AND id = %s
        """
        self.session.execute(step_query, (thread_id, created_at, uuid.UUID(step_id)))

        # Delete from lookup table
        lookup_delete_query = "DELETE FROM steps WHERE id = %s"
        self.session.execute(lookup_delete_query, (uuid.UUID(step_id),))

    # Thread methods

    async def get_thread_author(self, thread_id: str) -> str:
        """Get the author (user identifier) of a thread."""
        query = "SELECT user_identifier FROM threads WHERE id = %s"
        rows = self.session.execute(query, (uuid.UUID(thread_id),))  # Convert to UUID
        row = rows.one()

        if not row or not row.user_identifier:
            raise ValueError(f"Author not found for thread_id {thread_id}")

        return row.user_identifier

    async def get_thread(self, thread_id: str) -> ThreadDict | None:
        """Get a thread with all its steps and elements."""
        # Get thread metadata
        thread_query = "SELECT id, user_id, user_identifier, name, created_at, metadata, tags FROM threads WHERE id = %s"
        thread_rows = self.session.execute(
            thread_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        thread_row = thread_rows.one()

        if not thread_row:
            return None

        # Get all steps for this thread
        steps_query = """
            SELECT id, thread_id, parent_id, name, type, streaming,
                   wait_for_answer, is_error, metadata, tags, input, output,
                   created_at, start, "end", generation, show_input, language
            FROM steps_by_thread_id WHERE thread_id = %s
        """
        step_rows = self.session.execute(
            steps_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID

        steps = []
        step_ids = []
        for row in step_rows:
            step_ids.append(row.id)

            # Get feedback for this step
            feedback_query = "SELECT id, value, comment FROM feedbacks WHERE for_id = %s ALLOW FILTERING"
            feedback_rows = self.session.execute(feedback_query, (row.id,))
            feedback_row = feedback_rows.one()

            feedback = None
            if feedback_row:
                feedback = FeedbackDict(
                    id=str(feedback_row.id),  # Convert UUID to string
                    forId=str(row.id),  # Convert UUID to string
                    value=feedback_row.value,
                    comment=feedback_row.comment,
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
                createdAt=self._to_utc_datetime(row.created_at).isoformat() if row.created_at else None,
                start=self._to_utc_datetime(row.start).isoformat() if row.start else None,
                end=self._to_utc_datetime(row.end).isoformat() if row.end else None,
                generation=_unpack_metadata(row.generation) if row.generation else None,
                showInput=row.show_input,
                language=row.language,
                feedback=feedback,
            )
            steps.append(step_dict)

        # Get all elements for this thread - single partition read!
        elements_query = """
            SELECT thread_id, id, for_id, mime, name, object_key, url,
                   chainlit_key, display, size, language, page, props
            FROM elements_by_thread_id WHERE thread_id = %s
        """
        element_rows = self.session.execute(elements_query, (uuid.UUID(thread_id),))

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

        return ThreadDict(
            id=str(thread_row.id),  # Convert UUID to string
            createdAt=self._to_utc_datetime(thread_row.created_at).isoformat()
            if thread_row.created_at
            else None,
            name=thread_row.name,
            userId=str(thread_row.user_id)
            if thread_row.user_id
            else None,  # Convert UUID to string
            userIdentifier=thread_row.user_identifier,
            tags=thread_row.tags,
            metadata=_unpack_metadata(thread_row.metadata)
            if thread_row.metadata
            else None,
            steps=steps,
            elements=elements,
        )

    async def update_thread(
        self,
        thread_id: str,
        name: str | None = None,
        user_id: str | None = None,
        metadata: dict | None = None,
        tags: list[str] | None = None,
    ):
        """Create or update a thread."""
        # Check if thread exists
        existing_query = "SELECT created_at, metadata FROM threads WHERE id = %s"
        existing_rows = self.session.execute(
            existing_query, (uuid.UUID(thread_id),)
        )  # Convert to UUID
        existing_row = existing_rows.one()

        created_at = existing_row.created_at if existing_row else datetime.now()

        # Merge metadata if updating
        if metadata is not None and existing_row and existing_row.metadata:
            base_metadata = _unpack_metadata(existing_row.metadata)
            incoming = {k: v for k, v in metadata.items() if v is not None}
            metadata = {**base_metadata, **incoming}

        # Get user_identifier if user_id is provided
        user_identifier = None
        if user_id:
            user_query = "SELECT identifier FROM users WHERE id = %s ALLOW FILTERING"
            user_rows = self.session.execute(
                user_query, (uuid.UUID(user_id),)
            )  # Convert to UUID
            user_row = user_rows.one()
            if user_row:
                user_identifier = user_row.identifier

        # Determine final values
        final_user_id = (
            uuid.UUID(user_id)
            if user_id
            else (existing_row and getattr(existing_row, "user_id", None))
        )
        final_name = (
            name if name else (existing_row and getattr(existing_row, "name", None))
        )

        # Build the INSERT statement
        query = """
            INSERT INTO threads (id, user_id, user_identifier, name, created_at, metadata, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        self.session.execute(
            query,
            (
                uuid.UUID(thread_id),  # Convert to UUID
                final_user_id,
                user_identifier
                if user_identifier
                else (existing_row and getattr(existing_row, "user_identifier", None)),
                final_name,
                created_at,
                _pack_metadata(metadata)
                if metadata
                else (existing_row.metadata if existing_row else None),
                tags
                if tags
                else (existing_row and getattr(existing_row, "tags", None)),
            ),
        )

        # Update activity tracking if thread has a user
        if final_user_id:
            self._update_thread_activity(
                thread_id=thread_id,
                user_id=str(final_user_id),
                activity_timestamp=created_at,
                thread_name=final_name,
                thread_created_at=created_at,
            )

    async def delete_thread(self, thread_id: str):
        """Delete a thread and all related data."""
        thread_id_uuid = uuid.UUID(thread_id)

        # Get thread info for activity cleanup
        thread_query = "SELECT user_id, last_activity_at FROM threads WHERE id = %s"
        thread_row = self.session.execute(thread_query, (thread_id_uuid,)).one()

        # Get all elements for this thread and delete files from storage
        elements_select_query = (
            "SELECT id, object_key FROM elements_by_thread_id WHERE thread_id = %s"
        )
        element_rows = list(
            self.session.execute(elements_select_query, (thread_id_uuid,))
        )

        if self.storage_client is not None:
            for element_row in element_rows:
                if element_row.object_key:
                    await self.storage_client.delete_file(
                        object_key=element_row.object_key
                    )

        # Get all steps for this thread to delete related feedback
        steps_query = "SELECT id FROM steps WHERE thread_id = %s ALLOW FILTERING"
        step_rows = list(self.session.execute(steps_query, (thread_id_uuid,)))

        for row in step_rows:
            # Delete feedback for each step individually (get all feedback IDs first)
            feedback_select_query = (
                "SELECT id FROM feedbacks WHERE for_id = %s ALLOW FILTERING"
            )
            feedback_rows = self.session.execute(feedback_select_query, (row.id,))
            for feedback_row in feedback_rows:
                feedback_delete_query = "DELETE FROM feedbacks WHERE id = %s"
                self.session.execute(feedback_delete_query, (feedback_row.id,))

        # Delete all elements from both tables
        for element_row in element_rows:
            # Delete from lookup table
            elements_delete_query = "DELETE FROM elements WHERE id = %s"
            self.session.execute(elements_delete_query, (element_row.id,))

        # Delete all elements_by_thread_id for this thread at once
        if element_rows:
            elements_by_thread_delete_query = (
                "DELETE FROM elements_by_thread_id WHERE thread_id = %s"
            )
            self.session.execute(elements_by_thread_delete_query, (thread_id_uuid,))

        # Delete all steps from both tables
        # For steps_by_thread_id, we can delete all steps for the thread at once
        if step_rows:
            steps_by_thread_delete_query = (
                "DELETE FROM steps_by_thread_id WHERE thread_id = %s"
            )
            self.session.execute(steps_by_thread_delete_query, (uuid.UUID(thread_id),))

        # Delete from lookup table individually
        for row in step_rows:
            steps_delete_query = "DELETE FROM steps WHERE id = %s"
            self.session.execute(steps_delete_query, (row.id,))

        # Delete from threads_by_user_activity if thread has a user
        if thread_row and thread_row.user_id and thread_row.last_activity_at:
            activity_delete_query = """
                DELETE FROM threads_by_user_activity
                WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s
            """
            self.session.execute(
                activity_delete_query,
                (thread_row.user_id, thread_row.last_activity_at, thread_id_uuid),
            )

        # Delete the thread itself
        thread_delete_query = "DELETE FROM threads WHERE id = %s"
        self.session.execute(thread_delete_query, (thread_id_uuid,))

    async def list_threads(
        self, pagination: Pagination, filters: ThreadFilter
    ) -> PaginatedResponse[ThreadDict]:
        """List threads for a user with efficient cursor-based pagination.

        Uses composite cursor (timestamp,thread_id) for efficient range queries
        on threads_by_user_activity clustering keys. Deduplicates and actively
        cleans up any duplicate entries found.

        Cursor format: "2025-10-27T10:00:00.123456+00:00,thread-uuid"
        """
        if not filters.userId:
            raise ValueError("userId is required")

        user_id = uuid.UUID(filters.userId)

        # Parse composite cursor if provided
        if pagination.cursor:
            try:
                cursor_parts = pagination.cursor.split(",", 1)
                cursor_timestamp = datetime.fromisoformat(cursor_parts[0])
                # Convert to naive datetime for Cassandra comparison (Cassandra uses naive datetimes)
                if cursor_timestamp.tzinfo is not None:
                    cursor_timestamp = cursor_timestamp.replace(tzinfo=None)
                cursor_thread_id = uuid.UUID(cursor_parts[1])

                # Efficient range query using composite clustering key
                # (last_activity_at, thread_id) < (cursor_timestamp, cursor_thread_id)
                query = """
                    SELECT thread_id, thread_name, thread_created_at, last_activity_at
                    FROM threads_by_user_activity
                    WHERE user_id = %s
                    AND (last_activity_at, thread_id) < (%s, %s)
                    LIMIT %s
                """
                rows = self.session.execute(
                    query,
                    (user_id, cursor_timestamp, cursor_thread_id, pagination.first + 5),
                )
            except (ValueError, IndexError) as e:
                logger.warning(
                    f"Invalid cursor format: {pagination.cursor}, error: {e}"
                )
                # Fall back to first page if cursor is invalid
                pagination.cursor = None

        if not pagination.cursor:
            # First page - no cursor
            query = """
                SELECT thread_id, thread_name, thread_created_at, last_activity_at
                FROM threads_by_user_activity
                WHERE user_id = %s
                LIMIT %s
            """
            rows = self.session.execute(query, (user_id, pagination.first + 5))

        # Deduplicate and identify old duplicates to clean up
        seen_threads = {}
        duplicates_to_delete = []

        for row in rows:
            thread_id_str = str(row.thread_id)

            if thread_id_str not in seen_threads:
                # First occurrence - keep it
                seen_threads[thread_id_str] = row
            else:
                # Duplicate found!
                existing = seen_threads[thread_id_str]

                if row.last_activity_at > existing.last_activity_at:
                    # This row is newer - keep it, mark old one for deletion
                    duplicates_to_delete.append(existing)
                    seen_threads[thread_id_str] = row
                else:
                    # Existing row is newer - mark this one for deletion
                    duplicates_to_delete.append(row)

        # Clean up old duplicates in background (don't block pagination)
        if duplicates_to_delete:
            logger.info(
                f"Cleaning up {len(duplicates_to_delete)} duplicate entries "
                f"for user {filters.userId}"
            )
            for dup in duplicates_to_delete:
                try:
                    delete_query = """
                        DELETE FROM threads_by_user_activity
                        WHERE user_id = %s AND last_activity_at = %s AND thread_id = %s
                    """
                    self.session.execute(
                        delete_query, (user_id, dup.last_activity_at, dup.thread_id)
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to delete duplicate entry for thread {dup.thread_id}: {e}"
                    )

        # Get unique rows (already in correct order from Cassandra)
        unique_rows = list(seen_threads.values())

        # Take only requested number
        paginated_rows = unique_rows[: pagination.first]

        # Fetch full thread data for paginated results
        threads = []
        for row in paginated_rows:
            thread = await self.get_thread(str(row.thread_id))
            if thread:
                threads.append(thread)

        # Build composite cursor for last row: "timestamp,thread_id"
        end_cursor = None
        start_cursor = None
        if paginated_rows:
            last_row = paginated_rows[-1]
            end_cursor = (
                f"{self._to_utc_datetime(last_row.last_activity_at).isoformat()},{last_row.thread_id}"
            )
            first_row = paginated_rows[0]
            start_cursor = (
                f"{self._to_utc_datetime(first_row.last_activity_at).isoformat()},{first_row.thread_id}"
            )

        # Determine if there are more pages
        # We fetched pagination.first + 5, so if we have more than pagination.first unique results, there are more pages
        has_next_page = len(unique_rows) > pagination.first

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
        self.cluster.shutdown()
