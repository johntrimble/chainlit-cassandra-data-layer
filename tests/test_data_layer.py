"""Integration tests for CassandraDataLayer."""

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from chainlit.types import Feedback, PageInfo, Pagination, ThreadFilter
from chainlit.user import User

from chainlit_cassandra_data_layer.data import smallest_uuid7_for_datetime


@pytest.mark.asyncio
class TestUserOperations:
    """Test user-related operations."""

    async def test_create_and_get_user(self, data_layer, test_user_id):
        """Test creating and retrieving a user."""
        # Create a user
        user = User(
            identifier=test_user_id, metadata={"name": "Test User", "role": "tester"}
        )
        persisted_user = await data_layer.create_user(user)

        assert persisted_user is not None
        assert persisted_user.identifier == test_user_id
        assert persisted_user.metadata["name"] == "Test User"
        assert persisted_user.metadata["role"] == "tester"
        assert persisted_user.id is not None
        assert persisted_user.createdAt is not None

        # Get the user
        retrieved_user = await data_layer.get_user(test_user_id)
        assert retrieved_user is not None
        assert retrieved_user.identifier == test_user_id
        assert retrieved_user.id == persisted_user.id
        assert retrieved_user.metadata["name"] == "Test User"

    async def test_update_user_metadata(self, data_layer, test_user_id):
        """Test updating user metadata."""
        # Create user
        user = User(identifier=test_user_id, metadata={"key1": "value1"})
        await data_layer.create_user(user)

        # Update user metadata
        updated_user = User(identifier=test_user_id, metadata={"key2": "value2"})
        await data_layer.create_user(updated_user)

        # Verify update
        retrieved_user = await data_layer.get_user(test_user_id)
        assert retrieved_user is not None
        assert retrieved_user.metadata["key2"] == "value2"

    async def test_get_nonexistent_user(self, data_layer):
        """Test getting a user that doesn't exist."""
        user = await data_layer.get_user("nonexistent-user")
        assert user is None


@pytest.mark.asyncio
class TestThreadOperations:
    """Test thread-related operations."""

    async def test_create_and_get_thread(
        self, data_layer, test_user_id, test_thread_id
    ):
        """Test creating and retrieving a thread."""
        # Create user first
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)

        # Create thread
        await data_layer.update_thread(
            thread_id=test_thread_id,
            name="Test Thread",
            user_id=persisted_user.id,
            metadata={"category": "testing"},
            tags=["test", "demo"],
        )

        # Get thread
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert thread["id"] == test_thread_id
        assert thread["name"] == "Test Thread"
        assert thread["userId"] == persisted_user.id
        assert thread["metadata"]["category"] == "testing"
        assert thread["tags"] == ["test", "demo"]
        assert thread["steps"] == []
        assert thread["elements"] == []

    async def test_update_thread(self, data_layer, test_user_id, test_thread_id):
        """Test updating a thread."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Original Name", user_id=persisted_user.id
        )

        # Update thread
        await data_layer.update_thread(
            thread_id=test_thread_id,
            name="Updated Name",
            metadata={"updated": True},
        )

        # Verify update
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert thread["name"] == "Updated Name"
        assert thread["metadata"]["updated"] is True

    async def test_delete_thread(self, data_layer, test_user_id, test_thread_id):
        """Test deleting a thread."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Verify thread exists
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None

        # Delete thread
        await data_layer.delete_thread(test_thread_id)

        # Verify thread is deleted
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is None

    async def test_get_thread_author(self, data_layer, test_user_id, test_thread_id):
        """Test getting thread author."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Get author
        author = await data_layer.get_thread_author(test_thread_id)
        assert author == test_user_id

    async def test_list_threads(self, data_layer, test_user_id):
        """Test listing threads for a user."""
        # Create user
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)

        # Create multiple threads with steps (needed for activity tracking)
        thread_ids = []
        for i in range(3):
            thread_id = str(uuid.uuid4())  # Generate valid UUID string
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Thread {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"Message {i}",
                }
            )

        # List threads
        pagination = Pagination(first=10)
        filters = ThreadFilter(userId=persisted_user.id)
        result = await data_layer.list_threads(pagination, filters)

        assert result.pageInfo is not None
        assert isinstance(result.pageInfo, PageInfo)
        assert len(result.data) >= 3
        assert all(thread["userId"] == persisted_user.id for thread in result.data)

        # Clean up
        for thread_id in thread_ids:
            await data_layer.delete_thread(thread_id)

    async def test_update_thread_preserves_existing_fields(
        self, data_layer, test_user_id, test_thread_id
    ):
        """Ensure update_thread does not overwrite name/user/tags when omitted."""
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)

        await data_layer.update_thread(
            thread_id=test_thread_id,
            name="Original Thread",
            user_id=persisted_user.id,
            tags=["initial"],
        )

        # Update metadata without restating user/name/tags
        await data_layer.update_thread(
            thread_id=test_thread_id,
            metadata={"status": "active"},
        )

        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert thread["name"] == "Original Thread"
        assert thread["userId"] == persisted_user.id
        assert thread["tags"] == ["initial"]
        assert thread["metadata"]["status"] == "active"

    async def test_list_threads_pagination(self, data_layer, test_user_id):
        """Test thread pagination."""
        # Create user
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)

        # Create 5 threads
        thread_ids = []
        for i in range(5):
            thread_id = str(uuid.uuid4())  # Generate valid UUID string
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Thread {i}",
                user_id=persisted_user.id,
            )

        # Get first page (2 items)
        pagination = Pagination(first=2)
        filters = ThreadFilter(userId=persisted_user.id)
        result = await data_layer.list_threads(pagination, filters)

        assert len(result.data) <= 2
        if len(result.data) == 2:
            assert result.pageInfo.hasNextPage is True
            assert result.pageInfo.endCursor is not None

            # Get second page
            pagination2 = Pagination(first=2, cursor=result.pageInfo.endCursor)
            result2 = await data_layer.list_threads(pagination2, filters)
            assert len(result2.data) >= 1

        # Clean up
        for thread_id in thread_ids:
            await data_layer.delete_thread(thread_id)

    async def test_list_threads_search_with_iterative_fetching(
        self, data_layer, test_user_id
    ):
        """Test that list_threads with search filter correctly iterates to fill page.

        Setup: 20 non-matching threads + 5 matching threads + 20 non-matching threads
        Request: 5 threads with search filter "PROJECT"
        Expected: Returns exactly 5 matching threads with hasNextPage=False

        This verifies the iterative fetching works correctly when most results
        are filtered out by the search term, and that hasNextPage is only true
        when there are actually more matching results.
        """
        # Create user
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)

        thread_ids = []

        # Create 20 threads that DON'T match the search string "PROJECT"
        # These should be skipped during iteration
        for i in range(20):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Discussion Thread {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"Message {i}",
                }
            )
            # Small delay to ensure ordering by activity time
            await asyncio.sleep(0.01)

        # Create 5 threads that DO match the search string "PROJECT"
        # These are the ones we expect to get back
        matching_thread_ids = []
        for i in range(5):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            matching_thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"PROJECT Alpha {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"PROJECT Message {i}",
                }
            )
            await asyncio.sleep(0.01)

        # Create 20 more threads that DON'T match
        # These come after our matching threads
        for i in range(20):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Discussion After {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"After Message {i}",
                }
            )
            await asyncio.sleep(0.01)

        # Now request 5 threads with search filter "PROJECT"
        # Should iterate through non-matching threads and return exactly 5 matching ones
        pagination = Pagination(first=5)
        filters = ThreadFilter(userId=persisted_user.id, search="PROJECT")
        result = await data_layer.list_threads(pagination, filters)

        # Verify we got exactly 5 results
        assert len(result.data) == 5, f"Expected 5 results, got {len(result.data)}"

        # Verify all results contain "PROJECT" in the name
        for thread in result.data:
            assert "PROJECT" in thread["name"], (
                f"Thread name '{thread['name']}' should contain 'PROJECT'"
            )

        # Verify the thread IDs match our expected matching threads
        result_ids = {thread["id"] for thread in result.data}
        expected_ids = set(matching_thread_ids)
        assert result_ids == expected_ids, (
            f"Result IDs {result_ids} should match expected {expected_ids}"
        )

        # Verify hasNextPage is False since there are no more matching results
        assert result.pageInfo.hasNextPage is False, (
            "hasNextPage should be False when no more matching results exist"
        )

        # Clean up
        for thread_id in thread_ids:
            await data_layer.delete_thread(thread_id)

    async def test_list_threads_search_pagination_with_cursor(
        self, data_layer, test_user_id
    ):
        """Test that cursor-based pagination correctly skips non-matching threads.

        Setup (oldest to newest): 1 matching + 21 non-matching + 5 matching + 20 non-matching
        Query returns DESC order (newest first): 20 non-matching + 5 matching + 21 non-matching + 1 matching
        Request: Page 1 with 5 threads, then Page 2 using cursor
        Expected:
        - Page 1: 5 matching results (PROJECT Alpha) with hasNextPage=True
        - Page 2: 1 matching result (PROJECT Beta) with hasNextPage=False
        - Cursor should skip the 21 non-matching threads between pages

        This verifies that cursor-based pagination doesn't rescan non-matching results.
        """
        # Create user
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)

        thread_ids = []
        page1_thread_ids = []
        page2_thread_ids = []

        # Create threads in REVERSE order so newest (last created) appear first in results
        # Database returns ORDER BY last_activity_at DESC (newest first)

        # Create 1 thread that DOES match (page 2 results - oldest)
        thread_id = str(uuid.uuid4())
        thread_ids.append(thread_id)
        page2_thread_ids.append(thread_id)
        await data_layer.update_thread(
            thread_id=thread_id,
            name="PROJECT Beta",
            user_id=persisted_user.id,
        )
        # Create a step to trigger activity tracking
        step_id = str(uuid.uuid4())
        await data_layer.create_step(
            {
                "id": step_id,
                "threadId": thread_id,
                "type": "user_message",
                "output": "PROJECT Beta Message",
            }
        )
        await asyncio.sleep(0.01)

        # Create 21 threads that DON'T match (between the two pages)
        for i in range(21):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Discussion Between {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"Between Message {i}",
                }
            )
            await asyncio.sleep(0.01)

        # Create 5 threads that DO match (page 1 results)
        for i in range(5):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            page1_thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"PROJECT Alpha {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"PROJECT Alpha Message {i}",
                }
            )
            await asyncio.sleep(0.01)

        # Create 20 threads that DON'T match (newest)
        for i in range(20):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Discussion Thread {i}",
                user_id=persisted_user.id,
            )
            # Create a step to trigger activity tracking
            step_id = str(uuid.uuid4())
            await data_layer.create_step(
                {
                    "id": step_id,
                    "threadId": thread_id,
                    "type": "user_message",
                    "output": f"Thread Message {i}",
                }
            )
            await asyncio.sleep(0.01)

        # Request page 1: 5 threads with search filter "PROJECT"
        pagination = Pagination(first=5)
        filters = ThreadFilter(userId=persisted_user.id, search="PROJECT")
        page1 = await data_layer.list_threads(pagination, filters)

        # Verify page 1 has exactly 5 results
        assert len(page1.data) == 5, f"Page 1 expected 5 results, got {len(page1.data)}"

        # Verify all page 1 results contain "PROJECT"
        for thread in page1.data:
            assert "PROJECT" in thread["name"], (
                f"Thread name '{thread['name']}' should contain 'PROJECT'"
            )

        # Verify page 1 thread IDs match expected
        page1_result_ids = {thread["id"] for thread in page1.data}
        page1_expected_ids = set(page1_thread_ids)
        assert page1_result_ids == page1_expected_ids, (
            f"Page 1 IDs {page1_result_ids} should match {page1_expected_ids}"
        )

        # Verify hasNextPage is True
        assert page1.pageInfo.hasNextPage is True, "Page 1 hasNextPage should be True"

        # Verify we have an endCursor
        assert page1.pageInfo.endCursor is not None, "Page 1 should have an endCursor"

        # To verify we don't rescan: request page 2 WITHOUT search filter first
        # This shows us what raw position the cursor points to in the database
        pagination2_no_filter = Pagination(first=5, cursor=page1.pageInfo.endCursor)
        filters_no_search = ThreadFilter(userId=persisted_user.id)
        page2_no_filter = await data_layer.list_threads(
            pagination2_no_filter, filters_no_search
        )

        # The cursor should position us close to the next matching thread
        # If we're rescanning, we'd get many "Discussion Between" threads
        # If cursor is efficient, we should get PROJECT Beta early in results
        thread_names_no_filter = [t["name"] for t in page2_no_filter.data]

        # Find position of PROJECT Beta in the unfiltered results
        project_beta_position = None
        for i, name in enumerate(thread_names_no_filter):
            if "PROJECT Beta" in name:
                project_beta_position = i
                break

        # PROJECT Beta should appear very early (ideally at position 0 or 1)
        # If we were rescanning all 21 "Discussion Between" threads, it wouldn't appear in first 5
        assert project_beta_position is not None, (
            "PROJECT Beta should appear in first 5 results after cursor"
        )
        assert project_beta_position <= 1, (
            f"PROJECT Beta at position {project_beta_position} - cursor should skip non-matches (position should be 0 or 1)"
        )

        # Now request page 2 WITH search filter
        pagination2 = Pagination(first=5, cursor=page1.pageInfo.endCursor)
        page2 = await data_layer.list_threads(pagination2, filters)

        # Verify page 2 has exactly 1 result
        assert len(page2.data) == 1, f"Page 2 expected 1 result, got {len(page2.data)}"

        # Verify the page 2 result contains "PROJECT"
        assert "PROJECT" in page2.data[0]["name"], (
            f"Thread name '{page2.data[0]['name']}' should contain 'PROJECT'"
        )

        # Verify page 2 thread ID matches expected
        page2_result_id = page2.data[0]["id"]
        assert page2_result_id == page2_thread_ids[0], (
            f"Page 2 ID {page2_result_id} should match {page2_thread_ids[0]}"
        )

        # Verify hasNextPage is False
        assert page2.pageInfo.hasNextPage is False, (
            "Page 2 hasNextPage should be False when no more matching results exist"
        )

        # Clean up
        for thread_id in thread_ids:
            await data_layer.delete_thread(thread_id)

    async def test_list_threads_handles_all_duplicate_batch(
        self, data_layer, test_user_id
    ):
        """Test that list_threads doesn't hang when a batch contains only duplicates.

        Reproduces bug: https://github.com/johntrimble/chainlit-cassandra-data-layer/issues/6

        Setup: Create threads and manually insert duplicate entries in threads_by_user_activity
        Expected: list_threads completes without hanging (cursor should advance even with empty unique_rows)
        """
        # Create user
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        user_id_uuid = uuid.UUID(persisted_user.id)

        thread_ids = []

        # Create several threads that will appear in the first batch
        for i in range(5):
            thread_id = str(uuid.uuid4())
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Thread {i}",
                user_id=persisted_user.id,
            )
            await asyncio.sleep(0.01)

        # Now manually insert duplicate entries for the first 3 threads
        # These duplicates will have older timestamps, so they'll appear in later batches
        # when those batches are fetched, all rows will be duplicates of already-seen threads
        from datetime import datetime as dt
        from datetime import timedelta

        for i in range(3):
            thread_id_uuid = uuid.UUID(thread_ids[i])

            # Insert multiple old duplicates for this thread
            for j in range(25):  # Create enough duplicates to fill a batch
                old_timestamp = dt.now() - timedelta(hours=1 + j)
                old_activity_uuid = smallest_uuid7_for_datetime(old_timestamp)
                created_at_uuid = smallest_uuid7_for_datetime(dt.now())

                insert_query = f"""
                    INSERT INTO {data_layer._table_threads_by_user_activity}
                    (user_id, activity_at, thread_id, thread_name, thread_created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """
                await data_layer._aexecute_prepared(
                    insert_query,
                    (
                        user_id_uuid,
                        old_activity_uuid,
                        thread_id_uuid,
                        f"Thread {i}",
                        created_at_uuid,
                    ),
                )

        # Now try to list threads with a small page size
        # The first batch will contain the 5 newest entries (one per thread)
        # Subsequent batches will contain only duplicates of threads already seen
        # With the bug: this will hang forever as cursor doesn't advance on duplicate-only batches
        # Without the bug: this should complete and return results
        pagination = Pagination(first=10)
        filters = ThreadFilter(userId=persisted_user.id)

        # This should complete without hanging - use asyncio.wait_for to enforce timeout
        # If it takes longer than 10 seconds, the bug is present
        try:
            result = await asyncio.wait_for(
                data_layer.list_threads(pagination, filters), timeout=10.0
            )
        except TimeoutError:
            pytest.fail(
                "list_threads hung for more than 10 seconds - infinite loop bug is present. "
                "Cursor is not advancing when all rows in a batch are duplicates."
            )

        # Verify we got results
        assert len(result.data) > 0
        assert result.pageInfo is not None

        # Clean up
        for thread_id in thread_ids:
            await data_layer.delete_thread(thread_id)


@pytest.mark.asyncio
class TestStepOperations:
    """Test step-related operations."""

    async def test_create_step(self, data_layer, test_user_id, test_thread_id):
        """Test creating a step."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Create step
        step_id = str(uuid.uuid4())
        step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Test Step",
            "type": "user_message",
            "output": "Hello, world!",
            "createdAt": datetime.now().isoformat() + "Z",
            "streaming": False,
            "isError": False,
        }
        await data_layer.create_step(step_dict)

        # Get thread and verify step
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert len(thread["steps"]) == 1
        assert thread["steps"][0]["id"] == step_id
        assert thread["steps"][0]["name"] == "Test Step"
        assert thread["steps"][0]["output"] == "Hello, world!"
        assert thread["userId"] == persisted_user.id

        # Clean up
        await data_layer.delete_thread(test_thread_id)

    async def test_update_step(self, data_layer, test_user_id, test_thread_id):
        """Test updating a step."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Create step
        step_id = str(uuid.uuid4())
        created_at = datetime.now().isoformat() + "Z"
        step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Original Step",
            "type": "assistant_message",
            "output": "Original output",
            "createdAt": created_at,
        }
        await data_layer.create_step(step_dict)

        # Update step - preserve createdAt as it does in practice
        updated_step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Updated Step",
            "type": "assistant_message",
            "output": "Updated output",
            "createdAt": created_at,  # Same timestamp as creation
        }
        await data_layer.update_step(updated_step_dict)

        # Verify update
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert len(thread["steps"]) == 1
        assert thread["steps"][0]["name"] == "Updated Step"
        assert thread["steps"][0]["output"] == "Updated output"
        # Verify createdAt is preserved (note: Cassandra timestamp precision may differ slightly)
        assert (
            thread["steps"][0]["createdAt"][:23] == created_at[:23]
        )  # Compare up to milliseconds

        # Clean up
        await data_layer.delete_thread(test_thread_id)

    async def test_update_step_preserves_feedback(
        self, data_layer, test_user_id, test_thread_id
    ):
        """Updating a step should not clear existing feedback."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Create step
        step_id = str(uuid.uuid4())
        created_at = datetime.now().isoformat() + "Z"
        step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Original Step",
            "type": "assistant_message",
            "output": "Original output",
            "createdAt": created_at,
        }
        await data_layer.create_step(step_dict)

        # Attach feedback using dedicated API (requires thread context)
        from chainlit.context import context
        from chainlit.session import BaseSession

        if not context.session:
            context.session = BaseSession()
        context.session.thread_id = test_thread_id

        feedback = Feedback(
            forId=step_id,
            threadId=test_thread_id,
            value=1,
            comment="Great response!",
        )
        await data_layer.upsert_feedback(feedback)

        # Update step without providing feedback field
        updated_step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Updated Step",
            "type": "assistant_message",
            "output": "Updated output",
            "createdAt": created_at,
        }
        await data_layer.update_step(updated_step_dict)

        # Verify feedback is preserved
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert len(thread["steps"]) == 1
        step = thread["steps"][0]
        assert step["feedback"] is not None, "Feedback should persist after update_step"
        assert step["feedback"]["value"] == 1
        assert step["feedback"]["comment"] == "Great response!"

        # Clean up
        await data_layer.delete_thread(test_thread_id)

    async def test_update_step_preserves_omitted_fields(
        self, data_layer, test_user_id, test_thread_id
    ):
        """Test that update_step preserves fields not in the update dict.

        This verifies partial update semantics: omitted fields should preserve
        their existing values, especially createdAt which prevents duplicate
        activity entries.
        """
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id,
            name="Test Thread",
            user_id=persisted_user.id,
        )

        # Create step with all fields
        step_id = str(uuid.uuid4())
        original_timestamp = datetime.now(UTC).isoformat()
        full_step = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Original Name",
            "type": "user_message",
            "output": "Original Output",
            "input": "Original Input",
            "createdAt": original_timestamp,
            "streaming": True,
            "isError": False,
        }
        await data_layer.create_step(full_step)

        # Verify initial state
        thread = await data_layer.get_thread(test_thread_id)
        assert len(thread["steps"]) == 1
        initial_step = thread["steps"][0]
        assert initial_step["name"] == "Original Name"
        assert initial_step["output"] == "Original Output"
        assert initial_step["input"] == "Original Input"
        assert initial_step["type"] == "user_message"
        assert initial_step["streaming"] is True

        # Update only output field - partial update
        partial_update = {
            "id": step_id,
            "threadId": test_thread_id,
            "output": "Updated Output",
        }
        await data_layer.update_step(partial_update)

        # Verify other fields were preserved
        thread = await data_layer.get_thread(test_thread_id)
        assert len(thread["steps"]) == 1
        fetched = thread["steps"][0]

        # Updated field
        assert fetched["output"] == "Updated Output"

        # Preserved fields (CRITICAL - these should not change!)
        assert fetched["name"] == "Original Name", "name should be preserved"
        assert fetched["input"] == "Original Input", "input should be preserved"
        assert fetched["type"] == "user_message", "type should be preserved"
        assert fetched["streaming"] is True, "streaming should be preserved"
        assert fetched["isError"] is False, "isError should be preserved"

        # MOST CRITICAL: createdAt should be preserved (prevents duplicate activity entries)
        # Note: Cassandra has millisecond precision, so compare timestamps allowing for rounding
        fetched_time = datetime.fromisoformat(fetched["createdAt"])
        original_time = datetime.fromisoformat(original_timestamp)
        time_diff = abs((fetched_time - original_time).total_seconds())
        assert time_diff < 0.001, (
            f"createdAt MUST be preserved (diff: {time_diff}s) to prevent duplicate activity entries"
        )

        # Clean up
        await data_layer.delete_thread(test_thread_id)

    async def test_delete_step(self, data_layer, test_user_id, test_thread_id):
        """Test deleting a step."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Create step
        step_id = str(uuid.uuid4())
        step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Test Step",
            "type": "user_message",
            "output": "Test",
            "createdAt": datetime.now().isoformat() + "Z",
        }
        await data_layer.create_step(step_dict)

        # Verify step exists
        thread = await data_layer.get_thread(test_thread_id)
        assert len(thread["steps"]) == 1

        # Set context for delete_step (it needs context.session.thread_id)
        from chainlit.context import context
        from chainlit.session import BaseSession

        if not context.session:
            context.session = BaseSession()
        context.session.thread_id = test_thread_id

        # Delete step
        await data_layer.delete_step(step_id)

        # Verify step is deleted
        thread = await data_layer.get_thread(test_thread_id)
        assert len(thread["steps"]) == 0

        # Clean up
        await data_layer.delete_thread(test_thread_id)


@pytest.mark.asyncio
class TestFeedbackOperations:
    """Test feedback-related operations."""

    async def test_upsert_feedback(self, data_layer, test_user_id, test_thread_id):
        """Test creating feedback."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Create step
        step_id = str(uuid.uuid4())
        step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Test Step",
            "type": "assistant_message",
            "output": "Test output",
            "createdAt": datetime.now().isoformat() + "Z",
        }
        await data_layer.create_step(step_dict)

        # Set context for feedback operations (they need context.session.thread_id)
        from chainlit.context import context
        from chainlit.session import BaseSession

        if not context.session:
            context.session = BaseSession()
        context.session.thread_id = test_thread_id

        # Create feedback
        feedback = Feedback(
            forId=step_id,
            threadId=test_thread_id,
            value=1,
            comment="Great response!",
        )
        feedback_id = await data_layer.upsert_feedback(feedback)
        assert feedback_id is not None

        # Verify feedback is attached to step
        thread = await data_layer.get_thread(test_thread_id)
        assert thread is not None
        assert len(thread["steps"]) == 1
        assert thread["steps"][0]["feedback"] is not None
        assert thread["steps"][0]["feedback"]["value"] == 1
        assert thread["steps"][0]["feedback"]["comment"] == "Great response!"

        # Clean up
        await data_layer.delete_thread(test_thread_id)

    async def test_delete_feedback(self, data_layer, test_user_id, test_thread_id):
        """Test deleting feedback."""
        # Create user and thread
        user = User(identifier=test_user_id, metadata={})
        persisted_user = await data_layer.create_user(user)
        await data_layer.update_thread(
            thread_id=test_thread_id, name="Test Thread", user_id=persisted_user.id
        )

        # Create step
        step_id = str(uuid.uuid4())
        step_dict = {
            "id": step_id,
            "threadId": test_thread_id,
            "name": "Test Step",
            "type": "assistant_message",
            "output": "Test output",
            "createdAt": datetime.now().isoformat() + "Z",
        }
        await data_layer.create_step(step_dict)

        # Set context for feedback operations (they need context.session.thread_id)
        from chainlit.context import context
        from chainlit.session import BaseSession

        if not context.session:
            context.session = BaseSession()
        context.session.thread_id = test_thread_id

        # Create feedback
        feedback = Feedback(
            forId=step_id,
            threadId=test_thread_id,
            value=1,
        )
        feedback_id = await data_layer.upsert_feedback(feedback)

        # Verify feedback exists
        thread = await data_layer.get_thread(test_thread_id)
        assert thread["steps"][0]["feedback"] is not None

        # Delete feedback
        result = await data_layer.delete_feedback(feedback_id)
        assert result is True

        # Verify feedback is deleted
        thread = await data_layer.get_thread(test_thread_id)
        assert thread["steps"][0]["feedback"] is None

        # Clean up
        await data_layer.delete_thread(test_thread_id)


@pytest.mark.asyncio
class TestElementOperations:
    """Test element-related operations."""

    async def test_create_element(self, data_layer, test_user_id, test_thread_id):
        """Test creating an element."""
        # Note: This test is simplified since we don't have storage provider
        # In a real scenario, elements would include file uploads
        pass

    async def test_get_element(self, data_layer, test_user_id, test_thread_id):
        """Test getting an element."""
        # Simplified test - would need storage provider for full implementation
        pass

    async def test_delete_element(self, data_layer):
        """Test deleting an element."""
        # Simplified test - would need storage provider for full implementation
        pass


@pytest.mark.asyncio
class TestDataLayerLifecycle:
    """Test data layer lifecycle operations."""

    async def test_build_debug_url(self, data_layer):
        """Test build_debug_url."""
        url = await data_layer.build_debug_url()
        assert url == ""

    async def test_close(self, data_layer):
        """Test closing the data layer."""
        await data_layer.close()
        # After close, cluster should be shut down
        assert data_layer.cluster.is_shutdown
