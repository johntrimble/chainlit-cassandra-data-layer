"""Integration tests for CassandraDataLayer."""

import uuid
from datetime import datetime

import pytest
from chainlit.types import Feedback, PageInfo, Pagination, ThreadFilter
from chainlit.user import User

from chainlit_cassandra_data_layer.data import CassandraDataLayer


@pytest.fixture
def data_layer():
    """Create a CassandraDataLayer instance for testing."""
    layer = CassandraDataLayer(contact_points=["cassandra"], keyspace="chainlit")
    yield layer
    layer.cluster.shutdown()


@pytest.fixture
def test_user_id():
    """Generate a unique user ID for testing."""
    return str(uuid.uuid4())  # Return valid UUID string


@pytest.fixture
def test_thread_id():
    """Generate a unique thread ID for testing."""
    return str(uuid.uuid4())  # Return valid UUID string


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

        # Create multiple threads
        thread_ids = []
        for i in range(3):
            thread_id = str(uuid.uuid4())  # Generate valid UUID string
            thread_ids.append(thread_id)
            await data_layer.update_thread(
                thread_id=thread_id,
                name=f"Thread {i}",
                user_id=persisted_user.id,
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
