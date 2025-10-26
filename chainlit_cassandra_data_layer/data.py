from typing import Dict, List, Optional
from chainlit.data.base import BaseDataLayer
from chainlit.data.utils import queue_until_user_message
from chainlit.types import (
    Feedback,
    PaginatedResponse,
    Pagination,
    ThreadDict,
    ThreadFilter,
)
from chainlit.user import User, PersistedUser
from chainlit.element import Element, ElementDict
from chainlit.step import StepDict


class CassandraDataLayer(BaseDataLayer):
    async def get_user(self, identifier: str) -> Optional[PersistedUser]:
        raise NotImplementedError()

    async def create_user(self, user: User) -> Optional[PersistedUser]:
        raise NotImplementedError()

    async def delete_feedback(
        self,
        feedback_id: str,
    ) -> bool:
        raise NotImplementedError()

    async def upsert_feedback(
        self,
        feedback: Feedback,
    ) -> str:
        raise NotImplementedError()

    @queue_until_user_message()
    async def create_element(self, element: Element):
        raise NotImplementedError()

    async def get_element(
        self, thread_id: str, element_id: str
    ) -> Optional[ElementDict]:
        raise NotImplementedError()

    @queue_until_user_message()
    async def delete_element(self, element_id: str, thread_id: Optional[str] = None):
        raise NotImplementedError()

    @queue_until_user_message()
    async def create_step(self, step_dict: StepDict):
        raise NotImplementedError()

    @queue_until_user_message()
    async def update_step(self, step_dict: StepDict):
        raise NotImplementedError()

    @queue_until_user_message()
    async def delete_step(self, step_id: str):
        raise NotImplementedError()

    async def get_thread_author(self, thread_id: str) -> str:
        return ""

    async def delete_thread(self, thread_id: str):
        raise NotImplementedError()

    async def list_threads(
        self, pagination: Pagination, filters: ThreadFilter
    ) -> PaginatedResponse[ThreadDict]:
        raise NotImplementedError()

    async def get_thread(self, thread_id: str) -> Optional[ThreadDict]:
        raise NotImplementedError()

    async def update_thread(
        self,
        thread_id: str,
        name: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        tags: Optional[List[str]] = None,
    ):
        raise NotImplementedError()

    async def build_debug_url(self) -> str:
        raise NotImplementedError()

    async def close(self) -> None:
        raise NotImplementedError()
