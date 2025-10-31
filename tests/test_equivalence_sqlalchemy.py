"""Property-based equivalence tests between Cassandra and SQLAlchemy data layers."""

import asyncio
import copy
import json
import uuid
from datetime import datetime, timezone

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.strategies import composite
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from chainlit.context import context
from chainlit.data.sql_alchemy import SQLAlchemyDataLayer
from chainlit.user import User


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    reason="Cassandra update_thread ignores metadata name override when empty string",
)
async def test_update_thread_metadata_name_parity(data_layer, sqlite_data_layer):
    """Regression reproduction: metadata update should not desynchronise thread name."""
    await _run_equivalence_sequence(
        operation_sequence=[{"type": "update_thread", "metadata": {"name": ""}}],
        data_layer=data_layer,
        sqlite_data_layer=sqlite_data_layer,
    )


@pytest.fixture
def sqlite_data_layer(tmp_path):
    """Create a SQLAlchemyDataLayer backed by a temporary SQLite database."""
    db_path = tmp_path / f"chainlit_equiv_{uuid.uuid4().hex}.sqlite"
    conninfo = f"sqlite+aiosqlite:///{db_path}"

    async def _prepare() -> SQLAlchemyDataLayer:
        engine = create_async_engine(conninfo)
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS users (
                    "id" UUID PRIMARY KEY,
                    "identifier" TEXT NOT NULL UNIQUE,
                    "metadata" JSONB NOT NULL,
                    "createdAt" TEXT
                );
                    """
                )
            )
            await conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS threads (
                    "id" UUID PRIMARY KEY,
                    "createdAt" TEXT,
                    "name" TEXT,
                    "userId" UUID,
                    "userIdentifier" TEXT,
                    "tags" TEXT,
                    "metadata" JSONB,
                    FOREIGN KEY ("userId") REFERENCES users("id") ON DELETE CASCADE
                );
                    """
                )
            )
            await conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS steps (
                    "id" UUID PRIMARY KEY,
                    "name" TEXT NOT NULL,
                    "type" TEXT NOT NULL,
                    "threadId" UUID NOT NULL,
                    "parentId" UUID,
                    "disableFeedback" BOOLEAN,
                    "streaming" BOOLEAN,
                    "waitForAnswer" BOOLEAN,
                    "isError" BOOLEAN,
                    "metadata" JSONB,
                    "tags" TEXT[],
                    "input" TEXT,
                    "output" TEXT,
                    "createdAt" TEXT,
                    "start" TEXT,
                    "end" TEXT,
                    "generation" JSONB,
                    "showInput" TEXT,
                    "language" TEXT,
                    "indent" INT
                );
                    """
                )
            )
            await conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS elements (
                    "id" UUID PRIMARY KEY,
                    "threadId" UUID,
                    "type" TEXT,
                    "url" TEXT,
                    "chainlitKey" TEXT,
                    "name" TEXT NOT NULL,
                    "display" TEXT,
                    "objectKey" TEXT,
                    "size" TEXT,
                    "page" INT,
                    "language" TEXT,
                    "forId" UUID,
                    "mime" TEXT,
                    "props" TEXT
                );
                    """
                )
            )
            await conn.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS feedbacks (
                    "id" UUID PRIMARY KEY,
                    "forId" UUID NOT NULL,
                    "threadId" UUID NOT NULL,
                    "value" INT NOT NULL,
                    "comment" TEXT
                );
                    """
                )
            )
        await engine.dispose()
        return SQLAlchemyDataLayer(conninfo)

    data_layer = asyncio.run(_prepare())
    try:
        yield data_layer
    finally:
        asyncio.run(data_layer.close())


ALPHABET = "abcdefghijklmnopqrstuvwxyz"
SIMPLE_TEXT = st.text(alphabet=ALPHABET, min_size=1, max_size=8)
OPTIONAL_TEXT = st.text(alphabet=ALPHABET, min_size=0, max_size=12)
METADATA_VALUE = st.one_of(
    OPTIONAL_TEXT,
    st.integers(min_value=-3, max_value=3),
    st.booleans(),
)
METADATA_DICT = st.dictionaries(
    keys=st.text(alphabet=ALPHABET, min_size=1, max_size=5),
    values=METADATA_VALUE,
    max_size=3,
)
ISO_DATETIME = st.datetimes(
    min_value=datetime(2020, 1, 1),
    max_value=datetime(2030, 12, 31),
).map(
    lambda dt: dt.replace(microsecond=0, tzinfo=timezone.utc)
    .isoformat()
    .replace("+00:00", "Z")
)


@composite
def operation_sequences(draw):
    length = draw(st.integers(min_value=1, max_value=5))
    steps: list[str] = []
    sequence: list[dict[str, object]] = []

    for _ in range(length):
        available_ops = ["update_thread", "create_step"]
        if steps:
            available_ops.append("update_step")

        op_type = draw(st.sampled_from(available_ops))

        if op_type == "update_thread":
            op: dict[str, object] = {"type": "update_thread"}
            if draw(st.booleans()):
                op["name"] = draw(SIMPLE_TEXT)
            if draw(st.booleans()):
                op["metadata"] = draw(METADATA_DICT)
            sequence.append(op)
        elif op_type == "create_step":
            step_id = str(draw(st.uuids()))
            op = {
                "type": "create_step",
                "step_id": step_id,
                "name": draw(SIMPLE_TEXT),
                "output": draw(OPTIONAL_TEXT),
                "step_type": draw(
                    st.sampled_from(["assistant_message", "user_message"])
                ),
                "created_at": draw(ISO_DATETIME),
            }
            if draw(st.booleans()):
                op["metadata"] = draw(METADATA_DICT)
            sequence.append(op)
            steps.append(step_id)
        else:  # update_step
            step_id = draw(st.sampled_from(steps))
            op = {
                "type": "update_step",
                "step_id": step_id,
            }
            if draw(st.booleans()):
                op["name"] = draw(SIMPLE_TEXT)
            if draw(st.booleans()):
                op["output"] = draw(OPTIONAL_TEXT)
            if draw(st.booleans()):
                op["metadata"] = draw(METADATA_DICT)
            sequence.append(op)

    return sequence


def _build_step_dict(thread_id: str, op: dict[str, object]) -> dict[str, object]:
    metadata = copy.deepcopy(op.get("metadata", {})) if "metadata" in op else {}

    return {
        "id": op["step_id"],
        "threadId": thread_id,
        "name": op["name"],
        "type": op["step_type"],
        "output": op["output"],
        "input": "",
        "metadata": metadata,
        "tags": None,
        "createdAt": op["created_at"],
        "streaming": False,
        "waitForAnswer": False,
        "isError": False,
        "disableFeedback": False,
    }


def _canonicalize(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
        else:
            return _canonicalize(parsed)
    if isinstance(value, dict):
        return tuple(sorted((k, _canonicalize(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple, set)):
        return tuple(_canonicalize(v) for v in value)
    return value


def _normalize_timestamp(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("+00:00", "Z")
    return value


def _normalize_thread(thread: dict | None) -> dict | None:
    if not thread:
        return None

    tags = tuple(sorted(thread.get("tags") or []))
    steps = []
    for step in thread.get("steps", []):
        steps.append(
            (
                step["id"],
                step.get("name"),
                step.get("type"),
                _canonicalize(step.get("metadata")),
                tuple(sorted(step.get("tags") or [])),
                step.get("output") or "",
                _normalize_timestamp(step.get("createdAt")),
            )
        )
    steps.sort(key=lambda item: item[0])

    return {
        "name": thread.get("name"),
        "metadata": _canonicalize(thread.get("metadata")),
        "tags": tags,
        "steps": tuple(steps),
    }


async def _assert_equivalent_states(
    cassandra_layer,
    sqlalchemy_layer,
    thread_id: str,
    cassandra_user_id: str,
    sql_user_id: str,
) -> None:
    cass_thread = await cassandra_layer.get_thread(thread_id)
    sql_thread = await sqlalchemy_layer.get_thread(thread_id)
    left = _normalize_thread(cass_thread)
    right = _normalize_thread(sql_thread)
    assert left == right, f"Thread mismatch:\nCassandra={left}\nSQLAlchemy={right}"


@pytest.mark.xfail(
    strict=True,
    reason="Cassandra update_thread ignores metadata name override when empty string",
)
@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(operation_sequence=operation_sequences())
def test_cassandra_and_sqlalchemy_equivalence(
    operation_sequence, data_layer, sqlite_data_layer
):
    """Property test: CassandraDataLayer matches SQLAlchemyDataLayer behaviour."""
    asyncio.run(
        _run_equivalence_sequence(
            operation_sequence, data_layer, sqlite_data_layer
        )
    )


async def _run_equivalence_sequence(
    operation_sequence, data_layer, sqlite_data_layer
) -> None:
    user_identifier = f"user-{uuid.uuid4()}"
    base_user = User(identifier=user_identifier, metadata={})

    cassandra_user = await data_layer.create_user(base_user)
    sqlalchemy_user = await sqlite_data_layer.create_user(
        User(identifier=user_identifier, metadata={})
    )

    thread_id = str(uuid.uuid4())
    seed_metadata = {"initial": True}

    await data_layer.update_thread(
        thread_id=thread_id,
        name="Seed Thread",
        user_id=cassandra_user.id,
        metadata=copy.deepcopy(seed_metadata),
    )
    await sqlite_data_layer.update_thread(
        thread_id=thread_id,
        name="Seed Thread",
        user_id=sqlalchemy_user.id,
        metadata=copy.deepcopy(seed_metadata),
    )

    context.session.thread_id = thread_id
    await _assert_equivalent_states(
        data_layer,
        sqlite_data_layer,
        thread_id,
        cassandra_user.id,
        sqlalchemy_user.id,
    )

    step_state: dict[str, dict[str, object]] = {}

    for operation in operation_sequence:
        op_type = operation["type"]

        if op_type == "update_thread":
            kwargs = {}
            if "name" in operation:
                kwargs["name"] = operation["name"]
            if "metadata" in operation:
                kwargs["metadata"] = copy.deepcopy(operation["metadata"])

            await data_layer.update_thread(thread_id=thread_id, **kwargs)
            await sqlite_data_layer.update_thread(thread_id=thread_id, **kwargs)

        elif op_type == "create_step":
            step_dict = _build_step_dict(thread_id, operation)
            await data_layer.create_step(copy.deepcopy(step_dict))
            await sqlite_data_layer.create_step(copy.deepcopy(step_dict))
            step_state[operation["step_id"]] = step_dict

        elif op_type == "update_step":
            step_id = operation["step_id"]
            if step_id not in step_state:
                continue
            updated_step = copy.deepcopy(step_state[step_id])
            if "name" in operation:
                updated_step["name"] = operation["name"]
            if "output" in operation:
                updated_step["output"] = operation["output"]
            if "metadata" in operation:
                updated_step["metadata"] = copy.deepcopy(operation["metadata"])

            await data_layer.update_step(copy.deepcopy(updated_step))
            await sqlite_data_layer.update_step(copy.deepcopy(updated_step))
            step_state[step_id] = updated_step

        context.session.thread_id = thread_id
        await _assert_equivalent_states(
            data_layer,
            sqlite_data_layer,
            thread_id,
            cassandra_user.id,
            sqlalchemy_user.id,
        )
