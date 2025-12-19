from datetime import UTC, datetime

from chainlit_cassandra_data_layer.data import (
    smallest_uuid7_for_datetime,
    uuid7,
    uuid7_to_datetime,
)


def test_uuid7_to_datetime():
    dt = datetime(year=2025, month=11, day=11, tzinfo=UTC)
    id = uuid7(time_ms=int(dt.timestamp() * 1000))
    assert uuid7_to_datetime(id) == dt


def test_smallest_uuid7_for_datetime():
    dt = datetime(year=2025, month=11, day=11, tzinfo=UTC)
    smallest_uuid = smallest_uuid7_for_datetime(dt)
    assert uuid7_to_datetime(smallest_uuid) == dt
