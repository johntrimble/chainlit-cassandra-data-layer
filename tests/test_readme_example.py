import os
import re
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_readme_usage_example():
    # Clean up any existing keyspace
    from cassandra_asyncio.cluster import Cluster

    host = os.environ.get("CASSANDRA_HOST", "cassandra")
    cluster = Cluster(contact_points=[host])
    session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS chainlit")
    session.shutdown()
    cluster.shutdown()

    # Set environment variable before executing README code
    os.environ.setdefault("CASSANDRA_HOST", host)

    # Extract and execute the README example code
    readme_path = Path(__file__).resolve().parent.parent / "README.md"
    readme_text = readme_path.read_text(encoding="utf-8")

    code_block_match = re.search(
        r"<!-- EXAMPLE APP -->\s*```python\n(?P<code>.*?)\n```",
        readme_text,
        flags=re.DOTALL,
    )
    assert code_block_match, "README usage example code block not found."
    usage_code = code_block_match.group("code")

    # Execute the README code in a namespace
    namespace = {}
    exec(usage_code, namespace)

    # Verify the data layer was set up correctly
    from chainlit.data import get_data_layer

    from chainlit_cassandra_data_layer.data import CassandraDataLayer

    data_layer = get_data_layer()
    assert isinstance(data_layer, CassandraDataLayer)
    assert data_layer.keyspace == "chainlit"

    # Test the on_chat_start handler
    class DummyMessage:
        sent_contents = []

        def __init__(self, content, **kwargs):
            self.content = content
            self.kwargs = kwargs

        async def send(self):
            self.__class__.sent_contents.append(self.content)

    with patch("chainlit.Message", DummyMessage):
        await namespace["on_chat_start"]()
    assert DummyMessage.sent_contents == ["Hello from Cassandra-backed Chainlit!"]

    # Clean up
    await data_layer.close()

    cluster = Cluster(contact_points=[host])
    session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS chainlit")
    session.shutdown()
    cluster.shutdown()
