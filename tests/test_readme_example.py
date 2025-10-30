import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


def test_readme_usage_example(tmp_path):
    readme_path = Path(__file__).resolve().parent.parent / "README.md"
    readme_text = readme_path.read_text(encoding="utf-8")

    code_block_match = re.search(
        r"<!-- EXAMPLE APP -->\s*```python\n(?P<code>.*?)\n```",
        readme_text,
        flags=re.DOTALL,
    )
    assert code_block_match, "README usage example code block not found."
    usage_code = code_block_match.group("code")

    app_dir = tmp_path / "readme_app"
    site_packages = tmp_path / "site-packages"
    app_dir.mkdir()
    site_packages.mkdir()

    app_file = app_dir / "app.py"
    app_file.write_text(usage_code + "\n", encoding="utf-8")

    package_src = Path(__file__).resolve().parent.parent / "chainlit_cassandra_data_layer"
    package_dst = site_packages / "chainlit_cassandra_data_layer"
    shutil.copytree(package_src, package_dst)

    dist_info = site_packages / "chainlit_cassandra_data_layer-0.0.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Name: chainlit-cassandra-data-layer\nVersion: 0.0.0\n",
        encoding="utf-8",
    )
    (dist_info / "top_level.txt").write_text(
        "chainlit_cassandra_data_layer\n",
        encoding="utf-8",
    )

    script = f"""
import asyncio
import os
import sys
from unittest.mock import patch

sys.path.insert(0, r"{site_packages}")
sys.path.insert(0, r"{app_dir}")

from cassandra_asyncio.cluster import Cluster

host = os.environ["CASSANDRA_HOST"]
cluster = Cluster(contact_points=[host])
session = cluster.connect()
session.execute("DROP KEYSPACE IF EXISTS chainlit")
session.shutdown()
cluster.shutdown()

import app  # noqa: F401

from chainlit.data import get_data_layer
from chainlit_cassandra_data_layer.data import CassandraDataLayer

data_layer = get_data_layer()
assert isinstance(data_layer, CassandraDataLayer)
assert data_layer.keyspace == "chainlit"

class DummyMessage:
    sent_contents = []

    def __init__(self, content, **kwargs):
        self.content = content
        self.kwargs = kwargs

    async def send(self):
        self.__class__.sent_contents.append(self.content)

async def run_on_chat_start():
    with patch("chainlit.Message", DummyMessage):
        await app.on_chat_start()  # type: ignore[attr-defined]
    assert DummyMessage.sent_contents == ["Hello from Cassandra-backed Chainlit!"]

asyncio.run(run_on_chat_start())

async def cleanup():
    await data_layer.close()
asyncio.run(cleanup())

cluster = Cluster(contact_points=[host])
session = cluster.connect()
session.execute("DROP KEYSPACE IF EXISTS chainlit")
session.shutdown()
cluster.shutdown()
"""

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(
            None,
            [
                str(site_packages),
                str(app_dir),
                env.get("PYTHONPATH", ""),
            ],
        )
    )
    env["CASSANDRA_HOST"] = "cassandra"

    subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        check=True,
        cwd=str(app_dir),
    )
