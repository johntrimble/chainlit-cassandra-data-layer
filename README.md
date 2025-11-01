# Chainlit Cassandra Data Layer

This project implements the Chainlit `BaseDataLayer` for Apache Cassandra.


## Installation

```bash
pip install chainlit-cassandra-data-layer
```

You’ll also need a running Cassandra or DataStax Astra instance reachable from your Chainlit app.


## Usage

<!-- EXAMPLE APP -->
```python
# app.py
import os

import chainlit as cl
from cassandra_asyncio.cluster import Cluster
from chainlit_cassandra_data_layer.data import CassandraDataLayer

@cl.data_layer
def configure_data_layer() -> CassandraDataLayer:
    contact_points = [os.getenv("CASSANDRA_HOST", "127.0.0.1")]
    replication_factor = int(os.getenv("CASSANDRA_REPLICATION_FACTOR", "1"))
    cluster = Cluster(contact_points=contact_points)
    session = cluster.connect()
    layer = CassandraDataLayer(session=session, keyspace="chainlit")
    layer.setup(replication_factor=replication_factor)  # creates keyspace/tables on first run
    return layer

@cl.on_chat_start
async def on_chat_start():
    await cl.Message(content="Hello from Cassandra-backed Chainlit!").send()
```

Run `chainlit run app.py` once your Cassandra node is reachable (set `CASSANDRA_HOST` if it’s not `127.0.0.1`).


## Implementation Notes

- `list_threads()` requires `filters.userId` and can’t currently filter by feedback.
- Search only inspects `thread_name` and performs a case-insensitive substring match; the scan runs over the user’s entire partition, so expect higher latency with many threads.


## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for information on setting up a development environment, running tests, and contributing.


## License

MIT
