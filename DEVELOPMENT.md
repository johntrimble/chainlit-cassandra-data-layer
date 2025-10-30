# Development Guide

This guide covers how to set up a development environment for contributing to chainlit-cassandra-data-layer.

## Prerequisites

- Docker and Docker Compose
- Git
- Python 3.12+

## Development Environment Setup

### 1. Bootstrap the Environment

Clone the repository and run the bootstrap script:

```bash
git clone https://github.com/johntrimble/chainlit-cassandra-data-layer.git
cd chainlit-cassandra-data-layer
./script/bootstrap
```

The bootstrap script will:
- Setup a `.env` file for development

### 2. Development Container (VSCode)

If using VSCode, the project includes a devcontainer configuration:

1. Open the project in VSCode
2. When prompted, click "Reopen in Container"
3. VSCode will build and start the development container
4. All dependencies will be pre-installed

Alternatively, manually reopen:
- Press `Cmd/Ctrl+Shift+P`
- Select "Dev Containers: Reopen in Container"

## Connecting to Cassandra

### From Inside Docker Network

The Cassandra cluster is available at `cassandra:9042` within the Docker network:

```python
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect()
```

### Verify Connection

```python
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect()

# Test the connection
rows = session.execute("SELECT release_version FROM system.local")
version = rows[0].release_version
print(f"Connected to Cassandra version: {version}")
```

### CQL Shell Access

Access the Cassandra CQL shell:

```bash
cqlsh cassandra
```

## Running Tests

All commands in this section assume you run them from within the dev container (for example, after `docker compose exec dev /bin/bash`). The `dev` service depends on Cassandra, so the database will already be running.

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/test_data_layer.py
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Changes

Edit code in `chainlit_cassandra_data_layer/` or add tests in `tests/`.

### 3. Run Tests

```bash
pytest
```

### 4. Format Code

```bash
ruff check --fix
ruff format
```

### 5. Commit Changes

```bash
git add .
git commit -m "Description of changes"
```

### 6. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

### Adding Schema Changes

Schema changes should be made through the migration system:

1. Add a new migration to the `chainlit_cassandra_data_layer/migrations.toml` directory
2. Update migration version number
3. Test the migration thoroughly
4. Update documentation in relevant docs files

Note: The migration system is internal - users just call `.setup()` which applies all migrations automatically.
