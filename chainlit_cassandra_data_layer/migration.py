"""
Migration system for Cassandra checkpoint saver.
"""

from __future__ import annotations

import hashlib
import logging
import string
import time
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

from cassandra.cluster import Session

logger = logging.getLogger(__name__)

DEFAULT_KEYSPACE = "chainlit"
MIGRATION_LOCK_TTL = 300  # 5 minutes
MIGRATION_LOCK_KEY = "schema_migration_lock"
BASE_MODULE_NAME = "chainlit_cassandra_data_layer"


@dataclass
class Migration:
    """Represents a single migration."""

    version: int
    name: str
    description: str
    checksum: str
    statements: list[str]


def _compute_migration_checksum(statements: list[str]) -> str:
    """
    Compute a SHA256 checksum for migration statements.

    Args:
        statements: List of SQL statements (should be raw templates, not formatted)

    Returns:
        Hexadecimal checksum string
    """
    content = "\n".join(statements)
    return hashlib.sha256(content.encode()).hexdigest()


class MigrationManager:
    def __init__(
        self,
        session: Session,
        keyspace: str = DEFAULT_KEYSPACE,
        replication_factor: int = 3,
    ):
        """
        Initialize the migration manager.

        Args:
            session: Cassandra session
            keyspace: Keyspace name (defaults to session's keyspace)
            replication_factor: Replication factor for the keyspace (default: 3, use 1 for single-node clusters)
            checkpoint_id_type: Type for checkpoint_id column ("uuid" [default], "text")
            thread_id_type: Type for thread_id column ("uuid" [default], "text")
        """
        self.session = session
        self.keyspace = keyspace if keyspace is not None else session.keyspace
        if not self.keyspace:
            raise ValueError(
                "MigrationManager requires a target keyspace. "
                "Provide one explicitly or connect the session to a keyspace."
            )
        self.replication_factor = replication_factor
        self.migrations: list[Migration] = self._load_migrations(
            migrations_template=self._get_default_migrations_template(),
            template_params={
                "keyspace": self.keyspace,
                "replication_factor": replication_factor,
            },
        )

    def _ensure_migration_tables(self) -> None:
        """Ensure the migration tracking tables exist."""
        # Create keyspace if it doesn't exist
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {self.replication_factor}}}
        """)

        # Create migrations table if it doesn't exist
        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.migrations (
                version INT PRIMARY KEY,
                name TEXT,
                description TEXT,
                checksum TEXT,
                applied_at TIMESTAMP,
                execution_time_ms INT
            )
        """)

        # Create migration lock table if it doesn't exist
        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.migration_locks (
                lock_key TEXT PRIMARY KEY,
                locked_at TIMESTAMP,
                locked_by TEXT,
                expires_at TIMESTAMP
            ) WITH default_time_to_live = {MIGRATION_LOCK_TTL}
        """)

    def _acquire_lock(self, lock_id: str = "migration_runner") -> bool:
        """
        Acquire a distributed lock for migrations.

        Uses lightweight transactions (LWT) with TTL to prevent indefinite locks.

        Args:
            lock_id: Identifier for who is acquiring the lock

        Returns:
            True if lock was acquired, False otherwise
        """
        now = datetime.now(UTC)
        expires_at = datetime.fromtimestamp(
            now.timestamp() + MIGRATION_LOCK_TTL, tz=UTC
        )

        # Try to acquire lock using LWT
        result = self.session.execute(
            f"""
            INSERT INTO {self.keyspace}.migration_locks
            (lock_key, locked_at, locked_by, expires_at)
            VALUES (%s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (MIGRATION_LOCK_KEY, now, lock_id, expires_at),
        )

        # Check if the lock was acquired
        row = result.one()
        return row.applied if row else False

    def _release_lock(self) -> None:
        """Release the migration lock."""
        self.session.execute(
            f"DELETE FROM {self.keyspace}.migration_locks WHERE lock_key = %s",
            (MIGRATION_LOCK_KEY,),
        )

    def _get_applied_migrations(self) -> set[int]:
        """Get the set of already applied migration versions."""
        try:
            result = self.session.execute(
                f"SELECT version FROM {self.keyspace}.migrations"
            )
            return {row.version for row in result}
        except Exception as e:
            logger.debug(f"Could not get applied migrations: {e}")
            raise

    def _record_migration(
        self,
        migration: Migration,
        execution_time_ms: int,
    ) -> None:
        """Record a successfully applied migration."""
        now = datetime.now(UTC)

        self.session.execute(
            f"""
            INSERT INTO {self.keyspace}.migrations
            (version, name, description, checksum, applied_at, execution_time_ms)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                migration.version,
                migration.name,
                migration.description,
                migration.checksum,
                now,
                execution_time_ms,
            ),
        )

    def _get_default_migrations_template(self) -> Any:
        migrations_path = Path(__file__).with_name("migrations.toml")
        if migrations_path.is_file():
            with migrations_path.open("rb") as fp:
                migrations_data = tomllib.load(fp)
        else:
            try:
                migrations_resource = resources.files(
                    BASE_MODULE_NAME
                ).joinpath("migrations.toml")
            except FileNotFoundError as exc:  # pragma: no cover - defensive
                raise FileNotFoundError(
                    "migrations.toml not found alongside package or in resources"
                ) from exc
            with migrations_resource.open("rb") as fp:
                migrations_data = tomllib.load(fp)
        return migrations_data

    def _load_migrations(
        self, migrations_template: Any, template_params: Mapping
    ) -> list[Migration]:
        """
        Load default embedded migrations.

        Migrations are defined in DEFAULT_MIGRATIONS and automatically
        formatted with keyspace name and column types.
        """
        logger.debug("Loading default embedded migrations")

        raw_migrations = migrations_template.get("migration") or []
        migrations = []

        for migration_def in raw_migrations:
            if "statements" in migration_def:
                # Keep raw statements for checksum calculation
                raw_statements = migration_def["statements"]

                # Format statements for execution
                formatted_statements = [
                    string.Template(stmt).safe_substitute(**template_params)
                    for stmt in raw_statements
                ]

                checksum = _compute_migration_checksum(formatted_statements)

                # Create modified migration_def with formatted statements
                formatted_migration_def = {
                    **migration_def,
                    "statements": formatted_statements,
                }
            else:
                checksum = ""
                formatted_migration_def = migration_def

            migration = Migration(**formatted_migration_def, checksum=checksum)
            migrations.append(migration)

        # Sort by version
        migrations.sort(key=lambda m: m.version)
        logger.info(f"Loaded {len(migrations)} migration(s)")
        return migrations

    def get_pending_migrations(self) -> list[Migration]:
        """Get the list of pending migrations that need to be applied."""
        applied = self._get_applied_migrations()
        return [m for m in self.migrations if m.version not in applied]

    def apply_migration(self, migration: Migration) -> None:
        """
        Apply a single migration.

        Args:
            migration: The migration to apply
        """
        start_time = time.time()
        logger.info(f"Applying migration {migration.version}: {migration.name}")

        try:
            if migration.statements:
                # Apply CQL statements
                for i, statement in enumerate(migration.statements, 1):
                    logger.debug(
                        f"  Executing statement {i}/{len(migration.statements)}"
                    )
                    self.session.execute(statement)

            else:
                logger.warning(
                    f"Migration {migration.version} has no statements or function"
                )

            # Wait for schema agreement (with timeout)
            if not self.session.cluster.control_connection.wait_for_schema_agreement():
                logger.warning("Schema agreement not reached, but continuing anyway")

            # Record the migration
            execution_time_ms = int((time.time() - start_time) * 1000)
            self._record_migration(migration, execution_time_ms)

            logger.info(
                f"✓ Applied migration {migration.version} in {execution_time_ms}ms"
            )

        except Exception as e:
            logger.error(f"✗ Failed to apply migration {migration.version}: {e}")
            raise

    def migrate(
        self, lock_id: str = "migration_runner", max_wait_seconds: int = 60
    ) -> int:
        """
        Apply all pending migrations.

        Args:
            lock_id: Identifier for who is running the migration
            max_wait_seconds: Maximum time to wait for lock acquisition

        Returns:
            Number of migrations applied

        Raises:
            RuntimeError: If unable to acquire lock or migrations fail
        """
        # Ensure migration tables exist
        self._ensure_migration_tables()

        # Get pending migrations (initial check)
        pending = self.get_pending_migrations()

        if not pending:
            logger.info("No pending migrations")
            return 0

        logger.info(f"Found {len(pending)} pending migration(s)")

        # Try to acquire lock with retry
        start_wait = time.time()
        acquired = False

        while time.time() - start_wait < max_wait_seconds:
            if self._acquire_lock(lock_id):
                acquired = True
                break
            logger.debug("Waiting for migration lock...")
            time.sleep(1)

        if not acquired:
            raise RuntimeError(
                f"Could not acquire migration lock after {max_wait_seconds} seconds"
            )

        try:
            # Re-check pending migrations after acquiring lock
            # (another process may have applied them while we waited)
            pending = self.get_pending_migrations()

            if not pending:
                logger.info(
                    "No pending migrations (already applied by another process)"
                )
                return 0

            # Apply migrations
            for migration in pending:
                self.apply_migration(migration)

            logger.info(f"Successfully applied {len(pending)} migration(s)")
            return len(pending)

        finally:
            # Always release lock
            self._release_lock()

    def get_current_version(self) -> int | None:
        """Get the current schema version (highest applied migration)."""
        applied = self._get_applied_migrations()
        return max(applied) if applied else None

    def validate_migrations(self) -> bool:
        """
        Validate that applied migrations match their recorded checksums.

        Returns:
            True if all migrations are valid, False otherwise
        """
        try:
            result = self.session.execute(
                f"""
                SELECT version, name, checksum
                FROM {self.keyspace}.migrations
                """
            )

            applied = {row.version: (row.name, row.checksum) for row in result}

            for migration in self.migrations:
                if migration.version in applied:
                    recorded_name, recorded_checksum = applied[migration.version]

                    if migration.checksum != recorded_checksum:
                        logger.error(
                            f"Migration {migration.version} checksum mismatch! "
                            f"Expected {recorded_checksum}, got {migration.checksum}"
                        )
                        return False

            logger.info("All migration checksums are valid")
            return True

        except Exception as e:
            logger.error(f"Failed to validate migrations: {e}")
            return False
