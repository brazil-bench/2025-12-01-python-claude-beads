"""Neo4j database connection and operations for Brazilian Soccer Knowledge Graph."""

import os
from contextlib import contextmanager
from typing import Any, Generator, Optional

from neo4j import GraphDatabase, Driver, Session


class Neo4jDatabase:
    """Neo4j database connection manager."""

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        self.user = user or os.getenv("NEO4J_USER", "neo4j")
        self.password = password or os.getenv("NEO4J_PASSWORD", "password")
        self._driver: Optional[Driver] = None

    def connect(self) -> None:
        """Establish connection to Neo4j database."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
            )

    def close(self) -> None:
        """Close database connection."""
        if self._driver:
            self._driver.close()
            self._driver = None

    @property
    def driver(self) -> Driver:
        """Get the database driver, connecting if necessary."""
        if self._driver is None:
            self.connect()
        return self._driver  # type: ignore

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Create a database session context manager."""
        session = self.driver.session()
        try:
            yield session
        finally:
            session.close()

    def execute_query(
        self, query: str, parameters: Optional[dict[str, Any]] = None
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query and return results."""
        with self.session() as session:
            result = session.run(query, parameters or {})
            return [dict(record) for record in result]

    def execute_write(
        self, query: str, parameters: Optional[dict[str, Any]] = None
    ) -> None:
        """Execute a write query."""
        with self.session() as session:
            session.run(query, parameters or {})

    def clear_database(self) -> None:
        """Clear all data from the database."""
        self.execute_write("MATCH (n) DETACH DELETE n")

    def create_constraints(self) -> None:
        """Create uniqueness constraints for node types."""
        constraints = [
            "CREATE CONSTRAINT player_id IF NOT EXISTS FOR (p:Player) REQUIRE p.player_id IS UNIQUE",
            "CREATE CONSTRAINT team_id IF NOT EXISTS FOR (t:Team) REQUIRE t.team_id IS UNIQUE",
            "CREATE CONSTRAINT match_id IF NOT EXISTS FOR (m:Match) REQUIRE m.match_id IS UNIQUE",
            "CREATE CONSTRAINT competition_id IF NOT EXISTS FOR (c:Competition) REQUIRE c.competition_id IS UNIQUE",
            "CREATE CONSTRAINT stadium_id IF NOT EXISTS FOR (s:Stadium) REQUIRE s.stadium_id IS UNIQUE",
            "CREATE CONSTRAINT coach_id IF NOT EXISTS FOR (c:Coach) REQUIRE c.coach_id IS UNIQUE",
        ]
        for constraint in constraints:
            try:
                self.execute_write(constraint)
            except Exception:
                pass  # Constraint may already exist

    def create_indexes(self) -> None:
        """Create indexes for commonly queried properties."""
        indexes = [
            "CREATE INDEX player_name IF NOT EXISTS FOR (p:Player) ON (p.name)",
            "CREATE INDEX team_name IF NOT EXISTS FOR (t:Team) ON (t.name)",
            "CREATE INDEX match_date IF NOT EXISTS FOR (m:Match) ON (m.date)",
            "CREATE INDEX competition_name IF NOT EXISTS FOR (c:Competition) ON (c.name)",
            "CREATE INDEX competition_season IF NOT EXISTS FOR (c:Competition) ON (c.season)",
        ]
        for index in indexes:
            try:
                self.execute_write(index)
            except Exception:
                pass  # Index may already exist
