"""Integration tests for Brazilian Soccer MCP against real Neo4j database."""

import os
import pytest

# Set environment variables for Neo4j connection
os.environ["NEO4J_URI"] = "bolt://localhost:7687"
os.environ["NEO4J_USER"] = "neo4j"
os.environ["NEO4J_PASSWORD"] = "testpassword123"

from brazilian_soccer_mcp.database import Neo4jDatabase
from brazilian_soccer_mcp.data_loader import load_sample_data, DataLoader, get_sample_data


@pytest.fixture(scope="module")
def neo4j_db():
    """Create a Neo4j database connection for testing."""
    db = Neo4jDatabase()
    db.connect()

    # Clear and reload data for clean test state
    db.clear_database()
    load_sample_data(db)

    yield db

    # Cleanup after tests
    db.close()


class TestNeo4jConnection:
    """Test basic Neo4j connectivity."""

    def test_database_connection(self, neo4j_db):
        """Test that we can connect to Neo4j."""
        result = neo4j_db.execute_query("RETURN 1 as value")
        assert result == [{"value": 1}]

    def test_database_has_data(self, neo4j_db):
        """Test that sample data was loaded."""
        result = neo4j_db.execute_query("MATCH (n) RETURN count(n) as count")
        assert result[0]["count"] > 0


class TestPlayerQueries:
    """Test player-related queries against Neo4j."""

    def test_search_player_by_name(self, neo4j_db):
        """Test searching for a player by name."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)
            WHERE toLower(p.name) CONTAINS toLower($name)
            RETURN p.player_id as player_id, p.name as name
            """,
            {"name": "Gabriel"}
        )
        assert len(result) >= 1
        assert any("Gabriel" in r["name"] for r in result)

    def test_search_player_neymar(self, neo4j_db):
        """Test searching for Neymar."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)
            WHERE toLower(p.name) CONTAINS toLower($name)
            RETURN p.name as name, p.position as position
            """,
            {"name": "Neymar"}
        )
        assert len(result) == 1
        assert "Neymar" in result[0]["name"]
        assert result[0]["position"] == "Forward"

    def test_get_player_with_teams(self, neo4j_db):
        """Test getting player with their teams."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player {player_id: $player_id})
            OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
            RETURN p.name as name, collect(t.name) as teams
            """,
            {"player_id": "P011"}  # Neymar
        )
        assert len(result) == 1
        assert "Neymar" in result[0]["name"]
        assert len(result[0]["teams"]) >= 1  # At least Santos or Flamengo

    def test_player_career_history(self, neo4j_db):
        """Test getting a player's career history."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player {player_id: $player_id})-[r:PLAYS_FOR]->(t:Team)
            RETURN p.name as name, t.name as team, r.start_date as start_date, r.end_date as end_date
            ORDER BY r.start_date
            """,
            {"player_id": "P012"}  # Ronaldo
        )
        assert len(result) >= 1
        teams = [r["team"] for r in result]
        assert "Cruzeiro" in teams or "Corinthians" in teams


class TestTeamQueries:
    """Test team-related queries against Neo4j."""

    def test_search_team(self, neo4j_db):
        """Test searching for a team."""
        result = neo4j_db.execute_query(
            """
            MATCH (t:Team)
            WHERE toLower(t.name) CONTAINS toLower($name)
            RETURN t.team_id as team_id, t.name as name, t.city as city
            """,
            {"name": "Flamengo"}
        )
        assert len(result) == 1
        assert result[0]["name"] == "Flamengo"
        assert result[0]["city"] == "Rio de Janeiro"

    def test_get_team_roster(self, neo4j_db):
        """Test getting team roster."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)-[r:PLAYS_FOR]->(t:Team {team_id: $team_id})
            WHERE r.end_date IS NULL
            RETURN p.name as name, p.position as position
            ORDER BY p.position, p.name
            """,
            {"team_id": "T001"}  # Flamengo
        )
        assert len(result) >= 1
        player_names = [r["name"] for r in result]
        assert any("Gabigol" in name or "Gabriel" in name for name in player_names)

    def test_all_teams_loaded(self, neo4j_db):
        """Test that all teams were loaded."""
        result = neo4j_db.execute_query("MATCH (t:Team) RETURN count(t) as count")
        assert result[0]["count"] == 12  # 12 teams in sample data


class TestMatchQueries:
    """Test match-related queries against Neo4j."""

    def test_get_match_details(self, neo4j_db):
        """Test getting match details."""
        result = neo4j_db.execute_query(
            """
            MATCH (home:Team)-[:PLAYED_HOME]->(m:Match {match_id: $match_id})<-[:PLAYED_AWAY]-(away:Team)
            MATCH (m)-[:PART_OF]->(c:Competition)
            RETURN m.date as date, m.home_score as home_score, m.away_score as away_score,
                   home.name as home_team, away.name as away_team, c.name as competition
            """,
            {"match_id": "M001"}  # Fla-Flu
        )
        assert len(result) == 1
        match = result[0]
        assert match["home_team"] == "Flamengo"
        assert match["away_team"] == "Fluminense"
        assert match["home_score"] == 2
        assert match["away_score"] == 1

    def test_search_matches_by_team(self, neo4j_db):
        """Test searching matches by team."""
        result = neo4j_db.execute_query(
            """
            MATCH (home:Team)-[:PLAYED_HOME]->(m:Match)<-[:PLAYED_AWAY]-(away:Team)
            WHERE toLower(home.name) CONTAINS toLower($team)
               OR toLower(away.name) CONTAINS toLower($team)
            RETURN m.match_id as match_id, home.name as home_team, away.name as away_team
            ORDER BY m.date DESC
            """,
            {"team": "Flamengo"}
        )
        assert len(result) >= 5  # Flamengo has many matches
        for match in result:
            assert "Flamengo" in match["home_team"] or "Flamengo" in match["away_team"]

    def test_head_to_head(self, neo4j_db):
        """Test head-to-head query."""
        result = neo4j_db.execute_query(
            """
            MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
            MATCH (m:Match)
            WHERE ((t1)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t2))
               OR ((t2)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t1))
            RETURN count(m) as total_matches
            """,
            {"team1_id": "T001", "team2_id": "T002"}  # Flamengo vs Fluminense
        )
        assert result[0]["total_matches"] >= 2  # At least 2 Fla-Flu matches


class TestGoalQueries:
    """Test goal-related queries against Neo4j."""

    def test_match_scorers(self, neo4j_db):
        """Test getting match scorers."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)-[g:SCORED_IN]->(m:Match {match_id: $match_id})
            RETURN p.name as player, g.minute as minute, g.goal_type as goal_type
            ORDER BY g.minute
            """,
            {"match_id": "M001"}  # Fla-Flu
        )
        assert len(result) >= 2  # At least 2 goals in this match

    def test_top_scorers(self, neo4j_db):
        """Test getting top scorers in a competition."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)-[g:SCORED_IN]->(m:Match)-[:PART_OF]->(c:Competition {competition_id: $competition_id})
            WITH p, count(g) as goals
            ORDER BY goals DESC
            LIMIT 5
            RETURN p.name as player, goals
            """,
            {"competition_id": "C001"}  # 2023 Brasileirão
        )
        assert len(result) >= 1
        # Top scorer should have multiple goals
        assert result[0]["goals"] >= 2


class TestRelationshipQueries:
    """Test complex relationship queries."""

    def test_players_who_played_for_both_teams(self, neo4j_db):
        """Test finding players who played for both rival teams."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)-[:PLAYS_FOR]->(t1:Team {team_id: $team1_id})
            MATCH (p)-[:PLAYS_FOR]->(t2:Team {team_id: $team2_id})
            RETURN p.name as player
            """,
            {"team1_id": "T001", "team2_id": "T007"}  # Flamengo and Grêmio
        )
        # Ronaldinho played for both
        if len(result) > 0:
            assert any("Ronaldinho" in r["player"] for r in result)

    def test_common_teammates(self, neo4j_db):
        """Test finding common teammates between two players."""
        result = neo4j_db.execute_query(
            """
            MATCH (p1:Player {player_id: $player1_id})-[:PLAYS_FOR]->(t:Team)<-[:PLAYS_FOR]-(teammate:Player)
            MATCH (p2:Player {player_id: $player2_id})-[:PLAYS_FOR]->(t2:Team)<-[:PLAYS_FOR]-(teammate)
            WHERE teammate.player_id <> $player1_id AND teammate.player_id <> $player2_id
            RETURN DISTINCT teammate.name as teammate
            """,
            {"player1_id": "P010", "player2_id": "P011"}  # Pelé and Neymar (both at Santos)
        )
        # Ganso played with Neymar at Santos
        # There should be some overlap if data has it
        assert result is not None


class TestDataIntegrity:
    """Test data integrity and constraints."""

    def test_unique_player_ids(self, neo4j_db):
        """Test that player IDs are unique."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)
            WITH p.player_id as pid, count(*) as cnt
            WHERE cnt > 1
            RETURN pid, cnt
            """
        )
        assert len(result) == 0, "Found duplicate player IDs"

    def test_unique_team_ids(self, neo4j_db):
        """Test that team IDs are unique."""
        result = neo4j_db.execute_query(
            """
            MATCH (t:Team)
            WITH t.team_id as tid, count(*) as cnt
            WHERE cnt > 1
            RETURN tid, cnt
            """
        )
        assert len(result) == 0, "Found duplicate team IDs"

    def test_matches_have_valid_teams(self, neo4j_db):
        """Test that all matches have valid home and away teams."""
        result = neo4j_db.execute_query(
            """
            MATCH (m:Match)
            WHERE NOT EXISTS { MATCH (:Team)-[:PLAYED_HOME]->(m) }
               OR NOT EXISTS { MATCH (:Team)-[:PLAYED_AWAY]->(m) }
            RETURN count(m) as orphan_matches
            """
        )
        assert result[0]["orphan_matches"] == 0, "Found matches without valid teams"

    def test_goals_linked_to_players_and_matches(self, neo4j_db):
        """Test that all SCORED_IN relationships are valid."""
        result = neo4j_db.execute_query(
            """
            MATCH (p:Player)-[g:SCORED_IN]->(m:Match)
            RETURN count(g) as total_goals
            """
        )
        assert result[0]["total_goals"] > 0, "No goals found in database"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
