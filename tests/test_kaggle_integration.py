"""Integration tests for Kaggle data loading into Neo4j."""

import os
import pytest

# Set environment variables for Neo4j connection
os.environ["NEO4J_URI"] = "bolt://localhost:7687"
os.environ["NEO4J_USER"] = "neo4j"
os.environ["NEO4J_PASSWORD"] = "testpassword123"

from brazilian_soccer_mcp.database import Neo4jDatabase
from brazilian_soccer_mcp.kaggle_loader import load_kaggle_data


@pytest.fixture(scope="module")
def neo4j_db():
    """Create a Neo4j database connection and load Kaggle data."""
    db = Neo4jDatabase()
    db.connect()

    # Clear database and load fresh Kaggle data
    db.clear_database()
    stats = load_kaggle_data(db)
    print(f"\nLoaded data: {stats}")

    yield db, stats

    db.close()


class TestKaggleDataLoading:
    """Test that Kaggle data is loaded correctly."""

    def test_teams_loaded(self, neo4j_db):
        """Test that teams are loaded from Kaggle data."""
        db, stats = neo4j_db
        result = db.execute_query("MATCH (t:Team) RETURN count(t) as count")
        assert result[0]["count"] >= 10, "Expected at least 10 teams"
        assert stats["teams"] >= 10

    def test_competitions_loaded(self, neo4j_db):
        """Test that competitions are loaded."""
        db, stats = neo4j_db
        result = db.execute_query("MATCH (c:Competition) RETURN count(c) as count")
        assert result[0]["count"] >= 10, "Expected at least 10 competition-seasons"
        assert stats["competitions"] >= 10

    def test_matches_loaded(self, neo4j_db):
        """Test that matches are loaded from CSV files."""
        db, stats = neo4j_db
        result = db.execute_query("MATCH (m:Match) RETURN count(m) as count")
        assert result[0]["count"] >= 100, "Expected at least 100 matches"
        assert stats["matches"] >= 100

    def test_players_loaded(self, neo4j_db):
        """Test that players are loaded from FIFA data."""
        db, stats = neo4j_db
        result = db.execute_query("MATCH (p:Player) RETURN count(p) as count")
        assert result[0]["count"] >= 50, "Expected at least 50 players"

    def test_contracts_loaded(self, neo4j_db):
        """Test that player-team relationships exist."""
        db, stats = neo4j_db
        result = db.execute_query(
            "MATCH (:Player)-[r:PLAYS_FOR]->(:Team) RETURN count(r) as count"
        )
        assert result[0]["count"] >= 50, "Expected at least 50 contracts"


class TestKaggleTeamData:
    """Test team data from Kaggle."""

    def test_flamengo_exists(self, neo4j_db):
        """Test that Flamengo is loaded with correct data."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (t:Team)
            WHERE t.name CONTAINS 'Flamengo'
            RETURN t.name as name, t.city as city, t.stadium as stadium
            """
        )
        assert len(result) == 1
        assert result[0]["city"] == "Rio de Janeiro"

    def test_all_major_teams_exist(self, neo4j_db):
        """Test that all major Brazilian teams are loaded."""
        db, _ = neo4j_db
        major_teams = [
            "Flamengo", "Corinthians", "Palmeiras", "Santos",
            "São Paulo", "Grêmio", "Internacional", "Cruzeiro"
        ]
        for team_name in major_teams:
            result = db.execute_query(
                "MATCH (t:Team) WHERE t.name CONTAINS $name RETURN t.name as name",
                {"name": team_name}
            )
            assert len(result) >= 1, f"Team {team_name} not found"


class TestKaggleMatchData:
    """Test match data from Kaggle."""

    def test_matches_have_scores(self, neo4j_db):
        """Test that matches have score information."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (m:Match)
            WHERE m.home_score IS NOT NULL AND m.away_score IS NOT NULL
            RETURN count(m) as count
            """
        )
        assert result[0]["count"] >= 100

    def test_matches_have_teams(self, neo4j_db):
        """Test that matches are linked to teams."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (home:Team)-[:PLAYED_HOME]->(m:Match)<-[:PLAYED_AWAY]-(away:Team)
            RETURN count(m) as count
            """
        )
        assert result[0]["count"] >= 100

    def test_matches_have_dates(self, neo4j_db):
        """Test that matches have date information."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (m:Match)
            WHERE m.date IS NOT NULL
            RETURN count(m) as count
            """
        )
        assert result[0]["count"] >= 100

    def test_brasileirao_matches_exist(self, neo4j_db):
        """Test that Brasileirão matches are loaded."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (m:Match)-[:PART_OF]->(c:Competition)
            WHERE c.name CONTAINS 'Brasileiro'
            RETURN count(m) as count
            """
        )
        assert result[0]["count"] >= 50


class TestKagglePlayerData:
    """Test player data from Kaggle FIFA CSVs."""

    def test_players_have_names(self, neo4j_db):
        """Test that players have name information."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (p:Player)
            WHERE p.name IS NOT NULL AND p.name <> ''
            RETURN count(p) as count
            """
        )
        assert result[0]["count"] >= 50

    def test_players_have_positions(self, neo4j_db):
        """Test that players have position information."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (p:Player)
            WHERE p.position IS NOT NULL
            RETURN count(p) as count
            """
        )
        assert result[0]["count"] >= 40

    def test_players_linked_to_teams(self, neo4j_db):
        """Test that players are linked to Brazilian teams."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            RETURN count(DISTINCT p) as player_count, count(DISTINCT t) as team_count
            """
        )
        assert result[0]["player_count"] >= 50
        assert result[0]["team_count"] >= 5

    def test_brazilian_players_exist(self, neo4j_db):
        """Test that Brazilian nationality players exist."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (p:Player)
            WHERE p.nationality = 'Brazil'
            RETURN count(p) as count
            """
        )
        # Most players in Brazilian clubs are Brazilian
        assert result[0]["count"] >= 20


class TestKaggleComplexQueries:
    """Test complex queries on Kaggle data."""

    def test_head_to_head_flamengo_fluminense(self, neo4j_db):
        """Test head-to-head query for classic derby."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (t1:Team), (t2:Team)
            WHERE t1.name CONTAINS 'Flamengo' AND t2.name CONTAINS 'Fluminense'
            MATCH (m:Match)
            WHERE ((t1)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t2))
               OR ((t2)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t1))
            RETURN count(m) as matches
            """
        )
        # Should have multiple Fla-Flu matches
        assert result[0]["matches"] >= 1

    def test_team_with_most_matches(self, neo4j_db):
        """Test finding team with most matches."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (t:Team)
            OPTIONAL MATCH (t)-[:PLAYED_HOME]->(m1:Match)
            OPTIONAL MATCH (t)-[:PLAYED_AWAY]->(m2:Match)
            WITH t, count(DISTINCT m1) + count(DISTINCT m2) as total_matches
            ORDER BY total_matches DESC
            LIMIT 1
            RETURN t.name as team, total_matches
            """
        )
        assert len(result) == 1
        assert result[0]["total_matches"] >= 10

    def test_players_per_team(self, neo4j_db):
        """Test getting player count per team."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
            WITH t, count(p) as player_count
            WHERE player_count >= 5
            RETURN t.name as team, player_count
            ORDER BY player_count DESC
            """
        )
        # At least some teams should have 5+ players
        assert len(result) >= 1

    def test_matches_by_season(self, neo4j_db):
        """Test getting matches grouped by season."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (m:Match)-[:PART_OF]->(c:Competition)
            WITH c.season as season, count(m) as matches
            WHERE matches >= 10
            RETURN season, matches
            ORDER BY season DESC
            """
        )
        # Should have multiple seasons with matches
        assert len(result) >= 1


class TestKaggleDataIntegrity:
    """Test data integrity of Kaggle import."""

    def test_no_orphan_matches(self, neo4j_db):
        """Test that all matches have both home and away teams."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (m:Match)
            WHERE NOT EXISTS { MATCH (:Team)-[:PLAYED_HOME]->(m) }
               OR NOT EXISTS { MATCH (:Team)-[:PLAYED_AWAY]->(m) }
            RETURN count(m) as orphan_count
            """
        )
        assert result[0]["orphan_count"] == 0

    def test_unique_team_ids(self, neo4j_db):
        """Test that team IDs are unique."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (t:Team)
            WITH t.team_id as tid, count(*) as cnt
            WHERE cnt > 1
            RETURN tid, cnt
            """
        )
        assert len(result) == 0, "Found duplicate team IDs"

    def test_valid_match_scores(self, neo4j_db):
        """Test that match scores are valid (non-negative)."""
        db, _ = neo4j_db
        result = db.execute_query(
            """
            MATCH (m:Match)
            WHERE m.home_score < 0 OR m.away_score < 0
            RETURN count(m) as invalid_count
            """
        )
        assert result[0]["invalid_count"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
