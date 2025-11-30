"""Pytest configuration and fixtures for Brazilian Soccer MCP tests."""

import os
import pytest
from unittest.mock import MagicMock, patch

# Set up test environment
os.environ.setdefault("NEO4J_URI", "bolt://localhost:7687")
os.environ.setdefault("NEO4J_USER", "neo4j")
os.environ.setdefault("NEO4J_PASSWORD", "password")


class MockNeo4jDatabase:
    """Mock Neo4j database for testing without a real database."""

    def __init__(self):
        self.data = self._load_sample_data()
        self._connected = False

    def _load_sample_data(self):
        """Load sample data into memory for mocking."""
        from brazilian_soccer_mcp.data_loader import get_sample_data
        return get_sample_data()

    def connect(self):
        self._connected = True

    def close(self):
        self._connected = False

    def execute_query(self, query: str, parameters: dict = None) -> list:
        """Execute a mock query against in-memory data."""
        params = parameters or {}

        # Player search queries
        if "MATCH (p:Player)" in query and "toLower(p.name) CONTAINS" in query:
            name = params.get("name", "").lower()
            team = params.get("team", "").lower() if "team" in params else None
            position = params.get("position", "").lower() if "position" in params else None

            results = []
            for player in self.data["players"]:
                if name not in player.name.lower():
                    continue

                # Get teams for this player
                player_teams = [
                    self.data["teams"][i].name
                    for i, contract in enumerate(self.data["contracts"])
                    if contract.player_id == player.player_id
                    for i, t in enumerate(self.data["teams"])
                    if t.team_id == contract.team_id
                ]

                # Filter by team if specified
                if team:
                    if not any(team in t.lower() for t in player_teams):
                        continue

                # Filter by position if specified
                if position and position != player.position.lower():
                    continue

                results.append({
                    "player_id": player.player_id,
                    "name": player.name,
                    "nationality": player.nationality,
                    "position": player.position,
                    "birth_date": player.birth_date.isoformat() if player.birth_date else None,
                    "teams": player_teams,
                })

            return results

        # Team search queries
        if "MATCH (t:Team)" in query and "toLower(t.name) CONTAINS" in query:
            name = params.get("name", "").lower()
            results = []
            for team in self.data["teams"]:
                if name in team.name.lower():
                    results.append({
                        "team_id": team.team_id,
                        "name": team.name,
                        "city": team.city,
                        "stadium": team.stadium,
                        "founded_year": team.founded_year,
                        "colors": team.colors,
                    })
            return results

        # Get team by ID (only for simple name lookups, not stats queries)
        if "MATCH (t:Team {team_id:" in query and "PLAYED_HOME" not in query and "PLAYED_AWAY" not in query:
            team_id = params.get("team_id")
            for team in self.data["teams"]:
                if team.team_id == team_id:
                    return [{"name": team.name}]
            return []

        # Get player roster for team
        if "MATCH (p:Player)-[r:PLAYS_FOR]->(t:Team {team_id:" in query:
            team_id = params.get("team_id")
            results = []
            for contract in self.data["contracts"]:
                if contract.team_id == team_id and contract.end_date is None:
                    for player in self.data["players"]:
                        if player.player_id == contract.player_id:
                            results.append({
                                "player_id": player.player_id,
                                "name": player.name,
                                "position": player.position,
                                "jersey_number": player.jersey_number,
                                "joined": contract.start_date.isoformat(),
                            })
            return results

        # Match details query
        if "MATCH (home:Team)-[:PLAYED_HOME]->(m:Match {match_id:" in query:
            match_id = params.get("match_id")
            for match in self.data["matches"]:
                if match.match_id == match_id:
                    home_team = next((t for t in self.data["teams"] if t.team_id == match.home_team_id), None)
                    away_team = next((t for t in self.data["teams"] if t.team_id == match.away_team_id), None)
                    competition = next((c for c in self.data["competitions"] if c.competition_id == match.competition_id), None)

                    if home_team and away_team and competition:
                        return [{
                            "match_id": match.match_id,
                            "date": match.date.isoformat(),
                            "home_score": match.home_score,
                            "away_score": match.away_score,
                            "attendance": match.attendance,
                            "home_team": home_team.name,
                            "away_team": away_team.name,
                            "competition": competition.name,
                            "season": competition.season,
                        }]
            return []

        # Search matches
        if "MATCH (home:Team)-[:PLAYED_HOME]->(m:Match)<-[:PLAYED_AWAY]-(away:Team)" in query:
            team = params.get("team", "").lower() if "team" in params else None
            date_from = params.get("date_from")
            date_to = params.get("date_to")

            results = []
            for match in self.data["matches"]:
                home_team = next((t for t in self.data["teams"] if t.team_id == match.home_team_id), None)
                away_team = next((t for t in self.data["teams"] if t.team_id == match.away_team_id), None)
                competition = next((c for c in self.data["competitions"] if c.competition_id == match.competition_id), None)

                if not (home_team and away_team and competition):
                    continue

                # Filter by team
                if team:
                    if team not in home_team.name.lower() and team not in away_team.name.lower():
                        continue

                # Filter by date
                match_date = match.date.isoformat()
                if date_from and match_date < date_from:
                    continue
                if date_to and match_date > date_to:
                    continue

                results.append({
                    "match_id": match.match_id,
                    "date": match_date,
                    "home_score": match.home_score,
                    "away_score": match.away_score,
                    "home_team": home_team.name,
                    "away_team": away_team.name,
                    "competition": competition.name,
                    "season": competition.season,
                })

            return sorted(results, key=lambda x: x["date"], reverse=True)[:20]

        # Head-to-head team names
        if "MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})" in query and "RETURN t1.name" in query:
            team1_id = params.get("team1_id")
            team2_id = params.get("team2_id")
            team1 = next((t for t in self.data["teams"] if t.team_id == team1_id), None)
            team2 = next((t for t in self.data["teams"] if t.team_id == team2_id), None)
            if team1 and team2:
                return [{"team1_name": team1.name, "team2_name": team2.name}]
            return []

        # Head-to-head matches
        if "MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})" in query and "MATCH (m:Match)" in query:
            team1_id = params.get("team1_id")
            team2_id = params.get("team2_id")

            results = []
            for match in self.data["matches"]:
                if (match.home_team_id == team1_id and match.away_team_id == team2_id) or \
                   (match.home_team_id == team2_id and match.away_team_id == team1_id):
                    home_team = next((t for t in self.data["teams"] if t.team_id == match.home_team_id), None)
                    away_team = next((t for t in self.data["teams"] if t.team_id == match.away_team_id), None)
                    results.append({
                        "date": match.date.isoformat(),
                        "home_score": match.home_score,
                        "away_score": match.away_score,
                        "home_team": home_team.name,
                        "home_id": home_team.team_id,
                        "away_team": away_team.name,
                        "away_id": away_team.team_id,
                    })
            return sorted(results, key=lambda x: x["date"], reverse=True)

        # Player career
        if "MATCH (p:Player {player_id: $player_id})" in query and "PLAYS_FOR" in query:
            player_id = params.get("player_id")
            player = next((p for p in self.data["players"] if p.player_id == player_id), None)
            if not player:
                return []

            career = []
            for contract in self.data["contracts"]:
                if contract.player_id == player_id:
                    team = next((t for t in self.data["teams"] if t.team_id == contract.team_id), None)
                    if team:
                        career.append({
                            "team": team.name,
                            "team_id": team.team_id,
                            "start_date": contract.start_date.isoformat(),
                            "end_date": contract.end_date.isoformat() if contract.end_date else None,
                        })

            return [{
                "name": player.name,
                "birth_date": player.birth_date.isoformat() if player.birth_date else None,
                "nationality": player.nationality,
                "position": player.position,
                "career": career,
            }]

        # Players who played for both teams
        if "MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})" in query and "PLAYS_FOR" in query:
            team1_id = params.get("team1_id")
            team2_id = params.get("team2_id")

            team1 = next((t for t in self.data["teams"] if t.team_id == team1_id), None)
            team2 = next((t for t in self.data["teams"] if t.team_id == team2_id), None)

            if not team1 or not team2:
                return []

            # Find players with contracts at both teams
            players_team1 = {c.player_id: c for c in self.data["contracts"] if c.team_id == team1_id}
            players_team2 = {c.player_id: c for c in self.data["contracts"] if c.team_id == team2_id}

            common_players = set(players_team1.keys()) & set(players_team2.keys())

            results = []
            for pid in common_players:
                player = next((p for p in self.data["players"] if p.player_id == pid), None)
                if player:
                    c1 = players_team1[pid]
                    c2 = players_team2[pid]
                    results.append({
                        "player": player.name,
                        "player_id": player.player_id,
                        "team1_name": team1.name,
                        "team2_name": team2.name,
                        "team1_start": c1.start_date.isoformat(),
                        "team1_end": c1.end_date.isoformat() if c1.end_date else None,
                        "team2_start": c2.start_date.isoformat(),
                        "team2_end": c2.end_date.isoformat() if c2.end_date else None,
                    })

            return results

        # Top scorers
        if "SCORED_IN" in query and "Competition" in query:
            competition_id = params.get("competition_id")
            season = params.get("season")
            limit = params.get("limit", 10)

            # Find matches in this competition/season
            comp = next((c for c in self.data["competitions"]
                        if c.competition_id == competition_id and c.season == season), None)
            if not comp:
                return []

            match_ids = {m.match_id for m in self.data["matches"] if m.competition_id == competition_id}

            # Count goals per player
            goal_counts = {}
            for goal in self.data["goals"]:
                if goal.match_id in match_ids:
                    goal_counts[goal.player_id] = goal_counts.get(goal.player_id, 0) + 1

            # Sort by goals
            sorted_scorers = sorted(goal_counts.items(), key=lambda x: x[1], reverse=True)[:limit]

            results = []
            for player_id, goals in sorted_scorers:
                player = next((p for p in self.data["players"] if p.player_id == player_id), None)
                if player:
                    # Get a team for this player
                    contract = next((c for c in self.data["contracts"] if c.player_id == player_id), None)
                    team_name = ""
                    if contract:
                        team = next((t for t in self.data["teams"] if t.team_id == contract.team_id), None)
                        team_name = team.name if team else ""

                    results.append({
                        "player": player.name,
                        "player_id": player.player_id,
                        "goals": goals,
                        "team": team_name,
                    })

            return results

        # Competition lookup
        if "MATCH (c:Competition {competition_id: $competition_id, season: $season})" in query:
            competition_id = params.get("competition_id")
            season = params.get("season")
            comp = next((c for c in self.data["competitions"]
                        if c.competition_id == competition_id and c.season == season), None)
            if comp:
                return [{"name": comp.name}]
            return []

        # Player names lookup
        if "MATCH (p1:Player {player_id: $player1_id}), (p2:Player {player_id: $player2_id})" in query:
            p1_id = params.get("player1_id")
            p2_id = params.get("player2_id")
            p1 = next((p for p in self.data["players"] if p.player_id == p1_id), None)
            p2 = next((p for p in self.data["players"] if p.player_id == p2_id), None)
            if p1 and p2:
                return [{"player1_name": p1.name, "player2_name": p2.name}]
            return []

        # Common teammates - simplified
        if "PLAYS_FOR" in query and "teammate" in query.lower():
            p1_id = params.get("player1_id")
            p2_id = params.get("player2_id")

            # Get teams for each player
            p1_teams = {c.team_id for c in self.data["contracts"] if c.player_id == p1_id}
            p2_teams = {c.team_id for c in self.data["contracts"] if c.player_id == p2_id}

            # Find all players who were at any of p1's teams
            p1_teammates = set()
            for c in self.data["contracts"]:
                if c.team_id in p1_teams and c.player_id != p1_id:
                    p1_teammates.add(c.player_id)

            # Find all players who were at any of p2's teams
            p2_teammates = set()
            for c in self.data["contracts"]:
                if c.team_id in p2_teams and c.player_id != p2_id:
                    p2_teammates.add(c.player_id)

            common = p1_teammates & p2_teammates - {p1_id, p2_id}

            results = []
            for pid in common:
                player = next((p for p in self.data["players"] if p.player_id == pid), None)
                if player:
                    # Get teams where they played with p1 and p2
                    p1_shared = [t.name for c in self.data["contracts"] if c.player_id == pid
                                 for t in self.data["teams"] if t.team_id == c.team_id and c.team_id in p1_teams]
                    p2_shared = [t.name for c in self.data["contracts"] if c.player_id == pid
                                 for t in self.data["teams"] if t.team_id == c.team_id and c.team_id in p2_teams]
                    results.append({
                        "name": player.name,
                        "player_id": player.player_id,
                        "teams_with_p1": p1_shared,
                        "teams_with_p2": p2_shared,
                    })

            return results

        # Home/away stats queries - return empty to trigger default values
        if "PLAYED_HOME" in query or "PLAYED_AWAY" in query:
            team_id = params.get("team_id")
            season = params.get("season")

            is_home = "PLAYED_HOME" in query

            matches = 0
            wins = 0
            draws = 0
            losses = 0
            goals_for = 0
            goals_against = 0

            for match in self.data["matches"]:
                # Check season if specified
                if season:
                    comp = next((c for c in self.data["competitions"]
                                if c.competition_id == match.competition_id and c.season == season), None)
                    if not comp:
                        continue

                if is_home and match.home_team_id == team_id:
                    matches += 1
                    goals_for += match.home_score
                    goals_against += match.away_score
                    if match.home_score > match.away_score:
                        wins += 1
                    elif match.home_score == match.away_score:
                        draws += 1
                    else:
                        losses += 1
                elif not is_home and match.away_team_id == team_id:
                    matches += 1
                    goals_for += match.away_score
                    goals_against += match.home_score
                    if match.away_score > match.home_score:
                        wins += 1
                    elif match.away_score == match.home_score:
                        draws += 1
                    else:
                        losses += 1

            return [{
                "matches": matches,
                "wins": wins,
                "draws": draws,
                "losses": losses,
                "goals_for": goals_for,
                "goals_against": goals_against,
            }]

        # Default empty result
        return []

    def execute_write(self, query: str, parameters: dict = None) -> None:
        """Mock write operation - no-op for tests."""
        pass

    def create_constraints(self) -> None:
        pass

    def create_indexes(self) -> None:
        pass


@pytest.fixture
def mock_db():
    """Provide a mock database for testing."""
    return MockNeo4jDatabase()


@pytest.fixture
def db_with_sample_data(mock_db):
    """Provide a mock database pre-populated with sample data."""
    mock_db.connect()
    return mock_db
