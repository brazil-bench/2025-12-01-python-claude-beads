"""Load Kaggle Brazilian soccer data into Neo4j."""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from .database import Neo4jDatabase
from .models import Team, Match, Competition, Player, PlayerContract


DATA_DIR = Path(__file__).parent.parent.parent / "data" / "kaggle"

# Brazilian teams we want to track (major Serie A teams)
BRAZILIAN_TEAMS = {
    "Flamengo": ("Flamengo-RJ", "Flamengo", "Rio de Janeiro", "Maracanã", 1895, "Red and Black"),
    "Fluminense": ("Fluminense-RJ", "Fluminense", "Rio de Janeiro", "Maracanã", 1902, "Maroon, Green and White"),
    "Corinthians": ("Corinthians-SP", "Corinthians", "São Paulo", "Neo Química Arena", 1910, "Black and White"),
    "Palmeiras": ("Palmeiras-SP", "Palmeiras", "São Paulo", "Allianz Parque", 1914, "Green and White"),
    "Santos": ("Santos-SP", "Santos", "Santos", "Vila Belmiro", 1912, "Black and White"),
    "Sao Paulo": ("Sao Paulo-SP", "São Paulo FC", "São Paulo", "Morumbi", 1930, "Red, White and Black"),
    "Gremio": ("Gremio-RS", "Grêmio", "Porto Alegre", "Arena do Grêmio", 1903, "Blue, Black and White"),
    "Internacional": ("Internacional-RS", "Internacional", "Porto Alegre", "Beira-Rio", 1909, "Red and White"),
    "Cruzeiro": ("Cruzeiro-MG", "Cruzeiro", "Belo Horizonte", "Mineirão", 1921, "Blue and White"),
    "Atletico-MG": ("Atletico-MG", "Atlético Mineiro", "Belo Horizonte", "Mineirão", 1908, "Black and White"),
    "Botafogo": ("Botafogo-RJ", "Botafogo", "Rio de Janeiro", "Nilton Santos", 1904, "Black and White"),
    "Vasco": ("Vasco-RJ", "Vasco da Gama", "Rio de Janeiro", "São Januário", 1898, "Black and White"),
    "Athletico-PR": ("Athletico-PR", "Athletico Paranaense", "Curitiba", "Arena da Baixada", 1924, "Red and Black"),
    "Fortaleza": ("Fortaleza-CE", "Fortaleza", "Fortaleza", "Arena Castelão", 1918, "Blue, Red and White"),
    "Bahia": ("Bahia-BA", "Bahia", "Salvador", "Arena Fonte Nova", 1931, "Blue, Red and White"),
    "Sport": ("Sport-PE", "Sport Recife", "Recife", "Ilha do Retiro", 1905, "Red and Black"),
    "Ceara": ("Ceara-CE", "Ceará", "Fortaleza", "Arena Castelão", 1914, "Black and White"),
    "Goias": ("Goias-GO", "Goiás", "Goiânia", "Serrinha", 1943, "Green and White"),
    "Coritiba": ("Coritiba-PR", "Coritiba", "Curitiba", "Couto Pereira", 1909, "Green and White"),
    "America-MG": ("America-MG", "América Mineiro", "Belo Horizonte", "Independência", 1912, "Green and White"),
}


def normalize_team_name(name: str) -> Optional[str]:
    """Normalize team name to standard format."""
    if not name:
        return None

    # Handle BR-Football-Dataset format (no state suffix)
    name_clean = name.strip()

    # Direct match
    for key, (csv_name, _, _, _, _, _) in BRAZILIAN_TEAMS.items():
        if name_clean == csv_name or name_clean == key:
            return key

    # Match without state suffix
    for key in BRAZILIAN_TEAMS.keys():
        if name_clean.lower().replace(" ", "") == key.lower().replace(" ", ""):
            return key
        if name_clean.lower().startswith(key.lower().split("-")[0].split()[0]):
            return key

    # Partial match
    name_lower = name_clean.lower()
    for key in BRAZILIAN_TEAMS.keys():
        key_lower = key.lower()
        if key_lower in name_lower or name_lower in key_lower:
            return key

    return None


def get_team_id(team_name: str) -> str:
    """Generate consistent team ID from name."""
    normalized = normalize_team_name(team_name)
    if normalized:
        return f"T_{normalized.replace('-', '_').replace(' ', '_').upper()}"
    return f"T_{team_name.replace('-', '_').replace(' ', '_').upper()}"


class KaggleDataLoader:
    """Load Kaggle data into Neo4j."""

    def __init__(self, db: Neo4jDatabase, data_dir: Optional[Path] = None):
        self.db = db
        self.data_dir = data_dir or DATA_DIR
        self.team_cache = {}
        self.player_cache = {}

    def load_all(self) -> dict:
        """Load all Kaggle data into Neo4j."""
        stats = {
            "teams": 0,
            "competitions": 0,
            "matches": 0,
            "players": 0,
            "contracts": 0,
        }

        # Create constraints and indexes
        self.db.create_constraints()
        self.db.create_indexes()

        # Load teams first
        stats["teams"] = self._load_teams()

        # Load competitions
        stats["competitions"] = self._load_competitions()

        # Load matches from Brasileirao
        stats["matches"] += self._load_brasileirao_matches()

        # Load matches from BR-Football-Dataset
        stats["matches"] += self._load_br_football_matches()

        # Load players from FIFA data
        stats["players"], stats["contracts"] = self._load_players()

        return stats

    def _load_teams(self) -> int:
        """Load all Brazilian teams."""
        count = 0
        for key, (csv_name, name, city, stadium, founded, colors) in BRAZILIAN_TEAMS.items():
            team_id = get_team_id(key)
            self.team_cache[key] = team_id
            self.team_cache[csv_name] = team_id
            self.team_cache[name] = team_id

            query = """
            MERGE (t:Team {team_id: $team_id})
            SET t.name = $name,
                t.city = $city,
                t.stadium = $stadium,
                t.founded_year = $founded_year,
                t.colors = $colors
            """
            self.db.execute_write(query, {
                "team_id": team_id,
                "name": name,
                "city": city,
                "stadium": stadium,
                "founded_year": founded,
                "colors": colors,
            })
            count += 1
        return count

    def _load_competitions(self) -> int:
        """Load competition definitions."""
        competitions = [
            ("BRASILEIRAO", "Campeonato Brasileiro Série A", "league"),
            ("COPA_BRASIL", "Copa do Brasil", "cup"),
            ("LIBERTADORES", "Copa Libertadores", "cup"),
        ]

        count = 0
        for comp_id, name, comp_type in competitions:
            # Create seasons 2012-2023
            for year in range(2012, 2024):
                query = """
                MERGE (c:Competition {competition_id: $competition_id})
                SET c.name = $name,
                    c.season = $season,
                    c.type = $type,
                    c.tier = 1
                """
                self.db.execute_write(query, {
                    "competition_id": f"{comp_id}_{year}",
                    "name": name,
                    "season": str(year),
                    "type": comp_type,
                })
                count += 1
        return count

    def _load_brasileirao_matches(self) -> int:
        """Load matches from Brasileirao_Matches.csv."""
        csv_path = self.data_dir / "Brasileirao_Matches.csv"
        if not csv_path.exists():
            return 0

        df = pd.read_csv(csv_path)
        count = 0

        for _, row in df.iterrows():
            home_team = normalize_team_name(row["home_team"])
            away_team = normalize_team_name(row["away_team"])

            if not home_team or not away_team:
                continue

            home_id = self.team_cache.get(home_team)
            away_id = self.team_cache.get(away_team)

            if not home_id or not away_id:
                continue

            try:
                match_date = pd.to_datetime(row["datetime"]).date()
                season = int(row["season"])
                match_round = int(row["round"]) if pd.notna(row["round"]) else 0
                home_goals = int(row["home_goal"]) if pd.notna(row["home_goal"]) else 0
                away_goals = int(row["away_goal"]) if pd.notna(row["away_goal"]) else 0
            except (ValueError, TypeError):
                continue

            match_id = f"BR_{season}_{match_round}_{home_team}_{away_team}"
            competition_id = f"BRASILEIRAO_{season}"

            query = """
            MERGE (m:Match {match_id: $match_id})
            SET m.date = $date,
                m.home_score = $home_score,
                m.away_score = $away_score,
                m.round = $round
            WITH m
            MATCH (home:Team {team_id: $home_team_id})
            MATCH (away:Team {team_id: $away_team_id})
            MATCH (c:Competition {competition_id: $competition_id})
            MERGE (home)-[:PLAYED_HOME]->(m)
            MERGE (away)-[:PLAYED_AWAY]->(m)
            MERGE (m)-[:PART_OF]->(c)
            """
            try:
                self.db.execute_write(query, {
                    "match_id": match_id,
                    "date": match_date.isoformat(),
                    "home_score": home_goals,
                    "away_score": away_goals,
                    "round": match_round,
                    "home_team_id": home_id,
                    "away_team_id": away_id,
                    "competition_id": competition_id,
                })
                count += 1
            except Exception:
                continue

        return count

    def _load_br_football_matches(self) -> int:
        """Load matches from BR-Football-Dataset.csv."""
        csv_path = self.data_dir / "BR-Football-Dataset.csv"
        if not csv_path.exists():
            return 0

        df = pd.read_csv(csv_path)
        count = 0

        for _, row in df.iterrows():
            home_team = normalize_team_name(row["home"])
            away_team = normalize_team_name(row["away"])

            if not home_team or not away_team:
                continue

            home_id = self.team_cache.get(home_team)
            away_id = self.team_cache.get(away_team)

            if not home_id or not away_id:
                continue

            try:
                match_date = pd.to_datetime(row["date"]).date()
                home_goals = int(row["home_goal"]) if pd.notna(row["home_goal"]) else 0
                away_goals = int(row["away_goal"]) if pd.notna(row["away_goal"]) else 0
                tournament = row["tournament"]
            except (ValueError, TypeError):
                continue

            # Determine competition
            if "Copa do Brasil" in tournament:
                comp_prefix = "COPA_BRASIL"
            elif "Libertadores" in tournament:
                comp_prefix = "LIBERTADORES"
            else:
                comp_prefix = "BRASILEIRAO"

            season = match_date.year
            match_id = f"BRF_{match_date.isoformat()}_{home_team}_{away_team}"
            competition_id = f"{comp_prefix}_{season}"

            query = """
            MERGE (m:Match {match_id: $match_id})
            SET m.date = $date,
                m.home_score = $home_score,
                m.away_score = $away_score,
                m.home_corners = $home_corners,
                m.away_corners = $away_corners,
                m.home_shots = $home_shots,
                m.away_shots = $away_shots
            WITH m
            MATCH (home:Team {team_id: $home_team_id})
            MATCH (away:Team {team_id: $away_team_id})
            OPTIONAL MATCH (c:Competition {competition_id: $competition_id})
            MERGE (home)-[:PLAYED_HOME]->(m)
            MERGE (away)-[:PLAYED_AWAY]->(m)
            FOREACH (ignore IN CASE WHEN c IS NOT NULL THEN [1] ELSE [] END |
                MERGE (m)-[:PART_OF]->(c)
            )
            """
            try:
                self.db.execute_write(query, {
                    "match_id": match_id,
                    "date": match_date.isoformat(),
                    "home_score": home_goals,
                    "away_score": away_goals,
                    "home_corners": int(row["home_corner"]) if pd.notna(row.get("home_corner")) else None,
                    "away_corners": int(row["away_corner"]) if pd.notna(row.get("away_corner")) else None,
                    "home_shots": int(row["home_shots"]) if pd.notna(row.get("home_shots")) else None,
                    "away_shots": int(row["away_shots"]) if pd.notna(row.get("away_shots")) else None,
                    "home_team_id": home_id,
                    "away_team_id": away_id,
                    "competition_id": competition_id,
                })
                count += 1
            except Exception:
                continue

        return count

    def _load_players(self) -> tuple[int, int]:
        """Load players from FIFA player CSVs."""
        player_count = 0
        contract_count = 0

        # Brazilian club names in FIFA data
        brazilian_clubs = {
            "Flamengo": "Flamengo",
            "Fluminense": "Fluminense",
            "Corinthians": "Corinthians",
            "Palmeiras": "Palmeiras",
            "Santos": "Santos",
            "São Paulo": "Sao Paulo",
            "Grêmio": "Gremio",
            "Internacional": "Internacional",
            "Cruzeiro": "Cruzeiro",
            "Atlético Mineiro": "Atletico-MG",
            "Botafogo": "Botafogo",
            "Vasco da Gama": "Vasco",
            "Athletico Paranaense": "Athletico-PR",
            "Fortaleza": "Fortaleza",
            "Bahia": "Bahia",
            "Sport Recife": "Sport",
            "Ceará": "Ceara",
            "Goiás": "Goias",
            "Coritiba": "Coritiba",
            "América Mineiro": "America-MG",
        }

        # Load from most recent player files
        player_files = sorted(self.data_dir.glob("players_*.csv"), reverse=True)

        seen_players = set()

        for csv_path in player_files:
            try:
                df = pd.read_csv(csv_path, low_memory=False)
            except Exception:
                continue

            # Extract year from filename
            year = int(csv_path.stem.split("_")[1]) + 2000  # players_22 -> 2022

            for _, row in df.iterrows():
                club_name = row.get("club_name", "")
                if not club_name or not isinstance(club_name, str):
                    continue

                # Check if this is a Brazilian club
                team_key = None
                for fifa_name, key in brazilian_clubs.items():
                    if fifa_name in club_name:
                        team_key = key
                        break

                if not team_key:
                    continue

                team_id = self.team_cache.get(team_key)
                if not team_id:
                    continue

                # Player data
                try:
                    sofifa_id = str(int(row["sofifa_id"]))
                    player_id = f"P_{sofifa_id}"

                    if player_id in seen_players:
                        continue

                    short_name = row.get("short_name", "Unknown")
                    long_name = row.get("long_name", short_name)
                    positions = row.get("player_positions", "")
                    nationality = row.get("nationality_name", "Unknown")
                    dob = row.get("dob")
                    overall = int(row.get("overall", 0)) if pd.notna(row.get("overall")) else None
                    jersey = int(row.get("club_jersey_number", 0)) if pd.notna(row.get("club_jersey_number")) else None

                    # Parse date of birth
                    birth_date = None
                    if pd.notna(dob):
                        try:
                            birth_date = pd.to_datetime(dob).date().isoformat()
                        except Exception:
                            pass

                    # Determine primary position
                    position = positions.split(",")[0].strip() if positions else "Unknown"

                    # Create player
                    query = """
                    MERGE (p:Player {player_id: $player_id})
                    SET p.name = $name,
                        p.short_name = $short_name,
                        p.birth_date = $birth_date,
                        p.nationality = $nationality,
                        p.position = $position,
                        p.positions = $positions,
                        p.overall_rating = $overall,
                        p.jersey_number = $jersey_number
                    """
                    self.db.execute_write(query, {
                        "player_id": player_id,
                        "name": long_name,
                        "short_name": short_name,
                        "birth_date": birth_date,
                        "nationality": nationality,
                        "position": position,
                        "positions": positions,
                        "overall": overall,
                        "jersey_number": jersey,
                    })
                    seen_players.add(player_id)
                    player_count += 1

                    # Create contract relationship
                    contract_query = """
                    MATCH (p:Player {player_id: $player_id})
                    MATCH (t:Team {team_id: $team_id})
                    MERGE (p)-[r:PLAYS_FOR]->(t)
                    SET r.season = $season
                    """
                    self.db.execute_write(contract_query, {
                        "player_id": player_id,
                        "team_id": team_id,
                        "season": str(year),
                    })
                    contract_count += 1

                except Exception:
                    continue

        return player_count, contract_count


def load_kaggle_data(db: Neo4jDatabase, data_dir: Optional[Path] = None) -> dict:
    """Load all Kaggle data into Neo4j."""
    loader = KaggleDataLoader(db, data_dir)
    return loader.load_all()
