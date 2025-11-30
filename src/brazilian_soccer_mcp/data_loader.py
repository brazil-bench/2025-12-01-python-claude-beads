"""Data loader for populating Neo4j with Brazilian Soccer data."""

from datetime import date
from typing import Any

from .database import Neo4jDatabase
from .models import (
    Player,
    Team,
    Match,
    Competition,
    Stadium,
    Coach,
    PlayerContract,
    Goal,
    Card,
)


class DataLoader:
    """Load data into Neo4j database."""

    def __init__(self, db: Neo4jDatabase):
        self.db = db

    def load_player(self, player: Player) -> None:
        """Load a player into the database."""
        query = """
        MERGE (p:Player {player_id: $player_id})
        SET p.name = $name,
            p.birth_date = $birth_date,
            p.nationality = $nationality,
            p.position = $position,
            p.jersey_number = $jersey_number
        """
        self.db.execute_write(
            query,
            {
                "player_id": player.player_id,
                "name": player.name,
                "birth_date": player.birth_date.isoformat() if player.birth_date else None,
                "nationality": player.nationality,
                "position": player.position,
                "jersey_number": player.jersey_number,
            },
        )

    def load_team(self, team: Team) -> None:
        """Load a team into the database."""
        query = """
        MERGE (t:Team {team_id: $team_id})
        SET t.name = $name,
            t.city = $city,
            t.stadium = $stadium,
            t.founded_year = $founded_year,
            t.colors = $colors
        """
        self.db.execute_write(
            query,
            {
                "team_id": team.team_id,
                "name": team.name,
                "city": team.city,
                "stadium": team.stadium,
                "founded_year": team.founded_year,
                "colors": team.colors,
            },
        )

    def load_match(self, match: Match) -> None:
        """Load a match into the database."""
        query = """
        MERGE (m:Match {match_id: $match_id})
        SET m.date = $date,
            m.home_score = $home_score,
            m.away_score = $away_score,
            m.attendance = $attendance
        WITH m
        MATCH (home:Team {team_id: $home_team_id})
        MATCH (away:Team {team_id: $away_team_id})
        MATCH (c:Competition {competition_id: $competition_id})
        MERGE (home)-[:PLAYED_HOME]->(m)
        MERGE (away)-[:PLAYED_AWAY]->(m)
        MERGE (m)-[:PART_OF]->(c)
        """
        self.db.execute_write(
            query,
            {
                "match_id": match.match_id,
                "date": match.date.isoformat(),
                "home_team_id": match.home_team_id,
                "away_team_id": match.away_team_id,
                "home_score": match.home_score,
                "away_score": match.away_score,
                "competition_id": match.competition_id,
                "attendance": match.attendance,
            },
        )

    def load_competition(self, competition: Competition) -> None:
        """Load a competition into the database."""
        query = """
        MERGE (c:Competition {competition_id: $competition_id})
        SET c.name = $name,
            c.season = $season,
            c.type = $type,
            c.tier = $tier
        """
        self.db.execute_write(
            query,
            {
                "competition_id": competition.competition_id,
                "name": competition.name,
                "season": competition.season,
                "type": competition.type,
                "tier": competition.tier,
            },
        )

    def load_stadium(self, stadium: Stadium) -> None:
        """Load a stadium into the database."""
        query = """
        MERGE (s:Stadium {stadium_id: $stadium_id})
        SET s.name = $name,
            s.city = $city,
            s.capacity = $capacity,
            s.opened_year = $opened_year
        """
        self.db.execute_write(
            query,
            {
                "stadium_id": stadium.stadium_id,
                "name": stadium.name,
                "city": stadium.city,
                "capacity": stadium.capacity,
                "opened_year": stadium.opened_year,
            },
        )

    def load_coach(self, coach: Coach) -> None:
        """Load a coach into the database."""
        query = """
        MERGE (c:Coach {coach_id: $coach_id})
        SET c.name = $name,
            c.nationality = $nationality,
            c.birth_date = $birth_date
        """
        self.db.execute_write(
            query,
            {
                "coach_id": coach.coach_id,
                "name": coach.name,
                "nationality": coach.nationality,
                "birth_date": coach.birth_date.isoformat() if coach.birth_date else None,
            },
        )

    def load_player_contract(self, contract: PlayerContract) -> None:
        """Load a player contract (PLAYS_FOR relationship)."""
        query = """
        MATCH (p:Player {player_id: $player_id})
        MATCH (t:Team {team_id: $team_id})
        MERGE (p)-[r:PLAYS_FOR]->(t)
        SET r.start_date = $start_date,
            r.end_date = $end_date
        """
        self.db.execute_write(
            query,
            {
                "player_id": contract.player_id,
                "team_id": contract.team_id,
                "start_date": contract.start_date.isoformat(),
                "end_date": contract.end_date.isoformat() if contract.end_date else None,
            },
        )

    def load_goal(self, goal: Goal) -> None:
        """Load a goal (SCORED_IN relationship)."""
        query = """
        MATCH (p:Player {player_id: $player_id})
        MATCH (m:Match {match_id: $match_id})
        MERGE (p)-[r:SCORED_IN]->(m)
        SET r.minute = $minute,
            r.goal_type = $goal_type
        """
        self.db.execute_write(
            query,
            {
                "player_id": goal.player_id,
                "match_id": goal.match_id,
                "minute": goal.minute,
                "goal_type": goal.goal_type,
            },
        )

    def load_card(self, card: Card) -> None:
        """Load a card (YELLOW_CARD_IN or RED_CARD_IN relationship)."""
        rel_type = "YELLOW_CARD_IN" if card.card_type == "yellow" else "RED_CARD_IN"
        query = f"""
        MATCH (p:Player {{player_id: $player_id}})
        MATCH (m:Match {{match_id: $match_id}})
        MERGE (p)-[r:{rel_type}]->(m)
        SET r.minute = $minute
        """
        self.db.execute_write(
            query,
            {
                "player_id": card.player_id,
                "match_id": card.match_id,
                "minute": card.minute,
            },
        )


def get_sample_data() -> dict[str, Any]:
    """Get sample Brazilian soccer data for demo purposes."""
    teams = [
        Team("T001", "Flamengo", "Rio de Janeiro", "Maracanã", 1895, "Red and Black"),
        Team("T002", "Fluminense", "Rio de Janeiro", "Maracanã", 1902, "Maroon, Green and White"),
        Team("T003", "Corinthians", "São Paulo", "Neo Química Arena", 1910, "Black and White"),
        Team("T004", "Palmeiras", "São Paulo", "Allianz Parque", 1914, "Green and White"),
        Team("T005", "Santos", "Santos", "Vila Belmiro", 1912, "Black and White"),
        Team("T006", "São Paulo FC", "São Paulo", "Morumbi", 1930, "Red, White and Black"),
        Team("T007", "Grêmio", "Porto Alegre", "Arena do Grêmio", 1903, "Blue, Black and White"),
        Team("T008", "Internacional", "Porto Alegre", "Beira-Rio", 1909, "Red and White"),
        Team("T009", "Cruzeiro", "Belo Horizonte", "Mineirão", 1921, "Blue and White"),
        Team("T010", "Atlético Mineiro", "Belo Horizonte", "Mineirão", 1908, "Black and White"),
        Team("T011", "Botafogo", "Rio de Janeiro", "Nilton Santos", 1904, "Black and White"),
        Team("T012", "Vasco da Gama", "Rio de Janeiro", "São Januário", 1898, "Black and White"),
    ]

    stadiums = [
        Stadium("S001", "Maracanã", "Rio de Janeiro", 78838, 1950),
        Stadium("S002", "Neo Química Arena", "São Paulo", 49205, 2014),
        Stadium("S003", "Allianz Parque", "São Paulo", 43713, 2014),
        Stadium("S004", "Vila Belmiro", "Santos", 16068, 1916),
        Stadium("S005", "Morumbi", "São Paulo", 66795, 1960),
        Stadium("S006", "Arena do Grêmio", "Porto Alegre", 55662, 2012),
        Stadium("S007", "Beira-Rio", "Porto Alegre", 50128, 1969),
        Stadium("S008", "Mineirão", "Belo Horizonte", 61846, 1965),
        Stadium("S009", "Nilton Santos", "Rio de Janeiro", 46931, 2007),
        Stadium("S010", "São Januário", "Rio de Janeiro", 21880, 1927),
    ]

    competitions = [
        Competition("C001", "Campeonato Brasileiro Série A", "2023", "league", 1),
        Competition("C002", "Campeonato Brasileiro Série A", "2022", "league", 1),
        Competition("C003", "Copa do Brasil", "2023", "cup", 1),
        Competition("C004", "Copa do Brasil", "2022", "cup", 1),
        Competition("C005", "Campeonato Brasileiro Série A", "2024", "league", 1),
    ]

    players = [
        # Flamengo players
        Player("P001", "Gabriel Barbosa (Gabigol)", date(1996, 8, 30), "Brazilian", "Forward", 10),
        Player("P002", "Pedro", date(1997, 6, 20), "Brazilian", "Forward", 9),
        Player("P003", "Arrascaeta", date(1994, 6, 1), "Uruguayan", "Midfielder", 14),
        Player("P004", "Everton Ribeiro", date(1989, 4, 10), "Brazilian", "Midfielder", 7),
        # Palmeiras players
        Player("P005", "Endrick", date(2006, 7, 21), "Brazilian", "Forward", 9),
        Player("P006", "Raphael Veiga", date(1995, 6, 19), "Brazilian", "Midfielder", 23),
        Player("P007", "Dudu", date(1992, 1, 7), "Brazilian", "Forward", 7),
        # Corinthians players
        Player("P008", "Yuri Alberto", date(2001, 3, 18), "Brazilian", "Forward", 9),
        Player("P009", "Renato Augusto", date(1988, 2, 8), "Brazilian", "Midfielder", 8),
        # Santos historical players
        Player("P010", "Pelé", date(1940, 10, 23), "Brazilian", "Forward", 10),
        Player("P011", "Neymar Jr", date(1992, 2, 5), "Brazilian", "Forward", 11),
        # Other notable players
        Player("P012", "Ronaldo Nazário", date(1976, 9, 18), "Brazilian", "Forward", 9),
        Player("P013", "Romário", date(1966, 1, 29), "Brazilian", "Forward", 11),
        Player("P014", "Zico", date(1953, 3, 3), "Brazilian", "Midfielder", 10),
        Player("P015", "Ronaldinho", date(1980, 3, 21), "Brazilian", "Forward", 10),
        # Fluminense players
        Player("P016", "Germán Cano", date(1988, 1, 7), "Argentine", "Forward", 14),
        Player("P017", "Ganso", date(1989, 10, 12), "Brazilian", "Midfielder", 10),
        # São Paulo players
        Player("P018", "Luciano", date(1993, 5, 18), "Brazilian", "Forward", 10),
        Player("P019", "Calleri", date(1993, 7, 23), "Argentine", "Forward", 9),
        # Botafogo players
        Player("P020", "Tiquinho Soares", date(1991, 1, 17), "Brazilian", "Forward", 9),
    ]

    coaches = [
        Coach("CO001", "Tite", "Brazilian", date(1961, 5, 25)),
        Coach("CO002", "Jorge Jesus", "Portuguese", date(1954, 7, 22)),
        Coach("CO003", "Abel Ferreira", "Portuguese", date(1978, 12, 21)),
        Coach("CO004", "Fernando Diniz", "Brazilian", date(1974, 6, 27)),
        Coach("CO005", "Renato Gaúcho", "Brazilian", date(1962, 9, 9)),
    ]

    # Player contracts (PLAYS_FOR relationships)
    contracts = [
        # Flamengo
        PlayerContract("P001", "T001", date(2019, 1, 1), None),
        PlayerContract("P002", "T001", date(2020, 1, 1), None),
        PlayerContract("P003", "T001", date(2019, 1, 1), None),
        PlayerContract("P004", "T001", date(2017, 1, 1), date(2023, 12, 31)),
        PlayerContract("P014", "T001", date(1971, 1, 1), date(1983, 12, 31)),
        # Palmeiras
        PlayerContract("P005", "T004", date(2022, 1, 1), date(2024, 6, 30)),
        PlayerContract("P006", "T004", date(2019, 1, 1), None),
        PlayerContract("P007", "T004", date(2015, 1, 1), None),
        # Corinthians
        PlayerContract("P008", "T003", date(2022, 1, 1), None),
        PlayerContract("P009", "T003", date(2022, 1, 1), None),
        PlayerContract("P012", "T003", date(2009, 1, 1), date(2011, 12, 31)),
        # Santos
        PlayerContract("P010", "T005", date(1956, 1, 1), date(1974, 12, 31)),
        PlayerContract("P011", "T005", date(2009, 1, 1), date(2013, 5, 31)),
        # Players who played for multiple teams
        PlayerContract("P011", "T001", date(2025, 1, 1), None),  # Neymar returned to Brazil
        PlayerContract("P012", "T009", date(1993, 1, 1), date(1994, 5, 31)),  # Ronaldo at Cruzeiro
        PlayerContract("P013", "T012", date(1985, 1, 1), date(1988, 12, 31)),  # Romário at Vasco
        PlayerContract("P013", "T001", date(1995, 1, 1), date(1999, 12, 31)),  # Romário at Flamengo
        PlayerContract("P015", "T007", date(1998, 1, 1), date(2001, 12, 31)),  # Ronaldinho at Grêmio
        PlayerContract("P015", "T010", date(2012, 1, 1), date(2014, 12, 31)),  # Ronaldinho at Atlético
        PlayerContract("P015", "T001", date(2011, 1, 1), date(2012, 6, 30)),  # Ronaldinho at Flamengo
        # Fluminense
        PlayerContract("P016", "T002", date(2022, 1, 1), None),
        PlayerContract("P017", "T002", date(2019, 1, 1), None),
        PlayerContract("P017", "T005", date(2008, 1, 1), date(2012, 12, 31)),  # Ganso at Santos
        # São Paulo
        PlayerContract("P018", "T006", date(2020, 1, 1), None),
        PlayerContract("P019", "T006", date(2022, 1, 1), None),
        # Botafogo
        PlayerContract("P020", "T011", date(2023, 1, 1), None),
    ]

    # Matches
    matches = [
        # 2023 Brasileirão matches
        Match("M001", date(2023, 4, 16), "T001", "T002", 2, 1, "C001", "S001", 65000),  # Fla-Flu
        Match("M002", date(2023, 5, 21), "T003", "T004", 1, 1, "C001", "S002", 45000),  # Derby Paulista
        Match("M003", date(2023, 6, 10), "T005", "T006", 0, 2, "C001", "S004", 14000),
        Match("M004", date(2023, 7, 8), "T007", "T008", 2, 0, "C001", "S006", 50000),  # Grenal
        Match("M005", date(2023, 8, 12), "T001", "T004", 3, 2, "C001", "S001", 70000),
        Match("M006", date(2023, 9, 3), "T002", "T001", 0, 3, "C001", "S001", 60000),  # Fla-Flu 2
        Match("M007", date(2023, 10, 15), "T003", "T001", 1, 2, "C001", "S002", 47000),
        Match("M008", date(2023, 11, 5), "T004", "T003", 2, 0, "C001", "S003", 40000),  # Derby Paulista 2
        Match("M009", date(2023, 11, 20), "T001", "T003", 2, 1, "C001", "S001", 68000),
        Match("M010", date(2023, 12, 3), "T011", "T012", 1, 0, "C001", "S009", 35000),  # Clássico Glorioso
        # 2022 Brasileirão
        Match("M011", date(2022, 4, 10), "T001", "T002", 1, 0, "C002", "S001", 55000),
        Match("M012", date(2022, 5, 15), "T004", "T003", 3, 0, "C002", "S003", 42000),
        Match("M013", date(2022, 8, 20), "T002", "T001", 2, 2, "C002", "S001", 58000),
        # Copa do Brasil 2023
        Match("M014", date(2023, 5, 10), "T001", "T005", 4, 0, "C003", "S001", 52000),
        Match("M015", date(2023, 8, 23), "T006", "T001", 1, 2, "C003", "S005", 45000),
        # 2024 Brasileirão
        Match("M016", date(2024, 4, 14), "T001", "T002", 2, 1, "C005", "S001", 66000),
        Match("M017", date(2024, 5, 19), "T003", "T004", 0, 1, "C005", "S002", 43000),
        Match("M018", date(2024, 7, 7), "T011", "T001", 2, 2, "C005", "S009", 40000),
        Match("M019", date(2024, 9, 15), "T004", "T001", 1, 3, "C005", "S003", 41000),
        Match("M020", date(2024, 10, 20), "T002", "T001", 0, 1, "C005", "S001", 62000),
    ]

    # Goals
    goals = [
        # Match M001 (Fla-Flu 2-1)
        Goal("P001", "M001", 23, "regular"),
        Goal("P002", "M001", 67, "penalty"),
        Goal("P016", "M001", 45, "regular"),
        # Match M002 (Corinthians vs Palmeiras 1-1)
        Goal("P008", "M002", 34, "regular"),
        Goal("P006", "M002", 78, "free_kick"),
        # Match M005 (Flamengo vs Palmeiras 3-2)
        Goal("P001", "M005", 12, "regular"),
        Goal("P002", "M005", 55, "regular"),
        Goal("P003", "M005", 89, "regular"),
        Goal("P005", "M005", 30, "regular"),
        Goal("P007", "M005", 72, "regular"),
        # Match M006 (Fluminense vs Flamengo 0-3)
        Goal("P001", "M006", 15, "regular"),
        Goal("P001", "M006", 50, "penalty"),
        Goal("P002", "M006", 82, "regular"),
        # Match M014 (Copa do Brasil - Flamengo vs Santos 4-0)
        Goal("P001", "M014", 10, "regular"),
        Goal("P002", "M014", 35, "regular"),
        Goal("P003", "M014", 60, "regular"),
        Goal("P001", "M014", 88, "penalty"),
        # More 2024 goals
        Goal("P001", "M016", 25, "regular"),
        Goal("P003", "M016", 70, "regular"),
        Goal("P016", "M016", 85, "regular"),  # Flu goal
        Goal("P006", "M017", 65, "regular"),
        Goal("P020", "M018", 30, "regular"),
        Goal("P001", "M018", 55, "regular"),
        Goal("P002", "M019", 20, "regular"),
        Goal("P001", "M019", 45, "penalty"),
        Goal("P003", "M019", 78, "regular"),
        Goal("P007", "M019", 90, "regular"),  # Palmeiras consolation
        Goal("P002", "M020", 62, "regular"),
    ]

    # Cards
    cards = [
        Card("P008", "M002", 45, "yellow"),
        Card("P007", "M002", 60, "yellow"),
        Card("P003", "M005", 70, "yellow"),
        Card("P006", "M008", 55, "yellow"),
        Card("P009", "M008", 78, "red"),
        Card("P016", "M001", 88, "yellow"),
        Card("P001", "M007", 40, "yellow"),
    ]

    return {
        "teams": teams,
        "stadiums": stadiums,
        "competitions": competitions,
        "players": players,
        "coaches": coaches,
        "contracts": contracts,
        "matches": matches,
        "goals": goals,
        "cards": cards,
    }


def load_sample_data(db: Neo4jDatabase) -> None:
    """Load all sample data into the database."""
    loader = DataLoader(db)
    data = get_sample_data()

    # Create constraints and indexes first
    db.create_constraints()
    db.create_indexes()

    # Load in order respecting dependencies
    for team in data["teams"]:
        loader.load_team(team)

    for stadium in data["stadiums"]:
        loader.load_stadium(stadium)

    for competition in data["competitions"]:
        loader.load_competition(competition)

    for player in data["players"]:
        loader.load_player(player)

    for coach in data["coaches"]:
        loader.load_coach(coach)

    for match in data["matches"]:
        loader.load_match(match)

    for contract in data["contracts"]:
        loader.load_player_contract(contract)

    for goal in data["goals"]:
        loader.load_goal(goal)

    for card in data["cards"]:
        loader.load_card(card)
