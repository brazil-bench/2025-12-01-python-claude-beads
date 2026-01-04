"""MCP Server for Brazilian Soccer Knowledge Graph."""

from typing import Any, Optional
from mcp.server.fastmcp import FastMCP
from mcp.types import TextContent

from .database import Neo4jDatabase


# Initialize the server
server = FastMCP("brazilian-soccer-kb")

# Database connection (lazy initialization)
_db: Optional[Neo4jDatabase] = None


def get_db() -> Neo4jDatabase:
    """Get or create database connection."""
    global _db
    if _db is None:
        _db = Neo4jDatabase()
        _db.connect()
    return _db


# ============================================================================
# Player Tools
# ============================================================================


@server.tool()
async def search_player(
    name: str, team: Optional[str] = None, position: Optional[str] = None
) -> list[TextContent]:
    """Search for players by name, optionally filtering by team or position.

    Args:
        name: Player name (partial match supported)
        team: Optional team name to filter by
        position: Optional position to filter by (Forward, Midfielder, etc.)
    """
    db = get_db()

    query = """
    MATCH (p:Player)
    WHERE toLower(p.name) CONTAINS toLower($name)
    """
    params: dict[str, Any] = {"name": name}

    if team:
        query += """
        MATCH (p)-[:PLAYS_FOR]->(t:Team)
        WHERE toLower(t.name) CONTAINS toLower($team)
        """
        params["team"] = team

    if position:
        query += " AND toLower(p.position) = toLower($position)"
        params["position"] = position

    query += """
    OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
    RETURN p.player_id as player_id, p.name as name, p.nationality as nationality,
           p.position as position, p.birth_date as birth_date,
           collect(DISTINCT t.name) as teams
    """

    results = db.execute_query(query, params)

    if not results:
        return [TextContent(type="text", text=f"No players found matching '{name}'")]

    output = f"Found {len(results)} player(s):\n\n"
    for player in results:
        output += f"- **{player['name']}** ({player['player_id']})\n"
        output += f"  Position: {player['position']}, Nationality: {player['nationality']}\n"
        if player['teams']:
            output += f"  Teams: {', '.join(player['teams'])}\n"
        output += "\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_player_stats(player_id: str, season: Optional[str] = None) -> list[TextContent]:
    """Get statistics for a specific player.

    Args:
        player_id: The unique player identifier
        season: Optional season year (e.g., "2023")
    """
    db = get_db()

    # Get player info
    player_query = """
    MATCH (p:Player {player_id: $player_id})
    RETURN p.name as name, p.position as position, p.nationality as nationality
    """
    player_result = db.execute_query(player_query, {"player_id": player_id})

    if not player_result:
        return [TextContent(type="text", text=f"Player with ID '{player_id}' not found")]

    player = player_result[0]

    # Get goals
    goals_query = """
    MATCH (p:Player {player_id: $player_id})-[g:SCORED_IN]->(m:Match)
    """
    params: dict[str, Any] = {"player_id": player_id}

    if season:
        goals_query += "-[:PART_OF]->(c:Competition) WHERE c.season = $season"
        params["season"] = season

    goals_query += """
    RETURN count(g) as goals, collect({minute: g.minute, type: g.goal_type}) as goal_details
    """

    goals_result = db.execute_query(goals_query, params)
    goals = goals_result[0] if goals_result else {"goals": 0, "goal_details": []}

    # Get cards
    cards_query = """
    MATCH (p:Player {player_id: $player_id})
    OPTIONAL MATCH (p)-[y:YELLOW_CARD_IN]->(m1:Match)
    OPTIONAL MATCH (p)-[r:RED_CARD_IN]->(m2:Match)
    RETURN count(DISTINCT y) as yellow_cards, count(DISTINCT r) as red_cards
    """
    cards_result = db.execute_query(cards_query, {"player_id": player_id})
    cards = cards_result[0] if cards_result else {"yellow_cards": 0, "red_cards": 0}

    output = f"**{player['name']}** Statistics"
    if season:
        output += f" (Season {season})"
    output += "\n\n"
    output += f"- Position: {player['position']}\n"
    output += f"- Nationality: {player['nationality']}\n"
    output += f"- Goals: {goals['goals']}\n"
    output += f"- Yellow Cards: {cards['yellow_cards']}\n"
    output += f"- Red Cards: {cards['red_cards']}\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_player_career(player_id: str) -> list[TextContent]:
    """Get the career history of a player including all teams they've played for.

    Args:
        player_id: The unique player identifier
    """
    db = get_db()

    query = """
    MATCH (p:Player {player_id: $player_id})
    OPTIONAL MATCH (p)-[r:PLAYS_FOR]->(t:Team)
    RETURN p.name as name, p.birth_date as birth_date, p.nationality as nationality,
           p.position as position,
           collect({
               team: t.name,
               team_id: t.team_id,
               start_date: r.start_date,
               end_date: r.end_date
           }) as career
    """

    results = db.execute_query(query, {"player_id": player_id})

    if not results:
        return [TextContent(type="text", text=f"Player with ID '{player_id}' not found")]

    player = results[0]

    output = f"**{player['name']}** Career History\n\n"
    output += f"- Born: {player['birth_date']}\n"
    output += f"- Nationality: {player['nationality']}\n"
    output += f"- Position: {player['position']}\n\n"
    output += "**Teams:**\n"

    # Sort by start date
    career = sorted(
        [c for c in player["career"] if c["team"]],
        key=lambda x: x["start_date"] or ""
    )

    for stint in career:
        end = stint["end_date"] or "Present"
        output += f"- {stint['team']}: {stint['start_date']} to {end}\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_player_transfers(player_id: str) -> list[TextContent]:
    """Get the transfer history of a player between teams.

    Args:
        player_id: The unique player identifier
    """
    db = get_db()

    # Get player info
    player_query = """
    MATCH (p:Player {player_id: $player_id})
    RETURN p.name as name, p.nationality as nationality
    """
    player_result = db.execute_query(player_query, {"player_id": player_id})

    if not player_result:
        return [TextContent(type="text", text=f"Player with ID '{player_id}' not found")]

    player = player_result[0]

    # Get transfers (contracts sorted by start date)
    query = """
    MATCH (p:Player {player_id: $player_id})-[r:PLAYS_FOR]->(t:Team)
    RETURN t.team_id as team_id, t.name as team_name, t.city as city,
           r.start_date as start_date, r.end_date as end_date,
           r.transfer_fee as transfer_fee, r.transfer_type as transfer_type
    ORDER BY r.start_date
    """

    results = db.execute_query(query, {"player_id": player_id})

    output = f"**{player['name']}** Transfer History\n"
    output += f"Nationality: {player['nationality']}\n\n"

    if not results:
        output += "No transfer history found."
    else:
        output += "**Transfers:**\n"
        prev_team = None
        for i, transfer in enumerate(results):
            if i == 0:
                output += f"- Joined **{transfer['team_name']}** ({transfer['start_date']})"
            else:
                output += f"- **{prev_team}** → **{transfer['team_name']}** ({transfer['start_date']})"

            if transfer.get("transfer_type"):
                output += f" [{transfer['transfer_type']}]"
            if transfer.get("transfer_fee"):
                output += f" - Fee: ${transfer['transfer_fee']:,}"
            output += "\n"

            if transfer["end_date"]:
                output += f"  Left: {transfer['end_date']}\n"
            else:
                output += f"  Current club\n"

            prev_team = transfer["team_name"]

    return [TextContent(type="text", text=output)]


# ============================================================================
# Team Tools
# ============================================================================


@server.tool()
async def search_team(name: str) -> list[TextContent]:
    """Search for teams by name.

    Args:
        name: Team name (partial match supported)
    """
    db = get_db()

    query = """
    MATCH (t:Team)
    WHERE toLower(t.name) CONTAINS toLower($name)
    RETURN t.team_id as team_id, t.name as name, t.city as city,
           t.stadium as stadium, t.founded_year as founded_year, t.colors as colors
    """

    results = db.execute_query(query, {"name": name})

    if not results:
        return [TextContent(type="text", text=f"No teams found matching '{name}'")]

    output = f"Found {len(results)} team(s):\n\n"
    for team in results:
        output += f"- **{team['name']}** ({team['team_id']})\n"
        output += f"  City: {team['city']}\n"
        output += f"  Stadium: {team['stadium']}\n"
        output += f"  Founded: {team['founded_year']}\n"
        output += f"  Colors: {team['colors']}\n\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_team_roster(team_id: str, season: Optional[str] = None) -> list[TextContent]:
    """Get the roster of a team.

    Args:
        team_id: The unique team identifier
        season: Optional season year (e.g., "2023")
    """
    db = get_db()

    # Get team info
    team_query = "MATCH (t:Team {team_id: $team_id}) RETURN t.name as name"
    team_result = db.execute_query(team_query, {"team_id": team_id})

    if not team_result:
        return [TextContent(type="text", text=f"Team with ID '{team_id}' not found")]

    team_name = team_result[0]["name"]

    # Get players
    query = """
    MATCH (p:Player)-[r:PLAYS_FOR]->(t:Team {team_id: $team_id})
    WHERE r.end_date IS NULL OR r.end_date >= date()
    RETURN p.player_id as player_id, p.name as name, p.position as position,
           p.jersey_number as jersey_number, r.start_date as joined
    ORDER BY p.position, p.name
    """

    results = db.execute_query(query, {"team_id": team_id})

    output = f"**{team_name}** Current Roster\n\n"

    if not results:
        output += "No players found in current roster."
    else:
        for player in results:
            jersey = f"#{player['jersey_number']}" if player["jersey_number"] else ""
            output += f"- {jersey} {player['name']} ({player['position']})\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_team_stats(team_id: str, season: Optional[str] = None) -> list[TextContent]:
    """Get statistics for a team.

    Args:
        team_id: The unique team identifier
        season: Optional season year (e.g., "2023")
    """
    db = get_db()

    # Get team info
    team_query = "MATCH (t:Team {team_id: $team_id}) RETURN t.name as name"
    team_result = db.execute_query(team_query, {"team_id": team_id})

    if not team_result:
        return [TextContent(type="text", text=f"Team with ID '{team_id}' not found")]

    team_name = team_result[0]["name"]

    # Build query with optional season filter
    base_match = "MATCH (t:Team {team_id: $team_id})"
    params: dict[str, Any] = {"team_id": team_id}

    season_filter = ""
    if season:
        season_filter = "-[:PART_OF]->(c:Competition {season: $season})"
        params["season"] = season

    # Get home matches
    home_query = f"""
    {base_match}
    MATCH (t)-[:PLAYED_HOME]->(m:Match){season_filter}
    RETURN count(m) as matches,
           sum(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) as wins,
           sum(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) as draws,
           sum(CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END) as losses,
           sum(m.home_score) as goals_for,
           sum(m.away_score) as goals_against
    """

    # Get away matches
    away_query = f"""
    {base_match}
    MATCH (t)-[:PLAYED_AWAY]->(m:Match){season_filter}
    RETURN count(m) as matches,
           sum(CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END) as wins,
           sum(CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END) as draws,
           sum(CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END) as losses,
           sum(m.away_score) as goals_for,
           sum(m.home_score) as goals_against
    """

    home_result = db.execute_query(home_query, params)
    away_result = db.execute_query(away_query, params)

    home = home_result[0] if home_result else {}
    away = away_result[0] if away_result else {}

    # Combine stats
    total_matches = (home.get("matches") or 0) + (away.get("matches") or 0)
    total_wins = (home.get("wins") or 0) + (away.get("wins") or 0)
    total_draws = (home.get("draws") or 0) + (away.get("draws") or 0)
    total_losses = (home.get("losses") or 0) + (away.get("losses") or 0)
    goals_for = (home.get("goals_for") or 0) + (away.get("goals_for") or 0)
    goals_against = (home.get("goals_against") or 0) + (away.get("goals_against") or 0)

    output = f"**{team_name}** Statistics"
    if season:
        output += f" (Season {season})"
    output += "\n\n"
    output += f"- Matches Played: {total_matches}\n"
    output += f"- Wins: {total_wins}\n"
    output += f"- Draws: {total_draws}\n"
    output += f"- Losses: {total_losses}\n"
    output += f"- Goals For: {goals_for}\n"
    output += f"- Goals Against: {goals_against}\n"
    output += f"- Goal Difference: {goals_for - goals_against}\n"

    if total_matches > 0:
        win_rate = (total_wins / total_matches) * 100
        output += f"- Win Rate: {win_rate:.1f}%\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_team_history(team_id: str) -> list[TextContent]:
    """Get the historical performance of a team across all competitions.

    Args:
        team_id: The unique team identifier
    """
    db = get_db()

    # Get team info
    team_query = """
    MATCH (t:Team {team_id: $team_id})
    RETURN t.name as name, t.city as city, t.founded_year as founded_year,
           t.stadium as stadium, t.colors as colors
    """
    team_result = db.execute_query(team_query, {"team_id": team_id})

    if not team_result:
        return [TextContent(type="text", text=f"Team with ID '{team_id}' not found")]

    team = team_result[0]

    output = f"**{team['name']}** History\n\n"
    output += f"- City: {team['city']}\n"
    if team["founded_year"]:
        output += f"- Founded: {team['founded_year']}\n"
    if team["stadium"]:
        output += f"- Stadium: {team['stadium']}\n"
    if team["colors"]:
        output += f"- Colors: {team['colors']}\n"
    output += "\n"

    # Get performance by competition
    competition_query = """
    MATCH (t:Team {team_id: $team_id})
    MATCH (t)-[:PLAYED_HOME|PLAYED_AWAY]->(m:Match)-[:PART_OF]->(c:Competition)
    WITH c, t, m
    RETURN c.name as competition, c.season as season,
           count(m) as matches,
           sum(CASE
               WHEN (t)-[:PLAYED_HOME]->(m) AND m.home_score > m.away_score THEN 1
               WHEN (t)-[:PLAYED_AWAY]->(m) AND m.away_score > m.home_score THEN 1
               ELSE 0
           END) as wins,
           sum(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) as draws,
           sum(CASE
               WHEN (t)-[:PLAYED_HOME]->(m) AND m.home_score < m.away_score THEN 1
               WHEN (t)-[:PLAYED_AWAY]->(m) AND m.away_score < m.home_score THEN 1
               ELSE 0
           END) as losses
    ORDER BY c.season DESC, c.name
    """

    results = db.execute_query(competition_query, {"team_id": team_id})

    if results:
        output += "**Competition History:**\n\n"
        current_comp = None
        for row in results:
            if row["competition"] != current_comp:
                current_comp = row["competition"]
                output += f"**{current_comp}**\n"
            points = (row["wins"] or 0) * 3 + (row["draws"] or 0)
            output += f"  - {row['season']}: {row['matches']} matches, "
            output += f"{row['wins']}W-{row['draws']}D-{row['losses']}L ({points} pts)\n"
    else:
        output += "No competition history found."

    # Get notable achievements (titles, etc.) if stored
    titles_query = """
    MATCH (t:Team {team_id: $team_id})-[r:WON]->(c:Competition)
    RETURN c.name as competition, c.season as season
    ORDER BY c.season DESC
    """
    titles = db.execute_query(titles_query, {"team_id": team_id})

    if titles:
        output += "\n**Titles:**\n"
        for title in titles:
            output += f"- {title['competition']} ({title['season']})\n"

    return [TextContent(type="text", text=output)]


# ============================================================================
# Coach Tools
# ============================================================================


@server.tool()
async def get_team_coach(team_id: str) -> list[TextContent]:
    """Get the current coach for a team.

    Args:
        team_id: The unique team identifier
    """
    db = get_db()

    # Get team info
    team_query = "MATCH (t:Team {team_id: $team_id}) RETURN t.name as name"
    team_result = db.execute_query(team_query, {"team_id": team_id})

    if not team_result:
        return [TextContent(type="text", text=f"Team with ID '{team_id}' not found")]

    team_name = team_result[0]["name"]

    # Get current coach
    query = """
    MATCH (c:Coach)-[r:MANAGES]->(t:Team {team_id: $team_id})
    WHERE r.end_date IS NULL OR r.end_date >= date()
    RETURN c.coach_id as coach_id, c.name as name, c.nationality as nationality,
           c.birth_date as birth_date, r.start_date as start_date
    ORDER BY r.start_date DESC
    LIMIT 1
    """

    result = db.execute_query(query, {"team_id": team_id})

    output = f"**{team_name}** Current Coach\n\n"

    if not result:
        output += "No current coach found."
    else:
        coach = result[0]
        output += f"- **{coach['name']}** ({coach['coach_id']})\n"
        output += f"  Nationality: {coach['nationality']}\n"
        if coach["birth_date"]:
            output += f"  Born: {coach['birth_date']}\n"
        if coach["start_date"]:
            output += f"  Started: {coach['start_date']}\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_coach_teams(coach_id: str) -> list[TextContent]:
    """Get teams managed by a coach (current and past).

    Args:
        coach_id: The unique coach identifier
    """
    db = get_db()

    # Get coach info
    coach_query = """
    MATCH (c:Coach {coach_id: $coach_id})
    RETURN c.name as name, c.nationality as nationality
    """
    coach_result = db.execute_query(coach_query, {"coach_id": coach_id})

    if not coach_result:
        return [TextContent(type="text", text=f"Coach with ID '{coach_id}' not found")]

    coach = coach_result[0]

    # Get teams managed
    query = """
    MATCH (c:Coach {coach_id: $coach_id})-[r:MANAGES]->(t:Team)
    RETURN t.team_id as team_id, t.name as team_name, t.city as city,
           r.start_date as start_date, r.end_date as end_date
    ORDER BY r.start_date DESC
    """

    results = db.execute_query(query, {"coach_id": coach_id})

    output = f"**{coach['name']}** Coaching History\n"
    output += f"Nationality: {coach['nationality']}\n\n"

    if not results:
        output += "No teams managed."
    else:
        output += "**Teams:**\n"
        for team in results:
            end = team["end_date"] or "Present"
            output += f"- {team['team_name']} ({team['city']}): {team['start_date']} to {end}\n"

    return [TextContent(type="text", text=output)]


# ============================================================================
# Match Tools
# ============================================================================


@server.tool()
async def get_match_details(match_id: str) -> list[TextContent]:
    """Get details of a specific match.

    Args:
        match_id: The unique match identifier
    """
    db = get_db()

    query = """
    MATCH (home:Team)-[:PLAYED_HOME]->(m:Match {match_id: $match_id})<-[:PLAYED_AWAY]-(away:Team)
    MATCH (m)-[:PART_OF]->(c:Competition)
    OPTIONAL MATCH (m)-[:PLAYED_AT]->(s:Stadium)
    RETURN m.match_id as match_id, m.date as date, m.home_score as home_score,
           m.away_score as away_score, m.attendance as attendance,
           home.name as home_team, away.name as away_team,
           c.name as competition, c.season as season,
           s.name as stadium, s.city as stadium_city, s.capacity as stadium_capacity
    """

    result = db.execute_query(query, {"match_id": match_id})

    if not result:
        return [TextContent(type="text", text=f"Match with ID '{match_id}' not found")]

    match = result[0]

    # Get scorers
    scorers_query = """
    MATCH (p:Player)-[g:SCORED_IN]->(m:Match {match_id: $match_id})
    MATCH (p)-[:PLAYS_FOR]->(t:Team)
    WHERE (t)-[:PLAYED_HOME]->(m) OR (t)-[:PLAYED_AWAY]->(m)
    RETURN p.name as player, g.minute as minute, g.goal_type as type, t.name as team
    ORDER BY g.minute
    """
    scorers = db.execute_query(scorers_query, {"match_id": match_id})

    output = f"**Match Details**\n\n"
    output += f"**{match['home_team']}** {match['home_score']} - {match['away_score']} **{match['away_team']}**\n\n"
    output += f"- Date: {match['date']}\n"
    output += f"- Competition: {match['competition']} ({match['season']})\n"
    if match["stadium"]:
        output += f"- Stadium: {match['stadium']}"
        if match["stadium_city"]:
            output += f" ({match['stadium_city']})"
        if match["stadium_capacity"]:
            output += f" - Capacity: {match['stadium_capacity']:,}"
        output += "\n"
    if match["attendance"]:
        output += f"- Attendance: {match['attendance']:,}\n"

    if scorers:
        output += "\n**Goals:**\n"
        for goal in scorers:
            goal_type = f" ({goal['type']})" if goal["type"] != "regular" else ""
            output += f"- {goal['minute']}' {goal['player']} ({goal['team']}){goal_type}\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_matches_at_stadium(stadium_id: str, limit: int = 20) -> list[TextContent]:
    """Get matches played at a specific stadium.

    Args:
        stadium_id: The unique stadium identifier
        limit: Maximum number of matches to return (default 20)
    """
    db = get_db()

    # Get stadium info
    stadium_query = """
    MATCH (s:Stadium {stadium_id: $stadium_id})
    RETURN s.name as name, s.city as city, s.capacity as capacity
    """
    stadium_result = db.execute_query(stadium_query, {"stadium_id": stadium_id})

    if not stadium_result:
        return [TextContent(type="text", text=f"Stadium with ID '{stadium_id}' not found")]

    stadium = stadium_result[0]

    # Get matches at this stadium
    query = """
    MATCH (m:Match)-[:PLAYED_AT]->(s:Stadium {stadium_id: $stadium_id})
    MATCH (home:Team)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(away:Team)
    MATCH (m)-[:PART_OF]->(c:Competition)
    RETURN m.match_id as match_id, m.date as date, m.home_score as home_score,
           m.away_score as away_score, m.attendance as attendance,
           home.name as home_team, away.name as away_team,
           c.name as competition, c.season as season
    ORDER BY m.date DESC
    LIMIT $limit
    """

    results = db.execute_query(query, {"stadium_id": stadium_id, "limit": limit})

    output = f"**Matches at {stadium['name']}**"
    if stadium["city"]:
        output += f" ({stadium['city']})"
    if stadium["capacity"]:
        output += f" - Capacity: {stadium['capacity']:,}"
    output += "\n\n"

    if not results:
        output += "No matches found at this stadium."
    else:
        output += f"Found {len(results)} match(es):\n\n"
        for match in results:
            output += f"- **{match['date']}**: {match['home_team']} {match['home_score']}-{match['away_score']} {match['away_team']}\n"
            output += f"  ({match['competition']} {match['season']})"
            if match["attendance"]:
                output += f" - Attendance: {match['attendance']:,}"
            output += f" [ID: {match['match_id']}]\n\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def search_matches(
    team: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    competition: Optional[str] = None,
) -> list[TextContent]:
    """Search for matches with various filters.

    Args:
        team: Optional team name to filter by
        date_from: Optional start date (YYYY-MM-DD)
        date_to: Optional end date (YYYY-MM-DD)
        competition: Optional competition name to filter by
    """
    db = get_db()

    query = """
    MATCH (home:Team)-[:PLAYED_HOME]->(m:Match)<-[:PLAYED_AWAY]-(away:Team)
    MATCH (m)-[:PART_OF]->(c:Competition)
    """

    conditions = []
    params: dict[str, Any] = {}

    if team:
        conditions.append("(toLower(home.name) CONTAINS toLower($team) OR toLower(away.name) CONTAINS toLower($team))")
        params["team"] = team

    if date_from:
        conditions.append("m.date >= $date_from")
        params["date_from"] = date_from

    if date_to:
        conditions.append("m.date <= $date_to")
        params["date_to"] = date_to

    if competition:
        conditions.append("toLower(c.name) CONTAINS toLower($competition)")
        params["competition"] = competition

    if conditions:
        query += "WHERE " + " AND ".join(conditions)

    query += """
    RETURN m.match_id as match_id, m.date as date, m.home_score as home_score,
           m.away_score as away_score, home.name as home_team, away.name as away_team,
           c.name as competition, c.season as season
    ORDER BY m.date DESC
    LIMIT 20
    """

    results = db.execute_query(query, params)

    if not results:
        return [TextContent(type="text", text="No matches found matching the criteria")]

    output = f"Found {len(results)} match(es):\n\n"
    for match in results:
        output += f"- **{match['date']}**: {match['home_team']} {match['home_score']}-{match['away_score']} {match['away_team']}\n"
        output += f"  ({match['competition']} {match['season']}) [ID: {match['match_id']}]\n\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_head_to_head(team1_id: str, team2_id: str) -> list[TextContent]:
    """Get head-to-head statistics between two teams.

    Args:
        team1_id: First team's unique identifier
        team2_id: Second team's unique identifier
    """
    db = get_db()

    # Get team names
    teams_query = """
    MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
    RETURN t1.name as team1_name, t2.name as team2_name
    """
    teams_result = db.execute_query(teams_query, {"team1_id": team1_id, "team2_id": team2_id})

    if not teams_result:
        return [TextContent(type="text", text="One or both teams not found")]

    team1_name = teams_result[0]["team1_name"]
    team2_name = teams_result[0]["team2_name"]

    # Get matches between the two teams
    query = """
    MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
    MATCH (m:Match)
    WHERE ((t1)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t2))
       OR ((t2)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t1))
    MATCH (home:Team)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(away:Team)
    RETURN m.date as date, m.home_score as home_score, m.away_score as away_score,
           home.name as home_team, home.team_id as home_id,
           away.name as away_team, away.team_id as away_id
    ORDER BY m.date DESC
    """

    matches = db.execute_query(query, {"team1_id": team1_id, "team2_id": team2_id})

    if not matches:
        return [TextContent(type="text", text=f"No matches found between {team1_name} and {team2_name}")]

    # Calculate statistics
    team1_wins = 0
    team2_wins = 0
    draws = 0
    team1_goals = 0
    team2_goals = 0

    for match in matches:
        if match["home_id"] == team1_id:
            team1_goals += match["home_score"]
            team2_goals += match["away_score"]
            if match["home_score"] > match["away_score"]:
                team1_wins += 1
            elif match["home_score"] < match["away_score"]:
                team2_wins += 1
            else:
                draws += 1
        else:
            team1_goals += match["away_score"]
            team2_goals += match["home_score"]
            if match["away_score"] > match["home_score"]:
                team1_wins += 1
            elif match["away_score"] < match["home_score"]:
                team2_wins += 1
            else:
                draws += 1

    output = f"**Head-to-Head: {team1_name} vs {team2_name}**\n\n"
    output += f"Total Matches: {len(matches)}\n\n"
    output += f"- {team1_name} Wins: {team1_wins}\n"
    output += f"- {team2_name} Wins: {team2_wins}\n"
    output += f"- Draws: {draws}\n\n"
    output += f"Goals: {team1_name} {team1_goals} - {team2_goals} {team2_name}\n\n"

    output += "**Recent Matches:**\n"
    for match in matches[:5]:
        output += f"- {match['date']}: {match['home_team']} {match['home_score']}-{match['away_score']} {match['away_team']}\n"

    return [TextContent(type="text", text=output)]


# ============================================================================
# Competition Tools
# ============================================================================


@server.tool()
async def get_competition_top_scorers(
    competition_id: str, season: str, limit: int = 10
) -> list[TextContent]:
    """Get top scorers in a competition for a given season.

    Args:
        competition_id: The competition identifier
        season: The season year (e.g., "2023")
        limit: Maximum number of players to return (default 10)
    """
    db = get_db()

    # Get competition name
    comp_query = """
    MATCH (c:Competition {competition_id: $competition_id, season: $season})
    RETURN c.name as name
    """
    comp_result = db.execute_query(comp_query, {"competition_id": competition_id, "season": season})

    if not comp_result:
        return [TextContent(type="text", text=f"Competition not found for season {season}")]

    comp_name = comp_result[0]["name"]

    query = """
    MATCH (p:Player)-[g:SCORED_IN]->(m:Match)-[:PART_OF]->(c:Competition {competition_id: $competition_id, season: $season})
    WITH p, count(g) as goals
    ORDER BY goals DESC
    LIMIT $limit
    MATCH (p)-[:PLAYS_FOR]->(t:Team)
    RETURN p.name as player, p.player_id as player_id, goals, collect(DISTINCT t.name)[0] as team
    ORDER BY goals DESC
    """

    results = db.execute_query(query, {"competition_id": competition_id, "season": season, "limit": limit})

    output = f"**Top Scorers - {comp_name} ({season})**\n\n"

    if not results:
        output += "No goals recorded for this competition."
    else:
        for i, player in enumerate(results, 1):
            output += f"{i}. {player['player']} ({player['team']}) - {player['goals']} goals\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def get_competition_standings(
    competition_id: str, season: str
) -> list[TextContent]:
    """Get the standings/league table for a competition season.

    Args:
        competition_id: The competition identifier
        season: The season year (e.g., "2023")
    """
    db = get_db()

    # Get competition info
    comp_query = """
    MATCH (c:Competition {competition_id: $competition_id, season: $season})
    RETURN c.name as name, c.type as type
    """
    comp_result = db.execute_query(comp_query, {"competition_id": competition_id, "season": season})

    if not comp_result:
        return [TextContent(type="text", text=f"Competition not found for season {season}")]

    comp = comp_result[0]

    # Calculate standings from match results
    query = """
    MATCH (t:Team)-[:PLAYED_HOME|PLAYED_AWAY]->(m:Match)-[:PART_OF]->(c:Competition {competition_id: $competition_id, season: $season})
    WITH t, m,
         CASE WHEN (t)-[:PLAYED_HOME]->(m) THEN m.home_score ELSE m.away_score END as goals_for,
         CASE WHEN (t)-[:PLAYED_HOME]->(m) THEN m.away_score ELSE m.home_score END as goals_against
    WITH t,
         count(m) as played,
         sum(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END) as wins,
         sum(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END) as draws,
         sum(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END) as losses,
         sum(goals_for) as gf,
         sum(goals_against) as ga
    RETURN t.team_id as team_id, t.name as team_name,
           played, wins, draws, losses,
           gf as goals_for, ga as goals_against,
           gf - ga as goal_difference,
           wins * 3 + draws as points
    ORDER BY points DESC, goal_difference DESC, goals_for DESC
    """

    results = db.execute_query(query, {"competition_id": competition_id, "season": season})

    output = f"**{comp['name']} ({season}) Standings**\n\n"

    if not results:
        output += "No standings data available."
    else:
        output += "| Pos | Team | P | W | D | L | GF | GA | GD | Pts |\n"
        output += "|-----|------|---|---|---|---|----|----|-------|-----|\n"
        for i, team in enumerate(results, 1):
            output += f"| {i} | {team['team_name']} | {team['played']} | {team['wins']} | "
            output += f"{team['draws']} | {team['losses']} | {team['goals_for']} | "
            output += f"{team['goals_against']} | {team['goal_difference']:+d} | {team['points']} |\n"

    return [TextContent(type="text", text=output)]


# ============================================================================
# Analysis Tools
# ============================================================================


@server.tool()
async def find_common_teammates(player1_id: str, player2_id: str) -> list[TextContent]:
    """Find players who were teammates with both specified players.

    Args:
        player1_id: First player's unique identifier
        player2_id: Second player's unique identifier
    """
    db = get_db()

    # Get player names
    players_query = """
    MATCH (p1:Player {player_id: $player1_id}), (p2:Player {player_id: $player2_id})
    RETURN p1.name as player1_name, p2.name as player2_name
    """
    players_result = db.execute_query(players_query, {"player1_id": player1_id, "player2_id": player2_id})

    if not players_result:
        return [TextContent(type="text", text="One or both players not found")]

    player1_name = players_result[0]["player1_name"]
    player2_name = players_result[0]["player2_name"]

    # Find common teammates through shared teams
    query = """
    MATCH (p1:Player {player_id: $player1_id})-[:PLAYS_FOR]->(t:Team)<-[:PLAYS_FOR]-(teammate:Player)
    MATCH (p2:Player {player_id: $player2_id})-[:PLAYS_FOR]->(t2:Team)<-[:PLAYS_FOR]-(teammate)
    WHERE teammate.player_id <> $player1_id AND teammate.player_id <> $player2_id
    RETURN DISTINCT teammate.name as name, teammate.player_id as player_id,
           collect(DISTINCT t.name) as teams_with_p1,
           collect(DISTINCT t2.name) as teams_with_p2
    """

    results = db.execute_query(query, {"player1_id": player1_id, "player2_id": player2_id})

    output = f"**Common Teammates of {player1_name} and {player2_name}**\n\n"

    if not results:
        output += "No common teammates found."
    else:
        output += f"Found {len(results)} common teammate(s):\n\n"
        for player in results:
            output += f"- **{player['name']}**\n"
            output += f"  With {player1_name}: {', '.join(player['teams_with_p1'])}\n"
            output += f"  With {player2_name}: {', '.join(player['teams_with_p2'])}\n\n"

    return [TextContent(type="text", text=output)]


@server.tool()
async def find_players_who_played_for_both_teams(team1_id: str, team2_id: str) -> list[TextContent]:
    """Find players who have played for both specified teams.

    Args:
        team1_id: First team's unique identifier
        team2_id: Second team's unique identifier
    """
    db = get_db()

    query = """
    MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
    MATCH (p:Player)-[r1:PLAYS_FOR]->(t1)
    MATCH (p)-[r2:PLAYS_FOR]->(t2)
    RETURN t1.name as team1_name, t2.name as team2_name,
           p.name as player, p.player_id as player_id,
           r1.start_date as team1_start, r1.end_date as team1_end,
           r2.start_date as team2_start, r2.end_date as team2_end
    ORDER BY r1.start_date
    """

    results = db.execute_query(query, {"team1_id": team1_id, "team2_id": team2_id})

    if not results:
        return [TextContent(type="text", text="No players found who played for both teams")]

    team1_name = results[0]["team1_name"]
    team2_name = results[0]["team2_name"]

    output = f"**Players who played for both {team1_name} and {team2_name}**\n\n"
    output += f"Found {len(results)} player(s):\n\n"

    for player in results:
        output += f"- **{player['player']}**\n"
        t1_end = player["team1_end"] or "Present"
        t2_end = player["team2_end"] or "Present"
        output += f"  {team1_name}: {player['team1_start']} to {t1_end}\n"
        output += f"  {team2_name}: {player['team2_start']} to {t2_end}\n\n"

    return [TextContent(type="text", text=output)]


def main():
    """Run the MCP server."""
    server.run()


if __name__ == "__main__":
    main()
