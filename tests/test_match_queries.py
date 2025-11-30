"""BDD tests for match query functionality."""

import pytest
from pytest_bdd import scenarios, given, when, then, parsers

# Load scenarios from feature file
scenarios("match_queries.feature")


@pytest.fixture
def context():
    """Shared context for test steps."""
    return {"match": None, "matches": None, "h2h": None}


@given("the database is populated with sample data")
def database_populated(db_with_sample_data, context):
    """Ensure database is populated."""
    context["db"] = db_with_sample_data


@when(parsers.parse('I get details for match "{match_id}"'))
def get_match_details(context, match_id):
    """Get details for a specific match."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (home:Team)-[:PLAYED_HOME]->(m:Match {match_id: $match_id})<-[:PLAYED_AWAY]-(away:Team)
        MATCH (m)-[:PART_OF]->(c:Competition)
        RETURN m.match_id as match_id, m.date as date, m.home_score as home_score,
               m.away_score as away_score, m.attendance as attendance,
               home.name as home_team, away.name as away_team,
               c.name as competition, c.season as season
        """,
        {"match_id": match_id}
    )
    context["match"] = results[0] if results else None


@when(parsers.parse('I search for matches with team "{team}"'))
def search_matches_by_team(context, team):
    """Search for matches involving a team."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (home:Team)-[:PLAYED_HOME]->(m:Match)<-[:PLAYED_AWAY]-(away:Team)
        MATCH (m)-[:PART_OF]->(c:Competition)
        WHERE toLower(home.name) CONTAINS toLower($team)
           OR toLower(away.name) CONTAINS toLower($team)
        RETURN m.match_id as match_id, m.date as date, m.home_score as home_score,
               m.away_score as away_score, home.name as home_team, away.name as away_team,
               c.name as competition, c.season as season
        ORDER BY m.date DESC
        LIMIT 20
        """,
        {"team": team}
    )
    context["matches"] = results
    context["search_team"] = team


@when(parsers.parse('I search for matches from "{date_from}" to "{date_to}"'))
def search_matches_by_date(context, date_from, date_to):
    """Search for matches in a date range."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (home:Team)-[:PLAYED_HOME]->(m:Match)<-[:PLAYED_AWAY]-(away:Team)
        MATCH (m)-[:PART_OF]->(c:Competition)
        WHERE m.date >= $date_from AND m.date <= $date_to
        RETURN m.match_id as match_id, m.date as date, m.home_score as home_score,
               m.away_score as away_score, home.name as home_team, away.name as away_team,
               c.name as competition, c.season as season
        ORDER BY m.date DESC
        """,
        {"date_from": date_from, "date_to": date_to}
    )
    context["matches"] = results
    context["date_from"] = date_from
    context["date_to"] = date_to


@when(parsers.parse('I get head-to-head for teams "{team1_id}" and "{team2_id}"'))
def get_head_to_head(context, team1_id, team2_id):
    """Get head-to-head stats for two teams."""
    db = context["db"]

    # Get team names
    teams = db.execute_query(
        """
        MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
        RETURN t1.name as team1_name, t2.name as team2_name
        """,
        {"team1_id": team1_id, "team2_id": team2_id}
    )

    # Get matches between them
    matches = db.execute_query(
        """
        MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
        MATCH (m:Match)
        WHERE ((t1)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t2))
           OR ((t2)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(t1))
        MATCH (home:Team)-[:PLAYED_HOME]->(m)<-[:PLAYED_AWAY]-(away:Team)
        RETURN m.date as date, m.home_score as home_score, m.away_score as away_score,
               home.name as home_team, home.team_id as home_id,
               away.name as away_team, away.team_id as away_id
        ORDER BY m.date DESC
        """,
        {"team1_id": team1_id, "team2_id": team2_id}
    )

    if not teams or not matches:
        context["h2h"] = None
        return

    team1_name = teams[0]["team1_name"]
    team2_name = teams[0]["team2_name"]

    # Calculate statistics
    team1_wins = 0
    team2_wins = 0
    draws = 0

    for match in matches:
        if match["home_id"] == team1_id:
            if match["home_score"] > match["away_score"]:
                team1_wins += 1
            elif match["home_score"] < match["away_score"]:
                team2_wins += 1
            else:
                draws += 1
        else:
            if match["away_score"] > match["home_score"]:
                team1_wins += 1
            elif match["away_score"] < match["home_score"]:
                team2_wins += 1
            else:
                draws += 1

    context["h2h"] = {
        "team1_name": team1_name,
        "team2_name": team2_name,
        "total_matches": len(matches),
        "team1_wins": team1_wins,
        "team2_wins": team2_wins,
        "draws": draws,
        "matches": matches,
    }


@then("I should see the match score")
def check_match_score(context):
    """Check that match score is visible."""
    match = context["match"]
    assert match is not None, "Expected match details"
    assert "home_score" in match, "Expected home_score in match"
    assert "away_score" in match, "Expected away_score in match"


@then("I should see the competing teams")
def check_competing_teams(context):
    """Check that competing teams are visible."""
    match = context["match"]
    assert match is not None, "Expected match details"
    assert "home_team" in match, "Expected home_team in match"
    assert "away_team" in match, "Expected away_team in match"
    assert match["home_team"], "Expected non-empty home_team"
    assert match["away_team"], "Expected non-empty away_team"


@then("I should see the match date")
def check_match_date(context):
    """Check that match date is visible."""
    match = context["match"]
    assert match is not None, "Expected match details"
    assert "date" in match, "Expected date in match"
    assert match["date"], "Expected non-empty date"


@then(parsers.parse("I should find at least {count:d} match"))
@then(parsers.parse("I should find at least {count:d} matches"))
def check_min_match_count(context, count):
    """Check minimum number of matches found."""
    assert len(context["matches"]) >= count, \
        f"Expected at least {count} matches, found {len(context['matches'])}"


@then(parsers.parse('all matches should involve "{team}"'))
def check_all_matches_involve_team(context, team):
    """Check all matches involve a specific team."""
    for match in context["matches"]:
        team_lower = team.lower()
        home_lower = match["home_team"].lower()
        away_lower = match["away_team"].lower()
        assert team_lower in home_lower or team_lower in away_lower, \
            f"Match {match['match_id']} doesn't involve {team}: {match['home_team']} vs {match['away_team']}"


@then("I should find matches within that date range")
def check_matches_in_date_range(context):
    """Check that matches are within the specified date range."""
    date_from = context["date_from"]
    date_to = context["date_to"]

    for match in context["matches"]:
        assert date_from <= match["date"] <= date_to, \
            f"Match date {match['date']} is outside range {date_from} - {date_to}"


@then("I should see the total number of matches")
def check_h2h_total_matches(context):
    """Check that total matches is visible in head-to-head."""
    h2h = context["h2h"]
    assert h2h is not None, "Expected head-to-head data"
    assert "total_matches" in h2h, "Expected total_matches in h2h"
    assert h2h["total_matches"] >= 0, "Expected valid total_matches"


@then("I should see wins for each team")
def check_h2h_wins(context):
    """Check that wins for each team are visible."""
    h2h = context["h2h"]
    assert h2h is not None, "Expected head-to-head data"
    assert "team1_wins" in h2h, "Expected team1_wins in h2h"
    assert "team2_wins" in h2h, "Expected team2_wins in h2h"


@then("I should see the number of draws")
def check_h2h_draws(context):
    """Check that draws are visible."""
    h2h = context["h2h"]
    assert h2h is not None, "Expected head-to-head data"
    assert "draws" in h2h, "Expected draws in h2h"
