"""BDD tests for team operations functionality."""

import pytest
from pytest_bdd import scenarios, given, when, then, parsers

# Load scenarios from feature file
scenarios("team_operations.feature")


@pytest.fixture
def context():
    """Shared context for test steps."""
    return {"results": None, "stats": None}


@given("the database is populated with sample data")
def database_populated(db_with_sample_data, context):
    """Ensure database is populated."""
    context["db"] = db_with_sample_data


@when(parsers.parse('I search for team "{name}"'))
def search_team_by_name(context, name):
    """Search for a team by name."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (t:Team)
        WHERE toLower(t.name) CONTAINS toLower($name)
        RETURN t.team_id as team_id, t.name as name, t.city as city,
               t.stadium as stadium, t.founded_year as founded_year, t.colors as colors
        """,
        {"name": name}
    )
    context["results"] = results


@when(parsers.parse('I get the roster for team "{team_id}"'))
def get_team_roster(context, team_id):
    """Get roster for a team."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (p:Player)-[r:PLAYS_FOR]->(t:Team {team_id: $team_id})
        WHERE r.end_date IS NULL OR r.end_date >= date()
        RETURN p.player_id as player_id, p.name as name, p.position as position,
               p.jersey_number as jersey_number, r.start_date as joined
        ORDER BY p.position, p.name
        """,
        {"team_id": team_id}
    )
    context["roster"] = results


@when(parsers.parse('I get statistics for team "{team_id}"'))
def get_team_stats(context, team_id):
    """Get statistics for a team."""
    db = context["db"]

    # Get home stats
    home_result = db.execute_query(
        """
        MATCH (t:Team {team_id: $team_id})
        MATCH (t)-[:PLAYED_HOME]->(m:Match)
        RETURN count(m) as matches,
               sum(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) as draws,
               sum(CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END) as losses
        """,
        {"team_id": team_id}
    )

    # Get away stats
    away_result = db.execute_query(
        """
        MATCH (t:Team {team_id: $team_id})
        MATCH (t)-[:PLAYED_AWAY]->(m:Match)
        RETURN count(m) as matches,
               sum(CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END) as draws,
               sum(CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END) as losses
        """,
        {"team_id": team_id}
    )

    home = home_result[0] if home_result else {"matches": 0, "wins": 0, "draws": 0, "losses": 0}
    away = away_result[0] if away_result else {"matches": 0, "wins": 0, "draws": 0, "losses": 0}

    context["stats"] = {
        "matches": home["matches"] + away["matches"],
        "wins": home["wins"] + away["wins"],
        "draws": home["draws"] + away["draws"],
        "losses": home["losses"] + away["losses"],
    }


@when(parsers.parse('I get statistics for team "{team_id}" for season "{season}"'))
def get_team_stats_for_season(context, team_id, season):
    """Get statistics for a team for a specific season."""
    db = context["db"]

    # Get home stats with season filter
    home_result = db.execute_query(
        """
        MATCH (t:Team {team_id: $team_id})
        MATCH (t)-[:PLAYED_HOME]->(m:Match)-[:PART_OF]->(c:Competition {season: $season})
        RETURN count(m) as matches,
               sum(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) as draws,
               sum(CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END) as losses
        """,
        {"team_id": team_id, "season": season}
    )

    # Get away stats with season filter
    away_result = db.execute_query(
        """
        MATCH (t:Team {team_id: $team_id})
        MATCH (t)-[:PLAYED_AWAY]->(m:Match)-[:PART_OF]->(c:Competition {season: $season})
        RETURN count(m) as matches,
               sum(CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END) as wins,
               sum(CASE WHEN m.away_score = m.home_score THEN 1 ELSE 0 END) as draws,
               sum(CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END) as losses
        """,
        {"team_id": team_id, "season": season}
    )

    home = home_result[0] if home_result else {"matches": 0, "wins": 0, "draws": 0, "losses": 0}
    away = away_result[0] if away_result else {"matches": 0, "wins": 0, "draws": 0, "losses": 0}

    context["stats"] = {
        "matches": home["matches"] + away["matches"],
        "wins": home["wins"] + away["wins"],
        "draws": home["draws"] + away["draws"],
        "losses": home["losses"] + away["losses"],
        "season": season,
    }


@then(parsers.parse("I should find {count:d} team"))
@then(parsers.parse("I should find {count:d} teams"))
def check_team_count(context, count):
    """Check exact number of teams found."""
    assert len(context["results"]) == count, \
        f"Expected {count} teams, found {len(context['results'])}"


@then(parsers.parse("I should find at least {count:d} team"))
@then(parsers.parse("I should find at least {count:d} teams"))
def check_min_team_count(context, count):
    """Check minimum number of teams found."""
    assert len(context["results"]) >= count, \
        f"Expected at least {count} teams, found {len(context['results'])}"


@then(parsers.parse('the team should be located in "{city}"'))
def check_team_city(context, city):
    """Check if the team is in the expected city."""
    team = context["results"][0]
    assert team["city"] == city, \
        f"Expected city '{city}', got '{team['city']}'"


@then("I should see players in the roster")
def check_roster_not_empty(context):
    """Check that the roster is not empty."""
    assert len(context["roster"]) > 0, "Expected players in roster"


@then(parsers.parse('the roster should include "{player_name}"'))
def check_player_in_roster(context, player_name):
    """Check if a specific player is in the roster."""
    player_names = [p["name"] for p in context["roster"]]
    assert any(player_name in name for name in player_names), \
        f"Player '{player_name}' not found in roster: {player_names}"


@then("I should see match statistics")
def check_stats_exist(context):
    """Check that statistics are returned."""
    assert context["stats"] is not None, "Expected statistics to be returned"
    assert context["stats"]["matches"] >= 0, "Expected valid match count"


@then("the statistics should include wins, draws, and losses")
def check_stats_fields(context):
    """Check that statistics include required fields."""
    stats = context["stats"]
    assert "wins" in stats, "Expected 'wins' in statistics"
    assert "draws" in stats, "Expected 'draws' in statistics"
    assert "losses" in stats, "Expected 'losses' in statistics"


@then("I should see match statistics for that season")
def check_season_stats(context):
    """Check that season-specific statistics are returned."""
    assert context["stats"] is not None, "Expected statistics to be returned"
    assert "season" in context["stats"], "Expected season in statistics"
