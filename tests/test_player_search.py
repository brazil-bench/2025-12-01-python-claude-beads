"""BDD tests for player search functionality."""

import pytest
from pytest_bdd import scenarios, given, when, then, parsers
from unittest.mock import patch

# Load scenarios from feature file
scenarios("player_search.feature")


@pytest.fixture
def context():
    """Shared context for test steps."""
    return {"results": None, "result_text": ""}


@given("the database is populated with sample data")
def database_populated(db_with_sample_data, context):
    """Ensure database is populated."""
    context["db"] = db_with_sample_data


@when(parsers.parse('I search for player "{name}"'))
def search_player_by_name(context, name):
    """Search for a player by name."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (p:Player)
        WHERE toLower(p.name) CONTAINS toLower($name)
        OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
        RETURN p.player_id as player_id, p.name as name, p.nationality as nationality,
               p.position as position, p.birth_date as birth_date,
               collect(DISTINCT t.name) as teams
        """,
        {"name": name}
    )
    context["results"] = results


@when(parsers.parse('I search for player "{name}" with team "{team}"'))
def search_player_with_team(context, name, team):
    """Search for a player by name and team."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (p:Player)
        WHERE toLower(p.name) CONTAINS toLower($name)
        MATCH (p)-[:PLAYS_FOR]->(t:Team)
        WHERE toLower(t.name) CONTAINS toLower($team)
        OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t2:Team)
        RETURN p.player_id as player_id, p.name as name, p.nationality as nationality,
               p.position as position, p.birth_date as birth_date,
               collect(DISTINCT t2.name) as teams
        """,
        {"name": name, "team": team}
    )
    context["results"] = results


@when(parsers.parse('I search for player "{name}" with position "{position}"'))
def search_player_with_position(context, name, position):
    """Search for a player by name and position."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (p:Player)
        WHERE toLower(p.name) CONTAINS toLower($name)
        AND toLower(p.position) = toLower($position)
        OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
        RETURN p.player_id as player_id, p.name as name, p.nationality as nationality,
               p.position as position, p.birth_date as birth_date,
               collect(DISTINCT t.name) as teams
        """,
        {"name": name, "position": position}
    )
    context["results"] = results


@then(parsers.parse("I should find at least {count:d} player"))
@then(parsers.parse("I should find at least {count:d} players"))
def check_min_player_count(context, count):
    """Check minimum number of players found."""
    assert len(context["results"]) >= count, \
        f"Expected at least {count} players, found {len(context['results'])}"


@then(parsers.parse("I should find {count:d} players"))
@then(parsers.parse("I should find {count:d} player"))
def check_exact_player_count(context, count):
    """Check exact number of players found."""
    assert len(context["results"]) == count, \
        f"Expected {count} players, found {len(context['results'])}"


@then(parsers.parse('the results should include a player named "{name}"'))
def check_player_in_results(context, name):
    """Check if a specific player is in the results."""
    player_names = [r["name"] for r in context["results"]]
    assert any(name in pn for pn in player_names), \
        f"Player '{name}' not found in results: {player_names}"


@then(parsers.parse('all results should include team "{team}"'))
def check_all_results_have_team(context, team):
    """Check all results include a specific team."""
    for result in context["results"]:
        teams = result.get("teams", [])
        assert any(team in t for t in teams), \
            f"Team '{team}' not found in result teams: {teams}"


@then(parsers.parse('all results should have position "{position}"'))
def check_all_results_have_position(context, position):
    """Check all results have a specific position."""
    for result in context["results"]:
        assert result["position"].lower() == position.lower(), \
            f"Expected position '{position}', got '{result['position']}'"
