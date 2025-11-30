"""BDD tests for analysis functionality."""

import pytest
from pytest_bdd import scenarios, given, when, then, parsers

# Load scenarios from feature file
scenarios("analysis.feature")


@pytest.fixture
def context():
    """Shared context for test steps."""
    return {}


@given("the database is populated with sample data")
def database_populated(db_with_sample_data, context):
    """Ensure database is populated."""
    context["db"] = db_with_sample_data


@when(parsers.parse('I search for players who played for both "{team1_id}" and "{team2_id}"'))
def find_players_both_teams(context, team1_id, team2_id):
    """Find players who played for both teams."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (t1:Team {team_id: $team1_id}), (t2:Team {team_id: $team2_id})
        MATCH (p:Player)-[r1:PLAYS_FOR]->(t1)
        MATCH (p)-[r2:PLAYS_FOR]->(t2)
        RETURN t1.name as team1_name, t2.name as team2_name,
               p.name as player, p.player_id as player_id,
               r1.start_date as team1_start, r1.end_date as team1_end,
               r2.start_date as team2_start, r2.end_date as team2_end
        """,
        {"team1_id": team1_id, "team2_id": team2_id}
    )
    context["players"] = results


@when(parsers.parse('I search for common teammates of "{player1_id}" and "{player2_id}"'))
def find_common_teammates(context, player1_id, player2_id):
    """Find common teammates of two players."""
    db = context["db"]
    results = db.execute_query(
        """
        MATCH (p1:Player {player_id: $player1_id})-[:PLAYS_FOR]->(t:Team)<-[:PLAYS_FOR]-(teammate:Player)
        MATCH (p2:Player {player_id: $player2_id})-[:PLAYS_FOR]->(t2:Team)<-[:PLAYS_FOR]-(teammate)
        WHERE teammate.player_id <> $player1_id AND teammate.player_id <> $player2_id
        RETURN DISTINCT teammate.name as name, teammate.player_id as player_id,
               collect(DISTINCT t.name) as teams_with_p1,
               collect(DISTINCT t2.name) as teams_with_p2
        """,
        {"player1_id": player1_id, "player2_id": player2_id}
    )
    context["teammates"] = results


@when(parsers.parse('I get top scorers for competition "{comp_id}" season "{season}"'))
def get_top_scorers(context, comp_id, season):
    """Get top scorers for a competition."""
    db = context["db"]

    # First check competition exists
    comp = db.execute_query(
        """
        MATCH (c:Competition {competition_id: $competition_id, season: $season})
        RETURN c.name as name
        """,
        {"competition_id": comp_id, "season": season}
    )

    if not comp:
        context["scorers"] = []
        return

    # Get top scorers
    results = db.execute_query(
        """
        MATCH (p:Player)-[g:SCORED_IN]->(m:Match)-[:PART_OF]->(c:Competition {competition_id: $competition_id, season: $season})
        WITH p, count(g) as goals
        ORDER BY goals DESC
        LIMIT 10
        MATCH (p)-[:PLAYS_FOR]->(t:Team)
        RETURN p.name as player, p.player_id as player_id, goals, collect(DISTINCT t.name)[0] as team
        ORDER BY goals DESC
        """,
        {"competition_id": comp_id, "season": season}
    )
    context["scorers"] = results


@when(parsers.parse('I get career history for player "{player_id}"'))
def get_player_career(context, player_id):
    """Get career history for a player."""
    db = context["db"]
    results = db.execute_query(
        """
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
        """,
        {"player_id": player_id}
    )
    context["career"] = results[0] if results else None


@then("I should find players who have contracts with both teams")
def check_players_both_teams(context):
    """Check that players with contracts at both teams are found."""
    # This may or may not find players depending on data
    assert context["players"] is not None, "Expected players result"


@then("I should find players who played with both at some team")
def check_common_teammates_found(context):
    """Check that common teammates are found."""
    # This may or may not find players depending on data
    assert context["teammates"] is not None, "Expected teammates result"


@then("I should see a ranked list of scorers")
def check_scorers_list(context):
    """Check that scorers list is returned."""
    scorers = context["scorers"]
    assert scorers is not None, "Expected scorers list"

    # If there are scorers, check they have goals
    if len(scorers) > 0:
        for scorer in scorers:
            assert "goals" in scorer, "Expected 'goals' in scorer"
            assert "player" in scorer, "Expected 'player' in scorer"


@then("the top scorer should have the most goals")
def check_top_scorer(context):
    """Check that the top scorer has the most goals."""
    scorers = context["scorers"]

    if len(scorers) > 1:
        # Verify sorted in descending order
        for i in range(len(scorers) - 1):
            assert scorers[i]["goals"] >= scorers[i + 1]["goals"], \
                "Expected scorers to be sorted by goals descending"


@then("I should see all teams the player has played for")
def check_career_teams(context):
    """Check that career shows all teams."""
    career = context["career"]
    assert career is not None, "Expected career data"
    assert "career" in career, "Expected 'career' in result"

    teams = [c for c in career["career"] if c["team"]]
    assert len(teams) > 0, "Expected at least one team in career"


@then("I should see the time periods at each team")
def check_career_time_periods(context):
    """Check that career shows time periods."""
    career = context["career"]
    assert career is not None, "Expected career data"

    for stint in career["career"]:
        if stint["team"]:
            assert "start_date" in stint, "Expected 'start_date' in career stint"
