Feature: Analysis Queries
  As a user of the Brazilian Soccer Knowledge Graph
  I want to perform analysis queries
  So that I can discover relationships and patterns

  Background:
    Given the database is populated with sample data

  Scenario: Find players who played for both rival teams
    When I search for players who played for both "T003" and "T004"
    Then I should find players who have contracts with both teams

  Scenario: Find common teammates
    When I search for common teammates of "P010" and "P011"
    Then I should find players who played with both at some team

  Scenario: Get top scorers in a competition
    When I get top scorers for competition "C001" season "2023"
    Then I should see a ranked list of scorers
    And the top scorer should have the most goals

  Scenario: Get player career history
    When I get career history for player "P012"
    Then I should see all teams the player has played for
    And I should see the time periods at each team
