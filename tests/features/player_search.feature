Feature: Player Search
  As a user of the Brazilian Soccer Knowledge Graph
  I want to search for players by name
  So that I can find information about specific players

  Background:
    Given the database is populated with sample data

  Scenario: Search for a player by full name
    When I search for player "Gabriel Barbosa"
    Then I should find at least 1 player
    And the results should include a player named "Gabriel Barbosa (Gabigol)"

  Scenario: Search for a player by partial name
    When I search for player "Neymar"
    Then I should find at least 1 player
    And the results should include a player named "Neymar Jr"

  Scenario: Search for a player that doesn't exist
    When I search for player "Unknown Player XYZ"
    Then I should find 0 players

  Scenario: Search for a player filtering by team
    When I search for player "Pedro" with team "Flamengo"
    Then I should find at least 1 player
    And all results should include team "Flamengo"

  Scenario: Search for a player filtering by position
    When I search for player "Gabriel" with position "Forward"
    Then I should find at least 1 player
    And all results should have position "Forward"
