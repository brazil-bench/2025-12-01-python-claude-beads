Feature: Team Operations
  As a user of the Brazilian Soccer Knowledge Graph
  I want to search for teams and get their statistics
  So that I can analyze team performance

  Background:
    Given the database is populated with sample data

  Scenario: Search for a team by name
    When I search for team "Flamengo"
    Then I should find 1 team
    And the team should be located in "Rio de Janeiro"

  Scenario: Search for teams by partial name
    When I search for team "São Paulo"
    Then I should find at least 1 team

  Scenario: Get team roster
    When I get the roster for team "T001"
    Then I should see players in the roster
    And the roster should include "Gabriel Barbosa"

  Scenario: Get team statistics
    When I get statistics for team "T001"
    Then I should see match statistics
    And the statistics should include wins, draws, and losses

  Scenario: Get team statistics for specific season
    When I get statistics for team "T001" for season "2023"
    Then I should see match statistics for that season
