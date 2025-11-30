Feature: Match Queries
  As a user of the Brazilian Soccer Knowledge Graph
  I want to query match information
  So that I can analyze game results and statistics

  Background:
    Given the database is populated with sample data

  Scenario: Get match details
    When I get details for match "M001"
    Then I should see the match score
    And I should see the competing teams
    And I should see the match date

  Scenario: Search matches by team
    When I search for matches with team "Flamengo"
    Then I should find at least 1 match
    And all matches should involve "Flamengo"

  Scenario: Search matches by date range
    When I search for matches from "2023-01-01" to "2023-12-31"
    Then I should find matches within that date range

  Scenario: Get head-to-head statistics
    When I get head-to-head for teams "T001" and "T002"
    Then I should see the total number of matches
    And I should see wins for each team
    And I should see the number of draws
