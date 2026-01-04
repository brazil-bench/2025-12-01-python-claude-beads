# Brazilian Soccer MCP Server

A Model Context Protocol (MCP) server providing access to a Neo4j knowledge graph of Brazilian soccer data. Built using Python with the official MCP SDK.

## Features

- **Neo4j Knowledge Graph** with 6 entity types: Player, Team, Match, Competition, Stadium, Coach
- **18 MCP Tools** for natural language queries about Brazilian soccer
- **BDD Test Suite** with 18 comprehensive scenarios
- **Kaggle Data Integration** for real-world soccer data

## Quick Start

### Prerequisites

- Python 3.11+
- Neo4j Database (local or cloud)

### Installation

```bash
# Clone the repository
git clone https://github.com/brazil-bench/2025-12-01-python-claude-beads.git
cd 2025-12-01-python-claude-beads

# Install dependencies
pip install -e ".[dev]"
```

### Environment Variables

Set the following environment variables for Neo4j connection:

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="your-password"
```

### Running the MCP Server

```bash
python -m brazilian_soccer_mcp.server
```

## MCP Configuration

### Claude Desktop / Claude Code

Add the server to your Claude configuration:

```bash
claude mcp add brazilian-soccer -- python -m brazilian_soccer_mcp.server
```

Or manually add to your MCP settings:

```json
{
  "mcpServers": {
    "brazilian-soccer": {
      "command": "python",
      "args": ["-m", "brazilian_soccer_mcp.server"],
      "env": {
        "NEO4J_URI": "bolt://localhost:7687",
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": "your-password"
      }
    }
  }
}
```

## Example Questions

Ask Claude about Brazilian soccer:

### Player Queries
- "Find players named Neymar"
- "Get Neymar's career statistics"
- "Show me transfer history for player ID 'player-123'"
- "Find players who played for both Flamengo and Fluminense"

### Team Queries
- "Search for teams in Rio de Janeiro"
- "Get the current roster for Palmeiras"
- "Show Flamengo's statistics for 2023"
- "Get the history of Corinthians"
- "Who is the current coach of Santos?"

### Match Queries
- "Find matches between Flamengo and Fluminense"
- "Get details for match ID 'match-456'"
- "Show head-to-head record between Palmeiras and Santos"
- "What matches were played at Maracana?"

### Competition Queries
- "Get top scorers in Brasileirao 2023"
- "Show the league standings for Brasileirao 2023"

### Analysis Queries
- "Find common teammates between two players"
- "Which players have played for both rival teams?"

## MCP Tools Reference

| Tool | Description |
|------|-------------|
| `search_player` | Search for players by name, team, or position |
| `get_player_stats` | Get statistics for a specific player |
| `get_player_career` | Get career history of a player |
| `get_player_transfers` | Get transfer history between teams |
| `search_team` | Search for teams by name |
| `get_team_roster` | Get current roster for a team |
| `get_team_stats` | Get team statistics |
| `get_team_history` | Get historical performance across competitions |
| `get_team_coach` | Get current coach for a team |
| `get_coach_teams` | Get teams managed by a coach |
| `get_match_details` | Get details of a specific match |
| `search_matches` | Search matches with various filters |
| `get_head_to_head` | Get head-to-head stats between two teams |
| `get_matches_at_stadium` | Get matches played at a stadium |
| `get_competition_top_scorers` | Get top scorers in a competition |
| `get_competition_standings` | Get league table for a competition |
| `find_common_teammates` | Find players who were teammates with both specified players |
| `find_players_who_played_for_both_teams` | Find players who played for both teams |

## Graph Schema

### Entities
- **Player**: player_id, name, birth_date, nationality, position, jersey_number
- **Team**: team_id, name, city, stadium, founded_year, colors
- **Match**: match_id, date, home_score, away_score, attendance
- **Competition**: competition_id, name, season, type, tier
- **Stadium**: stadium_id, name, city, capacity, opened_year
- **Coach**: coach_id, name, nationality, birth_date

### Relationships
- `Player -[:PLAYS_FOR]-> Team`
- `Player -[:SCORED_IN]-> Match`
- `Team -[:PLAYED_HOME]-> Match`
- `Team -[:PLAYED_AWAY]-> Match`
- `Match -[:PART_OF]-> Competition`
- `Match -[:PLAYED_AT]-> Stadium`
- `Coach -[:MANAGES]-> Team`

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=brazilian_soccer_mcp

# Run specific test file
pytest tests/test_player_search.py
```

## Development

This project uses [beads](https://github.com/steveyegge/beads) for task coordination with Claude Code.

```bash
# Check project status
bd stats

# Find available work
bd ready

# Sync changes
bd sync
```

## License

MIT
