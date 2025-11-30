"""Data models for Brazilian Soccer Knowledge Graph."""

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Player:
    player_id: str
    name: str
    birth_date: Optional[date]
    nationality: str
    position: str
    jersey_number: Optional[int] = None


@dataclass
class Team:
    team_id: str
    name: str
    city: str
    stadium: Optional[str] = None
    founded_year: Optional[int] = None
    colors: Optional[str] = None


@dataclass
class Match:
    match_id: str
    date: date
    home_team_id: str
    away_team_id: str
    home_score: int
    away_score: int
    competition_id: str
    stadium_id: Optional[str] = None
    attendance: Optional[int] = None


@dataclass
class Competition:
    competition_id: str
    name: str
    season: str
    type: str  # 'league' or 'cup'
    tier: int = 1


@dataclass
class Stadium:
    stadium_id: str
    name: str
    city: str
    capacity: Optional[int] = None
    opened_year: Optional[int] = None


@dataclass
class Coach:
    coach_id: str
    name: str
    nationality: str
    birth_date: Optional[date] = None


@dataclass
class PlayerContract:
    player_id: str
    team_id: str
    start_date: date
    end_date: Optional[date] = None


@dataclass
class Goal:
    player_id: str
    match_id: str
    minute: int
    goal_type: str = "regular"  # 'regular', 'penalty', 'own_goal', 'free_kick'


@dataclass
class Card:
    player_id: str
    match_id: str
    minute: int
    card_type: str  # 'yellow' or 'red'
