"""
Automated scoring engine for AI-generated Delta Lake pipeline implementations.

This package provides dimension-specific scorers that analyze code using static
analysis tools (pylint, radon, bandit, pytest) and map results to the 1-5 rubric scale
defined in benchmark/scoring_rubric.yaml.
"""

from .correctness import score_correctness
from .maintainability import score_maintainability
from .security import score_security
from .performance import score_performance
from .documentation import score_documentation

__all__ = [
    'score_correctness',
    'score_maintainability',
    'score_security',
    'score_performance',
    'score_documentation',
]
