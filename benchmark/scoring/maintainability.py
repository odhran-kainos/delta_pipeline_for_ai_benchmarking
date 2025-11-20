"""
Maintainability dimension scorer (Weight: 15%).

Evaluates:
- Code quality (pylint score)
- Cyclomatic complexity (radon)
- Code structure and readability
- Type hints and docstrings
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional


def score_maintainability(
    pylint_json_path: Optional[Path],
    radon_json_path: Optional[Path],
    implementation_path: Path
) -> Dict[str, Any]:
    """
    Calculate maintainability score based on static analysis.
    
    Args:
        pylint_json_path: Path to pylint JSON output
        radon_json_path: Path to radon cyclomatic complexity JSON
        implementation_path: Path to implementation code directory
    
    Returns:
        Dict with score, confidence, method, and evidence
    """
    evidence = []
    issues = []
    
    # 1. Parse pylint results
    pylint_score = 0.0
    if pylint_json_path and pylint_json_path.exists():
        try:
            with open(pylint_json_path, 'r') as f:
                # Pylint JSON format varies; try to extract score
                pylint_data = json.load(f)
                # Modern pylint uses 'score' key in statistics
                if isinstance(pylint_data, dict) and 'score' in pylint_data:
                    pylint_score = pylint_data['score']
                # Fallback: parse from messages
                elif isinstance(pylint_data, list):
                    # Count issues by type
                    error_count = sum(1 for msg in pylint_data if msg.get('type') == 'error')
                    warning_count = sum(1 for msg in pylint_data if msg.get('type') == 'warning')
                    # Estimate score (rough heuristic)
                    pylint_score = max(0, 10 - (error_count * 1.0) - (warning_count * 0.3))
                
            evidence.append(f"pylint: {pylint_score:.1f}/10")
        except Exception as e:
            evidence.append(f"pylint: parse error ({str(e)})")
            issues.append("Failed to parse pylint results")
    else:
        evidence.append("pylint: not available")
        issues.append("Missing pylint analysis")
    
    # 2. Parse radon complexity
    avg_complexity = 0.0
    complexity_grade = "A"
    if radon_json_path and radon_json_path.exists():
        try:
            with open(radon_json_path, 'r') as f:
                radon_data = json.load(f)
            
            # Radon CC output is dict of file -> list of functions
            complexities = []
            for file_path, functions in radon_data.items():
                for func in functions:
                    if isinstance(func, dict) and 'complexity' in func:
                        complexities.append(func['complexity'])
            
            if complexities:
                avg_complexity = sum(complexities) / len(complexities)
                complexity_grade = complexity_to_grade(avg_complexity)
                evidence.append(f"complexity: {avg_complexity:.1f} (grade {complexity_grade})")
            else:
                evidence.append("complexity: no functions analyzed")
        except Exception as e:
            evidence.append(f"radon: parse error ({str(e)})")
    else:
        evidence.append("radon: not available")
    
    # 3. Check for type hints and docstrings (basic heuristic)
    type_hint_coverage = check_type_hints(implementation_path)
    evidence.append(f"type hints: ~{type_hint_coverage:.0%} coverage")
    
    # 4. Calculate score
    score = calculate_score(pylint_score, avg_complexity, type_hint_coverage)
    
    # 5. Determine confidence
    confidence = "medium" if pylint_json_path and pylint_json_path.exists() else "low"
    
    return {
        'score': score,
        'confidence': confidence,
        'method': 'automated',
        'evidence': ' | '.join(evidence),
        'issues': issues if issues else None,
        'details': {
            'pylint_score': pylint_score,
            'avg_complexity': avg_complexity,
            'complexity_grade': complexity_grade,
            'type_hint_coverage': type_hint_coverage
        }
    }


def calculate_score(pylint_score: float, avg_complexity: float, type_hint_coverage: float) -> int:
    """
    Map maintainability metrics to 1-5 score.
    
    Scoring logic (weighted combination):
    - pylint: 60% weight
    - complexity: 30% weight
    - type hints: 10% weight
    
    Thresholds:
    - 5: pylint ≥9.0, complexity A (≤5), type hints ≥80%
    - 4: pylint ≥7.0, complexity A-B (≤10), type hints ≥50%
    - 3: pylint ≥5.0, complexity B-C (≤15), type hints ≥20%
    - 2: pylint ≥3.0, complexity C-D (≤20)
    - 1: pylint <3.0 or complexity F (>20)
    """
    # Normalize metrics to 0-1 scale
    pylint_normalized = min(1.0, max(0.0, pylint_score / 10.0))
    complexity_normalized = max(0.0, 1.0 - (avg_complexity / 20.0))  # Invert: lower is better
    
    # Weighted combination
    weighted_score = (
        pylint_normalized * 0.6 +
        complexity_normalized * 0.3 +
        type_hint_coverage * 0.1
    )
    
    # Map to 1-5 scale
    if weighted_score >= 0.85:
        return 5
    elif weighted_score >= 0.65:
        return 4
    elif weighted_score >= 0.45:
        return 3
    elif weighted_score >= 0.25:
        return 2
    else:
        return 1


def complexity_to_grade(complexity: float) -> str:
    """Convert cyclomatic complexity to letter grade."""
    if complexity <= 5:
        return "A"
    elif complexity <= 10:
        return "B"
    elif complexity <= 20:
        return "C"
    elif complexity <= 30:
        return "D"
    else:
        return "F"


def check_type_hints(implementation_path: Path) -> float:
    """
    Estimate type hint coverage using basic heuristics.
    
    Returns:
        Estimated coverage as 0.0-1.0
    """
    # Files to exclude from checking (framework/example files)
    exclude_files = {
        'sample_etl_pipeline.py',
        'base_pipeline.py',
        'spark_session.py',
        'delta_operations.py',
        'prefect_flows.py',
        '__init__.py'
    }
    
    pipeline_files = list(implementation_path.glob('pipelines/**/*.py'))
    
    total_functions = 0
    typed_functions = 0
    
    for py_file in pipeline_files:
        if py_file.name.startswith('__') or py_file.name in exclude_files:
            continue
        
        try:
            content = py_file.read_text()
            lines = content.split('\n')
            
            for line in lines:
                stripped = line.strip()
                # Simple heuristic: look for function definitions
                if stripped.startswith('def '):
                    total_functions += 1
                    # Check if line contains type hints (-> or : for args)
                    if '->' in line or ': ' in line:
                        typed_functions += 1
        except:
            pass
    
    if total_functions == 0:
        return 0.0
    
    return typed_functions / total_functions
