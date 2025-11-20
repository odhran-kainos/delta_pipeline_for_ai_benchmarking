"""
Documentation dimension scorer (Weight: 10%).

Evaluates:
- Docstring coverage
- README presence and quality
- Inline comments
- Usage examples
"""

import re
from pathlib import Path
from typing import Dict, Any


def score_documentation(
    implementation_path: Path
) -> Dict[str, Any]:
    """
    Calculate documentation score based on code inspection.
    
    Args:
        implementation_path: Path to implementation code directory
    
    Returns:
        Dict with score, confidence, method, and evidence
    """
    evidence = []
    issues = []
    
    # 1. Check for README
    readme_found = False
    readme_quality = 0  # 0-3 scale
    
    for readme_name in ['README.md', 'readme.md', 'README.txt', 'README']:
        readme_path = implementation_path / readme_name
        if readme_path.exists():
            readme_found = True
            readme_quality = assess_readme_quality(readme_path)
            evidence.append(f"README: found (quality: {readme_quality}/3)")
            break
    
    if not readme_found:
        evidence.append("README: not found")
        issues.append("Missing README documentation")
    
    # 2. Calculate docstring coverage
    docstring_coverage = calculate_docstring_coverage(implementation_path)
    evidence.append(f"docstrings: {docstring_coverage:.0%} coverage")
    
    # 3. Check for inline comments
    comment_ratio = calculate_comment_ratio(implementation_path)
    evidence.append(f"comments: {comment_ratio:.1%} of LOC")
    
    # 4. Check for usage examples
    has_examples = check_for_examples(implementation_path)
    if has_examples:
        evidence.append("examples: found")
    else:
        evidence.append("examples: not found")
        issues.append("No usage examples provided")
    
    # 5. Calculate score
    score = calculate_score(
        readme_found, readme_quality, docstring_coverage, 
        comment_ratio, has_examples
    )
    
    return {
        'score': score,
        'confidence': 'medium',
        'method': 'automated',
        'evidence': ' | '.join(evidence),
        'issues': issues if issues else None,
        'details': {
            'readme_found': readme_found,
            'readme_quality': readme_quality,
            'docstring_coverage': docstring_coverage,
            'comment_ratio': comment_ratio,
            'has_examples': has_examples
        }
    }


def calculate_score(
    readme_found: bool,
    readme_quality: int,
    docstring_coverage: float,
    comment_ratio: float,
    has_examples: bool
) -> int:
    """
    Map documentation metrics to 1-5 score.
    
    Scoring logic:
    - README: 40% weight
    - Docstrings: 35% weight
    - Comments: 15% weight
    - Examples: 10% weight
    
    Thresholds:
    - 5: Quality README, ≥80% docstrings, ≥10% comments, examples
    - 4: README present, ≥60% docstrings, ≥5% comments
    - 3: README or ≥40% docstrings
    - 2: Some documentation present
    - 1: Minimal or no documentation
    """
    # Normalize README (0-1)
    readme_score = 0.0
    if readme_found:
        readme_score = (readme_quality / 3.0)  # 0-3 scale to 0-1
    
    # Normalize docstrings (already 0-1)
    
    # Normalize comments (target 10% as ideal)
    comment_score = min(1.0, comment_ratio / 0.10)
    
    # Examples (binary)
    examples_score = 1.0 if has_examples else 0.0
    
    # Weighted combination
    weighted_score = (
        readme_score * 0.4 +
        docstring_coverage * 0.35 +
        comment_score * 0.15 +
        examples_score * 0.1
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


def assess_readme_quality(readme_path: Path) -> int:
    """
    Assess README quality on 0-3 scale.
    
    Quality indicators:
    +1: Longer than 500 chars
    +1: Contains code examples (```  blocks)
    +1: Contains sections (## headers)
    """
    try:
        content = readme_path.read_text()
        quality = 0
        
        if len(content) > 500:
            quality += 1
        
        if '```' in content:
            quality += 1
        
        if re.search(r'^##', content, re.MULTILINE):
            quality += 1
        
        return quality
    except:
        return 0


def calculate_docstring_coverage(implementation_path: Path) -> float:
    """
    Calculate percentage of functions/classes with docstrings.
    
    Returns:
        Coverage as 0.0-1.0
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
    
    total_definitions = 0
    documented_definitions = 0
    
    for py_file in pipeline_files:
        if py_file.name.startswith('__') or py_file.name in exclude_files:
            continue
        
        try:
            content = py_file.read_text()
            lines = content.split('\n')
            
            for i, line in enumerate(lines):
                stripped = line.strip()
                
                # Check for function or class definition
                if stripped.startswith('def ') or stripped.startswith('class '):
                    total_definitions += 1
                    
                    # Check if next non-empty line is a docstring
                    for j in range(i + 1, min(i + 5, len(lines))):
                        next_line = lines[j].strip()
                        if next_line:
                            if next_line.startswith('"""') or next_line.startswith("'''"):
                                documented_definitions += 1
                            break
        except:
            pass
    
    if total_definitions == 0:
        return 0.0
    
    return documented_definitions / total_definitions


def calculate_comment_ratio(implementation_path: Path) -> float:
    """
    Calculate ratio of comment lines to total lines of code.
    
    Returns:
        Ratio as 0.0-1.0
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
    
    total_lines = 0
    comment_lines = 0
    
    for py_file in pipeline_files:
        if py_file.name.startswith('__') or py_file.name in exclude_files:
            continue
        
        try:
            content = py_file.read_text()
            lines = content.split('\n')
            
            for line in lines:
                stripped = line.strip()
                if stripped:  # Non-empty line
                    total_lines += 1
                    if stripped.startswith('#'):
                        comment_lines += 1
        except:
            pass
    
    if total_lines == 0:
        return 0.0
    
    return comment_lines / total_lines


def check_for_examples(implementation_path: Path) -> bool:
    """
    Check if usage examples exist (README or example files).
    
    Returns:
        True if examples found
    """
    # Check README for examples
    for readme_name in ['README.md', 'readme.md']:
        readme_path = implementation_path / readme_name
        if readme_path.exists():
            try:
                content = readme_path.read_text()
                if '```' in content or 'example' in content.lower():
                    return True
            except:
                pass
    
    # Check for example files
    example_files = list(implementation_path.glob('**/example*.py'))
    example_files.extend(list(implementation_path.glob('**/demo*.py')))
    example_files.extend(list(implementation_path.glob('**/run_*.py')))
    
    return len(example_files) > 0
