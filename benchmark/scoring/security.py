"""
Security dimension scorer (Weight: 5%).

Evaluates:
- Security vulnerabilities (bandit scan)
- Hardcoded credentials
- Input validation
- Dependency vulnerabilities
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional


def score_security(
    bandit_json_path: Optional[Path],
    implementation_path: Path
) -> Dict[str, Any]:
    """
    Calculate security score based on bandit analysis.
    
    Args:
        bandit_json_path: Path to bandit JSON output
        implementation_path: Path to implementation code directory
    
    Returns:
        Dict with score, confidence, method, and evidence
    """
    evidence = []
    issues = []
    
    # Parse bandit results
    high_severity = 0
    medium_severity = 0
    low_severity = 0
    
    if bandit_json_path and bandit_json_path.exists():
        try:
            with open(bandit_json_path, 'r') as f:
                bandit_data = json.load(f)
            
            # Bandit format: {"results": [...], "metrics": {...}}
            results = bandit_data.get('results', [])
            
            for result in results:
                severity = result.get('issue_severity', '').upper()
                if severity == 'HIGH':
                    high_severity += 1
                    issues.append(f"HIGH: {result.get('issue_text', 'Unknown')}")
                elif severity == 'MEDIUM':
                    medium_severity += 1
                elif severity == 'LOW':
                    low_severity += 1
            
            evidence.append(f"bandit: {high_severity}H/{medium_severity}M/{low_severity}L")
            
            # Check for specific critical patterns
            metrics = bandit_data.get('metrics', {})
            total_loc = sum(m.get('loc', 0) for m in metrics.values() if isinstance(m, dict))
            if total_loc > 0:
                evidence.append(f"scanned: {total_loc} LOC")
        
        except Exception as e:
            evidence.append(f"bandit: parse error ({str(e)})")
            issues.append("Failed to parse bandit results")
    else:
        evidence.append("bandit: not available")
        issues.append("Missing security scan")
    
    # Additional checks: hardcoded secrets (basic pattern matching)
    secret_patterns = check_for_secrets(implementation_path)
    if secret_patterns:
        high_severity += len(secret_patterns)
        issues.extend([f"Potential hardcoded secret: {p}" for p in secret_patterns])
        evidence.append(f"secret patterns: {len(secret_patterns)} found")
    
    # Calculate score
    score = calculate_score(high_severity, medium_severity, low_severity)
    
    # Determine confidence
    confidence = "high" if bandit_json_path and bandit_json_path.exists() else "medium"
    
    return {
        'score': score,
        'confidence': confidence,
        'method': 'automated',
        'evidence': ' | '.join(evidence),
        'issues': issues if issues else None,
        'details': {
            'high_severity': high_severity,
            'medium_severity': medium_severity,
            'low_severity': low_severity
        }
    }


def calculate_score(high: int, medium: int, low: int) -> int:
    """
    Map security findings to 1-5 score.
    
    Scoring logic:
    - 5 (Excellent): 0 high, 0 medium, 0-2 low
    - 4 (Good): 0 high, 1-2 medium, any low
    - 3 (Acceptable): 0 high, 3+ medium OR 1 high, 0-1 medium
    - 2 (Poor): 1-2 high, any medium
    - 1 (Unacceptable): 3+ high
    """
    if high >= 3:
        return 1
    elif high >= 1:
        return 2 if medium <= 1 else 2
    elif medium >= 3:
        return 3
    elif medium >= 1:
        return 4
    elif low <= 2:
        return 5
    else:
        return 4  # Some low-severity findings


def check_for_secrets(implementation_path: Path) -> list[str]:
    """
    Check for potential hardcoded secrets using pattern matching.
    
    Returns:
        List of potential secret patterns found
    """
    patterns = []
    
    # Files to exclude from checking (framework/example files)
    exclude_files = {
        'sample_etl_pipeline.py',
        'base_pipeline.py',
        'spark_session.py',
        'delta_operations.py',
        'prefect_flows.py',
        '__init__.py'
    }
    
    # Common secret patterns
    secret_keywords = [
        'password', 'passwd', 'pwd', 'secret', 'api_key', 
        'apikey', 'access_key', 'secret_key', 'token'
    ]
    
    pipeline_files = list(implementation_path.glob('**/*.py'))
    
    for py_file in pipeline_files:
        if py_file.name.startswith('__') or py_file.name in exclude_files:
            continue
        
        try:
            content = py_file.read_text()
            lines = content.split('\n')
            
            for i, line in enumerate(lines, 1):
                # Skip comments
                if line.strip().startswith('#'):
                    continue
                
                lower_line = line.lower()
                for keyword in secret_keywords:
                    if keyword in lower_line and '=' in line:
                        # Check if it's not reading from env/config
                        if 'os.environ' not in line and 'config' not in lower_line:
                            patterns.append(f"{py_file.name}:{i}")
                            break
        except:
            pass
    
    return patterns
