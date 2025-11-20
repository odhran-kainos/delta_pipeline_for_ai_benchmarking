"""
Correctness dimension scorer (Weight: 25%).

Evaluates:
- Test pass rate (pytest results)
- Schema validation (output matches expected schema)
- Constitution compliance (bronze layer rules)
- Edge case handling
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional


def score_correctness(
    pytest_json_path: Optional[Path],
    metrics_json_path: Optional[Path],
    task_spec: Dict[str, Any],
    implementation_path: Path
) -> Dict[str, Any]:
    """
    Calculate correctness score based on test results and validation.
    
    Args:
        pytest_json_path: Path to pytest JSON report
        metrics_json_path: Path to pipeline metrics JSON
        task_spec: Task specification YAML loaded as dict
        implementation_path: Path to implementation code directory
    
    Returns:
        Dict with score, confidence, method, and evidence
    """
    evidence = []
    issues = []
    
    # 1. Parse pytest results
    test_pass_rate = 0.0
    if pytest_json_path and pytest_json_path.exists():
        with open(pytest_json_path, 'r') as f:
            pytest_data = json.load(f)
        
        total = pytest_data.get('summary', {}).get('total', 0)
        passed = pytest_data.get('summary', {}).get('passed', 0)
        
        if total > 0:
            test_pass_rate = passed / total
            evidence.append(f"pytest: {passed}/{total} passed ({test_pass_rate:.1%})")
        else:
            evidence.append("pytest: no tests found")
            issues.append("No test execution results")
    else:
        evidence.append("pytest: results not available")
        issues.append("Missing pytest results")
    
    # 2. Validate schema from metrics
    schema_valid = True
    if metrics_json_path and metrics_json_path.exists():
        with open(metrics_json_path, 'r') as f:
            metrics = json.load(f)
        
        # Check for expected metrics
        expected_metrics = ['rows_raw', 'rows_loaded', 'rows_invalid']
        missing_metrics = [m for m in expected_metrics if m not in metrics]
        
        if missing_metrics:
            schema_valid = False
            issues.append(f"Missing metrics: {', '.join(missing_metrics)}")
        else:
            evidence.append("Schema validation: passed")
    else:
        schema_valid = False
        issues.append("Missing metrics JSON")
    
    # 3. Check constitution compliance (basic checks)
    constitution_issues = check_constitution_compliance(implementation_path)
    if constitution_issues:
        issues.extend(constitution_issues)
        evidence.append(f"Constitution violations: {len(constitution_issues)}")
    else:
        evidence.append("Constitution compliance: passed")
    
    # 4. Map to score (1-5 scale)
    score = calculate_score(test_pass_rate, schema_valid, len(constitution_issues))
    
    # 5. Determine confidence
    confidence = "high" if pytest_json_path and pytest_json_path.exists() else "low"
    
    return {
        'score': score,
        'confidence': confidence,
        'method': 'automated',
        'evidence': ' | '.join(evidence),
        'issues': issues if issues else None,
        'details': {
            'test_pass_rate': test_pass_rate,
            'schema_valid': schema_valid,
            'constitution_violations': len(constitution_issues)
        }
    }


def calculate_score(test_pass_rate: float, schema_valid: bool, constitution_violations: int) -> int:
    """
    Map metrics to 1-5 score based on thresholds.
    
    Scoring logic:
    - 5 (Excellent): 100% tests pass, schema valid, no constitution violations
    - 4 (Good): 80-99% tests pass, schema valid, 0-1 violations
    - 3 (Acceptable): 60-79% tests pass, schema valid OR minor violations
    - 2 (Poor): 40-59% tests pass OR schema invalid
    - 1 (Unacceptable): <40% tests pass OR critical failures
    """
    # Critical failures
    if test_pass_rate < 0.4:
        return 1
    
    if not schema_valid:
        return max(1, 2)  # Schema invalid is serious but not always score=1
    
    # Constitution violations are critical for Delta Lake pipelines
    if constitution_violations >= 3:
        return max(1, min(2, int(test_pass_rate * 5)))
    
    # Score based on test pass rate with adjustments
    if test_pass_rate == 1.0 and constitution_violations == 0:
        return 5
    elif test_pass_rate >= 0.8 and constitution_violations <= 1:
        return 4
    elif test_pass_rate >= 0.6:
        return 3
    else:
        return 2


def check_constitution_compliance(implementation_path: Path) -> list[str]:
    """
    Check for basic constitution violations via code inspection.
    
    Returns:
        List of violation descriptions
    """
    violations = []
    
    # Files to exclude from checking (framework/example files)
    exclude_files = {
        'sample_etl_pipeline.py',
        'base_pipeline.py',
        'spark_session.py',
        'delta_operations.py',
        'prefect_flows.py',
        '__init__.py'
    }
    
    # Look for pipeline Python files
    pipeline_files = list(implementation_path.glob('pipelines/**/*.py'))
    
    for py_file in pipeline_files:
        if py_file.name.startswith('__') or py_file.name in exclude_files:
            continue
        
        try:
            content = py_file.read_text()
            
            # Check for destructive write mode
            if 'mode("overwrite")' in content or 'mode=\'overwrite\'' in content or 'mode=\"overwrite\"' in content:
                violations.append(f"{py_file.name}: Uses overwrite mode (should be append)")
            
            # Check for required metadata columns (basic heuristic)
            required_cols = ['_ingest_ts', '_ingest_date', '_pipeline_run_id']
            missing_cols = [col for col in required_cols if col not in content]
            if len(missing_cols) == len(required_cols):  # All missing
                violations.append(f"{py_file.name}: Missing metadata columns")
            
            # Check for quarantine logic
            if 'quarantine' not in content.lower() and 'invalid' in content.lower():
                violations.append(f"{py_file.name}: May be missing quarantine handling")
        
        except Exception as e:
            # Skip files that can't be read
            pass
    
    return violations
