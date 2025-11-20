"""
Performance dimension scorer (Weight: 10%).

Evaluates:
- Execution time (relative to baseline)
- Resource efficiency
- Optimization patterns (caching, partitioning)
- Throughput (rows per second)
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional


def score_performance(
    metrics_json_path: Optional[Path],
    baseline_metrics_path: Optional[Path],
    implementation_path: Path
) -> Dict[str, Any]:
    """
    Calculate performance score based on execution metrics.
    
    Args:
        metrics_json_path: Path to implementation metrics JSON
        baseline_metrics_path: Path to baseline metrics for comparison
        implementation_path: Path to implementation code directory
    
    Returns:
        Dict with score, confidence, method, and evidence
    """
    evidence = []
    issues = []
    
    # 1. Parse metrics
    duration = None
    rows_loaded = 0
    throughput = 0.0
    
    if metrics_json_path and metrics_json_path.exists():
        try:
            with open(metrics_json_path, 'r') as f:
                metrics = json.load(f)
            
            duration = metrics.get('ingestion_duration_seconds', 0)
            rows_loaded = metrics.get('rows_loaded', 0)
            
            if duration and duration > 0:
                throughput = rows_loaded / duration
                evidence.append(f"duration: {duration:.2f}s")
                evidence.append(f"throughput: {throughput:.0f} rows/s")
            else:
                evidence.append("duration: not available")
        except Exception as e:
            evidence.append(f"metrics: parse error ({str(e)})")
            issues.append("Failed to parse metrics")
    else:
        evidence.append("metrics: not available")
        issues.append("Missing performance metrics")
    
    # 2. Compare to baseline (if available)
    relative_performance = 1.0
    if baseline_metrics_path and baseline_metrics_path.exists() and duration:
        try:
            with open(baseline_metrics_path, 'r') as f:
                baseline_metrics = json.load(f)
            
            baseline_duration = baseline_metrics.get('ingestion_duration_seconds', 0)
            if baseline_duration > 0:
                relative_performance = baseline_duration / duration  # >1 is better
                evidence.append(f"vs baseline: {relative_performance:.2f}x")
        except:
            pass
    
    # 3. Check for optimization patterns
    optimizations = check_optimizations(implementation_path)
    evidence.append(f"optimizations: {len(optimizations)} found")
    
    # 4. Calculate score
    score = calculate_score(duration, throughput, relative_performance, optimizations)
    
    # 5. Determine confidence
    confidence = "medium" if metrics_json_path and metrics_json_path.exists() else "low"
    
    return {
        'score': score,
        'confidence': confidence,
        'method': 'automated',
        'evidence': ' | '.join(evidence),
        'issues': issues if issues else None,
        'details': {
            'duration_seconds': duration,
            'throughput_rows_per_sec': throughput,
            'relative_performance': relative_performance,
            'optimizations_found': optimizations
        }
    }


def calculate_score(
    duration: Optional[float],
    throughput: float,
    relative_performance: float,
    optimizations: list[str]
) -> int:
    """
    Map performance metrics to 1-5 score.
    
    Scoring logic:
    - Relative to baseline: 40% weight
    - Optimization patterns: 40% weight
    - Absolute throughput: 20% weight
    
    Thresholds:
    - 5: ≥1.2x baseline, 4+ optimizations, good throughput
    - 4: ≥1.0x baseline, 2-3 optimizations
    - 3: ≥0.8x baseline, 1-2 optimizations
    - 2: ≥0.6x baseline or missing optimizations
    - 1: <0.6x baseline and no optimizations
    """
    # Normalize relative performance (1.0 = same as baseline)
    perf_score = 0.0
    if relative_performance >= 1.2:
        perf_score = 1.0
    elif relative_performance >= 1.0:
        perf_score = 0.8
    elif relative_performance >= 0.8:
        perf_score = 0.6
    elif relative_performance >= 0.6:
        perf_score = 0.4
    else:
        perf_score = 0.2
    
    # Score optimizations
    opt_count = len(optimizations)
    opt_score = min(1.0, opt_count / 4.0)  # 4+ optimizations = full score
    
    # Throughput score (basic threshold - dataset dependent)
    throughput_score = 0.5  # Neutral if unknown
    if throughput > 0:
        if throughput >= 100:  # 100+ rows/sec is good for small datasets
            throughput_score = 1.0
        elif throughput >= 50:
            throughput_score = 0.7
        else:
            throughput_score = 0.4
    
    # Weighted combination
    weighted_score = (
        perf_score * 0.4 +
        opt_score * 0.4 +
        throughput_score * 0.2
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


def check_optimizations(implementation_path: Path) -> list[str]:
    """
    Check for performance optimization patterns in code.
    
    Returns:
        List of optimization patterns found
    """
    optimizations = []
    
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
    
    optimization_patterns = {
        'cache': ['.cache()', 'df.cache'],
        'persist': ['.persist(', 'df.persist'],
        'unpersist': ['.unpersist(', 'df.unpersist'],
        'partitioning': ['partitionBy', 'partition_by', 'partitioned'],
        'broadcast': ['broadcast(', 'F.broadcast'],
        'repartition': ['.repartition(', 'df.repartition'],
        'coalesce': ['.coalesce(', 'df.coalesce'],
    }
    
    for py_file in pipeline_files:
        if py_file.name.startswith('__') or py_file.name in exclude_files:
            continue
        
        try:
            content = py_file.read_text()
            
            for opt_name, patterns in optimization_patterns.items():
                for pattern in patterns:
                    if pattern in content and opt_name not in optimizations:
                        optimizations.append(opt_name)
                        break
        except:
            pass
    
    return optimizations
