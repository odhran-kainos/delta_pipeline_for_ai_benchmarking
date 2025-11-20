#!/usr/bin/env python3
"""
Automated scoring engine for AI-generated Delta Lake pipeline implementations.

This script evaluates code implementations against the scoring rubric using
static analysis tools and test results. It generates partial scorecards with
automated scores for Correctness, Maintainability, Security, Performance,
and Documentation dimensions.

Usage:
    python benchmark/scripts/auto_score.py \\
        --baseline benchmark-foundation \\
        --implementation tool/copilot/task-T1 \\
        --task T1 \\
        --output evaluations/copilot/T1_scorecard_auto.yaml

Requirements:
    - pylint (pip install pylint)
    - radon (pip install radon)
    - bandit (pip install bandit)
    - pytest with json-report plugin (pip install pytest-json-report)
"""

import argparse
import json
import subprocess
import sys
import tempfile
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from benchmark.scoring import (
    score_correctness,
    score_maintainability,
    score_security,
    score_performance,
    score_documentation,
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Automated scoring for Delta Lake pipeline implementations'
    )
    parser.add_argument(
        '--baseline',
        required=True,
        help='Baseline branch or tag (e.g., benchmark-foundation)'
    )
    parser.add_argument(
        '--implementation',
        required=True,
        help='Implementation branch or tag (e.g., tool/copilot/task-T1)'
    )
    parser.add_argument(
        '--task',
        required=True,
        help='Task ID (e.g., T1)'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='Output path for scorecard YAML'
    )
    parser.add_argument(
        '--use-cached',
        action='store_true',
        help='Use cached metrics instead of running tests'
    )
    parser.add_argument(
        '--workspace',
        default='.',
        help='Workspace root directory (default: current directory)'
    )
    
    return parser.parse_args()


def run_command(cmd: list[str], cwd: Path) -> tuple[int, str, str]:
    """
    Run shell command and return exit code, stdout, stderr.
    
    Args:
        cmd: Command and arguments as list
        cwd: Working directory
    
    Returns:
        Tuple of (exit_code, stdout, stderr)
    """
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)


def checkout_branch(branch: str, workspace: Path) -> bool:
    """
    Checkout git branch or tag.
    
    Args:
        branch: Branch or tag name
        workspace: Workspace root
    
    Returns:
        True if successful
    """
    print(f"Checking out {branch}...")
    exit_code, stdout, stderr = run_command(
        ['git', 'checkout', branch],
        workspace
    )
    
    if exit_code != 0:
        print(f"Error checking out {branch}: {stderr}")
        return False
    
    return True


def run_pytest(workspace: Path, output_path: Path) -> bool:
    """
    Run pytest with JSON report plugin.
    
    Args:
        workspace: Workspace root
        output_path: Path to save JSON report
    
    Returns:
        True if tests executed (pass or fail)
    """
    print("Running pytest...")
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    exit_code, stdout, stderr = run_command(
        [
            'pytest',
            '--json-report',
            f'--json-report-file={output_path}',
            '--json-report-indent=2',
            'tests/',
            '-v'
        ],
        workspace
    )
    
    # pytest returns non-zero for test failures, but that's expected
    if output_path.exists():
        print(f"pytest results saved to {output_path}")
        return True
    else:
        print(f"Warning: pytest did not generate report: {stderr}")
        return False


def run_pylint(workspace: Path, output_path: Path) -> bool:
    """
    Run pylint on pipelines directory.
    
    Args:
        workspace: Workspace root
        output_path: Path to save JSON report
    
    Returns:
        True if analysis completed
    """
    print("Running pylint...")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    exit_code, stdout, stderr = run_command(
        [
            'pylint',
            'pipelines/',
            '--output-format=json',
            '--exit-zero'  # Don't fail on lint errors
        ],
        workspace
    )
    
    # Parse stdout as JSON and save
    try:
        if stdout:
            pylint_data = json.loads(stdout)
            with open(output_path, 'w') as f:
                json.dump(pylint_data, f, indent=2)
            print(f"pylint results saved to {output_path}")
            return True
    except json.JSONDecodeError:
        pass
    
    # Try to save what we got
    with open(output_path, 'w') as f:
        f.write(stdout or stderr)
    
    return bool(stdout or stderr)


def run_radon(workspace: Path, output_path: Path) -> bool:
    """
    Run radon cyclomatic complexity analysis.
    
    Args:
        workspace: Workspace root
        output_path: Path to save JSON report
    
    Returns:
        True if analysis completed
    """
    print("Running radon...")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    exit_code, stdout, stderr = run_command(
        [
            'radon',
            'cc',
            'pipelines/',
            '-j',  # JSON output
            '-a'   # Average complexity
        ],
        workspace
    )
    
    if stdout:
        with open(output_path, 'w') as f:
            f.write(stdout)
        print(f"radon results saved to {output_path}")
        return True
    
    return False


def run_bandit(workspace: Path, output_path: Path) -> bool:
    """
    Run bandit security scanner.
    
    Args:
        workspace: Workspace root
        output_path: Path to save JSON report
    
    Returns:
        True if scan completed
    """
    print("Running bandit...")
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    exit_code, stdout, stderr = run_command(
        [
            'bandit',
            '-r', 'pipelines/',
            '-f', 'json',
            '--exit-zero'  # Don't fail on findings
        ],
        workspace
    )
    
    if stdout:
        with open(output_path, 'w') as f:
            f.write(stdout)
        print(f"bandit results saved to {output_path}")
        return True
    
    return False


def load_task_spec(task: str, workspace: Path) -> Dict[str, Any]:
    """
    Load task specification YAML.
    
    Args:
        task: Task ID (e.g., T1)
        workspace: Workspace root
    
    Returns:
        Task spec as dict
    """
    # Find task YAML
    task_files = list((workspace / 'benchmark' / 'tasks').glob(f'{task}_*.yaml'))
    
    if not task_files:
        print(f"Warning: No task spec found for {task}")
        return {}
    
    task_file = task_files[0]
    
    try:
        with open(task_file, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Failed to load task spec: {e}")
        return {}


def extract_tool_name(implementation_branch: str) -> str:
    """
    Extract tool name from branch name.
    
    Args:
        implementation_branch: Branch name like tool/copilot/task-T1
    
    Returns:
        Tool name like 'copilot' or full branch if pattern doesn't match
    """
    parts = implementation_branch.split('/')
    if len(parts) >= 2 and parts[0] == 'tool':
        return parts[1]
    return implementation_branch


def generate_scorecard(
    task: str,
    tool: str,
    scores: Dict[str, Dict[str, Any]],
    output_path: Path
):
    """
    Generate scorecard YAML file.
    
    Args:
        task: Task ID
        tool: Tool name
        scores: Dict of dimension -> score details
        output_path: Output file path
    """
    # Load weights from scoring.yaml
    workspace = Path.cwd()
    weights_file = workspace / 'benchmark' / 'scoring.yaml'
    
    weights = {
        'correctness': 0.25,
        'maintainability': 0.15,
        'data_quality': 0.15,
        'planning': 0.15,
        'performance': 0.10,
        'documentation': 0.10,
        'security': 0.05,
        'productivity': 0.05,
    }
    
    if weights_file.exists():
        with open(weights_file, 'r') as f:
            weights_data = yaml.safe_load(f)
            weights.update(weights_data.get('weights', {}))
    
    # Calculate total weighted score (out of 5)
    total_weighted = 0.0
    automation_weight = 0.0
    
    for dimension, score_data in scores.items():
        if score_data and 'score' in score_data:
            weight = weights.get(dimension, 0.0)
            total_weighted += score_data['score'] * weight
            automation_weight += weight
    
    # Convert to 100-point scale
    total_score = total_weighted * 20
    
    # Build scorecard structure
    scorecard = {
        'task': task,
        'tool': tool,
        'date': datetime.now().strftime('%Y-%m-%d'),
        'automation_version': '1.0.0',
        'automation_coverage': f'{automation_weight:.1%}',
        'scores': {},
        'total_score_automated': round(total_score, 1),
        'notes': [
            'This scorecard was generated automatically using static analysis.',
            'Manual review is required for Planning and Productivity dimensions.',
            'Some dimension scores may have medium/low confidence.',
        ]
    }
    
    # Add dimension scores
    for dimension in ['correctness', 'maintainability', 'data_quality',
                      'planning', 'performance', 'documentation',
                      'security', 'productivity']:
        
        if dimension in scores and scores[dimension]:
            scorecard['scores'][dimension] = {
                'score': scores[dimension]['score'],
                'weight': weights[dimension],
                'method': scores[dimension].get('method', 'automated'),
                'confidence': scores[dimension].get('confidence', 'medium'),
                'evidence': scores[dimension].get('evidence', ''),
            }
            
            if scores[dimension].get('issues'):
                scorecard['scores'][dimension]['issues'] = \
                    scores[dimension]['issues']
        else:
            # Placeholder for manual dimensions
            scorecard['scores'][dimension] = {
                'score': None,
                'weight': weights[dimension],
                'method': 'manual',
                'confidence': 'n/a',
                'evidence': 'Manual review required',
            }
    
    # Write scorecard
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        yaml.dump(scorecard, f, default_flow_style=False, sort_keys=False)
    
    print(f"\nScorecard saved to {output_path}")
    print(f"Total automated score: {total_score:.1f}/100")
    print(f"Automation coverage: {automation_weight:.1%}")


def main():
    """Main execution."""
    args = parse_args()
    
    workspace = Path(args.workspace).resolve()
    
    print(f"Workspace: {workspace}")
    print(f"Task: {args.task}")
    print(f"Implementation: {args.implementation}")
    
    # Create temp directory for analysis outputs
    # TODO: Re-enable automatic cleanup after debugging
    # Use tempfile.mkdtemp() for now to preserve outputs for inspection
    # with tempfile.TemporaryDirectory() as tmpdir:
    tmpdir = tempfile.mkdtemp(prefix='auto_score_')
    print(f"\nAnalysis outputs saved to: {tmpdir}")
    print("NOTE: Temporary files will NOT be automatically deleted (for debugging)")
    
    try:
        temp_path = Path(tmpdir)
        
        # Checkout implementation branch
        if not checkout_branch(args.implementation, workspace):
            print("Failed to checkout implementation branch")
            return 1
        
        # Run static analysis tools
        pytest_json = temp_path / 'pytest.json'
        pylint_json = temp_path / 'pylint.json'
        radon_json = temp_path / 'radon.json'
        bandit_json = temp_path / 'bandit.json'
        
        if not args.use_cached:
            run_pytest(workspace, pytest_json)
        
        run_pylint(workspace, pylint_json)
        run_radon(workspace, radon_json)
        run_bandit(workspace, bandit_json)
        
        # Load task specification
        task_spec = load_task_spec(args.task, workspace)
        
        # Find metrics JSON (if exists)
        metrics_json = workspace / 'benchmark' / 'metrics' / args.task / \
            'run_metrics.json'
        
        # Score each dimension
        print("\nCalculating scores...")
        
        scores = {}
        
        # Correctness
        scores['correctness'] = score_correctness(
            pytest_json if pytest_json.exists() else None,
            metrics_json if metrics_json.exists() else None,
            task_spec,
            workspace
        )
        print(f"Correctness: {scores['correctness']['score']}/5 "
              f"({scores['correctness']['confidence']})")
        
        # Maintainability
        scores['maintainability'] = score_maintainability(
            pylint_json if pylint_json.exists() else None,
            radon_json if radon_json.exists() else None,
            workspace
        )
        print(f"Maintainability: {scores['maintainability']['score']}/5 "
              f"({scores['maintainability']['confidence']})")
        
        # Security
        scores['security'] = score_security(
            bandit_json if bandit_json.exists() else None,
            workspace
        )
        print(f"Security: {scores['security']['score']}/5 "
              f"({scores['security']['confidence']})")
        
        # Performance
        baseline_metrics = None
        scores['performance'] = score_performance(
            metrics_json if metrics_json.exists() else None,
            baseline_metrics,
            workspace
        )
        print(f"Performance: {scores['performance']['score']}/5 "
              f"({scores['performance']['confidence']})")
        
        # Documentation
        scores['documentation'] = score_documentation(workspace)
        print(f"Documentation: {scores['documentation']['score']}/5 "
              f"({scores['documentation']['confidence']})")
        
        # Generate scorecard
        tool_name = extract_tool_name(args.implementation)
        output_path = Path(args.output)
        
        generate_scorecard(args.task, tool_name, scores, output_path)
        
        # Return to baseline
        checkout_branch(args.baseline, workspace)
    
    except Exception as e:
        print(f"\nError during scoring: {e}")
        checkout_branch(args.baseline, workspace)
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
