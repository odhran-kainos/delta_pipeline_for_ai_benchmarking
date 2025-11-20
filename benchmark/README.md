# Benchmark Framework

## Overview

This benchmark framework provides a standardized way to evaluate AI-assisted coding tools on realistic Delta Lake pipeline tasks. The framework uses a suite of 8 tasks covering common data engineering scenarios - from raw data ingestion to streaming and change data capture.

**Target Audience**: Engineers evaluating AI coding tools (GitHub Copilot, Cursor, Codeium, etc.) for data engineering work.

## Quick Start

### 1. Setup Environment
```bash
# From repository root
make setup
make data-gen
```

### 2. Run a Single Task Benchmark
```bash
# Execute task T1 (Bronze Ingestion)
make benchmark-task T=T1

# Or use the script directly
bash benchmark/scripts/run_task.sh T1
```

### 3. Run All Tasks
```bash
make benchmark-all
```

## How to Benchmark an AI Tool

### Step 1: Prepare Clean Environment
```bash
# Start from the baseline branch
git checkout benchmark-foundation

# Create a new branch for the AI tool being tested
git checkout -b tool/<tool-name>/task-<ID>

# Example: git checkout -b tool/copilot/task-T1
```

**Important**: Each task implementation gets its own isolated branch. These branches are never merged to main to avoid conflicts.

### Step 2: Review Task Specification
Each task has a YAML specification in `benchmark/tasks/` that defines:
- **Objective**: What needs to be implemented
- **Requirements**: Functional and non-functional requirements
- **Acceptance Criteria**: How success is measured
- **Expected Outputs**: Data schemas and file locations

Example: `benchmark/tasks/T1_ingest_transactions.yaml`

### Step 3: Use AI Tool to Implement Solution
1. Open the task YAML file
2. Provide the task requirements to your AI tool
3. Let the AI tool generate the implementation
4. Save the generated code in the appropriate location (usually `pipelines/`)

### Step 3a: Enable Tests for the Implementation

Before running tests, you need to enable the appropriate test cases for the task. Tests are organized into three categories:

**1. Tests Ready to Run** (just remove conditional skip):
These tests have complete assertion logic and only skip if the output doesn't exist yet. No code changes needed - they'll automatically run once the implementation creates the expected outputs.

Examples from `tests/test_t1_bronze_ingestion.py`:
- `test_required_columns_present` - validates bronze table schema
- `test_row_count_validation` - checks row counts match expectations
- `test_metadata_columns_populated` - ensures metadata fields are non-null
- `test_transaction_id_uniqueness` - validates data quality
- `test_data_types` - checks column types
- `test_bronze_table_exists` - verifies Delta table creation
- `test_invalid_records_rejected` - ensures validation logic works

**2. Tests to Keep Skipped** (validated elsewhere):
These tests are informational or validated by other parts of the benchmark framework:
- `test_metrics_file_created` - validated by benchmark harness
- `test_metrics_accuracy` - validated by benchmark harness  
- `test_no_hardcoded_paths` - checked in maintainability scoring
- `test_reuses_pipeline_patterns` - checked in maintainability scoring

**Why most tests are ready to run**: All T1 tests now have complete assertions. They use conditional skips (`if not table_path.exists(): pytest.skip()`) to handle missing outputs gracefully. Once your implementation creates the bronze table, these tests will automatically execute their validation logic.

**Expected test pass rate**: For a correct T1 implementation, you should see 7/7 passing tests in the `TestT1BronzeIngestion` class. The skipped tests in `TestT1Metrics` and `TestT1Configuration` don't count toward the correctness score.

### Step 4: Test the Implementation
```bash
# Run the task to see if it works
bash benchmark/scripts/run_task.sh T1

# Run tests
make test

# Commit the implementation
git add .
git commit -m "<Tool-name> implementation of <Task-ID>"
git push origin tool/<tool-name>/task-<ID>
```

### Step 5: Evaluate Using Scoring Rubric

#### Option A: Automated Scoring (Recommended for Initial Assessment)
Run the automated scoring engine to get objective scores for 5 dimensions:

```bash
# Run automated scoring
python benchmark/scripts/auto_score.py \
  --baseline benchmark-foundation \
  --implementation tool/<tool-name>/task-<ID> \
  --task <task-id> \
  --output evaluations/<tool-name>/<task-id>_scorecard_auto.yaml

# Or using make
make auto-score \
  BASELINE=benchmark-foundation \
  IMPL=tool/copilot/task-T1 \
  TASK=T1 \
  OUTPUT=evaluations/copilot/T1_scorecard_auto.yaml
```

**Automated Dimensions** (62.5% of total score):
- ✅ **Correctness** (25%): pytest results, schema validation, constitution compliance
- ✅ **Maintainability** (15%): pylint score, cyclomatic complexity, type hints
- ✅ **Security** (5%): bandit scan, hardcoded secrets detection
- ✅ **Performance** (10%): execution time, optimization patterns
- ✅ **Documentation** (10%): docstring coverage, README presence

**Manual Review Required** (37.5% of total score):
- ⚠️ **Data Quality** (15%): Quality gates appropriateness (partial automation)
- ⚠️ **Planning** (15%): Architecture assessment, design patterns
- ⚠️ **Productivity** (5%): Time tracking, iteration count

**Requirements**: Install analysis tools:
```bash
pip install pylint radon bandit pytest-json-report
```

#### Option B: Manual Scoring (Comprehensive)
Use `benchmark/scoring_rubric.yaml` to manually score across all 8 dimensions using the detailed 5-point criteria. This provides more nuanced assessment but requires more time.

**Combined Approach** (Best Practice):
1. Run automated scoring for objective baseline
2. Perform manual review for subjective dimensions
3. Validate/adjust automated scores based on code inspection
4. Merge into final scorecard

### Step 6: Generate Evaluation Artifacts

```bash
# Generate diff analysis and metrics
python benchmark/scripts/analyze_task_diff.py \
  --baseline benchmark-foundation \
  --implementation tool/<tool-name>/task-<ID> \
  --output evaluations/<tool-name>/<task-id> \
  --task <task-id>

# Example for T1:
python benchmark/scripts/analyze_task_diff.py \
  --baseline benchmark-foundation \
  --implementation tool/copilot/task-T1 \
  --output evaluations/copilot/T1 \
  --task T1

# This creates:
# - evaluations/<tool-name>/<task-id>_scorecard.yaml
# - evaluations/<tool-name>/<task-id>_diff_report.md
# - evaluations/<tool-name>/<task-id>_metrics.json
```

### Step 7: Tag and Archive Implementation Branch

```bash
# Tag the implementation for future reference
git tag eval/<tool-name>-<task-id>-YYYY-MM-DD tool/<tool-name>/task-<ID>
git push origin eval/<tool-name>-<task-id>-YYYY-MM-DD

# Example:
git tag eval/copilot-T1-2025-11-17 tool/copilot/task-T1
git push origin eval/copilot-T1-2025-11-17

# Optional: Delete branch to keep repo clean (tag preserves the state)
# git branch -D tool/<tool-name>/task-<ID>
# git push origin --delete tool/<tool-name>/task-<ID>
```

### Step 8: Submit Evaluation Results to Main

```bash
# Switch back to baseline
git checkout benchmark-foundation

# Create evaluation branch
git checkout -b eval/<tool-name>-<task-id>

# Add evaluation files
git add evaluations/<tool-name>/
git commit -m "Add <tool-name> evaluation for <task-id>"
git push origin eval/<tool-name>-<task-id>

# Create PR to merge evaluation data to main
# This is safe - only adding new files under evaluations/<tool-name>/
```

### Step 9: Compare Multiple Tools
Repeat steps 1-8 for each AI tool, then compare:
```bash
# Future: Compare multiple tools (script not yet implemented)
# For now, manually compare scorecard files
python benchmark/scripts/compare_implementations.py \
  --task T1 \
  --baseline benchmark-foundation \
  --implementations \
    eval/copilot-T1-2025-11-17 \
    eval/cursor-T1-2025-11-17 \
    eval/codeium-T1-2025-11-17

# This generates:
# - evaluations/comparison/T1_tool_comparison.md
# - evaluations/comparison/T1_comparison_metrics.json
```

Analyze:
- Overall scores
- Per-dimension strengths/weaknesses
- Consistency across tasks
- Best use cases for each tool

## Task Suite

| ID | Title | Complexity | Focus Area |
|----|-------|-----------|------------|
| T1 | Bronze Ingestion (transactions) | ⭐ Basic | Data ingestion, schema definition |
| T2 | Silver Enrichment | ⭐⭐ Intermediate | Joins, data enrichment, late arrivals |
| T3 | Quality Gate | ⭐⭐ Intermediate | Data validation, quality metrics |
| T4 | Gold Aggregate | ⭐⭐ Intermediate | Aggregations, business logic |
| T5 | Testing Upgrade | ⭐⭐⭐ Advanced | Unit testing, test coverage |
| T6 | CDC Merge | ⭐⭐⭐ Advanced | Delta merge, upserts, SCD |
| T7 | Streaming Variant | ⭐⭐⭐⭐ Expert | Structured Streaming, real-time |
| T8 | Lineage Metadata | ⭐⭐⭐ Advanced | Metadata, observability |

## Evaluation Dimensions Explained

### Automated Dimensions

These dimensions can be scored automatically using static analysis tools and test results.

### Correctness (25% weight)
- Do all tests pass?
- Does output match expected schema?
- Are edge cases handled?
- Does it execute without errors?

**Automated Checks**: pytest coverage, schema validation, data quality checks, constitution compliance

**Automation Status**: ✅ Fully automated (high confidence)

### Maintainability (15% weight)
- Is code readable and well-structured?
- Are best practices followed (PEP 8, DRY principle)?
- Is there proper separation of concerns?
- Are there type hints and docstrings?

**Automated Checks**: pylint score, cyclomatic complexity, code duplication, type hint coverage

**Automation Status**: ✅ Fully automated (medium confidence)

### Data Quality (15% weight)
- Are validations implemented?
- Is schema enforced?
- Are bad records handled?
- Are quality metrics tracked?

**Automated Checks**: null rates, duplicate counts, schema conformance

### Planning (15% weight)
- Is the solution well-architected?
- Are appropriate design patterns used?
- Is scalability considered?
- Is the approach modular and reusable?

**Manual Review**: Architecture assessment, design pattern evaluation

**Automation Status**: ❌ Manual only

### Productivity (5% weight)
- Is execution time reasonable?
- Are resources used efficiently?
- Are optimizations applied (partitioning, caching, etc.)?

**Automated Checks**: execution time, memory usage, optimization pattern detection (caching, partitioning, broadcast joins)

**Automation Status**: ✅ Fully automated (medium confidence)

### Documentation (10% weight)
- Is there a clear README?
- Are functions documented?
- Are there usage examples?
- Is there troubleshooting guidance?

**Automated Checks**: docstring coverage, README presence, comment ratio, example files

**Automation Status**: ✅ Fully automated (medium confidence)

### Security (5% weight)
- Are credentials handled securely?
- Is input sanitized?
- Are dependencies vulnerability-free?

**Automated Checks**: secrets scanning (bandit), hardcoded credential detection, dependency vulnerability scan

**Automation Status**: ✅ Fully automated (high confidence)

### Productivity (5% weight)
- How quickly was the solution implemented?
- How many iterations were needed?
- Was debugging efficient?

**Manual Tracking**: time to completion, iteration count

**Automation Status**: ❌ Manual only

## Directory Structure

```
benchmark/
├── README.md                    # This file
├── scoring.yaml                 # Dimension weights configuration
├── scoring_rubric.yaml          # Detailed 5-point scoring criteria
├── scoring/                     # Automated scoring engine
│   ├── __init__.py
│   ├── correctness.py           # Correctness dimension scorer
│   ├── maintainability.py       # Maintainability dimension scorer
│   ├── security.py              # Security dimension scorer
│   ├── performance.py           # Performance dimension scorer
│   └── documentation.py         # Documentation dimension scorer
├── tasks/                       # Task specifications
│   ├── T1_ingest_transactions.yaml
│   ├── T2_silver_enrichment.yaml
│   └── ...
├── scripts/
│   ├── run_task.sh              # Task execution script
│   ├── auto_score.py            # Automated scoring engine
│   ├── analyze_task_diff.py     # Generate diff analysis and metrics
│   └── compare_implementations.py  # Compare multiple tools
└── metrics/                     # Collected metrics (future)

evaluations/                     # Evaluation results (merged to main)
├── copilot/
│   ├── T1_scorecard.yaml
│   ├── T1_diff_report.md
│   ├── T1_metrics.json
│   └── ...
├── cursor/
│   ├── T1_scorecard.yaml
│   └── ...
└── comparison/
    ├── T1_tool_comparison.md
    └── T1_comparison_metrics.json
```

## Branch Workflow & Version Control

### Implementation Branches
- **Never merged to main**: Each AI tool's implementation lives in isolated branches
- **Naming**: `tool/<tool-name>/task-<ID>` (e.g., `tool/copilot/task-T1`)
- **Baseline**: All tools start from `benchmark-foundation` branch
- **Lifecycle**: Commit → Push → Tag → Optional deletion

### Evaluation Branches
- **Merged to main**: Evaluation results are submitted via PR
- **Naming**: `eval/<tool-name>-<task-id>` (e.g., `eval/copilot-T1`)
- **Content**: Only adds files under `evaluations/<tool-name>/`
- **No conflicts**: Each tool has its own subdirectory

### Tags for Reference
- **Format**: `eval/<tool-name>-<task-id>-YYYY-MM-DD`
- **Purpose**: Preserve implementation state even if branch is deleted
- **Usage**: Can checkout tag to review or re-analyze implementation

### Why This Approach?
1. **No merge conflicts**: Implementation code never merges (different approaches guaranteed to conflict)
2. **Preserved history**: Tags keep implementations accessible
3. **Clean main branch**: Only evaluation data (scorecards, reports, metrics)
4. **Easy comparison**: Diff between baseline and tagged implementations
5. **Isolated experiments**: Each tool/task combination is independent

## Fairness Principles

To ensure fair comparison across AI tools:

1. **Same Task**: All tools receive identical task specifications
2. **Same Baseline**: All tools start from `benchmark-foundation` branch
3. **Same Environment**: Same repository state, dependencies, and data
4. **Same Criteria**: All evaluated using the same rubric
5. **Same Resources**: Same compute resources and time budget
6. **Blind Evaluation**: Scorer doesn't know which tool generated which code (when possible)

## Synthetic Data

All test data is synthetically generated using scripts in `scripts/data/`:
- **Deterministic**: Uses fixed random seeds for reproducibility
- **Realistic**: Mirrors real-world data patterns without exposing sensitive information
- **Scalable**: Can generate datasets of varying sizes

Generate data:
```bash
make data-gen
```

## Best Practices for Benchmarking

### DO:
- ✅ Start each task from `benchmark-foundation` branch
- ✅ Create isolated branch per tool per task: `tool/<name>/task-<ID>`
- ✅ Use the same prompt/task description for all tools
- ✅ Commit and tag implementations: `eval/<name>-<ID>-YYYY-MM-DD`
- ✅ Submit evaluation data to main via PR
- ✅ Record both automated metrics and manual observations
- ✅ Document your evaluation rationale
- ✅ Run tests multiple times to account for AI non-determinism
- ✅ Keep detailed notes on the AI interaction process

### DON'T:
- ❌ Merge implementation branches to main (causes conflicts)
- ❌ Start from different baselines for different tools
- ❌ Cherry-pick the best result from multiple runs
- ❌ Manually fix AI-generated code before evaluation
- ❌ Skip tasks that an AI tool struggles with
- ❌ Use different task descriptions for different tools
- ❌ Evaluate your own AI-generated code (have someone else score it)
- ❌ Delete implementation branches without tagging first

## Calculating Final Scores

Use the weights from `scoring.yaml`:

```python
total_score = (
    correctness * 0.25 +
    maintainability * 0.15 +
    data_quality * 0.15 +
    planning * 0.15 +
    performance * 0.10 +
    documentation * 0.10 +
    security * 0.05 +
    productivity * 0.05
) * 20  # Convert 5-point scale to 100-point scale
```

Example:
- Correctness: 4/5
- Maintainability: 4/5
- Data Quality: 4/5
- Planning: 4/5
- Performance: 3/5
- Documentation: 4/5
- Security: 5/5
- Productivity: 4/5

**Calculation**: (4×0.25 + 4×0.15 + 4×0.15 + 4×0.15 + 3×0.10 + 4×0.10 + 5×0.05 + 4×0.05) × 20 = **79.0/100**

## Tips for Working with AI Tools

### Getting Better Results:
1. **Provide Context**: Share relevant files (config, schemas, existing code)
2. **Be Specific**: Clear requirements lead to better implementations
3. **Iterate Carefully**: Ask for improvements rather than starting over
4. **Review Critically**: AI tools make mistakes - validate everything
5. **Use Examples**: Reference existing code patterns in the repo

### Common AI Tool Patterns:
- **Copilot**: Excellent for completing code within context
- **ChatGPT/Claude**: Better for architectural planning and design
- **Codeium**: Good balance of completion and generation
- **Cursor**: Strong at understanding full codebase context

## Support & Troubleshooting

### Task Won't Run
```bash
# Check task file exists
ls benchmark/tasks/T1*.yaml

# Verify Python environment
python --version  # Should be 3.10.5

# Check dependencies
pip list | grep -E "pyspark|delta-spark"
```

### Tests Failing
```bash
# Run with verbose output
pytest -v

# Check specific test
pytest tests/test_<specific>.py -v
```

### Need Help?
- Review task YAML specifications carefully
- Check `benchmark/scoring_rubric.yaml` for evaluation criteria
- Look at existing pipeline code in `pipelines/` for patterns
- Consult main `README.md` for repository setup

## Working with Git Tags and Branches

### View All Evaluation Tags
```bash
git tag -l 'eval/*'
```

### Checkout a Specific Implementation
```bash
# Review Copilot's T1 implementation from Nov 17
git checkout eval/copilot-T1-2025-11-17

# Return to baseline
git checkout benchmark-foundation
```

### Re-run Diff Analysis on Tagged Implementation
```bash
python benchmark/scripts/analyze_task_diff.py \
  --baseline benchmark-foundation \
  --implementation eval/copilot-T1-2025-11-17 \
  --output evaluations/copilot/T1-reanalysis
```

### Clean Up Old Branches (After Tagging)
```bash
# List all tool branches
git branch -r | grep 'tool/'

# Delete remote branch (tag preserves the state)
git push origin --delete tool/copilot/task-T1

# Delete local branch
git branch -D tool/copilot/task-T1
```

## Future Enhancements

Planned improvements to the benchmark framework:
- [ ] Automated scoring calculator script
- [ ] Comparison dashboard for visualizing results
- [ ] Drift detection benchmarks
- [ ] Streaming stress tests
- [ ] Multi-tool parallel execution
- [ ] Results database for historical tracking
- [ ] Automated PR creation for evaluation results

## Contributing

To add new benchmark tasks:
1. Create task YAML in `benchmark/tasks/`
2. Follow existing task format (objective, requirements, criteria)
3. Add test cases
4. Update this README with task description

## License & Usage

This benchmark framework is designed for internal evaluation of AI coding tools. All synthetic data is safe to use. No real customer data should ever be used in benchmarks.
