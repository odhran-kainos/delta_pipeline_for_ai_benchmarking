# GitHub Copilot Chat Prompt Cheat Sheet (Data Engineering Benchmark)

This cheat sheet helps you craft consistent, high-quality prompts when using GitHub Copilot Chat in VS Code for your benchmark tasks (T1–T8). It is optimized for:
- PySpark + Delta Lake (medallion architecture)
- Config-driven pipelines
- Benchmark harness tasks
- AI-assisted comparative evaluation

---

## 1. Core Principles

1. Be Specific: Provide the task ID (e.g. T1), the desired outputs (code, tests, docs), and constraints (e.g. "must be config-driven").
2. Provide Context: Use selection or open relevant files so Copilot Chat automatically includes them.
3. Request Format: Ask for patches/diffs or explicit file-level outputs to reduce ambiguity.
4. Iterate: Start broad (plan) → refine (design) → implement (code) → validate (tests) → optimize (performance).
5. Maintain Benchmark Integrity: Ask for minimal edits outside the task scope; enforce architectural alignment.

---

## 2. Useful Chat Patterns

| Goal | Prompt Pattern |
|------|----------------|
| Summarize file | `@copilot Summarize what this file does and list extension points.` |
| Plan a task | `@copilot For task T3 (quality gate), propose a step-by-step implementation plan referencing existing project utilities.` |
| Generate code | `@copilot Implement the Bronze ingestion for T1 in a new file. Follow config-driven style like existing ingestion modules. Provide full file content.` |
| Add tests | `@copilot Write pytest unit tests for the new enrichment logic added in T2. Include edge cases: unknown customers, duplicate timestamps.` |
| Refactor | `@copilot Refactor this Spark transformation to reduce shuffles and explain the optimization.` |
| Explain errors | `@copilot Why is this Spark job failing with AnalysisException: "..."? Suggest fixes.` |
| Performance analysis | `@copilot Suggest optimizations for this Silver layer job using partitioning and caching strategies.` |
| Generate documentation | `@copilot Create documentation section for T4 Gold aggregates: include purpose, inputs, outputs, partition strategy.` |
| CDC design | `@copilot For T6, design a Delta MERGE strategy to handle updates and deletes. Provide pseudo-SQL + Python implementation.` |
| Streaming variant | `@copilot Convert the current batch ingestion job into a streaming ingestion job using Structured Streaming. Show only the changed parts.` |
| Lineage metadata | `@copilot Propose a JSON schema for run-level lineage metadata for T8.` |
| Diff output | `@copilot Provide a unified diff for adding the T1 ingestion job and its config entry.` |
| Troubleshoot test failure | `@copilot Test 'test_deduplication_retains_latest' is failing. Here is the test and output. Diagnose and fix.` |

---

## 3. Slash Commands (If Available)

| Command | Usage Example |
|---------|---------------|
| `/explain` | Select a failing code block → `/explain` to understand logic. |
| `/tests` | Select function → `/tests` to auto-generate test scaffolds. |
| `/fix` | Highlight broken code → `/fix` to request corrections. |
| `/docs` | Select module → `/docs` for docstring or README snippet generation. |
| `/generate` | Empty file open → `/generate` for full implementation draft. |

---

## 4. Participants & Context Variables

Use participants like `@copilot` (default) and context variables like workspace references if supported.

Example:
```
@copilot Using the existing config-driven pattern in src/config/, add a new config entry for 'transactions' ingestion required in T1. Keep naming consistent. Provide a patch.
```

---

## 5. Task-Specific Prompt Templates

### T1 – Bronze Ingestion
Planning Prompt:
```
@copilot Task T1: Add Bronze ingestion for synthetic transactions JSON. Requirements:
- Read JSON lines from data/raw_seed/transactions.json
- Enforce schema (transaction_id, customer_id, event_timestamp, amount, currency)
- Add _ingest_ts and _file_name columns
- Filter out rows missing transaction_id
Using existing ingestion utilities, propose:
1. File(s) to create or modify
2. Functions and responsibilities
3. Error handling strategy
Return as a numbered list.
```

Implementation Prompt:
```
@copilot Implement the Bronze ingestion job for T1:
- New module: src/pipelines/bronze_transactions.py
- Reuse existing Spark session initialization pattern
- Config-driven path (add to config file)
- Output Delta table 'bronze_transactions'
Provide full file content + any necessary config patch.
```

### T2 – Silver Enrichment
```
@copilot Task T2: Silver enrichment joining transactions with customers.
Rules:
- Keep latest duplicate by transaction_id based on event_timestamp
- Unknown customers → customer_segment='UNKNOWN'
- Add derived transaction_value_vat = amount * 1.20
Provide design outline + code changes (new file or function).
```

### T3 – Quality Gate
```
@copilot Task T3: Implement data quality gate before promoting Silver → Gold.
Checks:
- Null ratio on critical columns (transaction_id, amount) must be < 5%
- Deduplicate transaction_id
Output:
- Block promotion if fail
- Emit metrics to a Delta table 'quality_metrics'
Ask for: plan + pseudocode + test strategy.
```

### T4 – Gold Aggregates
```
@copilot Task T4: Create Gold daily revenue per customer segment.
Input: Silver enriched table
Output: gold_daily_revenue partitioned by date (event_date)
Metrics: total rows, distinct segments
Need optimization suggestions: partition columns, small file handling.
Return code + explanation.
```

### T5 – Testing Upgrade
```
@copilot Task T5: Increase test coverage.
Generate pytest tests for:
- Bronze ingestion (valid/invalid rows)
- Silver enrichment (duplicate handling, unknown segment)
Provide test file contents and suggest coverage measurement command.
```

### T6 – CDC Merge
```
@copilot Task T6: Add CDC support:
Assume incoming updates/deletes feed (transactions_updates.json) with fields:
(transaction_id, event_timestamp, amount, op_type ['UPDATE','DELETE'])
Implement Delta MERGE logic to apply changes to Silver table.
Return: MERGE SQL snippet + Python wrapper function + test plan.
```

### T7 – Streaming Variant
```
@copilot Task T7: Convert Bronze ingestion to streaming:
- Use rate source or simulated file stream
- Ensure watermarking on event_timestamp
- Write to Bronze with append + autoOptimize?
Provide streaming job code and comments on fault tolerance.
```

### T8 – Lineage Metadata
```
@copilot Task T8: Emit lineage metadata JSON per run:
Fields: task_id, source_tables, target_tables, columns_added, row_counts, duration_seconds, timestamp
Add function `emit_lineage(metadata: dict)` writing to Delta table 'lineage_runs'
Provide JSON schema + implementation + test stub.
```

---

## 6. Evaluation-Oriented Prompts (For Benchmarking AI Behavior)

Planning Quality:
```
@copilot For task T3, enumerate all implicit risks and edge cases (duplicates, null explosion, schema evolution). Return as a table with mitigation strategies.
```

Maintainability:
```
@copilot Review this new module and suggest refactors to reduce complexity or align naming to existing codebase conventions.
```

Performance:
```
@copilot Identify potential shuffle hotspots in this Silver transformation (paste code). Recommend 3 optimizations with rationale.
```

Documentation:
```
@copilot Draft a docstring + README section explaining the Gold aggregation job purpose, inputs, outputs, and performance considerations.
```

Security / Compliance:
```
@copilot Scan this code for potential issues: handling of paths, secrets, or unvalidated inputs. Suggest improvements if any.
```

---

## 7. Prompt Patterns for Unified Diffs

```
@copilot Provide a unified diff (no commentary) to:
1. Add configuration entry for 'transactions'
2. Create src/pipelines/bronze_transactions.py
3. Add pytest test file tests/test_bronze_transactions.py
```

If Copilot returns prose + code, follow up:
```
@copilot Reformat response strictly as a unified diff without explanations.
```

---

## 8. Debugging Interaction Flow

1. Ask for root cause:
```
@copilot Spark job failed with AnalysisException (paste stacktrace). Explain root cause.
```
2. Ask for targeted fix:
```
@copilot Provide a minimal code fix; do not rewrite unrelated logic.
```
3. Confirm improvement:
```
@copilot Show optimized version and highlight changed lines only.
```

---

## 9. Anti-Patterns to Avoid

| Anti-Pattern | Avoid Because | Better Approach |
|--------------|---------------|-----------------|
| “Help me with code” (vague) | Low relevance | Specify task ID + desired output format |
| Dump huge context + no question | Model may summarize only | Give targeted question per file |
| Mixing multiple tasks in one prompt | Hard to evaluate output | One task per prompt iteration |
| Asking for “best architecture” mid-implementation | Scope creep | Separate design vs implementation prompts |
| Accepting hallucinated APIs | Introduces errors | Ask: “List assumptions you made—confirm each.” |

---

## 10. Validation Prompts After Code Generation

```
@copilot List assumptions you made when generating this ingestion job. Mark any that are uncertain.
```

```
@copilot Provide test scenarios I should add that are NOT yet covered in tests I showed you.
```

```
@copilot Does this implementation align with config-driven architecture? If not, propose minimal adjustments.
```

---

## 11. Customizing for Comparison Across AI Tools

Use a canonical prompt format stored in `benchmark/prompts/<task>.md`:

Template:
```
TASK: <ID> - <Title>
CONTEXT SUMMARY: <Short 2–3 line description>
OBJECTIVE: <Exact expected outputs>
CONSTRAINTS: <Architecture, style, patterns>
EVALUATION METRICS: <List>
PLEASE RETURN: <Specify format – plan first OR code directly>
```

Example (T1):
```
TASK: T1 – Bronze Ingestion
CONTEXT SUMMARY: Add ingestion for synthetic transactions JSON into Bronze Delta.
OBJECTIVE: New module + config patch + filters invalid rows; add metadata columns.
CONSTRAINTS: Must reuse existing config + Spark session pattern; avoid hardcoding paths.
EVALUATION METRICS: rows_loaded, rows_invalid, duration_seconds.
PLEASE RETURN: Step-by-step plan (numbered), then full code (no placeholders).
```

---

## 12. Prompt Refinement Strategy (Iterative Loop)

1. Planning:
```
@copilot Provide a 5-step implementation plan for T2.
```
2. Verification:
```
@copilot Are any steps missing regarding duplicate handling or late arrivals? Suggest additions.
```
3. Implementation:
```
@copilot Implement steps 1–2 only. Stop before enrichment logic.
```
4. Completion:
```
@copilot Now implement the remaining steps and tests.
```
5. Improvement:
```
@copilot Optimize the enrichment join for skewed customer_id distribution.
```

---

## 13. Measuring Prompt Effectiveness

Track:
- Number of follow-up corrections needed
- Added assumptions discovered post-run
- Diff size vs task scope
- Time to working implementation

Prompt to request self-assessment:
```
@copilot Self-assess the code you generated for T4 against goals (list pass/fail per goal).
```

---

## 14. Quick Reference Cheat Table

| Scenario | Best Prompt Starter |
|----------|---------------------|
| New task design | `Plan task <ID>: ...` |
| Extend config | `Add config entry: ... Provide diff.` |
| Spark optimization | `Optimize this transformation: ...` |
| Delta MERGE logic | `Design MERGE for CDC: ...` |
| Data quality | `Implement quality checks: ...` |
| Streaming conversion | `Convert batch job to streaming: ...` |
| Test generation | `Generate pytest tests for: ...` |
| Documentation | `Write developer docs for: ...` |
| Lineage | `Propose lineage JSON schema for: ...` |
| Diff only | `Return unified diff only: ...` |
| Assumptions | `List your assumptions about: ...` |
| Edge cases | `Enumerate edge cases for: ...` |

---

## 15. Next Enhancements

Consider adding:
- A “prompt log” capturing exact prompts used per tool run for audit.
- A script to inject canonical prompt text into Chat (copy/paste).
- A rubric file for manual planning quality scoring.

---

## 16. Example Full Conversation Flow (T3)

1. Plan:
```
@copilot Task T3: Need quality gate. Provide plan with functions, data structures, and metrics.
```
2. Validate:
```
@copilot Are null ratio computed before or after deduplication? Clarify and adjust plan.
```
3. Implement Part 1:
```
@copilot Implement null ratio computation function only with docstring + test.
```
4. Implement Part 2:
```
@copilot Implement deduplication and integrate into pipeline. Provide diff.
```
5. Integrate Metrics:
```
@copilot Add metrics emission to quality_metrics Delta table; show code patch.
```
6. Self-assess:
```
@copilot Self-assess if any quality dimensions still missing (e.g. schema drift).
```

---

## 17. Final Tip

If Copilot output drifts from spec:
```
@copilot Highlight lines in your last response that violate constraints (e.g. hardcoded path). Provide corrected version only.
```

---

Happy benchmarking! Keep prompts consistent across tools to ensure fair comparison.