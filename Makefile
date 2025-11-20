.PHONY: setup test benchmark-task benchmark-all data-gen

setup:
	pip install --trusted-host pypi.org -r requirements.txt || poetry install

data-gen:
	python scripts/data/generate_transactions.py --rows 1000

test:
	python -m pytest tests/ -v --cov=pipelines --cov-report=term

auto-score:
	python benchmark/scripts/auto_score.py \
	  --baseline $(BASELINE) \
	  --implementation $(IMPL) \
	  --task $(TASK) \
	  --output $(OUTPUT)

benchmark-task:
	bash benchmark/scripts/run_task.sh $(T)

benchmark-all:
	for f in benchmark/tasks/*.yaml; do \
	  id=$$(basename $$f .yaml); \
	  echo "Running $$id"; \
	  T=$$id bash benchmark/scripts/run_task.sh $$id; \
	done