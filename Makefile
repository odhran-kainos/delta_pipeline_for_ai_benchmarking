.PHONY: setup test benchmark-task benchmark-all data-gen data-gen-all data-gen-cdc data-gen-streaming

setup:
	pip install --trusted-host pypi.org -r requirements.txt || poetry install

# Generate transaction data (T1/T2)
data-gen:
	python scripts/data/generate_transactions.py --rows 1000

# Generate CDC customer change data (T4)
data-gen-cdc:
	python scripts/data/generate_customer_changes.py \
	  --num-customers 500 \
	  --num-events 2000 \
	  --num-batches 3

# Generate streaming transaction data (T5)
data-gen-streaming:
	python scripts/data/generate_streaming_transactions.py \
	  --num-customers 200 \
	  --num-events 5000 \
	  --num-micro-batches 10

# Generate all sample data for all tasks
data-gen-all: data-gen data-gen-cdc data-gen-streaming
	@echo "All sample data generated successfully"

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