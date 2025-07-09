.PHONY: test
test:
	docker compose up --wait
	docker compose exec rabbitmq bash -c 'rabbitmqctl list_queues -s| cut -f1-1 | xargs -r -n1 -P4 rabbitmqctl delete_queue'
	uv run pytest -n 4
