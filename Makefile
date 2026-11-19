.PHONY: up down reset seed-clean seed-dirty seed-stale seed-drift run test urls logs-alerts

up:            ## build and start the whole stack
	docker compose up -d --build
	@$(MAKE) --no-print-directory urls

down:
	docker compose down

reset:         ## wipe all volumes (fresh start)
	docker compose down -v

seed-clean seed-dirty seed-stale seed-drift:  ## load a demo scenario into the warehouse
	docker compose run --rm seed --scenario $(subst seed-,,$@)

run:           ## trigger a validation run right now
	docker compose exec airflow-scheduler airflow dags trigger dq_pipeline

test:          ## unit tests (no services needed)
	pip install -q -r requirements-dev.txt && python -m pytest -q

logs-alerts:   ## watch alerts arrive
	docker compose logs -f alert-sink

urls:
	@echo "Airflow    http://localhost:8080  (admin / admin)"
	@echo "Grafana    http://localhost:3000  (admin / admin) -> Data Quality folder"
	@echo "Prometheus http://localhost:9090  (alerts: /alerts)"
	@echo "Alertmgr   http://localhost:9093"
