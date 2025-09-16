VENV=.venv
PY=$(VENV)/bin/python
FLASK=$(VENV)/bin/flask
PIDFILE=.flask.pid
HOST=127.0.0.1
PORT=5000

.PHONY: venv install dev-start dev-stop dev-restart dev-status

venv:
	python3 -m venv $(VENV)

install: venv
	$(PY) -m pip install -U pip wheel
	$(PY) -m pip install -r requirements.txt

dev-start:
	@[ -f $(PIDFILE) ] && kill -0 $$(cat $(PIDFILE)) 2>/dev/null && \
	 echo "Already running (PID $$(cat $(PIDFILE))) on $(HOST):$(PORT)" && exit 0 || true
	FLASK_APP=cpi_app.app:create_app FLASK_RUN_HOST=$(HOST) FLASK_RUN_PORT=$(PORT) \
	nohup $(FLASK) run --debug > .flask.log 2>&1 & echo $$! > $(PIDFILE)
	@sleep 0.3
	@echo "Flask started on http://$(HOST):$(PORT) (PID $$(cat $(PIDFILE)))"

dev-stop:
	@if [ -f $(PIDFILE) ]; then \
	  PID=$$(cat $(PIDFILE)); \
	  if kill -0 $$PID 2>/dev/null; then \
	    echo "Stopping Flask (PID $$PID)"; \
	    kill -INT $$PID; \
	    sleep 0.5; \
	    kill -TERM $$PID 2>/dev/null || true; \
	  fi; \
	  rm -f $(PIDFILE); \
	else \
	  echo "No PID file found."; \
	fi

dev-restart: dev-stop dev-start

dev-status:
	@if [ -f $(PIDFILE) ]; then \
	  PID=$$(cat $(PIDFILE)); \
	  if kill -0 $$PID 2>/dev/null; then \
	    echo "Running (PID $$PID) at http://$(HOST):$(PORT)"; \
	  else \
	    echo "Stale PID file ($(PIDFILE))"; \
	  fi; \
	else \
	  echo "Not running."; \
	fi
