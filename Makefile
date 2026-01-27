.PHONY: dev dev-backend dev-frontend

BACKEND_DIR := backend
FRONTEND_DIR := frontend

UVICORN := $(BACKEND_DIR)/.venv/bin/uvicorn
ifeq ($(wildcard $(UVICORN)),)
UVICORN := uvicorn
endif

FRONTEND_RUNNER ?= bun
FRONTEND_ARGS ?= run dev

dev:
	@$(MAKE) -j2 dev-backend dev-frontend

dev-backend:
	@$(UVICORN) --app-dir $(BACKEND_DIR) app.main:app --reload --host 0.0.0.0 --port 8000

dev-frontend:
	@cd $(FRONTEND_DIR) && $(FRONTEND_RUNNER) $(FRONTEND_ARGS)
