# ZAHEER-MART-API

A microservices backend for an e-commerce store. Each business domain (users, products, inventory, orders, payments, notifications) is an independent FastAPI service with its own PostgreSQL database. Services talk to each other through Kafka events and request/reply topics, and all public traffic goes through a Kong API gateway.

## Architecture

```
client
  │
  ▼
Kong API gateway  (rate limiting, path routing)
  ├─ /user-service          → user          :8007
  ├─ /product-service       → product       :8002
  ├─ /order-service         → order         :8003
  ├─ /inventory-service     → inventory     :8005
  ├─ /payment-service       → payment       :8006
  ├─ /notification-service  → notification  :8004
  └─ /specs-combiner        → specs-combiner :9000  (merges the services' OpenAPI specs)

Kafka (broker :9092, Kafka UI :8080) connects the services asynchronously:
  order ⇄ inventory (stock check request/response)
  order ⇄ user, payment ⇄ user (user lookups)
  product → inventory (product events), inventory → product (stock level)
  order / payment → notification (order created, payment done)

Each service has its own PostgreSQL instance (ports 5433–5438 on localhost).
```

| Service | Responsibility |
|---|---|
| `UserServices` | Registration, login and JWT-based authentication |
| `ProductServices` | Product catalogue |
| `InventoryServices` | Stock levels and stock checks |
| `OrderServices` | Order creation and processing |
| `PaymentServices` | Payments through Stripe |
| `NotificationServices` | Email notifications |
| `Specs-Combiner` | Merges the OpenAPI specs of all services into one document |

## Tech stack

Python 3.12 · FastAPI · Poetry · Kafka (aiokafka) · PostgreSQL · Stripe · Kong · Docker Compose · Pytest

## Repository layout

```
codebase/                 # all services, docker-compose stack and env template
  compose.yml             # services, databases, Kafka, Kafka UI
  .env.example            # environment variables template (copy, never commit real values)
  e2e_smoke.py            # end-to-end smoke script for the running stack
  <Service>/              # one folder per service: app/, tests/, pyproject.toml, Dockerfile.dev
Specs-Combiner/           # OpenAPI merger service
kong.yaml                 # Kong declarative configuration (routes, rate limiting)
kong-docker-compose.yml   # Kong container setup
```

## Getting started

Requirements: Docker with Compose, and Python 3.12 + Poetry if you want to run tests outside Docker.

1. Create your environment files from the template:
   ```bash
   cd codebase
   cp .env.example .env
   ```
   Fill in the values (database credentials, JWT secret, Stripe test key, email account). The same `SECRET_KEY`, `ALGORITHM` and `JWT_EXPIRY_TIME` must be used by every service that verifies tokens. Do not commit `.env` files.

2. Start the stack:
   ```bash
   docker compose -f compose.yml up --build
   ```

3. Start the Kong gateway (from the repository root):
   ```bash
   docker compose -f kong-docker-compose.yml up -d
   ```

4. Check the services directly on their ports (for example `http://localhost:8007/docs` for the user service) or through Kong on `http://localhost:8000`.

5. Optional: run the end-to-end smoke script against the running stack:
   ```bash
   python codebase/e2e_smoke.py
   ```

## Running tests

Each service has its own test suite (`pytest`, `pytest-asyncio`). From a service folder:

```bash
cd codebase/UserServices
poetry install
poetry run pytest
```

## Status

Work in progress. The services are built and wired together, but this README has not yet been validated against a fresh clone.

## Security

- `.env` files are ignored by git. Only `.env.example` (placeholders) is tracked.
- Never commit real secrets, API keys or passwords.
