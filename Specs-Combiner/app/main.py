import asyncio
import logging

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from jsonmerge import merge

from app import settings

app = FastAPI(docs_url="/docs", root_path="/specs-combiner")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVICES = [
    {"name": "user-service", "url": "http://user:8007/openapi.json", "base_route": "/user-service"},
    {"name": "product-service", "url": "http://product:8002/openapi.json", "base_route": "/product-service"},
    {"name": "order-service", "url": "http://order:8003/openapi.json", "base_route": "/order-service"},
    {"name": "notification-service", "url": "http://notification:8004/openapi.json", "base_route": "/notification-service"},
    {"name": "inventory-service", "url": "http://inventory:8005/openapi.json", "base_route": "/inventory-service"},
    {"name": "payment-service", "url": "http://payment:8006/openapi.json", "base_route": "/payment-service"},
]


def add_base_route_to_paths(spec: dict, base_route: str) -> dict:
    spec["paths"] = {
        f"{base_route}{path}": item for path, item in spec.get("paths", {}).items()
    }
    return spec


def _fetch_one(service: dict):
    logger.info(f"Fetching spec from {service['url']}")
    response = requests.get(
        service["url"],
        headers={"Cache-Control": "no-cache"},
        timeout=settings.REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return add_base_route_to_paths(response.json(), service["base_route"])


async def fetch_openapi_specs():
    """Fetch every spec in parallel. A service that is down is skipped (and
    reported) instead of breaking the whole document."""
    results = await asyncio.gather(
        *(asyncio.to_thread(_fetch_one, s) for s in SERVICES), return_exceptions=True
    )
    specs, failed = [], []
    for service, result in zip(SERVICES, results):
        if isinstance(result, Exception):
            logger.error(f"Error fetching {service['name']} spec: {result}")
            failed.append(service["name"])
        else:
            specs.append(result)
    return specs, failed


def merge_openapi_specs(specs: list, unavailable: list) -> dict:
    merged_spec = {
        "openapi": specs[0].get("openapi", "3.1.0") if specs else "3.1.0",
        "info": {
            "title": "Merged API",
            "version": "1.0.0",
            "description": (
                "Unavailable services: " + ", ".join(unavailable) if unavailable else ""
            ),
        },
        "servers": [{"url": "/"}],
        "paths": {},
        "components": {"schemas": {}},
    }
    for spec in specs:
        merged_spec["paths"] = merge(merged_spec["paths"], spec.get("paths", {}))
        for component, items in spec.get("components", {}).items():
            merged_spec["components"][component] = merge(
                merged_spec["components"].get(component, {}), items
            )
    return merged_spec


@app.get("/merged/openapi.json")
async def get_merged_openapi_spec():
    specs, failed = await fetch_openapi_specs()
    response = JSONResponse(content=merge_openapi_specs(specs, failed))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
