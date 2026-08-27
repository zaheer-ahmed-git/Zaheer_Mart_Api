from starlette.config import Config

try:
    config = Config(".env")
except FileNotFoundError:
    config = Config()

# Not used by the combiner itself; optional so that the service can start without it.
KONG_ADMIN_URL = config("KONG_ADMIN_URL", cast=str, default="")
REQUEST_TIMEOUT_SECONDS = config("REQUEST_TIMEOUT_SECONDS", cast=float, default=5.0)
