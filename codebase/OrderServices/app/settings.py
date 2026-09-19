from starlette.config import Config
from starlette.datastructures import Secret

try:
    config = Config(".env")
except FileNotFoundError:
    config = Config()


SECRET_KEY = config("SECRET_KEY", cast=str)
ALGORITHM = config("ALGORITHM", cast=str)
JWT_EXPIRY_TIME = config("JWT_EXPIRY_TIME", cast=int)

DATABASE_URL = config("DATABASE_URL", cast=Secret)
BOOTSTRAP_SERVER = config("BOOTSTRAP_SERVER", cast=str)

# Commands for the order service (create / update / delete an order)
KAFKA_TOPIC_ORDER = config("KAFKA_TOPIC_ORDER", cast=str)
KAFKA_CONSUMER_GROUP_ID_FOR_ORDER = config(
    "KAFKA_CONSUMER_GROUP_ID_FOR_ORDER", cast=str
)

# Events published AFTER an order was accepted / deleted
# (consumed by the payment and notification services).
KAFKA_TOPIC_ORDER_EVENTS = config(
    "KAFKA_TOPIC_ORDER_EVENTS", cast=str, default="order-events"
)

# Request / reply with the inventory service
KAFKA_TOPIC_INVENTORY_CHECK_REQUEST = config(
    "KAFKA_TOPIC_INVENTORY_CHECK_REQUEST", cast=str
)
KAFKA_TOPIC_INVENTORY_CHECK_RESPONSE = config(
    "KAFKA_TOPIC_INVENTORY_CHECK_RESPONSE", cast=str
)

# Request / reply with the user service
KAFKA_TOPIC_REQUEST_TO_USER = config("KAFKA_TOPIC_REQUEST_TO_USER", cast=str)
KAFKA_TOPIC_RESPONSE_FROM_USER_TO_ORDER = config(
    "KAFKA_TOPIC_RESPONSE_FROM_USER_TO_ORDER", cast=str
)

# Payment results
KAFKA_TOPIC_PAYMENT_DONE_FROM_PAYMENT = config(
    "KAFKA_TOPIC_PAYMENT_DONE_FROM_PAYMENT", cast=str
)
KAFKA_CONSUMER_GROUP_ID_FOR_RESPONSE_FROM_PAYMENT = config(
    "KAFKA_CONSUMER_GROUP_ID_FOR_RESPONSE_FROM_PAYMENT", cast=str
)

REQUEST_TIMEOUT_SECONDS = config("REQUEST_TIMEOUT_SECONDS", cast=float, default=10.0)
