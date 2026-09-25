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

# Stripe: the secret key must come from the environment, never from the code.
STRIPE_API_KEY = config("STRIPE_API_KEY", cast=Secret)
STRIPE_SUCCESS_URL = config(
    "STRIPE_SUCCESS_URL", cast=str, default="http://localhost:8000/payment-service/success"
)
STRIPE_CANCEL_URL = config(
    "STRIPE_CANCEL_URL", cast=str, default="http://localhost:8000/payment-service/cancel"
)

# Topic where this service PUBLISHES the result of a payment
# (consumed by the order and notification services).
KAFKA_TOPIC_PAYMENT = config("KAFKA_TOPIC_PAYMENT", cast=str)

# Events of the order service (order accepted / deleted): consumed here.
KAFKA_TOPIC_GET_FROM_ORDER = config("KAFKA_TOPIC_GET_FROM_ORDER", cast=str)
KAFKA_CONSUMER_GROUP_ID_FOR_ORDER = config(
    "KAFKA_CONSUMER_GROUP_ID_FOR_ORDER", cast=str
)

# Request / reply with the user service
KAFKA_TOPIC_REQUEST_TO_USER = config("KAFKA_TOPIC_REQUEST_TO_USER", cast=str)
KAFKA_TOPIC_RESPONSE_FROM_USER_TO_PAYMENT = config(
    "KAFKA_TOPIC_RESPONSE_FROM_USER_TO_PAYMENT", cast=str
)

REQUEST_TIMEOUT_SECONDS = config("REQUEST_TIMEOUT_SECONDS", cast=float, default=10.0)
