from os import getenv

from dotenv import load_dotenv

load_dotenv()


BUCKET_ENDPOINT = getenv("BUCKET_ENDPOINT", "localhost:9000")
BUCKET_NAME = getenv("BUCKET_NAME", "chintai")
BUCKET_REGION = getenv("BUCKET_REGION", "us-east-1")
AWS_ACCESS_KEY = getenv("AWS_ACCESS_KEY", "accesskey")
AWS_SECRET_KEY = getenv("AWS_SECRET_KEY", "secretpass")
ENVIRONMENT = getenv("ENVIRONMENT", "dev")
PAGES_TO_FETCH = 10

# Configuration for locations to crawl
# Each location is a dict with 'prefecture' and 'city' keys
# City should be the short name (e.g., 'sendai' for Sendai, URL will be 'sa_sendai')

CRAWL_LOCATIONS = [
    {"prefecture": "miyagi", "city": "sendai"},
    {"prefecture": "chiba", "city": "chiba"},
]
