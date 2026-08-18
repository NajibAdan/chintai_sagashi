from dotenv import load_dotenv
from os import getenv

load_dotenv()


BUCKET_ENDPOINT = getenv("BUCKET_ENDPOINT", "localhost:9000")
BUCKET_NAME = getenv("BUCKET_NAME", "chintai")
BUCKET_REGION = getenv("BUCKET_REGION", "us-east-1")
AWS_ACCESS_KEY = getenv("AWS_ACCESS_KEY", "accesskey")
AWS_SECRET_KEY = getenv("AWS_SECRET_KEY", "secretpass")


# Configuration for locations to scrape
# Each location is a dict with 'prefecture' and 'city' keys
# City should be the short name (e.g., 'sendai' for Sendai, URL will be 'sa_sendai')

SCRAPE_LOCATIONS = [
    {"prefecture": "miyagi", "city": "sendai"},
    {"prefecture": "chiba", "city": "chiba"}
]
