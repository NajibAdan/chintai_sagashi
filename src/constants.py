from dotenv import load_dotenv
from os import getenv

load_dotenv()

BUCKET_ENDPOINT = getenv("BUCKET_ENDPOINT", "localhost:9000")
BUCKET_NAME = getenv("BUCKET_NAME", "chintai")
BUCKET_REGION = getenv("BUCKET_REGION", "us-east-1")
AWS_ACCESS_KEY = getenv("AWS_ACCESS_KEY", "accesskey")
AWS_SECRET_KEY = getenv("AWS_SECRET_KEY", "secretpass")
