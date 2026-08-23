# 賃貸探し - Chintai Sagashi

A Python-based rental-listing crawler for SUUMO that collects apartment listings for cities in Japan, stores raw HTML and parsed JSONL data in S3-compatible object storage, compacts the data to Parquet, and exposes it to dbt analytics models.

This project is designed to help monitor the rental market over time in selected prefectures and cities. 

## Overview

The pipeline has three core stages:

1. Crawl SUUMO listing pages for configured locations.
2. Save raw HTML and parsed listing records to local disk and S3-compatible storage.
3. Compact the daily crawl output into Parquet and transform it using dbt.

## Architecture

- Python crawler using requests + BeautifulSoup
- Local log output under the logs directory
- S3-compatible object storage backend (MinIO in local dev, but not required)
- DuckDB compaction step to convert JSONL shards into a single Parquet file
- dbt models for downstream analytics and reporting

## Project structure

```text
.
├── dbt/
│   └── suumo/
│       ├── models/
│       ├── macros/
│       ├── dbt_project.yml
│       └── README.md
├── logs/
├── src/
│   └── crawler/
│       ├── client.py
│       ├── compactor.py
│       ├── crawler.py
│       ├── parser.py
│       ├── results.py
│       ├── settings.py
│       └── storage.py
├── tests/
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── README.md
└── .env
```

## Requirements

- Python 3.13+
- uv
- Any S3-compatible object storage backend, such as MinIO, AWS S3, or a compatible hosted service

## Setup

1. Install dependencies:

```bash
uv sync
```

2. Create a local environment file if needed:

```bash
cp .env.example .env
```

If `.env.example` does not exist, create `.env` with values matching the crawler settings, for example:

```bash
BUCKET_ENDPOINT=http://localhost:9000
BUCKET_ENDPOINT_SHORT=localhost:9000
BUCKET_NAME=chintai
BUCKET_REGION=us-east-1
AWS_ACCESS_KEY=accesskey
AWS_SECRET_KEY=secretpass
ENVIRONMENT=dev
```

3. Optional: start the local MinIO bootstrap for development:

```bash
docker compose up -d
```

This docker-compose file is a sample development environment to quickly provision a local object storage endpoint. It is not required for the crawler to function; it is simply a convenient way to bootstrap MinIO when working locally. The crawler can also use AWS S3 or any other S3-compatible service by setting the corresponding `BUCKET_*` environment variables.

The compose file starts:

- MinIO object storage on port 9000
- MinIO console on port 9001
- An initialization service that creates the configured bucket

## Configuration

The main crawler configuration is in `src/crawler/settings.py`.

Key settings:

- `CRAWL_LOCATIONS`: list of prefecture/city targets to crawl
- `PAGES_TO_FETCH`: page cap used in dev mode
- `ENVIRONMENT`: `dev` or production-like mode
- `BUCKET_*`: S3-compatible object storage endpoint and credentials

Default locations:

```python
CRAWL_LOCATIONS = [
    {"prefecture": "miyagi", "city": "sendai"},
    {"prefecture": "chiba", "city": "chiba"},
]
```

## Running the crawler

Run the scraper in the project root:

```bash
uv run -m crawler.crawler
```

This will:

- determine total pages for each configured location
- fetch listing pages with retries and soft-block handling
- save HTML snapshots to `data/raw/suumo/.../html/`
- parse listing data into JSONL records
- upload the artifacts to the configured object store
- compact daily crawl data into a Parquet file using DuckDB

## Data format

The crawler writes:

- raw HTML archives: `page-000001.html.gz`
- parsed listing shards: `page-000001-part-0000.jsonl.gz`
- compacted daily parquet output under `data/compacted/suumo/...`

Each parsed listing record includes fields such as:

- `source`
- `crawl_ts`
- `property_name`
- `location`
- `stations`
- `monthly_rent`
- `management_fee`
- `deposit`
- `gratuity`
- `madori`
- `menseki`
- `building_age`
- `building_type`
- `url`
- `url_key`

## dbt analytics

The dbt project is located at `dbt/suumo`.

Useful commands:

```bash
make dbt-debug
make dbt-run
make dbt-build
```

These commands call `dbt` with the project directory and environment variables loaded from `.env`.

## Testing

This project includes parser and storage checks under the `tests` directory.

Run the suite with:

```bash
uv run pytest
```

## Useful Make targets

```bash
make docker-up
make dbt-run
make dbt-build
make docker-restart
```

## Notes

- The crawler is intentionally resilient to SUUMO soft blocks and retries pages that return temporary error pages.
- In dev mode, the total number of pages is capped by `PAGES_TO_FETCH`.
- The project is set up for local experimentation and analytics, but the architecture is compatible with broader ETL-style workflows.
- MinIO is included as a sample local bootstrap for development, not as a hard requirement or vendor lock-in.

## License

This project is provided as-is for local research and analysis purposes.
