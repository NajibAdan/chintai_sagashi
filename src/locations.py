# Configuration for locations to scrape
# Each location is a dict with 'prefecture' and 'city' keys
# City should be the short name (e.g., 'sendai' for Sendai, URL will be 'sa_sendai')

# TODO: It seems SUUMO uses a different approach for naming cities in 東京.

LOCATIONS = [
    {"prefecture": "miyagi", "city": "sendai"},
    {"prefecture": "chiba", "city": "chiba"},
    {"prefecture": "osaka", "city": "osaka"},
    {"prefecture": "kanagawa", "city": "yokohama"},
    {"prefecture": "saitama", "city": "saitama"},
    {"prefecture": "fukuoka", "city": "kitakyushu"},
]
