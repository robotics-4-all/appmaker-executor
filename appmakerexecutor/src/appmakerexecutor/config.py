import os

ZERO_LOGS = int(os.getenv('APPMAKER_ZERO_LOGS', 0))
LOG_LEVEL = os.getenv("APPMAKER_LOG_LEVEL", "INFO")

REDIS_HOST=os.getenv("REDIS_HOST", "localhost")
REDIS_PORT=os.getenv('REDIS_PORT', '6379')
REDIS_USERNAME=os.getenv('REDIS_USERNAME', '')
REDIS_PASSWORD=os.getenv('REDIS_PASSWORD', '')
REDIS_DB=os.getenv('REDIS_DB', 0)
