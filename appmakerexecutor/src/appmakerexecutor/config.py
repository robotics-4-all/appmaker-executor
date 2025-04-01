import os

ZERO_LOGS = int(os.getenv('APPMAKER_ZERO_LOGS', 0))
LOG_LEVEL = os.getenv("APPMAKER_LOG_LEVEL", "INFO")
