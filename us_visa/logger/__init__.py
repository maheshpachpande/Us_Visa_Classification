import logging
import os
from datetime import datetime

# Create log filename with timestamp
LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

# Use project root (directory where this file lives)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Logs folder inside project root
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Full path to the log file
LOG_PATH = os.path.join(LOG_DIR, LOG_FILE)

# Logging configuration
logging.basicConfig(
    filename=LOG_PATH,
    format="[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Example usage
logger = logging.getLogger(__name__)
logger.info("Logging is now set up!")
