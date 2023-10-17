# Standard Library Imports
import logging

# Local Application Imports
from CarCrashAnalysis import analysis as car_crash_analysis
from CarCrashAnalysis import utils as car_crash_utils

# Constants
LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
DEFAULT_LOG_LEVEL = logging.INFO
PY4J_LOG_LEVEL = logging.INFO

def configure_logging():
    """
    Configures the logging for the application.

    Args:
        None

    Returns:
        None
    """
    logging.basicConfig(
        format=LOG_FORMAT,
        level=DEFAULT_LOG_LEVEL
    )
    logging.getLogger("py4j").setLevel(PY4J_LOG_LEVEL)

configure_logging()
