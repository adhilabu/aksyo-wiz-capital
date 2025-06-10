from datetime import datetime
import logging
import os

from app.analyse.schemas import TradeAnalysisType
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)

TRADE_ANALYSIS_TYPE = os.getenv("TRADE_ANALYSIS_TYPE", TradeAnalysisType.NORMAL)

def setup_logger(name: str, log_file: str, level=logging.INFO):
    """
    Sets up a logger with the specified name, log file, and level.

    Args:
        name (str): Name of the logger.
        log_file (str): Path to the log file.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a file handler for logging to a file
    date_str = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists("logs") is False:
        os.makedirs("logs")

    log_path = f"logs/{log_file}_{date_str}_{TRADE_ANALYSIS_TYPE}.log"
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(level)

    # Create a console handler for logging to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # Define the format for logs
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
