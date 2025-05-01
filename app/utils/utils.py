from datetime import datetime
import logging
import os

def get_accesstoken_filename():
    date = datetime.now().strftime('%d/%m/%Y').replace('/', '_')
    access_token_file = f"accessToken_{date}.txt"
    access_token_dir = "accessToken"
    return os.path.join(access_token_dir, access_token_file)

def get_logging_level() -> int:
    logging_level = logging.DEBUG if os.getenv('DEBUG', '') == 'True' else logging.INFO
    return logging_level