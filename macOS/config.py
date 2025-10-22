# config.py
from pathlib import Path
from dotenv import load_dotenv
import os, re, ast

# Always resolve path so it works no matter where you run from
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)  # set override=True if you want .env to win over OS env


ES_URL = (os.getenv("ES_URL"))
SOURCE_INDEX = os.getenv("SOURCE_INDEX")
API_KEY_B64 = os.getenv("API_KEY_B64")
DEST_INDEX = os.getenv("DEST_INDEX")