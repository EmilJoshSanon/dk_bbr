# Packages

import dotenv
import os

dotenv.load_dotenv()
POSTGRES_CONNECTION_STRING = os.getenv("POSTGRES_CONNECTION_STRING")
API_KEY = os.getenv("API_KEY")
