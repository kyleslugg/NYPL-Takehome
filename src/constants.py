
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ROOT_DIRECTORY = os.path.dirname(os.path.dirname(__file__))

# Socrata API Endpoints via NYC Open Data
NYCODP_BASE_URL = "https://data.cityofnewyork.us/resource"
CASE_311_ID = 'erm2-nwe9'
FILM_PERMITS_ID = 'tg4x-b46p'

# Get API credentials
un = os.environ.get('API_KEY')
pw = os.environ.get('API_SECRET')

APP_CREDENTIALS = {'username': un, 'password': pw} if (un & pw) else None
