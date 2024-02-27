
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ROOT_DIRECTORY = os.path.dirname(os.path.dirname(__file__))

# Socrata API Endpoints via NYC Open Data
NYCODP_BASE_URL = "https://data.cityofnewyork.us/resource"
CASE_311_ID = 'erm2-nwe9'
FILM_PERMITS_ID = 'tg4x-b46p'


APP_CREDENTIALS = {'username': os.environ.get(
    'API_KEY'), 'password': os.environ.get('API_SECRET')}
