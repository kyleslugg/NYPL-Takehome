import pathlib
from constants import PROJECT_ROOT_DIRECTORY, CASE_311_ID, FILM_PERMITS_ID, NYCODP_BASE_URL, APP_CREDENTIALS
from socrata_operations import get_socrata, save_socrata


# Prepare for extraction: define options for Socrata when retrieving generators
AGENCIES_311 = ['NYPD', 'DOT', 'DSNY']
EXCLUDED_COMPLAINT_TYPES = ['Encampment', 'Panhandling',
                            'Bike/Roller/Skate Chronic', 'Snow or Ice', 'Drug Activity']
COLUMNS_311 = ['unique_key', 'created_date', 'closed_date', 'agency', 'agency_name', 'complaint_type',
               'descriptor', 'location_type', 'x_coordinate_state_plane', 'y_coordinate_state_plane', 'latitude', 'longitude']
COLUMNS_PERMITS = ['eventid', 'eventtype', 'startdatetime', 'enddatetime',
                   'enteredon', 'eventagency', 'parkingheld', 'borough', 'category', 'subcategoryname']

START_DATE = '2023-8-1'
END_DATE = '2023-10-31'
INTERMEDIARY_DB_PATH = pathlib.Path(
    PROJECT_ROOT_DIRECTORY, 'data/1_Preliminary_Data.db')
INTERSECTION_MAPPER_PATH = pathlib.Path(PROJECT_ROOT_DIRECTORY, 'data/intersection_mapper.geojson')


def extract_socrata(refresh=False):

    # Assume that intermediary DB is completely populated on each run
    # Check to see if intermediary DB exists (or we want to refresh data)
    if refresh or not INTERMEDIARY_DB_PATH.exists():

        # Try to remove DB, to handle refresh case
        INTERMEDIARY_DB_PATH.unlink(missing_ok=True)

        # Film Permits Generator
        film_permits_gen = get_socrata(
            base_url=NYCODP_BASE_URL,
            resource_id=FILM_PERMITS_ID,
            api_credentials=APP_CREDENTIALS,
            query_params={
                '$select': f'''{', '.join(COLUMNS_PERMITS)}''',
                '$where': f'''date_trunc_ymd(startdatetime) >= '{START_DATE}'
                              AND date_trunc_ymd(enddatetime) <= '{END_DATE}' '''})

        # 311 Data Generator
        cases_311_gen = get_socrata(
            base_url=NYCODP_BASE_URL,
            resource_id=CASE_311_ID,
            api_credentials=APP_CREDENTIALS,
            page_size=8000,
            query_params={
                '$select': f'''{', '.join(COLUMNS_311)}''',
                '$where': f'''date_trunc_ymd(created_date) >= '{START_DATE}'
                              AND date_trunc_ymd(created_date) <= '{END_DATE}'
                              AND agency in({', '.join(["'"+ agency + "'" for agency in AGENCIES_311])})
                              AND complaint_type NOT in({', '.join(["'"+t+"'" for t in EXCLUDED_COMPLAINT_TYPES])})'''})

        # Extract and save
        save_socrata(film_permits_gen, db_path=pathlib.Path(
            PROJECT_ROOT_DIRECTORY, 'data/1_Preliminary_Data.db'), table_name='permits')

        save_socrata(cases_311_gen, db_path=pathlib.Path(
            PROJECT_ROOT_DIRECTORY, 'data/1_Preliminary_Data.db'), table_name='311')

def prepare_intersection_mapper(refresh=False):
  if refresh or not INTERSECTION_MAPPER_PATH.exists():


def transform():
    pass


if __name__ == "__main__":
    extract(refresh=True)
