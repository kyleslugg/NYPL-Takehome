import pathlib
from constants import PROJECT_ROOT_DIRECTORY, CASE_311_ID, FILM_PERMITS_ID, NYCODP_BASE_URL, APP_CREDENTIALS
from socrata_operations import get_socrata, save_socrata, save_processed_dataset, SocrataLoader
import transformations as tr

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
INTERMEDIARY_DB_PATH = pathlib.Path(PROJECT_ROOT_DIRECTORY, 'data/1_Preliminary_Data.db')
INTERSECTION_LOCATIONS = pathlib.Path(PROJECT_ROOT_DIRECTORY, 'data/node_street_pairs.geojson')
INTERSECTION_MAPPER_PATH = pathlib.Path(PROJECT_ROOT_DIRECTORY, 'data/intersection_mapper.geojson')
TAX_BLOCK_PATH = pathlib.Path(PROJECT_ROOT_DIRECTORY, 'data/tax_blocks.geojson')
OUTPUT_PATH = pathlib.Path(PROJECT_ROOT_DIRECTORY,'data/points_by_tax_block.db')

def extract_socrata(refresh=False):

    # Check to see if intermediary DB exists (or we want to refresh data)
    if refresh or not INTERMEDIARY_DB_PATH.exists():
        print("Retrieving data...")
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
        print("Saving film permits...")
        save_socrata(film_permits_gen, db_path=INTERMEDIARY_DB_PATH, table_name='permits')

        print("Saving 311 cases...")
        save_socrata(cases_311_gen, db_path=INTERMEDIARY_DB_PATH, table_name='311')
    else:
        print("Intermediary data already present. Moving on to transformation...")


def transform():
    # Get Socrata loader and load datasets
    loader = SocrataLoader(INTERMEDIARY_DB_PATH)
    cases_311 = tr.load_311(loader)
    film_permits = tr.load_permits(loader)

    # Load supplementary geodata
    intersection_mapper = tr.load_intersection_mapper(INTERSECTION_MAPPER_PATH, INTERSECTION_LOCATIONS)
    tax_blocks = tr.load_tax_blocks(TAX_BLOCK_PATH)
    tax_block_ids = ['BORO', 'BLOCK']
    minimal_tax_blocks = tax_blocks[tax_block_ids+['geometry']]

    # Fit film permits with location data
    shooting_days_locations = tr.get_shooting_days_locations(film_permits, intersection_mapper)

    # Group 311 cases and film permits by day and tax block
    bbd = tr.tax_block_date_matrix(tax_blocks, tax_block_ids, [
                                   'boro_cd'], START_DATE, END_DATE, 'date')
    bbd_permits = tr.points_by_day_tax_block(
        shooting_days_locations, minimal_tax_blocks, bbd, tax_block_ids, 'date', 'permits')
    bbd_permits_cases = tr.points_by_day_tax_block(
        cases_311, minimal_tax_blocks, bbd_permits, tax_block_ids, 'date', 'cases_311')

    # Fill NaN values for permit, case counts with zeroes, and drop null CDs
    bbd_permits_cases = bbd_permits_cases.dropna(subset=['boro_cd']).fillna(0)

    return bbd_permits_cases


if __name__ == "__main__":
    extract_socrata()
    transformed_data = transform()
    save_processed_dataset(transformed_data, OUTPUT_PATH, format='sqlite', table_name="points_by_block")