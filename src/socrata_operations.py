import requests
import pandas as pd
import sqlite3 as db
from requests.auth import HTTPBasicAuth


def get_socrata(base_url, resource_id, api_credentials=None, query_params=None, page_size=1000, page_offset=0, max_pages=None, headers=None):

    # Endpoint URL
    url = f"{base_url}/{resource_id}.json"

    # Choose whether to include basic authentication
    basic_auth = HTTPBasicAuth(
        **api_credentials) if api_credentials is not None else None

    # Assign empty dictionary if query_params is None
    if query_params is None:
        query_params = {}

    # Assign empty dictionary if headers is None
    if headers is None:
        headers = {}

    while max_pages is None or page_offset/page_size < max_pages:
        response = requests.get(url, params=dict(
            {'$limit': page_size, '$offset': page_offset, '$order': ':id'}, **query_params), headers=headers, auth=basic_auth)

        # Raise exception if request is unsuccessful
        if response.status_code != 200:
            response.raise_for_status()

        result = response.json()

        if not result:
            print("Encountered null result. Exiting...")
            return

        yield result

        if len(result) < page_size:
            print("Completed pull. Exiting...")
            return

        page_offset += page_size
        print(f"Page offset is now {page_offset}", end='\r', flush=True)


def save_socrata(data, db_path, table_name, preliminary_transform=None):
    # Apply data transformations if specified
    with db.connect(db_path) as conn:
        for chunk in data:
            df = pd.DataFrame.from_dict(chunk)
            if preliminary_transform:
                df = preliminary_transform(df)
            df.to_sql(table_name, conn, if_exists='append')


class SocrataLoader:
    def __init__(self, db_path):
        self.path = db_path

    def load_dataset(self, tablename):
        with db.connect(self.path) as conn:
            df = pd.read_sql_query(f'select * from "{tablename}"', conn)
        return df
