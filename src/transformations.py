from shapely import MultiPoint, centroid
import geopandas as gpd
import pandas as pd
import pathlib
import re
import sqlite3 as db
from itertools import permutations
from socrata_operations import SocrataLoader
from tqdm import tqdm
tqdm.pandas()

#######################
#                     #
# Load data from disk #
#                     #
#######################


def load_311(loader: SocrataLoader):
    print("Loading 311 data...")
    cases = loader.load_dataset('311')
    cases['date'] = pd.to_datetime(
        cases['created_date']).apply(lambda x: x.date())
    cases = gpd.GeoDataFrame(cases, geometry=gpd.points_from_xy(
        cases['x_coordinate_state_plane'], cases['y_coordinate_state_plane'], crs='2263'))
    cases = cases[cases['geometry'].is_valid]
    cases['geometry'] = cases['geometry'].buffer(200)
    print("Loaded 311 data.")
    return cases


def load_permits(loader: SocrataLoader):
    print("Loading permits data...")
    permits = loader.load_dataset('permits')
    permits['startdatetime'] = pd.to_datetime(permits['startdatetime'])
    permits['enddatetime'] = pd.to_datetime(permits['enddatetime'])
    permits['parkingheld'] = permits['parkingheld'].apply(
        lambda x: re.sub('\s+', ' ', x))
    permits['parkingheld'] = permits['parkingheld'].apply(
        lambda x: [st.strip() for st in x.split(',')])
    print("Loaded permits data.")
    return permits


def load_tax_blocks(tax_block_path: pathlib.Path):
    tb = gpd.read_file(tax_block_path)
    print(f"Loaded tax blocks with crs {tb.crs}")
    return tb


def load_intersection_mapper(intersection_mapper_path: pathlib.Path, intersection_locations: pathlib.Path, refresh=False):
    print("Loading intersection mapper...")
    pairs_to_points = None

    if refresh or not intersection_mapper_path.exists():
        print("Creating intersection mapper...")
        node_street_pairs = gpd.GeoDataFrame.from_file(
            intersection_locations, crs='2263')
        node_street_pairs['street_combos'] = node_street_pairs['streets'].progress_apply(
            lambda streets: [[p[0], p[1]] for p in permutations(streets, 2)])
        pairs_to_points = node_street_pairs.explode(
            'street_combos')[['street_combos', 'geometry']]
        pairs_to_points.to_file(intersection_mapper_path,
                                driver='GeoJSON', index=False, engine='pyogrio')
        print("Intersection mapper saved to disk.")

    if pairs_to_points is None:
        print("Loading intersection mapper from disk.")
        pairs_to_points = gpd.GeoDataFrame.from_file(
            intersection_mapper_path, crs='2263')
    print("Loaded intersection mapper.")

    pairs_to_points = pairs_to_points[pairs_to_points['geometry'].is_valid]
    pairs_to_points['street_combos'] = pairs_to_points['street_combos'].apply(
        lambda x: tuple(x))

    return pairs_to_points


#########################
#                       #
# Geodata Manipulations #
#                       #
#########################
def parking_loc_to_intersections(row):
    parking_loc = row.loc['parkingheld']
    [main_st, side_streets] = [st.strip()
                               for st in parking_loc.split('between')]
    [ss_from, ss_to] = [st.strip() for st in side_streets.split('and')]
    row['from_intersection'], row['to_intersection'] = (
        main_st, ss_from), (main_st, ss_to)
    return row


def find_intersection_midpoint(row, locator_df, start_int_col='from_intersection', end_int_col='to_intersection', locator_geometry_col='geometry', locator_street_col='street_combos'):
    try:
        start_geom = locator_df[locator_df[locator_street_col]
                                == row[start_int_col]].iloc[0][locator_geometry_col]
        end_geom = locator_df[locator_df[locator_street_col]
                              == row[end_int_col]].iloc[0][locator_geometry_col]
        return centroid(MultiPoint([start_geom, end_geom]))
    except:
        return None


def get_shooting_days_locations(permits: pd.DataFrame, intersection_point_mapper: gpd.GeoDataFrame):

    # Match permits to locations
    locs = permits[['eventid', 'startdatetime', 'enddatetime', 'borough', 'parkingheld']
                   ][permits['parkingheld'].apply(lambda x: len(x) > 1)].explode('parkingheld')

    print("Geolocating film parking blocks...")
    locs = locs.apply(lambda row: parking_loc_to_intersections(row), axis=1)
    locs['midpoint'] = locs.progress_apply(
        lambda row: find_intersection_midpoint(row, intersection_point_mapper), axis=1)

    print("Calculating filming days...")
    # Calculate relevant date range
    locs['date'] = locs.progress_apply(lambda row: pd.date_range(
        row['startdatetime'].date(), row['enddatetime'].date(), freq='D', inclusive='both'), axis=1)

    print("Buffering block midpoints...")
    # Convert to GeoDataFrame and create midpoint buffers
    locs = gpd.GeoDataFrame(locs, geometry='midpoint', crs='2263')
    locs = locs[locs['midpoint'].is_valid]
    locs['midpoint'] = locs['midpoint'].buffer(200)

    # Generate day-location matrix
    days_locations = locs.explode('date')[
        ['midpoint', 'date']]
    return days_locations


def tax_block_date_matrix(tax_block_gdf: gpd.GeoDataFrame, tax_block_ids, tax_block_keep_fields, start_date, end_date, date_column):
    # Borough-Block-Date Combinations
    print("Creating tax block/date matrix...")
    bb = tax_block_gdf[tax_block_ids +
                       tax_block_keep_fields].groupby(tax_block_ids).first().reset_index()
    bb[date_column] = [pd.date_range(start_date, end_date, freq='D')
                       for _n in range(len(bb))]
    return bb.explode('date').set_index(tax_block_ids+[date_column])


def points_by_day_tax_block(point_gdf: gpd.GeoDataFrame, tax_block_gdf: gpd.GeoDataFrame, tax_block_date_matrix: pd.DataFrame, tax_block_ids, point_date_field, point_count_field_id):
    print(f"Tabulating {point_count_field_id} by tax block and day...")
    # Spatially join tax blocks and points
    joined = tax_block_gdf.sjoin(point_gdf, how='left')
    block_day_counts = joined.groupby(
        tax_block_ids+[point_date_field]).count().iloc[:, :1]
    block_day_counts.columns = [point_count_field_id]

    # Join to full slate of combinations
    return tax_block_date_matrix.join(block_day_counts, how='left')
