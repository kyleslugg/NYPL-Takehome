# Camera-Shy: Proxying disruptions from filming activity with 311 case data

At best, having your block cordoned off for filming is an exciting facet of life in a particularly telegenic city (watch for your neighborhood on a screen near you!). At worst, it's simply a disruption: sidewalks are blocked, cars can't pass through, generators pump out noise and exhaust -- and residents look for someone to complain to.

### Data Sources

Enter 311, the all-purpose, multichannel portal to city services, and the receptacle of decades' worth of New Yorkers' annoyance. In this analysis, I draw upon two public datasets -- OTI's repository of [311 service requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) and a set of [film permits data](https://data.cityofnewyork.us/City-Government/Film-Permits/tg4x-b46p/about_data) from the Mayor's Office of Media and Entertainment -- to measure how sensitive various portions of the city are to filming-related disruptions.

### Supplementary Geodata

In addition, two static geodatasets are used to locate and group points in these datasets:

- DCP's [LION](https://data.cityofnewyork.us/City-Government/LION/2v4z-66xt) basemap, which was loaded into PostGIS and used to match street intersections to geographic locations (see `transform_lion_lines.sql` for the script used to obtain these datapoints once the underlying dataset had been imported using `ogr2ogr`)
- DOF's [Digital Tax Map](https://data.cityofnewyork.us/Housing-Development/Department-of-Finance-Digital-Tax-Map/smk3-tmxj), which contains, among many other data, polygons representing each NYC tax block, used here as the basic unit of analysis. (This dataset was further enriched with community districts, for follow-up analysis.)

## Analytical Workflow

At a high level, the analytical flow is as follows:

- Retrieve excerpts of both datasets for a specified time window;
- Using geodata derived from DCP's [LION](https://data.cityofnewyork.us/City-Government/LION/2v4z-66xt) basemap, map the street intersections used on film permits to precise locations;
- Map both daily 311 complaints and an indicator of daily filming activity onto tax blocks using the ; and
- Measure deviations from baseline 311 call activity (by blocks and aggregates thereof) at times when filming is active (after the fact).

This specific workflow:

- Defines generators over paginated Socrata datasets, enabling retrieval of the full dataset (or a relevant subset, defined by SoQL) in a controlled manner
- Retrieves these datasets, saving them to an intermediary SQLite database
- Loads into memory (using a `SocrataLoader` helper class) both of the above datasets, alongside supplementary geodata
- Geolocates each block held for parking in the film permits dataset, and creates a separate entry for each day of filming. (311 cases already contain geographic locations, which are checked for validity)
- Creates 200-foot buffers around locations in both datasets
- Using GeoPandas, joins both 311 cases and film permits (by their buffers) to tax blocks
- Produces counts of 311 cases and film permits by day and tax block
- Writes the resulting table to a SQLite database for further analysis (found here in `data/points_by_tax_block.db` in the table `points_by_block`)

In the interest of complying with task instructions vis a vis time and level of detail, I have left the comparison of 311 activity among blocks with and without filming activity as an exercise to the reader (at least, for the time being). Note that, for purposes of completeness, many entries contain zeroes in the `permits` and/or `cases_311` field(s); these would wash out in further aggregation during follow-up analysis.

## Notes

Before running this workflow, ensure that all Python packages specified in `requirements.txt` have been installed. You may also need to install the appropriate GDAL development packages, if they are not present on your system.
