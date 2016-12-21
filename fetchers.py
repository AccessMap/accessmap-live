import datetime
import geopandas as gpd
import requests
from shapely import geometry, wkt
import sqlalchemy as sa
import geoalchemy2 as ga

from db import engine


def construction():
    #
    # Fetch data from the 'street use permits by use' and 'street use permits
    # by impact' datasets
    #

    print('Fetching construction data...')
    use_url = 'https://data.seattle.gov/resource/hyub-wfuv.json'
    impact_url = 'https://data.seattle.gov/resource/brf3-mqwc.json'

    today = datetime.datetime.now().isoformat()

    starts_before = 'sdwlk_close_start_dt <= \'{}\''.format(today)
    ends_after = 'sdwlk_close_end_dt >= \'{}\''.format(today)
    where = [starts_before, ends_after]

    starts_exists = 'sdwlk_close_start_dt IS NOT NULL'
    ends_exists = 'sdwlk_close_end_dt IS NOT NULL'
    having = [starts_exists, ends_exists]

    # TODO: see if there are any other useful ways to trim the data in the
    # GET request itself
    params = {
        '$where': ' AND '.join(where),
        '$having': ' AND '.join(having),
        '$limit': '10000'
    }
    print('    Getting street use permits by use')
    use_response = requests.get(use_url, params=params)

    print('    Getting street use permits by impact')
    impact_response = requests.get(impact_url, params=params)

    failed = False
    # FIXME: should log/alert when this doesn't work
    if use_response.status_code != requests.codes.ok:
        failed = True
        print('failed  to download "by use" permits')

    if impact_response.status_code != requests.codes.ok:
        failed = True
        print('failed  to download "by impact" permits')

    if failed:
        return

    print('        Done')

    #
    # The data has been downloaded in JSON format - process into geopandas
    #

    print('    Processing data...')

    use_json = use_response.json()
    for row in use_json:
        lon, lat = row['shape']['longitude'], row['shape']['latitude']
        # geometry is reported as string, so will construct from WKT
        row['geom'] = wkt.loads('POINT ({} {})'.format(lon, lat))

    use = gpd.GeoDataFrame(use_json)
    use.crs = {'init': 'epsg:4326'}

    impact_json = impact_response.json()
    for row in impact_json:
        path = row['shape']['geometry']['paths'][0]
        row['geom'] = geometry.LineString(path)

    impact = gpd.GeoDataFrame(impact_json, geometry='geom')
    impact.crs = {'init': 'epsg:4326'}

    # Create the 'address-to-street' lines
    impact['wkt'] = impact.geometry.apply(lambda x: x.wkt)
    to_keep = impact.loc[:, ['permit_no_num', 'wkt']].drop_duplicates().index
    unique = impact.loc[to_keep]
    unique.drop('wkt', axis=1)

    unique['add_geom'] = gpd.GeoSeries([None] * unique.shape[0],
                                       crs={'init': 'epsg:4326'})

    unique['st_geom'] = gpd.GeoSeries(unique['geom'],
                                      crs={'init': 'epsg:4326'})

    for i, row in unique.iterrows():
        permit_no = row['permit_no_num']

        # Sometimes the two permits datasets don't line up - ignore them
        try:
            add_geom = use[use['permit_no_num'] == permit_no].iloc[0]['geom']
            unique.loc[i, 'add_geom'] = add_geom
        except:
            pass

    unique = unique[~unique['add_geom'].isnull()]

    # Reproject so distances are in meters
    add_geom = gpd.GeoSeries(unique['add_geom'])
    add_geom.crs = {'init': 'epsg:4326'}
    unique['add_geom'] = add_geom.to_crs(epsg=26910)
    st_geom = gpd.GeoSeries(unique['st_geom'])
    st_geom.crs = {'init': 'epsg:4326'}
    unique['st_geom'] = st_geom.to_crs(epsg=26910)

    # Calculate the line between the address and street
    unique['permit_geom'] = None
    unique.astype('object')
    unique['line_between'] = None
    unique.astype('object')

    # Connect to the database - we'll need to talk to the sidewalks table
    sql = '''
    SELECT *
      FROM sidewalks
    '''

    # TODO: this part is slow - reading the whole sidewalks table into memory
    # the data processing operations after this point could be done entirely in
    # SQL
    sidewalks = gpd.GeoDataFrame.from_postgis(sql, engine.raw_connection(),
                                              crs={'init': 'epsg:4326'})
    sidewalks = sidewalks.to_crs(epsg=26910)

    # sidewalks = gpd.GeoDataFrame.from_postgis(:qw
    # Note: by referring to .sindex, the spatial index is created
    sindex = sidewalks.sindex
    for i, permit in unique.iterrows():
        add = permit['add_geom']
        st = permit['st_geom']
        st_nearpoint = st.interpolate(st.project(add))
        line_between = geometry.LineString(list(add.coords) +
                                           list(st_nearpoint.coords))

        # FIXME: some of the streets seem to be in the wrong
        # location, resulting in (incorrect) long lines.

        # Step 1: Skip ultra-distant street-address lines
        if line_between.length > 200:
            continue

        # Get the spatial index results (an iterable)
        options = sindex.intersection(line_between.bounds, objects=True)
        # Use their index references to get the actual sidewalk rows
        sw_options = sidewalks.loc[[option.object for option in options]]

        # Do a true intersection test (not bounding boxes), remove those that
        # don't intersect
        points = sw_options.intersection(line_between)
        points = points[~points.isnull()]

        # Step 2: Ignore case where multiple intersection points are found
        # FIXME: this shouldn't happen - suggests an error in permit data
        if points.shape[0] > 1:
            continue

        if not points.empty:
            unique.loc[i, 'line_between'] = line_between
            unique.loc[i, 'permit_geom'] = points.iloc[0]

    unique = unique[~unique['permit_geom'].isnull()]

    # Create a simpler (subsetted) table for writing to the database
    permits_df = unique[['permit_address_text', 'sdwlk_closed_flag',
                         'sdwlk_close_start_dt', 'sdwlk_close_end_dt',
                         'permit_geom']]
    permits_df = permits_df.rename(columns={
        'permit_address_text': 'address',
        'sdwlk_closed_flag': 'closed',
        'sdwlk_close_start_dt': 'start_date',
        'sdwlk_close_end_dt': 'end_date',
        'permit_geom': 'geom'
    })

    #
    # Ensure correct data types.
    #

    # For some reason, the dates are queries on socrata using ISO8601 format
    # and returned as unix timestamp
    def timestamp_date(timestamp):
        timestamp = int(timestamp)
        return datetime.datetime.fromtimestamp(timestamp).date().isoformat()

    permits_df['start_date'] = permits_df['start_date'].apply(timestamp_date)
    permits_df['end_date'] = permits_df['end_date'].apply(timestamp_date)

    # Geometry needs to be latlon EWKT for writing to database
    geom = gpd.GeoSeries(permits_df['geom'])
    geom.crs = {'init': 'epsg:26910'}
    geom = geom.to_crs(epsg=4326)
    permits_df['geom'] = geom.apply(lambda x: 'SRID=4326;' + wkt.dumps(x))

    #
    # Update the database
    #

    print('    Updating database...')
    # Put all of the commands in a single transaction - ensures we don't
    # destroy data when there's a buggy commit
    with engine.begin() as conn:
        try:
            # Define the construction table
            meta = sa.MetaData()
            construction_t = sa.Table('construction', meta,  # noqa: E128
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('geom', ga.Geometry('Point', srid=4326)),
                sa.Column('address', sa.String),
                sa.Column('start_date', sa.Date),
                sa.Column('end_date', sa.Date),
                sa.Column('closed', sa.String)
            )

            # Drop the construction table if it exists
            construction_t.drop(conn, checkfirst=True)

            # Create the construction table
            # TODO: we should eventually come up with a consistent schema and
            # use migrations
            construction_t.create(conn)

            # Populate the construction table with our processed data
            permits_df.reset_index(inplace=True, drop=True)
            permits_df_list = permits_df.to_dict(orient='records')
            conn.execute(construction_t.insert(), permits_df_list)
            print('        Success')
        # except:
        except Exception as e:
            raise e
            print('    Failed')

        print('    Done')
