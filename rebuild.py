from db import engine


def sidewalks():
    # Update construction info from the construction table
    # TODO: sqlalchemy-ify this?
    print('Updating sidewalks table...')

    with engine.connect() as conn:
        has_column = conn.execute("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_name='sidewalks'
           AND column_name='construction'
        """)

        if not has_column.first():
            conn.execute('''
            ALTER TABLE sidewalks
             ADD COLUMN construction boolean
                DEFAULT FALSE
            ''')

    # Note: checking within 1e-6 degrees, which is ~10 cm
    # This method is used (rather than reprojecting) because it's fast, at the
    # expense of having non-equal lat/lon units.
    with engine.begin() as conn:
        conn.execute('''
        UPDATE sidewalks s
           SET construction=TRUE
          FROM (SELECT s2.gid
                  FROM construction c
                  JOIN sidewalks s2
                    ON ST_DWithin(c.geom, s2.geom, 0.000001)) q
         WHERE q.gid = s.gid
        ''')

    print('Done')


def routing():
    # Rebuild the routing table

    # Process sidewalks + crossings into single table, sets up pgrouting
    # columns

    # Sets up pgrouting vertices table

    pass
