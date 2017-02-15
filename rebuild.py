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
    '''Rebuilds the routing table.'''

    # Process sidewalks + crossings into single table, sets up pgrouting
    # columns
    print('Updating routing table...')

    with engine.begin() as conn:
        try:
            # If the routing table doesn't have a construction column, add it
            has_column = conn.execute("""
            SELECT column_name
              FROM information_schema.columns
             WHERE table_name='routing'
               AND column_name='construction'
            """)

            if not has_column.first():
                conn.execute('''
                ALTER TABLE routing
                 ADD COLUMN construction boolean
                    DEFAULT FALSE
                ''')

            # Update routing table based on sidewalks table
            conn.execute('''
            UPDATE routing r
               SET construction=s.construction
              FROM sidewalks s
             WHERE NOT r.iscrossing
               AND r.o_id = s.gid
            ''')

            # Update routing table based on crossings table
            conn.execute('''
            UPDATE routing r
               SET curbramps = c.curbramps
              FROM crossings c
             WHERE r.iscrossing
               AND r.o_id = c.gid
            ''')

            # Create node network table
            print('Creating node network...')
            conn.execute('''
            DROP TABLE IF EXISTS routing_noded;
            SELECT pgr_nodeNetwork('routing', 0.000001, 'id', 'geom');
            ''')

            # Add metadata do node network table
            print('Updating node network metadata...')
            conn.execute('''
            ALTER TABLE routing_noded
             ADD COLUMN length numeric(6, 2) DEFAULT 0.0;
            ALTER TABLE routing_noded
             ADD COLUMN grade numeric(6, 4) DEFAULT 0.0;
            ALTER TABLE routing_noded
             ADD COLUMN curbramps boolean DEFAULT FALSE;
            ALTER TABLE routing_noded
             ADD COLUMN iscrossing boolean DEFAULT FALSE;
            ALTER TABLE routing_noded
             ADD COLUMN construction boolean DEFAULT FALSE;
            ''')

            conn.execute('''
            UPDATE routing_noded rn
               SET length = ST_Length(rn.geom::geography),
                   grade = r.grade,
                   curbramps = r.curbramps,
                   iscrossing = r.iscrossing::boolean
              FROM routing r
             WHERE rn.old_id = r.id
            ''')

            conn.execute('''
            UPDATE routing_noded n
               SET construction = TRUE
              FROM construction c
             WHERE ST_DWithin(n.geom, c.geom, 0.000001)
            ''')

            # Set up pgrouting vertices table
            conn.execute('''
            SELECT pgr_createTopology('routing_noded', 0.000001, 'geom', 'id');
            ''')
        except Exception as e:
            raise e
            print('    Failed')

    print('Done')
