from db import engine


NODE_DIST = 0.0000001  # Nodes within ~10 cm will be joined for routing


def sidewalks():
    # Update construction info from the construction table
    # FIXME: this should be handled by proper schema definitions and migrations
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
                    ON ST_DWithin(c.geom, s2.geom, {})
                 WHERE c.start_date <= current_timestamp
                   AND c.end_date >= current_timestamp) q
         WHERE q.gid = s.gid
        '''.format(NODE_DIST))

    print('Done')


def routing():
    '''Rebuilds the routing table.'''

    # Process sidewalks + crossings into single table, sets up pgrouting
    # columns
    print('Updating routing table...')

    with engine.begin() as conn:
        try:
            # If the routing table doesn't have a construction column, add it
            # FIXME: this should be handled by a schema/migration?
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

            print('    Creating node network...')
            # Create node network table
            conn.execute('''
            SELECT pgr_nodeNetwork('routing', {}, 'id', 'geom', 'noded_new');
            '''.format(NODE_DIST))

            print('    Updating node network metadata...')
            # Add metadata do node network table
            conn.execute('''
            ALTER TABLE routing_noded_new
             ADD COLUMN length numeric(6, 2) DEFAULT 0.0,
             ADD COLUMN grade numeric(6, 4) DEFAULT 0.0,
             ADD COLUMN curbramps boolean DEFAULT FALSE,
             ADD COLUMN iscrossing boolean DEFAULT FALSE,
             ADD COLUMN construction boolean DEFAULT FALSE;
            ''')

            conn.execute('''
            UPDATE routing_noded_new rn
               SET length = ST_Length(rn.geom::geography),
                   grade = r.grade,
                   curbramps = r.curbramps,
                   iscrossing = r.iscrossing::boolean
              FROM routing r
             WHERE rn.old_id = r.id
            ''')

            conn.execute('''
            UPDATE routing_noded_new rn
               SET construction = TRUE
              FROM construction c
             WHERE ST_DWithin(rn.geom, c.geom, {})
               AND start_date <= current_timestamp
               AND end_date >= current_timestamp
            '''.format(NODE_DIST))

            print('    Recreating routing graph...')
            # Set up pgrouting vertices table
            conn.execute('''
            SELECT pgr_createTopology('routing_noded_new', {}, 'geom', 'id');
            '''.format(NODE_DIST))

            # Rename tables - faster to rename than drop/replace
            print('    Renaming tables...')

            tables = ['routing_noded{}', 'routing_noded{}_vertices_pgr',
                      'routing_noded{}_vertices_pgr', 'routing_noded{}_id_seq',
                      'routing_noded{}_vertices_pgr_id_seq']

            idxs = ['routing_noded{}_pkey', 'routing_noded{}_geom_idx',
                    'routing_noded{}_source_idx', 'routing_noded{}_target_idx',
                    'routing_noded{}_vertices_pgr_pkey',
                    'routing_noded{}_vertices_pgr_the_geom_idx']

            rename_t = 'ALTER TABLE IF EXISTS {} RENAME TO {};'
            rename_i = 'ALTER INDEX IF EXISTS {} RENAME TO {};'
            drop_t = 'DROP TABLE IF EXISTS {};'
            drop_i = 'DROP INDEX IF EXISTS {};'

            sql_list = []
            for t in tables:
                orig = t.format('')
                old = t.format('_old')
                sql_list.append(rename_t.format(orig, old))

            for i in idxs:
                orig = i.format('')
                old = i.format('_old')
                sql_list.append(rename_i.format(orig, old))

            for t in tables:
                new = t.format('_new')
                orig = t.format('')
                sql_list.append(rename_t.format(new, orig))

            for i in idxs:
                new = i.format('_new')
                orig = i.format('')
                sql_list.append(rename_i.format(new, orig))

            for t in tables:
                sql_list.append(drop_t.format(t.format('_old')))

            for i in idxs:
                sql_list.append(drop_i.format(i.format('_old')))

            rename_tables = '\n\n'.join(sql_list)

            conn.execute(rename_tables)
        except Exception as e:
            raise e
            print('    Failed')

    print('Done')
