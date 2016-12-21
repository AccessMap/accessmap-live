from sqlalchemy import create_engine
import os


db_uri = os.environ['DATABASE_URL']
engine = create_engine(db_uri)
