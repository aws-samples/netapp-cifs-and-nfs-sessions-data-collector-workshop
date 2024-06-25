import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])

from sqlalchemy import create_engine, Column, String, TIMESTAMP, Table, Index, MetaData, LargeBinary
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import ProgrammingError
from urllib.parse import quote_plus


# Create a base class for declarative models
Base = declarative_base()


# Class for sessions table with columns and their types as created in Postgres database.
class volSessions(Base):
    __tablename__ = 'sessions'
    timestamp = Column(TIMESTAMP(), primary_key=True)
    storage = Column(String())
    vserver = Column(String())
    lifaddress = Column(String())
    server = Column(String())
    volume = Column(String())
    username = Column(String())
    protocol = Column(String())


# Class for storageconfigs table with columns and their types as created in Postgres database.
class storage(Base):
    __tablename__ = 'storageconfigs'
    storagename = Column(String(), primary_key=True)
    storageip = Column(String())
    storageuser = Column(String())
    storagepassword = Column(String())


def create_tables(engine):
    try:
        Table(
            'storageconfigs', 
            MetaData(),
            Column('storagename', String()),
            Column('storageip', String()),
            Column('storageuser', String()),
            Column('storagepassword', LargeBinary)
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table storageconfigs already exists. No action needed.")
            

    try:
        Table(
            'sessions', 
            MetaData(),
            Column('timestamp', TIMESTAMP),
            Column('storage', String()),
            Column('vserver', String()),
            Column('lifaddress', String()),
            Column('server', String()),
            Column('volume', String()),
            Column('username', String()),
            Column('protocol', String())
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table sessions already exists. No action needed.")


def create_indexes(engine, volSessions):
    idx_servers = Index('idx_servers', volSessions.server)
    idx_volumes = Index('idx_volumes', volSessions.volume)
    idx_usernames = Index('idx_usernames', volSessions.username)
    idx_srv_vol_user = Index('idx_srv_vol_user', volSessions.server, volSessions.volume, volSessions.username)

    try:
        idx_servers.create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Index idx_servers already exists. No action needed.")

    try:
        idx_volumes.create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Index idx_volumes already exists. No action needed.")

    try:
        idx_usernames.create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            raise
        else:
            print("Index idx_usernames already exists. No action needed.")

    try:
        idx_srv_vol_user.create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            raise
        else:
            print("Index idx_srv_vol_user already exists. No action needed.")


if __name__ == '__main__':
    db = {
        'db_host':os.environ['POSTGRES_HOSTNAME'],
        'db_port':os.environ['POSTGRES_PORT'],
        'db_name':os.environ['POSTGRES_DATABASE'],
        'db_user':os.environ['POSTGRES_USER'],
        'db_password':os.environ['POSTGRES_PASSWORD']
    }
    password = quote_plus(db['db_password'])
    engine = create_engine(f"postgresql://{db['db_user']}:{password}@{db['db_host']}/{db['db_name']}")

    # Create a session
    session = Session(engine)

    create_tables(engine)
    create_indexes(engine, volSessions)
