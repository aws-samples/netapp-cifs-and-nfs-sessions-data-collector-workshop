import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])

from sqlalchemy import create_engine, Column, String, TIMESTAMP, Table, Index, MetaData, LargeBinary, Boolean, select
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import ProgrammingError, IntegrityError
from urllib.parse import quote_plus
from commons.encryptionKey import encryptionKey

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


class User(Base):
    __tablename__ = 'users'
    username = Column(String, primary_key=True)
    password = Column(String)


<<<<<<< HEAD
class Servers(Base):
    __tablename__ = 'servers'
    serverip = Column(String, primary_key=True)
    servername = Column(String)


class SessionUsers(Base):
    __tablename__ = 'sessionusers'
    username = Column(String, primary_key=True)
    userprotocol = Column(String)


=======
>>>>>>> origin/main
def create_tables(engine):
    try:
        Table(
            'users', 
            MetaData(),
            Column('username', String()),
            Column('password', LargeBinary),
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table users already exists. No action needed.")

    try:
        Table(
<<<<<<< HEAD
            'servers', 
            MetaData(),
            Column('serverip', String()),
            Column('servername', String()),
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table servers already exists. No action needed.")

    try:
        Table(
            'sessionusers', 
            MetaData(),
            Column('username', String()),
            Column('userprotocol', String()),
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table sessionusers already exists. No action needed.")

    try:
        Table(
=======
>>>>>>> origin/main
            'storageconfigs', 
            MetaData(),
            Column('storagename', String()),
            Column('storageip', String()),
            Column('storageuser', String()),
            Column('storagepassword', LargeBinary),
            Column('collectdata', Boolean, default=True, nullable=False)
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table storageconfigs already exists. No action needed.")
            
<<<<<<< HEAD
            
    try:
        Table(
            'volumes', 
            MetaData(),
            Column('storage', String()),
            Column('vserver', String()),
            Column('volume', String()),
            Column('protocol', String())
        ).create(bind=engine)
    except ProgrammingError as e:
        if "already exists" not in str(e):
            print("Table volumes already exists. No action needed.")


=======
>>>>>>> origin/main
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


def create_decipher_user(username, password):
    fernet_key = encryptionKey.get_key()
    try:
        existing_user = session.query(User).filter(User.username == username).first()
        if existing_user is None:
            new_user = User(
                username=username, 
                password=fernet_key.encrypt(password.encode())
            )
            session.add(new_user)
            session.commit()
            return new_user, "User created successfully"
        else:
            return existing_user, "User already exists"            
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

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

    try:
        user, message = create_decipher_user(username='admin', password=os.environ['DECIPHER_ADMIN_PASSWORD'])
        user, message = create_decipher_user(username=os.environ['DECIPHER_USERNAME'], password=os.environ['DECIPHER_PASSWORD'])
        print(message)
    except Exception as e:
        print(f"Error: {e}")

