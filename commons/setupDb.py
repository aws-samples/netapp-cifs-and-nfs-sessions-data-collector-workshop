import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])

from commons.database import pgDb

import traceback
import psycopg2


def create_tables(db):
    conn, cursor = pgDb.get_db_cursor(db=db)

    # Create a table to store NFS and CIFS sessions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS  public.sessions (
            timestamp timestamp,
            storage TEXT NOT NULL,
            vserver TEXT NOT NULL,
            lifaddress TEXT NOT NULL,
            server TEXT NOT NULL,
            volume TEXT NOT NULL,
            username TEXT NOT NULL,
            protocol TEXT NOT NULL
        );

        CREATE TABLE public.storageconfigs (
            storagename varchar NOT NULL,
            storageip varchar NOT NULL,
            storageuser varchar NOT NULL,
            storagepassword bytea NOT NULL,
            CONSTRAINT storageconfigs_pk PRIMARY KEY (storagename)
        );
                   
    """)
    conn.commit()


if __name__ == '__main__':
    db = {}
    db['db_host'] = os.environ['POSTGRES_HOSTNAME']
    db['db_port'] = os.environ['POSTGRES_PORT']
    db['db_name'] = os.environ['POSTGRES_DATABASE']
    db['db_user'] = os.environ['POSTGRES_USER']
    db['db_password'] = os.environ['POSTGRES_PASSWORD']

    try:
        create_tables(db)
        print('Database tables created.')
    except psycopg2.errors.DuplicateTable:
        print('Tables already exists. No action needed.')
    except Exception as e:
        print(e)
        traceback.print_exc()
        print('Failed to create tables.')
