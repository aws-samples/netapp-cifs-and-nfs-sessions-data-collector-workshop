
import psycopg2 as pg
from datetime import datetime


class pgDb:

    def get_db_cursor(db):
        # Connect to the database
        conn = pg.connect(
            database=db['db_name'],
            user=db['db_user'], 
            password=db['db_password'],
            host=db['db_host'], 
            port=db['db_port'],
        )

        # Create a cursor object
        cursor = conn.cursor()
        return (conn, cursor)


    def store_sessions(conn, cursor, data):
        for row in data:
            cursor.execute(f"""
                INSERT INTO public.sessions (timestamp, storage, vserver, lifaddress, server, volume, username, protocol)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (datetime.strptime(row['Timestamp'], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S'), row['Storage'], row['vserver'], row['lifaddress'], row['ServerIP'], row['Volume'], row['Username'], row['Protocol']))
            conn.commit()


    def store_storage_config(conn, cursor, data):
        """
        Stores Storage IP, Username and Password details.
        
        Passwords are encrypted using encryption key in the project folder "encryption.key"
        Default Data refresh interval to collect NFS and CIFS sessions data is 600seconds.

        Args:
            storage_name (str): Storage Name column is primary key and should be unique.
            storage_ip (str): IP address stored as String.
            storage_user (str): NetApp Username to login to storage using HTTPS.
            storage_password (str): Password encrypted and byte encoded using "encryption.key" in project folder.

        Returns:
            None
        """
        cursor.execute(f"""
            INSERT INTO public.storageconfigs (storagename, storageip, storageuser, storagepassword, collectdata)
            VALUES (
                '{data['storage_name']}', 
                '{data['storage_ip']}', 
                '{data['storage_user']}', 
                {pg.Binary(data['storage_password'])},
                {data['collectdata']}
            )
        """)
        conn.commit()


    def update_storage_collection(conn, cursor, data):
        query=f"""
            UPDATE 
                public.storageconfigs 
            SET 
                collectdata={data['collectdata']} 
            WHERE 
                storagename='{data['storagename']}';
        """
        print(query)
        cursor.execute(query)
        conn.commit()

