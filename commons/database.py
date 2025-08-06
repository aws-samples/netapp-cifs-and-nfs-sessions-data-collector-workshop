
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
            INSERT INTO public.storageconfigs (storagetype, storagename, storageip, storageuser, storagepassword, collectdata)
            VALUES (
                '{data['storage_type']}',
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

    def store_sessions(conn, cursor, data):
        for row in data:

            # Save data to sessions table
            cursor.execute(f"""
                INSERT INTO public.sessions (timestamp, storagetype, storage, vserver, lifaddress, server, volume, username, protocol)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                    datetime.strptime(row['Timestamp'], '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S'), 
                    row['StorageType'], 
                    row['Storage'], 
                    row['vserver'], 
                    row['lifaddress'], 
                    row['ServerIP'], 
                    row['Volume'], 
                    row['Username'], 
                    row['Protocol']
                )
            )
            # Save data to volumes table
            cursor.execute(f"""
                INSERT INTO public.volumes (storagetype, storage, vserver, volume, protocol)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (storagetype, storage, vserver, volume, protocol) 
                DO NOTHING
            """, (
                    row['StorageType'],
                    row['Storage'],
                    row['vserver'],
                    row['Volume'],
                    row['Protocol']
                )
            )

            # Save data to sessionusers table
            cursor.execute(f"""
                INSERT INTO public.sessionusers (username, userprotocol)
                VALUES (%s, %s)
                ON CONFLICT (username, userprotocol)
                DO NOTHING
            """, (
                    row['Username'],
                    row['Protocol']
                )
            )
            
            # Save data to servers table
            cursor.execute(f"""
                INSERT INTO public.servers (serverip, username)
                VALUES (%s, %s)
                ON CONFLICT (serverip, username)
                DO NOTHING
            """, (
                    row['ServerIP'],
                    row['Username']
                )
            )

            conn.commit()