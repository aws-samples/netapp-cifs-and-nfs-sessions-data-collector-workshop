import pandas as pd 
import traceback

class stContainersDf:

    def get_configured_storage(cursor):
        try:
            cursor.execute("""
                SELECT * FROM storageconfigs s ORDER by s.storagename;
            """)
            storage_list = cursor.fetchall()
            storage_df = pd.DataFrame(storage_list, columns=['StorageType', 'Name', 'StorageIP', 'StorageUser', 'StoragePassEnc', 'CollectData'])
            return storage_df
        except Exception as e:
            return traceback.format_exc()


    def get_servers(cursor):
        cursor.execute("""
            select * from servers s;
        """)
        server_list = cursor.fetchall()
        server_list_df = pd.DataFrame(server_list, columns=['ServerIP', 'SessionUser'])
        return server_list_df


    def get_session_users(cursor):
        cursor.execute("""
            select * from sessionusers
        """)
        user_list = cursor.fetchall()
        user_list_df = pd.DataFrame(user_list, columns=['Username', 'Protocol'])
        return user_list_df


    def get_protocol_server_count(cursor):
        cursor.execute("""
            select protocol, count(distinct ("server")) from sessions group by protocol 
        """)        
        protocol_server_count = cursor.fetchall()
        protocol_server_count_df = pd.DataFrame(protocol_server_count, columns=['Protocol', 'ServerCount'])
        return protocol_server_count_df


    def get_protocol_volume_count(cursor):
        cursor.execute("""
            select protocol, count(distinct (volume)) from sessions group by protocol order by protocol 
        """)
        protocol_volume_count = cursor.fetchall()
        protocol_volume_count_df = pd.DataFrame(protocol_volume_count, columns=['Protocol', 'VolumeCount'])
        return protocol_volume_count_df


    def get_sessions_details(cursor):
        # Get the total number of sessions records stored.
        cursor.execute(f"""
            select max(timestamp) as timeLast, min(timestamp) as timeFirst, count(*) as count from sessions s
        """)
        sessions_count = cursor.fetchall()
        return sessions_count


    def yield_storage_sessions(cursor, limit, offset):
        # Fetch the limit number of rows with an offset
        # Increase the offset value by limit while returning the results
        # Return a tuple of DataFrame, limit and offset+limit
        cursor.execute(f"""
            select * from sessions order by (timestamp, storage, vserver, "server", volume, username) desc limit {limit} OFFSET {offset}
        """)
        storage_sessions = cursor.fetchall()
        
        while storage_sessions:
            yield pd.DataFrame(storage_sessions, columns=['Timestamp', 'StorageName', 'vserver', 'lifaddress', 'ServerIP', 'VolumeName', 'Username', 'Protocol'])
            storage_sessions = cursor.fetchall()


    def yield_volumes(cursor, limit, offset):
        vdl = []
        cursor.execute(f"""
            select storagetype, storage, vserver, volume, protocol from volumes order by protocol limit {limit} offset {offset}
        """)
        volumes = cursor.fetchall()
        volumes_df=pd.DataFrame(volumes, columns=["StorageType", "Storage", "vserver", "Volume", "Protocol"])

        while volumes:
            yield volumes_df
            volumes = cursor.fetchall()
            volumes_df=pd.DataFrame(volumes, columns=["StorageType", "Storage", "vserver", "Volume", "Protocol"])


    def get_all_volumes(cursor):
        cursor.execute(f"""
            select * from volumes order by protocol
        """)
        volume_list = cursor.fetchall()
        volume_list_df=pd.DataFrame(volume_list, columns=["StorageType", "Storage", "vserver", "Volume", "Protocol"])
        return volume_list_df


    def get_grouped_vols(cursor):
        grouped_vols = []
        cursor.execute(f"""
            select distinct (s.server, s.storage, s.vserver, s.protocol) serverRow, string_agg(distinct s.volume, ', ') as groupedVolList, count (distinct s.volume) as volCount from sessions s group by s.server, s.storage, s.vserver, s.protocol
        """)
        gvl = cursor.fetchall()
        for row in gvl:
            serverrow = row[0][1:-1].split(',')
            grouped_vols.append(
                {
                    "ServerIP":serverrow[0],
                    "StorageName":serverrow[1],
                    "vserver":serverrow[2],
                    "Protocol":serverrow[3],
                    "VolumeList":row[1],
                    "VolumeCount":row[2]
                }
            )
        grouped_vols_df = pd.DataFrame(grouped_vols)

        return grouped_vols_df


    def get_grouped_servers(cursor):
        grouped_servers = []
        cursor.execute(f"""
            select distinct (s.storage, s.vserver, s.volume, s.protocol) volRow, string_agg(distinct s.server, ', ') as groupedServerList, count (distinct s.server) as serverCount from sessions s group by s.storage, s.vserver, s.volume, s.protocol order by serverCount desc
        """)
        gsl = cursor.fetchall()
        for row in gsl:
            volrow = row[0][1:-1].split(',')
            grouped_servers.append(
                {
                    "StorageName":volrow[0],
                    "vserver":volrow[1],
                    "Volume":volrow[2],
                    "Protocol":volrow[3],
                    "ServerList":row[1],
                    "ServerCount":row[2]
                }
            )
        grouped_vols_df = pd.DataFrame(grouped_servers)

        return grouped_vols_df


    def get_grouped_volumes(cursor):
        grouped_volumes = []
        cursor.execute(f"""
            select distinct (s.server, s.storage, s.vserver, s.protocol) serverRow, string_agg(distinct s.volume, ', ') as groupedVolList, count (distinct s.volume) as volCount from sessions s group by s.server, s.storage, s.vserver, s.protocol order by volCount desc
        """)
        gsl = cursor.fetchall()
        for row in gsl:
            serverrow = row[0][1:-1].split(',')
            grouped_volumes.append(
                {
                    "ServerIP":serverrow[0],
                    "StorageName":serverrow[1],
                    "vserver":serverrow[2],
                    "Protocol":serverrow[3],
                    "VolumeList":row[1],
                    "VolumeCount":row[2]
                }
            )

        grouped_volumes_df = pd.DataFrame(grouped_volumes)
        return grouped_volumes_df


    def get_all_sessions(protocol_list, limit, offset, cursor):
        p_list = ','.join([f"'{protocol}'" for protocol in protocol_list])
        cursor.execute(f"""
            select 
                * 
            from sessions s  
            where 
                protocol in ({p_list})
            order by "timestamp" desc
            limit {limit}
            offset {offset}
        """)
        fsl = cursor.fetchall()
        filtered_session_df = pd.DataFrame(fsl, columns=['Timestamp', 'StorageType', 'Storage', 'vserver', 'lifaddress', 'ServerIP', 'Volume', 'Username', 'Protocol'])

        return filtered_session_df
    

    def get_filtered_sessions(session_users_list, server_list, volume_list, protocol_list, limit, offset, cursor, start_date=None, end_date=None):
        print(session_users_list)
        print(volume_list)
        su_list = ','.join([f"'{username}'" for username in session_users_list])
        sr_list = ','.join([f"'{server}'" for server in server_list])
        v_list = ','.join([f"'{volume}'" for volume in volume_list])
        p_list = ','.join([f"'{protocol}'" for protocol in protocol_list])
        
        # Build the timestamp condition based on date range parameters
        timestamp_condition = ""
        if start_date and end_date:
            timestamp_condition = f"and timestamp::date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            timestamp_condition = f"and timestamp::date >= '{start_date}'"
        elif end_date:
            timestamp_condition = f"and timestamp::date <= '{end_date}'"
        
        cursor.execute(f"""
            select 
                * 
            from sessions s  
            where 
                username in ({su_list})
                and server in ({sr_list})
                and volume  in ({v_list})
                and protocol in ({p_list})
                {timestamp_condition}
            order by "timestamp" desc
            limit {limit}
            offset {offset}
        """)
        fsl = cursor.fetchall()
        filtered_session_df = pd.DataFrame(fsl, columns=['Timestamp', 'StorageType', 'Storage', 'vserver', 'lifaddress', 'ServerIP', 'Volume', 'Username', 'Protocol'])

        return filtered_session_df
    

    def filtered_sessions_summary(session_users_list, server_list, volume_list, protocol_list, cursor):
        # Get the total number of sessions records stored.
        # st_list = ','.join([f"'{storage}'" for storage in storage_list])
        su_list = ','.join([f"'{username}'" for username in session_users_list])
        sr_list = ','.join([f"'{server}'" for server in server_list])
        v_list = ','.join([f"'{volume}'" for volume in volume_list])
        p_list = ','.join([f"'{protocol}'" for protocol in protocol_list])
        cursor.execute(f"""
            select 
                max(timestamp) as timeLast, 
                min(timestamp) as timeFirst, 
                count(*) as count 
            from sessions s
            where 
                username in ({su_list})
                and server in ({sr_list})
                and volume  in ({v_list})
                and protocol in ({p_list})
        """)
        sessions_count = cursor.fetchall()

        return sessions_count[0]

