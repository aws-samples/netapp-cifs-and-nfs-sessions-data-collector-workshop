import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])


from commons.database import pgDb
from commons.streamlitDfs import stContainersDf

import psycopg2 as pg
import pandas as pd
import streamlit as st
import logging
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(f'{os.environ["PROJECT_HOME"]}/logs/app.log')
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s :: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)



st.set_page_config(
    page_title="NetApp Volume Sessions",
    page_icon="🏠",
    initial_sidebar_state="expanded",
    layout='wide'    
    )


def main():
    db = {
        'db_host':os.environ['POSTGRES_HOSTNAME'],
        'db_port':os.environ['POSTGRES_PORT'],
        'db_name':os.environ['POSTGRES_DATABASE'],
        'db_user':os.environ['POSTGRES_USER'],
        'db_password':os.environ['POSTGRES_PASSWORD']
    }


    # Using Streamlit cache for Database connection resource
    @st.cache_resource
    def get_conn_cursor(db):
        conn, cursor = pgDb.get_db_cursor(db=db)
        return conn, cursor

    conn, cursor = get_conn_cursor(db)

    if 'sessions_limit' not in st.session_state and 'sessions_offset' not in st.session_state:
        st.session_state.sessions_page_num = 1
        st.session_state.sessions_limit = 1000
        st.session_state.sessions_offset = 0
        st.session_state.time_first = 0
        st.session_state.time_last = 0
        st.session_state.sessions_count = 0
        st.session_state.num_pages = 1

    # Display widgets in Sidebar

    ## Show the storge systems configured
    storage_configured_df = stContainersDf.get_configured_storage(cursor=cursor)[['Name','StorageIP']]
    volumes_df = stContainersDf.get_all_volumes(cursor=cursor)
    servers_df = stContainersDf.get_servers(cursor=cursor)


    selected_storage = st.sidebar.multiselect(
        "Select Storage systems",
        storage_configured_df['Name'].to_list(),
        storage_configured_df['Name'].to_list()
    )
    if not selected_storage:
        st.sidebar.error("Please select at least one Storage system.")


    # Drop duplicates to avoid duplications of multiprotocol volumes
    selected_volumes = st.sidebar.multiselect(
    "Select Volumes",
        volumes_df[['Storage','vserver', 'Volume']].drop_duplicates()['Volume'].to_list(),
        volumes_df[['Storage','vserver', 'Volume']].drop_duplicates()['Volume'].to_list()
    )
    if not selected_volumes:
        st.sidebar.error("Please select at least one volume.")


    # st.sidebar.write("Select Server IPs")
    selected_address = st.sidebar.multiselect(
        "Select Server IPs",
        servers_df['ServerIP'].to_list(),
        servers_df['ServerIP'].to_list()
    )
    if not selected_address:
        st.sidebar.error("Please select at least one address.")

    selected_protocols = st.sidebar.multiselect(
        "Select Protocols",
        ['CIFS', 'NFS'],
        ['CIFS', 'NFS']
    )
    if not selected_protocols:
        st.sidebar.error("Please select at least one protocol.")

    st.title(f"{' & '.join(selected_protocols)} Sessions :: Table")

    if selected_address and selected_volumes and selected_protocols and selected_storage and selected_protocols:
        st.session_state.time_first, st.session_state.time_last, st.session_state.selected_count = stContainersDf.get_filtered_sessions_details(cursor=cursor, storage_list=selected_storage, server_list=selected_address, volume_list=selected_volumes, protocol_list=selected_protocols)
        st.session_state.num_pages = round(st.session_state.selected_count/st.session_state.sessions_limit)
        sessions_df = stContainersDf.get_filtered_sessions(cursor=cursor, storage_list=selected_storage, server_list=selected_address, volume_list=selected_volumes, protocol_list=selected_protocols, limit=st.session_state.sessions_limit, offset=st.session_state.sessions_offset)
        
        col1, col2, col3 = st.columns([1, 45, 1])
        with col1:
            st.empty()

        with col3:
            st.empty()

        with col2:
            with st.container(border=True):
                st.header(f":spiral_note_pad: {' & '.join(selected_protocols)} Shares accessed by Servers.")
                st.write(f"From :blue[{sessions_df['Timestamp'].min()}] till :green[{sessions_df['Timestamp'].max()}]")
                st.subheader(f":blue[{len(selected_address)}] servers and :green[{len(selected_volumes)}] volumes selected.", divider="blue")
                st.caption("Table is by default sorted by :grey[timestamp]. Click on any column name to sort by that column.")

                st.dataframe(
                    sessions_df,
                    use_container_width=True,
                    height=600
                )
                if st.session_state.num_pages !=0 and (st.session_state.sessions_page_num/(st.session_state.num_pages)) <=1:
                    st.info(f"Page number {st.session_state.sessions_page_num} of {st.session_state.num_pages} with {len(sessions_df)} rows.")
                elif st.session_state.num_pages !=0 and (st.session_state.sessions_page_num/st.session_state.num_pages >1):
                    st.info(f"Page number {st.session_state.sessions_page_num} of {st.session_state.num_pages}")
                    st.warning("Navigate few pages back.")
                else:
                    st.info(f"Page number {st.session_state.sessions_page_num} of {st.session_state.num_pages} with {len(sessions_df)} rows.")

            try:
                startTime = str(st.session_state.time_first).replace(".","_").replace(" ","_").replace(":","")
                endTime = str(st.session_state.time_last).replace(".","_").replace(" ","_").replace(":","")
                st.download_button(
                    label="Download Table as CSV",
                    data=sessions_df.to_csv().encode("utf-8"),
                    file_name=f'sessions-{startTime}-{endTime}.csv',
                    mime="text/csv",
                )
            except Exception as e:
                logger.error("Error : %s", e)
                st.error(traceback.format_exc())

        ## pseudo-pagination
        col121, col122, col123 = st.columns([5, 25, 5])
        with col121:
            st.empty()

        with col122:
            st.empty()

        with col123:
            st.empty()

        col131, col132, col133, col134, col135 = st.columns([5, 5, 15, 5, 5])
        with col131:
            st.empty()

        with col132:
            if st.button(":arrow_left: Previous Page" ) and st.session_state.sessions_offset/st.session_state.sessions_limit > 0:
                st.session_state.sessions_offset = st.session_state.sessions_offset - st.session_state.sessions_limit
                st.session_state.sessions_page_num -= 1
                        
        with col133:
            st.empty()

        with col134:
            if st.button("Next Page :arrow_right:") and st.session_state.sessions_offset/st.session_state.sessions_limit < st.session_state.num_pages:
                st.session_state.sessions_offset = st.session_state.sessions_offset + st.session_state.sessions_limit
                st.session_state.sessions_page_num += 1

        with col135:
            st.empty()
    else:
        st.info("Select Storage, Servers, Volumes and Protocols from Sidebar")




if __name__ == "__main__":
    main()

