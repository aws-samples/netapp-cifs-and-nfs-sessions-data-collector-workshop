import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])


from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey
from commons.auth import userAuth

import pandas as pd
import psycopg2 as pg
import json
import traceback

import streamlit as st
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(f'{os.environ["PROJECT_HOME"]}/logs/app.log')
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s :: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


st.set_page_config(
    page_title="Home",
    page_icon="🏠",
    initial_sidebar_state="expanded",
    layout='wide'
    
    )
st.title("NetApp Storage CIFS and NFS clients")
st.markdown(
    """
    <style>
        section[data-testid="stSidebar"] {
            width: 500px !important; # Set the width to your desired value
        }
    </style>
    """,
    unsafe_allow_html=True,
)

def main():
    fernet_key = encryptionKey.get_key()
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
    # conn, cursor = pgDb.get_db_cursor(db=db)

    # Check authentication
    if not st.session_state.get('authenticated'):
        st.warning("Please login to continue.")
        with st.sidebar:
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            if st.button("Login"):
                user_authenticated = userAuth.verify_user(cursor, username, password, fernet_key)
                # if verify_user(username, password):
                if user_authenticated:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.success("Logged in successfully!")
                    st.rerun()
                else:
                    st.error("Invalid username or password")
        st.stop()
    else:
        with st.sidebar:
            st.success(f"Logged in as {st.session_state.username}")
            if st.button("Logout"):
                st.session_state.authenticated = False
                st.rerun()

    # Display widgets in Sidebar

    ## Show the storge systems configured
    with st.sidebar.container(border=True):
        sidebar_storage_df = stContainersDf.get_configured_storage(cursor=cursor)[['Name', 'StorageIP', 'CollectData']]
        st.dataframe(sidebar_storage_df, hide_index=True, use_container_width=True)

    ## Show the range of timestamps of the data collected.
    time_last, time_first, sessionserver_count = stContainersDf.get_sessions_details(cursor=cursor)[0]
    vol_count = stContainersDf.get_protocol_volume_count(cursor=cursor)
    server_count = stContainersDf.get_servers(cursor=cursor)['ServerIP'].count()

    # Update time in Title bar when Home page loaded
    st.sidebar.title(f"""
        From :rainbow[{time_first}]
        To :rainbow[{time_last}]"""
    )

    ## Show summary of servers, Volumes and Users discovered.

    st.sidebar.header('Servers discovered:', divider='rainbow')
    st.sidebar.subheader(server_count, divider='grey')
    st.sidebar.header('Volumes accessed:', divider='rainbow')
    st.sidebar.subheader(vol_count['VolumeCount'].sum(), divider='grey')
    st.sidebar.header(':red[Total records:]', divider='red')
    st.sidebar.subheader(f":red[{sessionserver_count}]", divider='grey')
    
    if st.sidebar.button("Load New Data", type="secondary"):
        st.write("Cache Updated: Reloading with new data")
        st.cache_resource.clear()

    col11, col12, col13, col14 = st.columns([1, 20, 10, 5])
    with col11:
        st.empty()

    # Cell to show the summary of volumes discovered
    with col12:
        with st.container(border=True, height=500):
            st.subheader(f"Volumes (Count = {vol_count['VolumeCount'].sum()})")
            st.dataframe(
                stContainersDf.get_all_volumes(cursor=cursor),
                use_container_width=True,
                height=250
            )
            st.table(vol_count.set_index('Protocol'))

    # Cell to show the summary of servers discovered
    with col13:
        with st.container(border=True, height=500):
            st.subheader(f"Servers (Count = {server_count})")
            st.session_state.serverLimit = 100
            st.session_state.serverOffset = 0

            st.dataframe(
                stContainersDf.get_servers(cursor=cursor),
                use_container_width=True,
                height=400
            )

    with col14:
        st.empty()


    # Row to show Top servers grouped by volumes
    col21, col22, col23 = st.columns([1, 30, 5])
    with col21:
        st.empty()
    with col23:
        st.empty()
    with col22:
        with st.container(border=True):
            st.subheader("Top Servers grouped by Volumes accessed")
            st.write("[Server IP :: Storage Name :: vserver :: Volumes :: :blue[VolumeCount]]")
            st.dataframe(
                stContainersDf.get_grouped_vols(cursor=cursor),
                use_container_width=True,
                height=350
            )

    # Row to show Top volumes grouped by servers
    col31, col32, col33 = st.columns([1, 30, 5])
    with col31:
        st.empty()
    with col33:
        st.empty()
    with col32:
        with st.container(border=True):
            st.subheader("Top Volumes grouped by Servers")
            st.write("[Volume :: Storage Name :: vserver :: Servers :: :blue[ServerCount]]") 
            st.dataframe(
                stContainersDf.get_grouped_servers(cursor=cursor),
                use_container_width=True,
                height=350
            )


if __name__ == "__main__":
    main()
