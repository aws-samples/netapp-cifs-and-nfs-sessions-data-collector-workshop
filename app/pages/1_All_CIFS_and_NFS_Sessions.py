import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])


from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey
from commons.auth import userAuth

import psycopg2 as pg
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
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
st.title("CIFS & NFS Sessions :: Table")
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


def create_selectors(df, sidebar=True):
    # Configure grid options
    gb = GridOptionsBuilder.from_dataframe(df)
    
    # Configure columns with specific filter types
    for column_name in list(df.columns):
        gb.configure_column(column_name, 
            filter=True,
            filterParams={
                "filterOptions": ['contains', 'notContains', 'equals', 'notEqual'],
                "suppressAndOrCondition": True,
                "maxNumConditions": 5,
                "buttons": ['clear']
            },
            type=["numericColumn", "numberColumnFilter"]
        )

    
    # Enable sorting for all columns
    gb.configure_default_column(sorteable=True)
    
    # Add selection functionality
    gb.configure_selection('multiple', use_checkbox=True)
    
    # Configure grid default properties
    gb.configure_grid_options(domLayout='normal')
    
    # Build grid options
    grid_options = gb.build()

    # Create the AgGrid
    if sidebar:
        with st.sidebar:
            grid_response = AgGrid(
                df, 
                gridOptions=grid_options,
                height=200,
                width='100%',
                data_return_mode='AS_INPUT',
                update_mode='MODEL_CHANGED',
                fit_columns_on_grid_load=True,
                allow_unsafe_jscode=True,
                theme='alpine',
                # enable_enterprise_modules=False
            )    
            
            # Get selected rows
            selected_items = grid_response['selected_rows']
            try:
                if not selected_items.empty:
                    items_list = selected_items[df.columns[0]].to_list()
            except:
                st.sidebar.error("Please select at least one item.")
                items_list = []
                pass

            return items_list
    else:
        grid_response = AgGrid(
            df, 
            gridOptions=grid_options,
            # height=200,
            width='100%',
            data_return_mode='AS_INPUT',
            update_mode='MODEL_CHANGED',
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True,
            theme='alpine',
            # enable_enterprise_modules=False
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


    if 'sessions_limit' not in st.session_state and 'sessions_offset' not in st.session_state:
        st.session_state.sessions_page_num = 1
        st.session_state.sessions_limit = 1000
        st.session_state.sessions_offset = 0
        st.session_state.time_first = 0
        st.session_state.time_last = 0
        st.session_state.sessions_count = 0
        st.session_state.num_pages = 1

    # Display widgets in Sidebar
    
    ## Add date range selector
    st.sidebar.subheader("Date Range Filter")
    col_date1, col_date2 = st.sidebar.columns(2)
    with col_date1:
        start_date = st.date_input("Start Date", value=None)
    with col_date2:
        end_date = st.date_input("End Date", value=None)
    
    ## Show the storage systems configured
    # storage_df = stContainersDf.get_configured_storage(cursor=cursor)[['Name','StorageIP']]
    session_users_df = stContainersDf.get_session_users(cursor=cursor)[['Username', 'Protocol']]
    volumes_df = stContainersDf.get_all_volumes(cursor=cursor)[['Volume', 'vserver', 'Storage' ]]
    servers_df = stContainersDf.get_servers(cursor=cursor)[['ServerIP']]
    
    session_users_list = create_selectors(session_users_df)
    volume_list = create_selectors(volumes_df)
    server_list = create_selectors(servers_df)
    

    selected_protocols = st.sidebar.multiselect(
        "Select Protocols",
        ['CIFS', 'NFS'],
        ['CIFS', 'NFS']
    )
    if not selected_protocols:
        selected_protocols = ['CIFS', 'NFS']
        st.sidebar.error("Please select at least one protocol.")

    

    if server_list and volume_list and selected_protocols:
        st.session_state.time_first, st.session_state.time_last, st.session_state.selected_count = stContainersDf.filtered_sessions_summary(cursor=cursor, server_list=server_list, volume_list=volume_list, session_users_list=session_users_list, protocol_list=selected_protocols)
        st.session_state.num_pages = round(st.session_state.selected_count/st.session_state.sessions_limit)
        
        # Convert date inputs to string format for SQL query if they exist
        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
        
        # Pass date range to get_filtered_sessions
        sessions_df = stContainersDf.get_filtered_sessions(
            cursor=cursor, 
            server_list=server_list, 
            volume_list=volume_list, 
            session_users_list=session_users_list,
            protocol_list=selected_protocols, 
            limit=st.session_state.sessions_limit, 
            offset=st.session_state.sessions_offset,
            start_date=start_date_str,
            end_date=end_date_str
        )
    else:
        st.info("Select Storage, Servers, Volumes and Protocols from Sidebar")
        sessions_df = stContainersDf.get_all_sessions(cursor=cursor,protocol_list=selected_protocols, limit=st.session_state.sessions_limit, offset=st.session_state.sessions_offset)

    col1, col2, col3 = st.columns([1, 45, 1])
    with col1:
        st.empty()

    with col3:
        st.empty()

    with col2:
        with st.container(border=True):
            st.header(f"{' & '.join(selected_protocols)} Shares accessed by Servers.")
            st.write(f"From :blue[{sessions_df['Timestamp'].min()}] till :green[{sessions_df['Timestamp'].max()}]")
            st.subheader(f":blue[{len(server_list)}] servers and :green[{len(volume_list)}] volumes selected.", divider="blue")
            st.caption("Table is by default sorted by :grey[timestamp]. Click on any column name to sort by that column.")

            st.dataframe(
                sessions_df,
                use_container_width=True,
                height=800
            )
            # create_selectors(sessions_df, sidebar=False)
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
    # else:
        # st.info("Select Storage, Servers, Volumes and Protocols from Sidebar")




if __name__ == "__main__":
    main()

