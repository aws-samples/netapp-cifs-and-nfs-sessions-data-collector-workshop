import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])


from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey
from commons.auth import userAuth
from commons.netappCollector import get_cluster_information, get_cifs_sessions_data, get_nfs_clients_data
from commons.isilonCollector import get_isilon_cluster_information, get_isilon_statistics_client


import streamlit as st
import psycopg2 as pg
import logging
import requests


logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(f'{os.environ["PROJECT_HOME"]}/logs/app.log')
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s :: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


global SSL_VERIFY
SSL_VERIFY = os.environ['SSL_VERIFY'].upper() == 'TRUE'


st.set_page_config(
    page_title="Manage Storage systems",
    page_icon="🏠",
    initial_sidebar_state="expanded",
    layout='wide'
    )
st.title("Manage Storage systems data collection")
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

def add_storage(fernet_key, data):
    for key in data.keys():
        if data[key] == None or data[key] == "" or data[key] == False:
            st.error(f"Please fill all fields. Missing value in : {key}")
            return False
    try:
        get_cluster_information(
            {
                'Address':data['storage_ip'], 
                'Credentials':[data['storage_user'], 
                fernet_key.decrypt(data['storage_password']).decode()]
            },
            SSL_VERIFY=SSL_VERIFY
        )
        return True
    except requests.exceptions.ConnectTimeout as rcte:
        st.error(f"Connect Timeout error occured. Check network reachability to {rcte.request.url}")
        return False
    except requests.exceptions.RequestException as rre:
        st.write(rre)
        st.error(f"Requests Exception occured. Check the network reachability to {data['storage_ip']}")
        return False
    except ValueError as ve:
        if "unauthorized" in str(ve).lower():
            st.error("Incorrect username and password.")
            st.warning(f"{ve}")
        return False
    except (ConnectionError) as ce:
        st.write(ce)
        st.error("Network connection error occurred.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in add_storage: {e}")
        st.write(e)
        st.error("Unknown error occurred.")
        return False

def manage_storage_systems(fernet_key, conn, cursor):
    # Show Configured storage systems in Sidebar
    with st.sidebar.container(border=True):
        sidebar_storage_df = stContainersDf.get_configured_storage(cursor=cursor)[['Name', 'StorageIP', 'CollectData','StorageType']]
        st.dataframe(sidebar_storage_df, hide_index=True, use_container_width=True)

    col11, col12, col13, col14 = st.columns([1, 8, 5, 4])
    with col11:
        st.empty()

    with col12:
        with st.container(border=True):
            with st.form("Add Storage form", border=1):
                st.subheader(f"Add Storage Config details")
                storage_system = {}
                storage_type = st.selectbox("Select NetApp or Isilon", index=0, ["NetApp", "Isilon"])
                storage_name = st.text_input("Storage system Name")
                storage_ip = st.text_input("Storage system IP address")
                storage_user = st.text_input("Storage system Username")
                storage_password = st.text_input("Storage system Password", type="password")

                submitted = st.form_submit_button("Submit")
                if submitted:
                    with st.spinner('Processing form...'):
                        try:
                            formData = {
                                "storage_type": storage_type.lower(),
                                "storage_name":storage_name, 
                                "storage_ip":storage_ip, 
                                "storage_user":storage_user, 
                                "storage_password":fernet_key.encrypt(storage_password.encode()),
                                "collectdata" : True
                            }
                            if add_storage(data=formData, fernet_key=fernet_key):
                                pgDb.store_storage_config(conn=conn, cursor=cursor, data=formData)
                                storage_system = {'Name':storage_name, 'Address':storage_ip, 'Credentials':[storage_user, storage_password]}
                                if storage_type.lower() == 'netapp':
                                    storage_system['netapp'] = get_cluster_information(storage_system, SSL_VERIFY)
                                    get_nfs_clients_data(conn, cursor, storage_system, SSL_VERIFY)
                                    get_cifs_sessions_data(conn, cursor, storage_system, SSL_VERIFY)
                                elif storage_type.lower() == 'isilon':
                                    storage_system['isilon'] = get_isilon_cluster_information(storage_system, SSL_VERIFY)
                                    get_isilon_statistics_client(conn, cursor, storage_system, SSL_VERIFY)
                                st.rerun()
                        except (pg.errors.UniqueViolation, pg.errors.IntegrityError) as e:
                            st.error(e.pgerror.split('DETAIL:  Key ')[1])

    def update_storage_collection(conn, cursor, storage):
        data = {
            'collectdata' : st.session_state[storage['Name']],
            'storagename' : storage['Name']
        }
        pgDb.update_storage_collection(conn, cursor, data)
        return storage
    with col13:
        with st.container(border=True):
            st.subheader("Update data collection")
            for storage in  sidebar_storage_df.iterrows():
                if storage[1]['CollectData']:
                    collection_label = f":green[{storage[1]['Name'].upper()}] data collection :green[ENABLED]"
                else:
                    collection_label = f":orange[{storage[1]['Name'].upper()}] data collection :orange[DISABLED]"

                st.toggle(
                    collection_label, 
                    key=storage[1]['Name'], 
                    value=storage[1]['CollectData'],
                    on_change=update_storage_collection,
                    kwargs={"conn":conn, "cursor":cursor, 'storage':storage[1]}
                )
    with col14:
        st.empty()

def verify_user_login(conn, cursor, fernet_key):
    # Check authentication
    if st.session_state.get('authenticated'):
        with st.sidebar:
            st.success(f"Logged in as {st.session_state.username}")
            if st.button("Logout"):
                st.session_state.authenticated = False
                st.rerun()
        return True
    else:
        st.warning("Please login to continue.")
        with st.sidebar:
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            if st.button("Login"):
                user_authenticated = userAuth.verify_user(cursor, username, password, fernet_key)
                if user_authenticated:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    st.success("Logged in successfully!")
                    st.rerun()
                else:
                    st.error("Invalid username or password")
        return False

def verify_admin_access():
    # Verify Admin user
    if st.session_state.username == 'admin':
        # Enable admin access
        st.info("Admin access enabled.")
        return True
    else:
        st.warning("Access restricted. Login as Admin user.")
        return False

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

    if verify_user_login(conn, cursor, fernet_key) and verify_admin_access():
        manage_storage_systems(fernet_key, conn, cursor)
    else:
        st.stop()

if __name__ == "__main__":
    main()

