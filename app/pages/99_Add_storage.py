import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])


from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey
from commons.netappCollector import get_cluster_information, get_cifs_sessions_data, get_nfs_clients_data


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
    page_title="Add NetApp Storage system",
    page_icon="🏠",
    initial_sidebar_state="expanded",
    layout='wide'
    )
st.title("Add NetApp Storage system to data collector")


def verify_add_storage_form(fernet_key, data):
    for key in data.keys():
        if data[key] == None or data[key] == "" or data[key] == False:
            st.error(f"Please fill all fields. Missing value in : {key}")
            return False
    try:
        get_cluster_information({'Address':data['storage_ip'], 'Credentials':[data['storage_user'], fernet_key.decrypt(data['storage_password']).decode()]}, SSL_VERIFY=SSL_VERIFY)
    except requests.exceptions.ConnectTimeout as rcte:
        st.error(f"Connect Timeout error occured. Check network reachability to {rcte.request.url}")
        return False
    except requests.exceptions.RequestException as rre:
        # st.write(rre)
        st.error(f"Requests Exception occured. Check the network reachability to {data['storage_ip']}")
        return False
    except ValueError as ve:
        if "unauthorized" in str(ve).lower():
            st.error("Incorrect username and password.")
            # st.warning(f"{ve}")
        return False
    except Exception as e:
        st.write(e)
        st.error("Unknown error occured.")
        return False
    return True


def main():
    db = {}
    db['db_host'] = os.environ['POSTGRES_HOSTNAME']
    db['db_port'] = os.environ['POSTGRES_PORT']
    db['db_name'] = os.environ['POSTGRES_DATABASE']
    db['db_user'] = os.environ['POSTGRES_USER']
    db['db_password'] = os.environ['POSTGRES_PASSWORD']
    fernet_key = encryptionKey.get_key()

    conn, cursor = pgDb.get_db_cursor(db=db)

    # Show Configured storage systems in Sidebar
    with st.sidebar.container(border=True):
        sidebar_storage_df = stContainersDf.get_configured_storage(cursor=cursor)[['Name', 'StorageIP']]
        st.dataframe(sidebar_storage_df, hide_index=True, use_container_width=True)

    col11, col12, col13 = st.columns([5, 10, 5])
    with col11:
        st.empty()

    with col12:
        with st.container(border=True):
            with st.form("Add Storage form", border=1):
                st.subheader(f"Add Storage Config details")
                storage_system = {}
                storage_name = st.text_input("Storage system Name")
                storage_ip = st.text_input("Storage system IP address")
                storage_user = st.text_input("Storage system Username")
                storage_password = st.text_input("Storage system Password", type="password")

                submitted = st.form_submit_button("Submit")
                if submitted:
                    with st.spinner('Processing form...'):
                        try:
                            formData = {
                                "storage_name":storage_name, 
                                "storage_ip":storage_ip, 
                                "storage_user":storage_user, 
                                "storage_password":fernet_key.encrypt(storage_password.encode())
                            }
                            if verify_add_storage_form(data=formData, fernet_key=fernet_key):
                                pgDb.store_storage_config(conn=conn, cursor=cursor, data=formData)
                                storage_system = {'Name':storage_name, 'Address':storage_ip, 'Credentials':[storage_user, storage_password]}
                                storage_system['netapp_storage'] = get_cluster_information(storage_system, SSL_VERIFY)
                                get_nfs_clients_data(conn, cursor, storage_system, SSL_VERIFY)
                                get_cifs_sessions_data(conn, cursor, storage_system, SSL_VERIFY)
                                st.rerun()
                        except (pg.errors.UniqueViolation, pg.errors.IntegrityError) as e:
                            st.error(e.pgerror.split('DETAIL:  Key ')[1])

    with col13:
        st.empty()

if __name__ == "__main__":
    main()

