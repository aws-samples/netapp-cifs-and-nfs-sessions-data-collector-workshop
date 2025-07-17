# PROJECT_HOME is the current working directory or /usr/app/
# /usr/app is set as the root directory for the NetApp CIFS and NFS collector

# These are packages and modules required for data collection using NetApp REST APIs
import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])
import json


# Classes and functions defined in the commons folder
from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey


import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

# Running tasks in parallel using multithreading and processing.
# Data is shared across threads using Queues..
import threading
from time import sleep
from datetime import datetime

# Packages required for data manipulation
import base64
import pandas as pd
import re

# Packages for logging
import traceback
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
file_handler = logging.FileHandler(f'{os.environ["PROJECT_HOME"]}/logs/collector.log')
file_handler.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s :: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


global SSL_VERIFY
global POLL_INTERVAL
POLL_INTERVAL = int(os.environ['DATA_COLLECTION_INTERVAL'])
SSL_VERIFY = os.environ['SSL_VERIFY'].upper() == 'TRUE'

# Function to get Cluster information and to test when new storage is added to data collection
def get_cluster_information(storage_system, SSL_VERIFY):
    #Variable for Cluster information
    cluster_dict = {}

    cluster_address = storage_system['Address']
    username = storage_system['Credentials'][0]
    password = storage_system['Credentials'][1]

    #Adding URL, usr, and password to the Cluster Dictionary
    cluster_dict['url'] = 'https://'+cluster_address
    AuthBase64String = base64.encodebytes(
            ('{}:{}'.format(username, password)
        ).encode()).decode().replace('\n', '')

    cluster_dict['header'] = {
        'authorization': "Basic %s" % AuthBase64String
    }

    #String for cluster api call
    cluster_string = "/api/cluster"

    #Get Call for cluster information
    try:
        cluster_name_req = requests.get(cluster_dict['url']+cluster_string,
            headers=cluster_dict['header'],
            verify=SSL_VERIFY,
            timeout=(5, 120))
        cluster_name_req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error('HTTP Error occurred for GetClusterInformation: %s', e.args[0])
        print(f"HTTP Error {e.args[0]}")
        raise ValueError(f"HTTPError occurred: {str(e)}")
        #catch cluster_name_req.status_code

    #Adding cluster's name to dictionary
    cluster_dict['name'] = cluster_name_req.json()['name']

    #String for getting intercluster IP Addresses (Needs to be updated to limit to specific SVM)
    network_intf_string = "/api/network/ip/interfaces?services=intercluster-core&fields=ip.address"

    #Get call for IP Addresses
    try:
        network_intf_req = requests.get(cluster_dict['url']+network_intf_string,
            headers=cluster_dict['header'],
            verify=SSL_VERIFY,
            timeout=(5, 120))
        network_intf_req.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error('HTTP Error occurred got GetNetworkInterfaces: %s', e.args[0])
        print(f"HTTP Error {e.args[0]}")
        return e

    #Adding interfaces to an array in the dictionary
    cluster_dict['interfaces'] = []
    for record in network_intf_req.json()['records']:
        cluster_dict['interfaces'].append(record['ip']['address'])

    return cluster_dict

# Collect CIFS sessions details and store in Postgres database
def get_cifs_sessions_data(conn, cursor, storage_system, SSL_VERIFY):
    netapp_storage = storage_system['netapp_storage']
    # Netapp cifs Sessions API
    # Get all fields for CIFS sessions
    cluster_string='/api/protocols/cifs/sessions'
    parameters='?return_timeout=15&return_records=true&max_records=10000&fields=*'
    try:
        filtered_sessions_data = []
        cifs_sessions_req = requests.get(netapp_storage['url']+cluster_string+parameters, headers=netapp_storage['header'], verify=SSL_VERIFY, timeout=(5, 120))
        cifs_sessions_req.raise_for_status()
        cifs_sessions = cifs_sessions_req.json()
        sessions_data=[]
        print(cifs_sessions['num_records'])
        if cifs_sessions['num_records'] > 0 :
            # Check for active CIFS sessions 
            for record in cifs_sessions['records']:
                # Check volume details in the CIFS sessions
                if 'volumes' in record:
                    sessions_data.append({
                        'Timestamp':datetime.strftime(datetime.now(),'%Y%m%d%H%M%S'),
                        'Storage':storage_system['Name'],
                        'vserver':record['svm']['name'],
                        'lifaddress':record['server_ip'],
                        'ServerIP':record['client_ip'],
                        'Volume':record['volumes'][0]['name'],
                        'Username':record['user'],
                        'Protocol':'CIFS'
                    })
                filtered_sessions_data = filtered_data(sessions_data)
            if len(filtered_sessions_data)>0:  
                pgDb.store_sessions(conn=conn, cursor=cursor, data=filtered_sessions_data)  
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error {e.args[0]}")        
    except Exception as e:
        print(f"Error {e}")
        traceback.print_exc()


# Function to convert idle time from PT to seconds.
def split_time_string(time_string):
    pattern1 = r"PT(\d+)S"
    pattern2 = r"PT(\d+)M(\d+)S"
    pattern3 = r"PT(\d+)H(\d+)M(\d+)S"

    if re.match(pattern1, time_string):
        t = re.match(pattern1, time_string)
        seconds = int(t.group(1))
        return seconds
    elif re.match(pattern2, time_string):
        t = re.match(pattern2, time_string)
        minutes = int(t.group(1))
        seconds = int(t.group(2))
        return minutes*60+seconds
    elif re.match(pattern3, time_string):
        t = re.match(pattern3, time_string)
        hours = int(t.group(1))
        minutes = int(t.group(2))
        seconds = int(t.group(3))
        return hours*3600+minutes*60+seconds
    else:
        # Return 1 when no pattern match and idle time is unknown.
        # Safe value to include NFS mounts with unmatched idle pattern.
        # This value can be set higher than POLL_INTERVAL / DATA_COLLECTION_INTERVAL when idle_duration patterns are safe to be not included in the data discovery process to support migration projects.
        return 1


def get_nfs_clients_data(conn, cursor, storage_system, SSL_VERIFY):
    netapp_storage = storage_system['netapp_storage']
    # Netapp NFS Sessions API
    clusterString='/api/protocols/nfs/connected-clients'
    parameters='?return_timeout=25&return_records=true&max_records=10000&idle_duration=PT*'
    try:
        cNfsClientsReq = requests.get(netapp_storage['url']+clusterString+parameters, headers=netapp_storage['header'], verify=SSL_VERIFY, timeout=(5,120))
        cNfsClientsReq.raise_for_status()
        cNfsClients = cNfsClientsReq.json()['records']
    except requests.exceptions.HTTPError as e:
        # logger.error('HTTP Error occurred: %s', {e.args[0]})
        print(f"HTTP Error {e.args[0]}")
    session_data = []

    try:
        filtered_session_data = []
        for cn in cNfsClients:
            idle = split_time_string(cn['idle_duration'])
            c1 = (idle <= int(POLL_INTERVAL))
            c2 = (cn['volume']['name'] != f"{cn['svm']['name']}_root")
            if c1 and c2:
                # rounded_timestr = pd.Timestamp(datetime.now()).round(f'{int(POLL_INTERVAL)}s')
                # timestr=datetime.strftime(rounded_timestr,'%Y%m%d%H%M%S')
                session_data.append({
                    'Timestamp':    datetime.strftime(datetime.now(),'%Y%m%d%H%M%S'), 
                    'Storage':  storage_system['Name'], 
                    'vserver':  cn['svm']['name'], 
                    'lifaddress':   cn['server_ip'], 
                    'ServerIP': cn['client_ip'], 
                    'Volume':   cn['volume']['name'],
                    'Username': 'None',
                    'Protocol': 'NFS'
                })
                filtered_session_data = filtered_data(session_data)
        if len(filtered_session_data) > 0:
            pgDb.store_sessions(conn=conn, cursor=cursor, data=filtered_session_data)
    except TypeError as e:
        print('TypeError: %s', e)
        # logger.error('Idle time error: %s', idle)
    except Exception as e:
        print('An error occurred: %s', e)
        # logger.error('An error occurred: %s', e)


def filtered_data(data):

    with open(f'{os.environ["PROJECT_HOME"]}/filters.json', 'r') as ff:
        filters = json.load(ff)

    include_set = {frozenset(item.items()) for item in filters["include"]}
    exclude_set = {frozenset(item.items()) for item in filters["exclude"]}

    result = []
    for item in data:
        item_set = frozenset(item.items())
        
        matches_include = any(include_filter.issubset(item_set) for include_filter in include_set)
        matches_exclude = any(exclude_filter.issubset(item_set) for exclude_filter in exclude_set)
        
        if (matches_include or not matches_exclude) or (matches_include and not matches_exclude):
            result.append(item)
            
    return result


def main():
    db = {
        'db_host':os.environ['POSTGRES_HOSTNAME'],
        'db_port':os.environ['POSTGRES_PORT'],
        'db_name':os.environ['POSTGRES_DATABASE'],
        'db_user':os.environ['POSTGRES_USER'],
        'db_password':os.environ['POSTGRES_PASSWORD']
    }
    fernet_key = encryptionKey.get_key()

    while True:
        print(f"{datetime.strftime(datetime.now(),'%Y%m%d%H%M%S')}: Reruning data collection")
        conn, cursor = pgDb.get_db_cursor(db)
        storage_list_df = stContainersDf.get_configured_storage(cursor=cursor)
        storage_list = []

        # Collect data for each Storage configured
        for index, storage in storage_list_df.iterrows():
            if storage['CollectData']:
                storage_system = {'Name':storage['Name'], 'Address':storage['StorageIP'], 'Credentials':[storage['StorageUser'], fernet_key.decrypt(storage['StoragePassEnc'].tobytes()).decode()], 'CollectData':storage['CollectData']}
                storage_system['netapp_storage'] = get_cluster_information(storage_system, SSL_VERIFY)
                storage_system['get_cifs_sessions_data'] = threading.Thread(target=get_cifs_sessions_data, args=(conn, cursor, storage_system, SSL_VERIFY,))
                storage_system['get_nfs_clients_data'] = threading.Thread(target=get_nfs_clients_data, args=(conn, cursor, storage_system, SSL_VERIFY,))
                storage_system['get_cifs_sessions_data'].start()
                storage_system['get_nfs_clients_data'].start()
                storage_list.append(storage_system)

        sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
