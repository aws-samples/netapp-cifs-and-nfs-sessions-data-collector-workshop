# PROJECT_HOME is the current working directory or /usr/app/
# /usr/app is set as the root directory for the NetApp CIFS and NFS collector

import os
import sys
sys.path.append(os.environ['PROJECT_HOME'])
import json
from commons.database import pgDb
from commons.streamlitDfs import stContainersDf
from commons.encryptionKey import encryptionKey
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
import threading
from time import sleep
from datetime import datetime
import base64
import pandas as pd
import re
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

def get_isilon_cluster_information(storage_system, SSL_VERIFY):
    cluster_dict = {}
    cluster_address = storage_system['Address']
    username = storage_system['Credentials'][0]
    password = storage_system['Credentials'][1]
    cluster_dict['url'] = 'https://'+cluster_address+':8080'
    AuthBase64String = base64.encodebytes(
            ('{}:{}'.format(username, password)
        ).encode()).decode().replace('\n', '')
    cluster_dict['header'] = {
        'authorization': "Basic %s" % AuthBase64String
    }
    cluster_string = ""
    try:
        cluster_name_req = requests.get(
            cluster_dict['url']+cluster_string,
            headers=cluster_dict['header'],
            verify=SSL_VERIFY,
            timeout=(5, 120)
        )
        cluster_name_req.raise_for_status()        
        if 'isisessid' in cluster_name_req.cookies:
            cluster_dict['session_id'] = cluster_name_req.cookies['isisessid']
    except requests.exceptions.HTTPError as e:
        logger.error('HTTP Error occurred for GetClusterInformation: %s', e.args[0])
        print(f"HTTP Error {e.args[0]}")
        raise ValueError(f"HTTPError occurred: {str(e)}") from e

    cluster_dict['name'] = cluster_name_req.json()['name']

    return cluster_dict


def get_isilon_statistics_client(conn, cursor, storage_system, SSL_VERIFY):
    isilon_storage = storage_system['isilon']
    cluster_string='/platform/14/statistics/summary/client '
    try:
        filtered_sessions_data = []
        statistics_client_req = requests.get(isilon_storage['url']+cluster_string, headers=isilon_storage['header'], verify=SSL_VERIFY, timeout=(5, 120))
        statistics_client_req.raise_for_status()
        statistics_client = statistics_client_req.json()
        sessions_data=[]
        if len(statistics_client['client']) > 0 :
            for record in statistics_client['client']:
                sessions_data.append({
                    'Timestamp':datetime.strftime(datetime.now(),'%Y%m%d%H%M%S'),
                    'StorageType':'isilon',
                    'Storage':storage_system['Name'],
                    'vserver':"NotAvailable",
                    'lifaddress':record['local_addr'],
                    'ServerIP':record['remote_addr'],
                    'Volume':"NotAvailable",
                    'Username':f"{record['user']['name']}_{record['user']['id']}",
                    'Protocol': record['protocol']
                })
            pgDb.store_sessions(conn=conn, cursor=cursor, data=sessions_data)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error {e.args[0]}")        
    except Exception as e:
        print(f"Error {e}")
        traceback.print_exc()


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
            if storage['CollectData'] and storage['StorageType'] == 'isilon':
                storage_system = {
                    'Name':storage['Name'], 
                    'Address':storage['StorageIP'], 
                    'Credentials':[
                        storage['StorageUser'],
                        fernet_key.decrypt(storage['StoragePassEnc'].tobytes()).decode()
                    ],
                    'CollectData':storage['CollectData']
                }
                storage_system['isilon'] = get_isilon_cluster_information(storage_system, SSL_VERIFY)
                storage_system['get_isilon_statistics_client'] = threading.Thread(target=get_isilon_statistics_client, args=(conn, cursor, storage_system, SSL_VERIFY,))
                storage_system['get_isilon_statistics_client'].start()
                storage_list.append(storage_system)

        sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
