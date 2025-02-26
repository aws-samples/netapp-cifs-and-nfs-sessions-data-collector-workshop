#!/usr/bin/env python3
# Use REST API to collect and store NFS, and CIFS sessions data from NetApp storage arrays, where data is saved in a Sqlite database file locally.

import sqlite3
from datetime import datetime
import time
import json
import threading
from time import sleep
import base64
import pandas as pd
import re
import os
import logging.handlers
import traceback
import requests
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


def setup_logging():
    """Configure logging with rotation and proper formatting"""
    logger = logging.getLogger('NetAppCollector')
    logger.setLevel(logging.INFO)

    # Create logs directory if it doesn't exist
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configure rotating file handler
    log_file = os.path.join(log_dir, 'netapp_data_collector.log')
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)

    # Configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Create logger instance
logger = setup_logging()
global SSL_VERIFY
global POLL_INTERVAL


class SessionsDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self.logger = logging.getLogger('NetAppCollector.Database')

    def __enter__(self):
        self.create_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def create_connection(self):
        try:
            if not hasattr(self._local, 'connection'):
                self._local.connection = sqlite3.connect(self.db_path)
                self.logger.debug(f"Created new database connection for thread {threading.current_thread().name}")
        except sqlite3.Error as e:
            self.logger.error(f"Failed to create database connection: {e}")
            raise

    def close_connection(self):
        if hasattr(self._local, 'connection'):
            try:
                self._local.connection.close()
                del self._local.connection
                self.logger.debug(f"Closed database connection for thread {threading.current_thread().name}")
            except sqlite3.Error as e:
                self.logger.error(f"Error closing connection: {e}")

    def create_sessions_table(self):
        try:
            with self._local.connection:
                cursor = self._local.connection.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        Timestamp TEXT NOT NULL,
                        Storage TEXT NOT NULL,
                        vserver TEXT NOT NULL,
                        lifaddress TEXT NOT NULL,
                        ServerIP TEXT NOT NULL,
                        Volume TEXT NOT NULL,
                        Username TEXT NOT NULL,
                        Protocol TEXT NOT NULL
                    )
                """)
            self.logger.info("Sessions table created/verified successfully")
        except sqlite3.Error as e:
            self.logger.error(f"Error creating sessions table: {e}")
            raise

    def insert_sessions_df(self, df: pd.DataFrame):
        start_time = time.time()
        try:
            df.to_sql('sessions', self._local.connection, if_exists='append', index=False)
            elapsed = time.time() - start_time
            self.logger.info(f"Inserted {len(df)} rows in {elapsed:.2f} seconds")
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting data: {e}")
            raise


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


# Collect CIFS sessions details and store in database
def get_cifs_sessions_data(storage_system, SSL_VERIFY, POLL_INTERVAL):
    logger = logging.getLogger('NetAppCollector.CIFS')
    logger.info(f"Starting CIFS data collection for storage: {storage_system['Name']}")

    netapp_storage = storage_system['netapp_storage']
    # Netapp cifs Sessions API
    cluster_string = '/api/protocols/cifs/sessions'
    parameters = '?return_timeout=15&return_records=true&max_records=10000'
    try:
        # logger.debug(f"Requesting CIFS sessions from {netapp_storage['url']}")
        cifs_sessions_req = requests.get(netapp_storage['url']+cluster_string+parameters, headers=netapp_storage['header'], verify=SSL_VERIFY, timeout=(5, 120))
        cifs_sessions_req.raise_for_status()
        cifs_sessions = cifs_sessions_req.json()
        logger.debug(f"Retrieved {len(cifs_sessions['records'])} CIFS sessions")
    except requests.exceptions.HTTPError as e:
        # print(f"HTTP Error {e.args[0]}")
        logger.error(f"HTTP Error during CIFS data collection: {e}")

    for record in cifs_sessions['records']:
        sessions_data = []
        sessions_link = record['_links']['self']['href']
        try:
            sessions_response_request = requests.get(netapp_storage['url']+sessions_link, headers=netapp_storage['header'], verify=SSL_VERIFY, timeout=(5, 120))
            sessions_response_request.raise_for_status()
            sessions_response = sessions_response_request.json()
            for vol in sessions_response['volumes']:
                # rounded_timestr = pd.Timestamp(datetime.now()).round(f'{int(POLL_INTERVAL)}s')
                # timestr = datetime.strftime(rounded_timestr,'%Y%m%d%H%M%S')
                sessions_data = [{
                    'Timestamp': datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'),
                    'Storage': storage_system['Name'],
                    'vserver': sessions_response['svm']['name'],
                    'lifaddress': sessions_response['server_ip'],
                    'ServerIP': sessions_response['client_ip'],
                    'Volume': vol['name'],
                    'Username': sessions_response['user'],
                    'Protocol': 'CIFS'
                }]
        except requests.exceptions.HTTPError as e:
            # print(f"HTTP Error {e.args[0]}")
            logger.error(f"HTTP Error {e.args[0]}")
        except Exception as e:
            # print(f"Error {e}")
            traceback.print_exc()
            logger.error(f"Unexpected error during CIFS data collection: {e}")
            logger.debug(traceback.format_exc())
            continue
        finally:
            with SessionsDB("netapp_sessions.db") as db:
                db.insert_sessions_df(pd.DataFrame(sessions_data))
            # cifs_data_collector_sqlite.close_connection()


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


# Collect NFS client details and store in database
def get_nfs_clients_data(storage_system, SSL_VERIFY, POLL_INTERVAL):
    logger = logging.getLogger('NetAppCollector.NFS')
    logger.info(f"Starting NFS data collection for storage: {storage_system['Name']}")

    netapp_storage = storage_system['netapp_storage']
    # Netapp NFS Sessions API
    clusterString = '/api/protocols/nfs/connected-clients'
    parameters = '?return_timeout=25&return_records=true&max_records=10000&idle_duration=PT*'
    try:
        # logger.debug(f"Requesting NFS sessions from {netapp_storage['url']}")
        cNfsClientsReq = requests.get(netapp_storage['url']+clusterString+parameters, headers=netapp_storage['header'], verify=SSL_VERIFY, timeout=(5,120))
        cNfsClientsReq.raise_for_status()
        cNfsClients = cNfsClientsReq.json()['records']
        logger.debug(f"Retrieved {len(cNfsClients)} NFS client sessions")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error during NFS data collection: {e}")

    sessions_data = []

    try:
        for cn in cNfsClients:
            idle = split_time_string(cn['idle_duration'])
            c1 = (idle <= int(POLL_INTERVAL))
            c2 = (cn['volume']['name'] != f"{cn['svm']['name']}_root")
            if c1 and c2:
                # rounded_timestr = pd.Timestamp(datetime.now()).round(f'{int(POLL_INTERVAL)}s')
                # timestr=datetime.strftime(rounded_timestr,'%Y%m%d%H%M%S')
                sessions_data.append({
                    'Timestamp': datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'),
                    'Storage': storage_system['Name'],
                    'vserver': cn['svm']['name'],
                    'lifaddress': cn['server_ip'],
                    'ServerIP': cn['client_ip'],
                    'Volume': cn['volume']['name'],
                    'Username': 'None',
                    'Protocol': 'NFS'
                })
    except requests.exceptions.HTTPError as e:
        # print(f"HTTP Error {e.args[0]}")
        logger.error(f"HTTP Error {e.args[0]}")
    except TypeError as e:
        # print('TypeError: %s', e)
        logger.error('Type error: %s', e)
    except Exception as e:
        # print(f"Error {e}")
        traceback.print_exc()
        logger.error(f"Unexpected error during NFS data collection: {e}")
        logger.debug(traceback.format_exc())
    finally:
        with SessionsDB("netapp_sessions.db") as db:
            db.insert_sessions_df(pd.DataFrame(sessions_data))


def main():
    logger = logging.getLogger('NetAppCollector.Main')
    logger.info("Starting NetApp Data Collector")

    try:
        with open('storage_credentials.json', 'r') as sf:
            storage = json.load(sf)
            logger.info(f"Loaded configuration for {len(storage['StorageList'])} storage systems")
    except FileNotFoundError:
        logger.critical("Storage credentials file not found")
        return
    except json.JSONDecodeError:
        logger.critical("Invalid JSON in storage credentials file")

    # Initialize the database and create tables
    try:
        with SessionsDB("netapp_sessions.db") as db:
            db.create_sessions_table()
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}")
        return

    POLL_INTERVAL = storage['POLL_INTERVAL']
    SSL_VERIFY = storage['SSL_VERIFY']
    storagelist = storage['StorageList']

    try:
        while True:
            cycle_start = time.time()
            logger.info("Starting new collection cycle")
            active_threads = []
            for storage in storagelist:
                try:
                    logger.debug(f"Initializing collection for storage: {storage['Name']}")
                    storage_system = {
                        'Name': storage['Name'],
                        'Address': storage['StorageIP'],
                        'Credentials': [storage['StorageUser'], storage['StoragePassword']]
                    }
                    storage_system['netapp_storage'] = get_cluster_information(storage_system, SSL_VERIFY)
                    
                    cifs_thread = threading.Thread(
                        target=get_cifs_sessions_data,
                        args=(storage_system, SSL_VERIFY, POLL_INTERVAL)
                    )
                    nfs_thread = threading.Thread(
                        target=get_nfs_clients_data,
                        args=(storage_system, SSL_VERIFY, POLL_INTERVAL)
                    )
                    
                    cifs_thread.start()
                    nfs_thread.start()
                    active_threads.extend([cifs_thread, nfs_thread])
                except Exception as e:
                    logger.error(f"Error setting up collection for storage {storage['Name']}: {e}")

            # Wait for all threads to complete
            for thread in active_threads:
                thread.join()
            cycle_duration = time.time() - cycle_start
            logger.info(f"Collection cycle completed in {cycle_duration:.2f} seconds")

            sleep_time = max(0, POLL_INTERVAL - cycle_duration)
            if sleep_time > 0:
                logger.debug(f"Sleeping for {sleep_time:.2f} seconds")
                sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.critical(f"Unexpected error in main loop: {e}")
        logger.debug(traceback.format_exc())
    finally:
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
