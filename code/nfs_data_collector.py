import threading
import queue
from time import sleep
import requests
import base64
import json
from defusedcsv.csv import writer 
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
import traceback
import re

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

global SSL_verify

def getClusterInformation(storageSystem, SSL_VERIFY):
    #Variable for Cluster information
    clusterDict = {}

    clusterAddress = storageSystem['Address']
    username = storageSystem['Credentials'][0]
    password = storageSystem['Credentials'][1]

    #Adding URL, usr, and password to the Cluster Dictionary
    clusterDict['url'] = 'https://'+clusterAddress
    AuthBase64String = base64.encodebytes(
            ('{}:{}'.format(username, password)
        ).encode()).decode().replace('\n', '')

    clusterDict['header'] = {
        'authorization': "Basic %s" % AuthBase64String
    }

    #String for cluster api call
    clusterString = "/api/cluster"

    #Get Call for cluster information
    try:
        clusterNameReq = requests.get(clusterDict['url']+clusterString,
            headers=clusterDict['header'],
            verify=SSL_VERIFY,
            timeout=(5, 120))
        clusterNameReq.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error {e.args[0]}")
    #catch clusterNameReq.status_code

    #Adding cluster's name to dictionary
    clusterDict['name'] = clusterNameReq.json()['name']

    #String for getting intercluster IP Addresses (Needs to be updated to limit to specific SVM)
    networkIntString = "/api/network/ip/interfaces?services=intercluster-core&fields=ip.address"

    #Get call for IP Addresses
    try:
        networkIntReq = requests.get(clusterDict['url']+networkIntString,
            headers=clusterDict['header'],
            verify=SSL_VERIFY,
            timeout=(5,120))
        networkIntReq.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error {e.args[0]}")

    #Adding interfaces to an array in the dictionary
    clusterDict['interfaces'] = []
    for record in networkIntReq.json()['records']:
        clusterDict['interfaces'].append(record['ip']['address'])

    return clusterDict


def getNfsClientsData(storageSystem, SSL_VERIFY):
    q = storageSystem['nfsClientQueue']
    netapp_storage = storageSystem['netapp_storage']
    pollInterval = storageSystem['pollInterval']

    while True:
        clusterString='/api/protocols/nfs/connected-clients'
        parameters='?return_timeout=25&return_records=true&max_records=10000&idle_duration=PT*'
        try:
            cNfsClientsReq = requests.get(netapp_storage['url']+clusterString+parameters,
                            headers=netapp_storage['header'],
                            verify=SSL_VERIFY,
                            timeout=(5,120))
            cNfsClientsReq.raise_for_status()
            cNfsClients = cNfsClientsReq.json()['records']
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error {e.args[0]}")

        try:

            for cn in cNfsClients:
                idle = split_time_string(cn['idle_duration'])
                c1 = (idle <= pollInterval)
                c2 = (cn['volume']['name'] != f"{cn['svm']['name']}_root")
                if c1 and c2:
                    rounded_dt = pd.Timestamp(datetime.now()).round(f'{pollInterval}s')
                    time=datetime.strftime(rounded_dt,'%Y%m%d%H%M%S')
                    nfsSessionData=[
                            time,
                            storageSystem['Name'],
                            cn['svm']['name'],
                            cn['server_ip'],
                            cn['client_ip'],
                            cn['volume']['name']          
                        ]
                    q.put(nfsSessionData)
            sleep(pollInterval)
        except TypeError:
            print("Ignored results with TypeErrors")


def readNfsClientsQueue(storageSystem):
    q = storageSystem['nfsClientQueue']
    storageName = storageSystem['Name']
    sessionColumns = [
            'timestamp',
            'storage-name',
            'vserver',
            'lif-address',
            'address',
            'volume'
        ]
    while  True:
        # Create new CSV file for each day
        date = pd.Timestamp(datetime.now()).strftime('%Y%m%d')
        target_folder = f'/usr/app/output/'
        # target_folder = f'./'
        csvfile = f'{target_folder}/{storageName}_{date}_nfs_sessions.csv'    
        if not os.path.isfile(csvfile):
            with open(csvfile, "w", encoding="utf-8") as cf:
                writer_object = writer(cf)
                writer_object.writerow(sessionColumns)

        # Write SMB Sessions data to CSV
        sessionData=q.get()
        # print(sessionData)
        with open(csvfile, "a", encoding="utf-8") as cf:
            writer_object = writer(cf)
            writer_object.writerow(sessionData)


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


def combineOutputs(storageConfigs):
    # Dedupe CSV files to summarize by date
    return True


def main():
    # Read the config json file
    with open('/usr/app/input/config_input.json', encoding="utf-8") as f:
        storageConfigs = json.load(f)
    storageList = storageConfigs['storageList']
    pollInterval = storageConfigs['pollInterval']
    SSL_VERIFY = storageConfigs['SSL_VERIFY']
    # Create queue and the threads for each storage system
    for storageSystem in storageList:
        storageSystem['pollInterval'] = pollInterval
        storageSystem['nfsClientQueue'] = queue.Queue()
        storageSystem['netapp_storage'] = getClusterInformation(storageSystem,SSL_VERIFY)
        storageSystem['getNfsClientsData'] = threading.Thread(target=getNfsClientsData, 
                                                args=(storageSystem,SSL_VERIFY,))
        storageSystem['readtNfsClientsData'] = threading.Thread(target=readNfsClientsQueue, 
                                                args=(storageSystem,))

    # Start all threads
    for storageSystem in storageList:
        storageSystem['getNfsClientsData'].start()
        storageSystem['readtNfsClientsData'].start()

    # Join all threads
    for storageSystem in storageList:
        storageSystem['getNfsClientsData'].join()
        storageSystem['readtNfsClientsData'].join()

    combineOutputsThread = threading.Thread(target=combineOutputs,
                                args=(storageConfigs,))    
    combineOutputsThread.start()
    combineOutputsThread.join()

if __name__ == "__main__":
    main()
