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

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

global SSL_VERIFY

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
            timeout=(5, 120))
        networkIntReq.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error {e.args[0]}")

    #Adding interfaces to an array in the dictionary
    clusterDict['interfaces'] = []
    for record in networkIntReq.json()['records']:
        clusterDict['interfaces'].append(record['ip']['address'])

    return clusterDict


def getSessionsData(storageSystem, SSL_VERIFY):
    q = storageSystem['sessionsQueue']
    netapp_storage = storageSystem['netapp_storage']
    pollInterval = storageSystem['pollInterval']
    while True:
        # Netapp cifs Sessions API
        clusterString='/api/protocols/cifs/sessions'
        parameters='?return_timeout=15&return_records=true&max_records=10000'
        try:
            cSessionsReq = requests.get(netapp_storage['url']+clusterString+parameters,
                            headers=netapp_storage['header'],
                            verify=SSL_VERIFY,
                            timeout=(5, 120))
            cSessionsReq.raise_for_status()
            cSessions = cSessionsReq.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error {e.args[0]}")

        for record in cSessions['records']:
            sessionData=[]
            sessionLink = record['_links']['self']['href']
            try:
                sessionResponseReq = requests.get(netapp_storage['url']+sessionLink,
                                    headers=netapp_storage['header'],
                                    verify=SSL_VERIFY,
                                    timeout=(5, 120))
                sessionResponseReq.raise_for_status()
                sessionResponse = sessionResponseReq.json()
            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error {e.args[0]}")
            for vol in sessionResponse['volumes']:
                rounded_dt = pd.Timestamp(datetime.now()).round(f'{pollInterval}s')
                time=datetime.strftime(rounded_dt,'%Y%m%d%H%M%S')
                sessionData = [
                    # Time rounded to latest 15th min
                    time,
                    storageSystem['Name'],
                    # Storage Node name (node)
                    netapp_storage['name'],
                    # SVM Name (vserver)
                    sessionResponse['svm']['name'],
                    # Session Identifier (session-id)
                    sessionResponse['identifier'],
                    # Client Identifier (connection-id)
                    sessionResponse['connection_id'],
                    # Volume name (volume)
                    vol['name'],
                    # SVM IP address (lif-address)
                    sessionResponse['server_ip'],
                    # Client IP address (address)
                    sessionResponse['client_ip'],
                    # Windows User (windows-user)
                    sessionResponse['user']
                ]
                if len(sessionData) > 0:
                    q.put(sessionData)
        sleep(pollInterval)


def getFilesData(storageSystem, SSL_VERIFY):
    q = storageSystem['filesQueue']
    netapp_storage = storageSystem['netapp_storage']
    pollInterval = storageSystem['pollInterval']
    while True:
        cFileString='/api/protocols/cifs/session/files'
        parameters='?return_timeout=15&return_records=true&max_records=10000'
        try:
            cFDataReq = requests.get(netapp_storage['url']+cFileString+parameters,
                        headers=netapp_storage['header'],
                        verify=SSL_VERIFY,
                        timeout=(5, 120))
            cFDataReq.raise_for_status()
            cFData = cFDataReq.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error {e.args[0]}")

        cFDetails = []
        if len(cFData['records']) > 0:
                for cFRecord in cFData['records']:
                    cFileString="/api/protocols/cifs/session/files/{}/{}/{}/{}/{}".format(
                                    cFRecord['node']['uuid'], 
                                    cFRecord['svm']['uuid'], 
                                    cFRecord['identifier'], 
                                    cFRecord['connection']['identifier'], 
                                    cFRecord['session']['identifier']
                                )
                    try:
                        cFDetailsReq = requests.get(netapp_storage['url']+cFileString,
                            headers=netapp_storage['header'],
                            verify=SSL_VERIFY,
                            timeout=(5, 120))
                        cFDetailsReq.raise_for_status()
                        cFDetails.append(cFDetailsReq.json())
                    except requests.exceptions.HTTPError as e:
                        print(f"HTTP Error {e.args[0]}")
        if len(cFDetails) > 0:
            for cFRecord in cFDetails:
                rounded_dt = pd.Timestamp(datetime.now()).round(f'{pollInterval}s')
                time=datetime.strftime(rounded_dt,'%Y%m%d%H%M%S')
                q.put([
                    time,
                    storageSystem['Name'],
                    cFRecord['node']['name'],
                    cFRecord['svm']['name'],
                    cFRecord['session']['identifier'],
                    cFRecord['connection']['identifier'],
                    cFRecord['volume']['name'],
                    cFRecord['share']['name'],
                    cFRecord['path']
                ])
        sleep(pollInterval)


def readSessionsQueue(storageSystem):
    q = storageSystem['sessionsQueue']
    storageName = storageSystem['Name']
    sessionColumns = [
        'timestamp',
        'storage-name',
        'node',
        'vserver',
        'session-id',
        'connection-id',
        'volume',
        'lif-address',
        'address',
        'windows-user'
    ]
    while  True:

        # Create new CSV file for each day
        date = pd.Timestamp(datetime.now()).strftime('%Y%m%d')
        target_folder = f'/usr/app/output/'
        csvfile = f'{target_folder}/{storageName}_{date}_sessions.csv'    
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


def readFilesQueue(storageSystem):
    q = storageSystem['filesQueue']
    storageName = storageSystem['Name']
    filesColumns = [
        'timestamp',
        'storage-name',
        'node',
        'vserver',
        'session-id',
        'connection-id',
        'volume',
        'share',
        'path'
    ]

    while True:
        # Create new CSV file for each day
        date = pd.Timestamp(datetime.now()).strftime('%Y%m%d')
        target_folder = f'/usr/app/output/'
        csvfile = f'{target_folder}/{storageName}_{date}_filesOpen.csv'  
        if not os.path.isfile(csvfile):
            with open(csvfile, "w", encoding="utf-8") as cf:
                writer_object = writer(cf)
                writer_object.writerow(filesColumns)

        # Write Open Files data to CSV
        fileData=q.get()
        # print(fileData)
        with open(csvfile, "a", encoding="utf-8") as cf:
            writer_object = writer(cf)
            writer_object.writerow(fileData)


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
        storageSystem['sessionsQueue'] = queue.Queue()
        storageSystem['filesQueue'] = queue.Queue()
        storageSystem['netapp_storage'] = getClusterInformation(storageSystem, SSL_VERIFY)
        storageSystem['getSessionsData'] = threading.Thread(target=getSessionsData, 
                                                args=(storageSystem, SSL_VERIFY,))
        storageSystem['getFilesData'] = threading.Thread(target=getFilesData, 
                                            args=(storageSystem, SSL_VERIFY,))
        storageSystem['readSessionsData'] = threading.Thread(target=readSessionsQueue, 
                                                args=(storageSystem,))
        storageSystem['readFilesData'] = threading.Thread(target=readFilesQueue, 
                                            args=(storageSystem,))

    
    # Start all threads
    for storageSystem in storageList:
        storageSystem['getSessionsData'].start()
        storageSystem['getFilesData'].start()
        storageSystem['readSessionsData'].start()
        storageSystem['readFilesData'].start()

    # Join all threads
    for storageSystem in storageList:
        storageSystem['getSessionsData'].join()
        storageSystem['getFilesData'].join()
        storageSystem['readSessionsData'].join()
        storageSystem['readFilesData'].join()

    combineOutputsThread = threading.Thread(target=combineOutputs,
                                args=(storageConfigs,))    
    combineOutputsThread.start()
    combineOutputsThread.join()


if __name__ == "__main__":
    main()
