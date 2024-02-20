import threading
import queue
from time import sleep
import requests
import base64
import json
from csv import writer
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
import traceback

from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SSL_VERIFY = False

def getClusterInformation(storageSystem):
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
    clusterNameReq = requests.get(clusterDict['url']+clusterString,
        headers=clusterDict['header'],
        verify=SSL_VERIFY,
        timeout=(5, 120))
    #catch clusterNameReq.status_code

    #Adding cluster's name to dictionary
    clusterDict['name'] = clusterNameReq.json()['name']

    #String for getting intercluster IP Addresses (Needs to be updated to limit to specific SVM)
    networkIntString = "/api/network/ip/interfaces?services=intercluster-core&fields=ip.address"

    #Get call for IP Addresses
    networkIntReq = requests.get(clusterDict['url']+networkIntString,
        headers=clusterDict['header'],
        verify=SSL_VERIFY,
        timeout=(5, 120))

    #Adding interfaces to an array in the dictionary
    clusterDict['interfaces'] = []
    for record in networkIntReq.json()['records']:
        clusterDict['interfaces'].append(record['ip']['address'])

    return clusterDict


def getSessionsData(storageSystem):
    q = storageSystem['sessionsQueue']
    netapp_storage = storageSystem['netapp_storage']
    pollInterval = storageSystem['pollInterval']
    while True:
        # Netapp cifs Sessions API
        clusterString='/api/protocols/cifs/sessions'
        parameters='?return_timeout=15&return_records=true&max_records=10000'
        cSessions = requests.get(netapp_storage['url']+clusterString+parameters,
                        headers=netapp_storage['header'],
                        verify=SSL_VERIFY,
                        timeout=(5, 120)).json()
        for record in cSessions['records']:
            sessionData=[]
            sessionLink = record['_links']['self']['href']
            sessionResponse = requests.get(netapp_storage['url']+sessionLink,
                                headers=netapp_storage['header'],
                                verify=SSL_VERIFY,
                                timeout=(5, 120)).json()
            sessionColumns = [
                'node',
                'storage-name',
                'vserver', 
                'session-id',
                'connection-id',
                'lif-address', 
                'address',
                'windows-user',
                'volume'
            ]
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


def getFilesData(storageSystem):
    q = storageSystem['filesQueue']
    netapp_storage = storageSystem['netapp_storage']
    pollInterval = storageSystem['pollInterval']
    while True:
        cFileString='/api/protocols/cifs/session/files'
        parameters='?return_timeout=15&return_records=true&max_records=10000'
        cFData = requests.get(netapp_storage['url']+cFileString+parameters,
                    headers=netapp_storage['header'],
                    verify=SSL_VERIFY,
                    timeout=(5, 120)).json()
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
                    cFDetails.append(requests.get(netapp_storage['url']+cFileString,
                        headers=netapp_storage['header'],
                        verify=SSL_VERIFY,
                        timeout=(5, 120)).json())
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
    netapp_storage = storageSystem['netapp_storage']
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
            with open(csvfile, "w") as cf:
                writer_object = writer(cf)
                writer_object.writerow(sessionColumns)

        # Write SMB Sessions data to CSV
        sessionData=q.get()
        # print(sessionData)
        with open(csvfile, "a") as cf:
            writer_object = writer(cf)
            writer_object.writerow(sessionData)


def readFilesQueue(storageSystem):
    q = storageSystem['filesQueue']
    storageName = storageSystem['Name']
    netapp_storage = storageSystem['netapp_storage']
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
            with open(csvfile, "w") as cf:
                writer_object = writer(cf)
                writer_object.writerow(filesColumns)

        # Write Open Files data to CSV
        fileData=q.get()
        # print(fileData)
        with open(csvfile, "a") as cf:
            writer_object = writer(cf)
            writer_object.writerow(fileData)


def combineOutputs(storageConfigs):
    pass


def main():
    # Read the config json file
    with open('/usr/app/input/config_input.json') as f:
        storageConfigs = json.load(f)
    storageList = storageConfigs['storageList']
    pollInterval = storageConfigs['pollInterval']
    # CSV File headers for Storage Assessment
    outColumns = ['timestamp',
                    'storageName',
                    'node',
                    'vserver',
                    'session-id',
                    'connection-id',
                    'share',
                    'lif-address',
                    'address',
                    'windows-user']
    # Create queue and the threads for each storage system
    for storageSystem in storageList:
        storageSystem['pollInterval'] = pollInterval
        storageSystem['sessionsQueue'] = queue.Queue()
        storageSystem['filesQueue'] = queue.Queue()
        storageSystem['netapp_storage'] = getClusterInformation(storageSystem)
        storageSystem['getSessionsData'] = threading.Thread(target=getSessionsData, 
                                                args=(storageSystem,))
        storageSystem['getFilesData'] = threading.Thread(target=getFilesData, 
                                            args=(storageSystem,))
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
