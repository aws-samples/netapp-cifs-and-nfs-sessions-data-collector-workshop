import pandas as pd
from matplotlib import pyplot as plt
from time import sleep

fileColumns     = ['timestamp','storage-name','node','vserver','session-id','connection-id','volume','share','path']
sessionColumns  = ['timestamp','storage-name','node','vserver','session-id','connection-id','volume','lif-address','address','windows-user']

with open('/usr/app/output/fsxn01_filesOpen.csv', encoding="utf-8") as f:
    filesOpen = pd.read_csv(f)

with open('/usr/app/output/fsxn01_sessions.csv', encoding="utf-8") as f:
    sessions = pd.read_csv(f)

cols_to_rename = {
    'node_y':'node',
    'storage-name_y': 'storage-name'
}

md=pd.merge(
    filesOpen, 
    sessions, 
    how="inner", 
    left_on=['timestamp','vserver', 'session-id', 'connection-id', 'volume'], 
    right_on=['timestamp','vserver', 'session-id', 'connection-id', 'volume']
    ).dropna(
        ).drop(
            columns=['node_x', 'storage-name_x']
            ).rename(
                    # Rename columns to remove _x labels
                    columns=cols_to_rename
                    )
mdg=md.groupby([ 'address',
                'storage-name',
                'node',
                'vserver',
                'lif-address',
                'volume',
                'share',
                'windows-user',
                'session-id',
                'connection-id', 
                'path',
                'Timestamp'
            ]).count().sort_values(by=['time'], ascending=True)

mdg.to_csv('/usr/app/output/out.csv')

print("Done combining data")