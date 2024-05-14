# NetApp CIFS and NFS sessions data collector

NetApp CIFS and NFS sessions data collector is a container based solution that uses NetApp REST APIs to discover hosts that are accessing the NetApp shares. This solution was developed to assist Storage Migration projects which helps to group the servers based on data accessed by these servers. 

# Setup

This solution requires a linux host with access to HTTPS (TCP/443) port of the NetApp storage system.
You can test the connectivity by running curl command to get the current version of the NetApp filesystem as shown in this example:

```curl -k --request GET "https://${NETAPPIP}/api/cluster?fields=version" --user ${NETAPPUSER}:${NETAPPPASSWORD}```
```json
{
  "version": {
    "full": "NetApp Release 9.13.1P6: Tue Dec 05 16:06:25 UTC 2023",
    "generation": 9,
    "major": 13,
    "minor": 1
  },
  "_links": {
    "self": {
      "href": "/api/cluster"
    }
  }
}
```

Once you finish testing network access to all the NetApp storage systems required to collect CIFS and NFS sessions continue to follow the steps to [setup the data collector](./Setup.md).

# Data collection sequence
A sequence diagram to summarize the data collection workflow:

<img src='./images/NFS and CIFS Sessions data collector to support storage migrations.png' alt='Data collection sequence'>


# License Summary
This sample code is made available under the MIT-0 license. See the LICENSE file.