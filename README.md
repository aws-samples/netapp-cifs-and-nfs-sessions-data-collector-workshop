# NetApp CIFS and NFS sessions data collector

NetApp CIFS and NFS sessions data collector is a container based solution that uses NetApp REST APIs to discover hosts that are accessing the NetApp shares. This solution was developed to assist Storage Migration projects which helps to group the servers based on data accessed by these servers.

## Prerequisites

To be able to run this solution, you will need:

- A linux host with a sudo access user account and 50GB of available storage.
- Firewall ports enabled to connect to NetApp Storage systems using HTTPS (Port 443) from the linux host.
  - Test the network from linux host to NetApp storage systems by running this curl command to get the current version of the NetApp filesystem:  

    ```bash
    curl -k --request GET "https://${NETAPPIP}/api/cluster?fields=version" --user ${NETAPPUSER}:${NETAPPPASSWORD}
    ```

    your output should look like this

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

## Next steps

Follow the steps listed in this [setup guide](./Setup.md) to start the data collection and start planning your next storage migration to Amazon FSx for NetApp ONTAP.

---

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.
