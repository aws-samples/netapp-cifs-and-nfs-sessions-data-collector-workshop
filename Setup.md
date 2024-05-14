# Steps to setup NetApp connected clients data collector

> Note: This NetApp Data collector solution requires a Linux host with Docker compute engine installed and the container created on this Linux host should be able to connect to https port of the NetApp Storage systems.

1. Enable docker service on the linux host used for this project and enable the non-root user to run docker container using the following commands. Then download docker-compose binary and restart the host.
    ```bash
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker $USER
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    sudo docker-compose --version
    sudo reboot
    ```

2. Verify the docker service is running by running *`docker info`*.
3. Verify the docker-compose is installed and is same or higher than version *v2.27.0* using this command `docker-compose -v`. (Docker Compose version v2.27.0)
4. Clone this git repository to this linux host and change directory to the cloned folder.
    ```
    git clone https://gitlab.aws.dev/atulac/netapp-connected-clients-datacollector
    cd netapp-connected-clients-datacollector
    ```
5. Add Netapp Storage Name, Username, Password and IP address in the **config_input.json** file in the **input** folder and save the file.
6. Start NetApp data collector container using **`docker-compose up -d`**.
    ```
    docker-compose up -d
    [+] Running 2/0
    ✔ Network netapp-connected-clients-datacollector-main_default  Created                                                                                                                                                                                                               0.0s 
    ✔ Container netappcollector                                    Created                                                                                                                                                                                                               0.0s 
    Attaching to netappcollector
    netappcollector  | + pid=0
    netappcollector  | + trap 'kill ${!}; term_handler' SIGTERM
    netappcollector  | + pid=8
    netappcollector  | + true
    netappcollector  | + /bin/python3 /usr/app/code/nfs_data_collector.py
    netappcollector  | + wait 9
    netappcollector  | + sleep infinity
    netappcollector  | + /bin/python3 /usr/app/code/smb_data_collector.py
    ```
