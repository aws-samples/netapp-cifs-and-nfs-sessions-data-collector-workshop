# Steps to setup NFS and CIFS Sessions data collector to support storage migrations

> Note: This NetApp Data collector solution requires a Linux host with Docker compute engine installed and the container created on this Linux host should be able to connect to https port of the NetApp Storage systems.

---
## Follow these steps when using Amazon Linux EC2 instance to connect to on-prem NetApp Storage systems and for Amazon FSx for NetApp ONTAP

1. Install docker engine if not installed on the linux host.
   ```bash
    sudo yum install -y docker
    ```
2. Enable docker service on the linux host used for this project and enable the non-root user to run docker container using the following commands. Then download docker-compose binary and restart the host.
    ```bash
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker $USER
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64" -o /usr/bin/docker-compose
    sudo chmod +x /usr/bin/docker-compose
    sudo docker-compose --version
    sudo reboot
    ```
3. Verify the docker service is running by running *`docker info`*.
4. Verify the docker-compose is installed and is same or higher than version *v2.27.0* using this command `docker-compose -v`. (Docker Compose version v2.27.0)
5. Clone this git repository to this linux host and change directory to the cloned folder.
    ```
    git clone https://gitlab.aws.dev/atulac/netapp-connected-clients-datacollector
    cd netapp-connected-clients-datacollector
    ```
6. Add Netapp Storage Name, Username, Password and IP address in the **config_input.json** file in the **input** folder and save the file.
7. Start NetApp data collector container using **`docker-compose up -d`**.
    ```
    docker-compose up -d
    ```
   > [+] Running 2/0  
   > ✔ Network netapp-connected-clients-datacollector-main_default  
   > Created                                           0.0s  
   > ✔ Container netappcollector  
   > Created                                           0.0s  
   > Attaching to netappcollector  
   > netappcollector  | + pid=0  
   > netappcollector  | + trap 'kill ${!}; term_handler' SIGTERM  
   > netappcollector  | + pid=8  
   > netappcollector  | + true  
   > netappcollector  | + /bin/python3 /usr/app/code/nfs_data_collector.py  
   > netappcollector  | + wait 9  
   > netappcollector  | + sleep infinity  
   > netappcollector  | + /bin/python3 /usr/app/code/smb_data_collector.py  
   
8. Check the running containers and list files in **output** folder and view the trailing output as needed.
   ```bash
   docker ps
   ls -lh output/
   ```
9.  User can now safely logout from this linux host.

---
## Follow these steps when using RHEL8.6 VM to connect to on-prem NetApp Storage systems and for Amazon FSx for NetApp ONTAP

1. Uninstall old versions of `docker` or `docker-engine`.
   ```
   sudo yum remove docker \
                  docker-client \
                  docker-client-latest \
                  docker-common \
                  docker-latest \
                  docker-latest-logrotate \
                  docker-logrotate \
                  docker-engine \
                  podman \
                  runc
   ```
2. Delete old docker folders
   ```
   sudo rm -rf /var/lib/docker
   ``` 
3. Add Docker CE yum repository to yum-config-manager
   ```
   sudo yum install -y yum-utils
   sudo yum-config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
   ```
4. Install latest docker engine and accept the GPG key when prompted
   ```
   sudo yum install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
   ```
5. Follow rest of the instruction 2 through 9 as listed above [when using Amazon Linux EC2 instance](#follow-these-steps-when-using-amazon-linux-ec2-instance-to-connect-to-on-prem-netapp-storage-systems-and-for-amazon-fsx-for-netapp-ontap)

---
# Stopping data collection

1. SSH to the linux host running **netappcollector** container.
2. Verify the container is still running with `docker ps` command.
3. Stop container with `docker stop netappcollector` command and verify again if the container is stopped with `docker ps` command.

