# Starting data collection steps

```
docker-compose up -d
[+] Running 1/1
 ! data_collector Warning                                                                                                                                                                                  0.8s 
[+] Building 12.2s (7/8)                                                                                                                                                                                        
 => [data_collector internal] load build definition from Dockerfile                                                                                                                                        0.0s
 => => transferring dockerfile: 31B                                                                                                                                                                        0.0s
 => [data_collector internal] load .dockerignore                                                                                                                                                           0.0s
 => => transferring context: 2B                                                                                                                                                                            0.0s
 => [data_collector internal] load metadata for docker.io/library/python:latest                                                                                                                            0.6s
 => [data_collector 1/4] FROM docker.io/library/python@sha256:2586dd7abe015eeb6673bc66d18f0a628a997c293b41268bc981e826bc0b5a92                                                                             0.0s
 => [data_collector internal] load build context                                                                                                                                                           0.0s
 => => transferring context: 107B                                                                                                                                                                          0.0s
 => CACHED [data_collector 2/4] WORKDIR /usr/app                                                                                                                                                           0.0s
 => [data_collector 3/4] COPY requirements.txt /usr/app/                                                                                                                                                   0.0s
 => [data_collector 4/4] RUN pip install -r requirements.txt && rm requirements.txt                                                                                                                       11.5s
 => => # Collecting pandas==2.0.3 (from -r requirements.txt (line 3))                                                                                                                                          
 => => #   Downloading pandas-2.0.3.tar.gz (5.3 MB)                                                                                                                                                            
 => => #      ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 5.3/5.3 MB 52.8 MB/s eta 0:00:00                                                                                                                        
 => => #   Installing build dependencies: started                                                                                                                                                              
 => => #   Installing build dependencies: finished with status 'done'                                                                                                                                          
 => => #   Getting requirements to build wheel: started
```

