#!/usr/bin/env bash
set -x
pid=0
term_handler() {
  if [ $pid -ne 0 ]; then
    /usr/local/bin/python /usr/app/code/combine_data.py & wait ${!}
    echo "Completed Running Summary task"
    kill -SIGTERM "$pid"
    wait "$pid"
  fi
  exit 143; # 128 + 15 -- SIGTERM
}

trap 'kill ${!}; term_handler' SIGTERM

/bin/python3 /usr/app/code/smb_data_collector.py &
/bin/python3 /usr/app/code/nfs_data_collector.py &
pid="$!"

while true
do
  sleep infinity & wait ${!}
done