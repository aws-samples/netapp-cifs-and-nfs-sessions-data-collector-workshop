#!/usr/bin/env bash
set -x
pid=0
term_handler() {
  if [ $pid -ne 0 ]; then
    echo "Stopping NetApp collector"
    kill -SIGTERM "$pid"
    wait "$pid"
  fi
  exit 143; # 128 + 15 -- SIGTERM
}

trap 'kill ${!}; term_handler' SIGTERM

/usr/local/bin/python3 /usr/netappcollector/commons/setupDb.py
/usr/local/bin/python3 /usr/netappcollector/commons/netappCollector.py &
/usr/local/bin/streamlit run /usr/netappcollector/app/Home.py --server.port=8080 --server.address=0.0.0.0 --browser.gatherUsageStats=false &
pid="$!"

while true
do
  sleep infinity & wait ${!}
done