#!/bin/sh
# nodes.sh start N | stop | kill I : cluster-mode test processes on ports 8900+i sharing /tmp/alarm-test-data
W=${WORKERD:-$(cd "$(dirname "$0")/../.." && pwd)/bazel-bin/src/workerd/server/workerd}
D=/tmp/alarm-test-data; here=$(cd "$(dirname "$0")" && pwd)
case "$1" in
  start) mkdir -p $D; i=0; while [ $i -lt $2 ]; do
           NODE=n$i nohup "$W" serve --experimental --verbose "$here/config.capnp" --directory-path do-storage=$D --socket-addr http=127.0.0.1:$((8900+i)) > /tmp/alarm-test-$i.log 2>&1 &
           echo $! > /tmp/alarm-test-$i.pid; i=$((i+1)); done; sleep 2 ;;
  kill)  kill -9 $(cat /tmp/alarm-test-$2.pid) ;;
  stop)  for f in /tmp/alarm-test-*.pid; do kill -9 $(cat $f) 2>/dev/null; rm -f $f; done; rm -rf $D ;;
esac
