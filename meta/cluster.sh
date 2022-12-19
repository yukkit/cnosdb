#!/bin/sh

rm -rf /tmp/cnosdb/meta

set -o errexit
cargo build

kill() {
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='metasrv_test'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall metasrv_test
        set -e
    fi
}

rpc() {
    local uri=$1
    local body="$2"

    echo '---'" rpc(:$uri, $body)"
    {
        if [ ".$body" = "." ]; then
            curl --silent "127.0.0.1:$uri"
        else
            curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
        fi
    } | {
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }
    echo
    echo
}

#export RUST_LOG=debug 
echo "Killing all running metasrv_test"

kill
sleep 1

echo "Start 3 uninitialized metasrv_test servers..."

nohup ../target/debug/metasrv_test  --id 1 --http-addr 127.0.0.1:21001 > ./1.log &
echo "Server 1 started"
sleep 1

nohup ../target/debug/metasrv_test  --id 2 --http-addr 127.0.0.1:21002 > ./2.log &
echo "Server 2 started"
sleep 1
#
nohup ../target/debug/metasrv_test  --id 3 --http-addr 127.0.0.1:21003 > ./3.log &

echo "Server 3 started"
sleep 1

echo "Initialize server 1 as a single-node cluster"
rpc 21001/init '{}'

echo "Server 1 is a leader now"
sleep 3

echo "Get metrics from the leader"
rpc 21001/metrics
sleep 1

echo "Adding node 2 and node 3 as learners, to receive log from leader node 1"

rpc 21001/add-learner       '[2, "127.0.0.1:21002"]'
echo "Node 2 added as leaner"
sleep 3

rpc 21001/add-learner       '[3, "127.0.0.1:21003"]'
echo "Node 3 added as leaner"
sleep 3

echo "Get metrics from the leader"
rpc 21001/metrics
sleep 1

echo "Changing membership from [1] to 3 nodes cluster: [1, 2, 3]"
echo
rpc 21001/change-membership '[1, 2, 3]'
sleep 1
echo "Membership changed"
sleep 1

echo "Get metrics from the leader again"
sleep 1
echo
rpc 21001/metrics
sleep 1

echo "Write data on leader 1"
sleep 1
echo
rpc 21001/write '{"Set":{"key":"foo3","value":"bar3"}}'
sleep 1
echo "Data written"
sleep 1

echo "Read on every node, including the leader"
sleep 1
echo "Read from node 1"
echo
rpc 21001/debug
echo "Read from node 2"
echo
rpc 21002/debug
echo "Read from node 3"
echo
rpc 21003/debug

kill