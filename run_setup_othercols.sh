#!/bin/bash
# Run ONCE before starting both concurrent clients.
# Wipes the cluster, creates the mixed table, starts Cassandra inside the cgroup
# (so the client script's memory cap takes effect without a restart).

CREATE_TABLE_MIX="create_table_mixed_compr_on"   # field0=EC, field1=REP -- CONFIRM
SSH_USER=rzp5412; CASS_DIR=/mydata/cassandra; CGROUP=/sys/fs/cgroup/mylimitedgroup

read -p "Nodes (3 or 5) [5]: " N; N=${N:-5}
[ "$N" = "3" ] && NODES=(2 3 4) || NODES=(2 3 4 5 6)

echo "=== HARD restart + wipe ==="
for node in "${NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill 2>/dev/null; true" & done; wait
for node in "${NODES[@]}"; do
    ip="10.10.1.$node"; a=0
    while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i cassandra >/dev/null 2>&1"; do
        sleep 10; a=$((a+1)); [ "$a" -ge 6 ] && { ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"; sleep 5; break; }
    done
done
for node in "${NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "rm -rf ${CASS_DIR}/data/" & done; wait

echo "=== Starting Cassandra inside cgroup (no memory cap yet) ==="
for node in "${NODES[@]}"; do
    ip="10.10.1.$node"
    ssh ${SSH_USER}@${ip} \
        "cd ${CASS_DIR} && echo \$\$ | sudo tee ${CGROUP}/cgroup.procs && bin/cassandra >/dev/null 2>&1"
    a=0
    until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
        sleep 10; a=$((a+1)); echo "  waiting ${ip} UN (${a}/30)"; [ "$a" -ge 30 ] && { echo "ERROR"; exit 1; }
    done; echo "  ${ip} UN (in cgroup)"
done

echo "=== Creating mixed table ==="
/mydata/${CREATE_TABLE_MIX}

echo ""; echo "Setup done. Now start run_othercols_client.sh on BOTH client machines."
