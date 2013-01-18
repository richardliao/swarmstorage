#!/usr/bin/env sh

killall -9 swarm
go build -o /tmp/swarm cmd/swarm/*.go
if [ $? != 0 ] ; then
    exit $?
fi

go build -o /tmp/swarmmount cmd/mount/*.go
if [ $? != 0 ] ; then
    exit $?
fi

sudo ifconfig eth0:1 192.168.200.1 netmask 255.255.255.248
sudo ifconfig eth0:2 192.168.200.2 netmask 255.255.255.248
sudo ifconfig eth0:3 192.168.200.3 netmask 255.255.255.248
sudo ifconfig eth0:4 192.168.200.4 netmask 255.255.255.248

/tmp/swarm daemonize /tmp/swarm serve -nodebase /data/swarm/node1 2>> /data/swarm/node1/logs/swarm.log
/tmp/swarm daemonize /tmp/swarm serve -nodebase /data/swarm/node2 2>> /data/swarm/node2/logs/swarm.log
/tmp/swarm daemonize /tmp/swarm serve -nodebase /data/swarm/node3 2>> /data/swarm/node3/logs/swarm.log
/tmp/swarm daemonize /tmp/swarm serve -nodebase /data/swarm/node4 2>> /data/swarm/node4/logs/swarm.log
