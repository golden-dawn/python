#!/bin/bash

echo 'Stopping the postgres server '
# sudo kill -INT `sudo head -1 /var/lib/postgresql/9.5/main/postmaster.pid`
sudo kill -INT `sudo head -1 /var/lib/postgresql/11/main/postmaster.pid`

NUM_PGSQL=`ps -C postgres | wc -l`
echo "After stopping server, $NUM_PGSQL lines returned by ps -C postgres"

if [ $NUM_PGSQL = 1 ]
then
    echo 'Successfully stopped the postgres server'
else
    echo 'Could not stop postgres, kill running processes first, then re-run script'
    exit 127
fi

echo "Thoroughly removing postgres"
sudo apt-get --purge remove postgresql\*

echo "Removing any postgres directories left"
sudo rm -rf /etc/postgresql/
sudo rm -rf /etc/postgresql-common/
sudo rm -rf /var/lib/postgresql/

echo "Removing postgres user and group"
sudo userdel -r postgres
sudo groupdel postgres

echo "Reinstalling postgres"
sudo apt-get install -y postgresql-11

echo "Create new super user cma with this command:"
echo "sudo -u postgres createuser -P -s -e cma"
