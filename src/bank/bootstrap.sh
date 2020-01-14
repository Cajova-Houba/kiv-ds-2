#!/bin/bash
sudo stop bank

export DEBIAN_FRONTEND=noninteractive

# python
sudo apt-get install --force-yes -y python3
sudo apt-get install --force-yes -y python3-pip
sudo apt-get install --force-yes -y libzmq3-dev python-zmq

# mysql
echo 'mysql-server mysql-server/root_password password r00t' | debconf-set-selections
echo 'mysql-server mysql-server/root_password_again password r00t' | debconf-set-selections
apt-get install --force-yes -y mysql-server
apt-get install --force-yes -y python3-dev libpython3-dev

# prepare db
mysql -u root -pr00t < bank/schema.sql

# install
pip3 install mysql-connector-python
sudo pip3 install pyzmq

# copy contents
sudo cp bank/bank.conf /etc/init

sudo rm -f bank/balance.txt
rm -f bank/MARKER
sudo initctl reload-configuration
sudo start bank BANK_ID=$1