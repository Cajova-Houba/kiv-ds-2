#!/bin/bash
sudo stop bank

export DEBIAN_FRONTEND=noninteractive

# python
apt-get install --force-yes -y python3
apt-get install --force-yes -y python3-pip

# mysql
echo 'mysql-server mysql-server/root_password password r00t' | debconf-set-selections
echo 'mysql-server mysql-server/root_password_again password r00t' | debconf-set-selections
apt-get install --force-yes -y mysql-server
apt-get install --force-yes -y python3-dev libpython3-dev

# prepare db
mysql -u root -pr00t < bank/schema.sql

# install
pip3 install mysql-connector-python
pip3 install pyzmq

# copy contents
sudo cp bank/bank.conf /etc/init

sudo rm -f bank/balance.txt
sudo initctl reload-configuration
sudo start bank