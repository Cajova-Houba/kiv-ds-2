#!/bin/bash
sudo stop bank

export DEBIAN_FRONTEND=noninteractive

# python
apt-get install --force-yes -y python3
apt-get install --force-yes -y python3-pip

# install
pip3 install pyzmq

# copy contents
sudo cp state-collector/state-collector.conf /etc/init

sudo initctl reload-configuration
sudo start state-collector