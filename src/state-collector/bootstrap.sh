#!/bin/bash
sudo stop state-collector

export DEBIAN_FRONTEND=noninteractive

# python
sudo apt-get install --force-yes -y python3
sudo apt-get install --force-yes -y python3-pip
sudo apt-get install --force-yes -y libzmq3-dev python-zmq

# install
sudo pip3 install pyzmq

# copy contents
sudo cp state-collector/state-collector.conf /etc/init

sudo initctl reload-configuration
sudo start state-collector