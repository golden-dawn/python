#!/bin/bash
sudo apt-get update
sudo apt-get install     apt-transport-https     ca-certificates     curl     software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo apt-key fingerprint 0EBFCD88
sudo add-apt-repository    "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs)  stable"
sudo apt-get update
sudo apt-get install docker-ce
sudo apt-get update
sudo apt-get install -y git python-pip python-dev build-essential emacs24
sudo apt-get -y autoremove
# Install virtualenv, autoenv, and emacs python IDE packages
sudo pip install virtualenv virtualenvwrapper autoenv
pip install rope jedi importmagic autopep8 flake8
mkdir -p /home/cma/.envs
echo 'export WORKON_HOME=/home/cma/.envs' >> /home/cma/.bashrc
echo 'source /usr/local/bin/virtualenvwrapper.sh' >> /home/cma/.bashrc
echo 'source /usr/local/bin/activate.sh' >> /home/cma/.bashrc
# generate SSH keys
mkdir -p /home/cma/.ssh
# configure git global email and name
git config --global user.name 'Golden Dawn'
git config --global user.email 'cma@yahoo.com'
