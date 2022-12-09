#!/bin/bash

wget https://www.python.org/ftp/python/3.6.15/Python-3.6.15.tar.xz
tar -xf Python-3.6.15.tar.xz
mv Python-3.6.15 /opt/
apt-get update && apt-get install -y apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev
cd /opt/Python-3.6.15
./configure --enable-optimizations --enable-shared
make
make altinstall
