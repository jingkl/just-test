#!/bin/bash

cd $1 || exit
git clone https://github.com/milvus-io/milvus-helm.git; git fetch; git checkout $2; git pull
