#!/bin/sh
export PATH=$PATH:/usr/local/bin:~/bin
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR && ruby prallel_test.rb $1
  
