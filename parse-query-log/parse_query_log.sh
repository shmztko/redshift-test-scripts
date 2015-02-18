#!/bin/sh
export PATH=$PATH:/usr/local/bin:~/bin
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR && ruby parse_query_log.rb $1
  
