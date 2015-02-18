#!/bin/sh
# 00 00 * * 1-5 
# TODO : gem の Whenever を使うと cron での実行が簡単になる？
export PATH=$PATH:/usr/local/bin:~/bin
SCRIPT_DIR=$(cd $(dirname $0);pwd)
cd $SCRIPT_DIR && bundle exec ruby redshift_ctl.rb $1