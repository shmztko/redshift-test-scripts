#!/bin/bash
SCRIPT_DIR=$(cd $(dirname $0);pwd)

start_date=`date +'%Y%m%d%H%M%S'`
output_dir=single-test-result_$start_date
mkdir ./$output_dir
log_file=./$output_dir/single-test-$start_date.log

# Exeuting test
echo "`date +'%Y-%m-%dT%H:%M:%S'` - Test script started." >> $log_file
test_started_at=`date +'%Y-%m-%dT%H:%M:%S'`
bash ./parallel-test-script/parallel_test.sh $1 >> $log_file

## TODO ps -ef | grep psql | wc -l ‚Ì”‚ÅŠ®‘S‚ÉI‚í‚Á‚½‚©‚Ç‚¤‚©”»’f

test_finished_at=`date +'%Y-%m-%dT%H:%M:%S'`
echo "`date +'%Y-%m-%dT%H:%M:%S'` - Test script finished." >> $log_file


# Extracting test logs
echo "`date +'%Y-%m-%dT%H:%M:%S'` - Test log extraction started." >> $log_file
bash ./extract-query-log/extract_query_log.sh $test_started_at $test_finished_at >> $log_file
echo "`date +'%Y-%m-%dT%H:%M:%S'` - Test log extraction finished." >> $log_file

mv ./extract-query-log/query-log-${test_started_at}_${test_finished_at}.log ./parse-query-log/

# Parsing test logs
echo "`date +'%Y-%m-%dT%H:%M:%S'` - Test log parse started." >> $log_file
bash ./parse-query-log/parse_query_log.sh  query-log-${test_started_at}_${test_finished_at}.log > ./$output_dir/test-result_${test_started_at}_${test_finished_at}.log
echo "`date +'%Y-%m-%dT%H:%M:%S'` - Test log parse finished." >> $log_file

mv ./parse-query-log/query-log-${test_started_at}_${test_finished_at}.log ./$output_dir/

tail ./$output_dir/test-result_${test_started_at}_${test_finished_at}.log
