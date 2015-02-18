require 'pg'
require 'date'
require 'json'
require 'logger'

# 日付入力チェック用の正規表現
DATETIME_REGEXP =  /\A\d{1,4}\-\d{1,2}\-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}\Z/
# 設定ファイルのパス
DB_CONFIG = 'conf/db-config.json'
# ロガー設定
log = Logger.new(STDOUT)
log.level = Logger::INFO

## Redshift からクエリ実行ログを抽出します。
def extract_query_logs extract_from, extract_to
  query = """
    SELECT
      q.query AS query_id,
      q.starttime AS started_time,
      date_diff(\'microseconds\'::text, q.starttime, q.endtime) AS elapsed_microsec,
      qt.sequence AS query_part_seq,
      qt.text AS query_part
    FROM stl_query q
    INNER JOIN pg_user u
      ON q.userid = u.usesysid
    INNER JOIN stl_querytext qt
      ON q.query = qt.query
    WHERE
      $1 <= q.starttime AND q.starttime < $2
      AND u.usename = $3
    ORDER BY query_id, query_part_seq
  """
  db_conf = JSON.parse(File.read(DB_CONFIG), symbolize_names: true)
  conn = PG.connect(db_conf)
  begin
    query_logs = []
    prev_query_id = nil
    count = -1
    result = conn.exec(query, [extract_from, extract_to, db_conf[:user]])
    result.each{|row|
      if prev_query_id.nil? or prev_query_id != row['query_id']
        prev_query_id = row['query_id']
        query_logs.push({
          id: row['query_id'],
          started_time: row['started_time'],
          elapsed_microsec: row['elapsed_microsec'],
          query_text: row['query_part']
        })
        count += 1
      else
        query_logs[count][:query_text] += row['query_part']
      end
    }
    query_logs
  ensure
    conn.finish
  end
end

if __FILE__ == $0
  if ARGV.length < 2
    log.warn """
      This script require two datetime arguments(format is 'YYYY-mm-DDThh:MM:ss').
      0 : (*required) datetime of extract_from.
      1 : (*required) datetime of extract_to.
    """
    exit 1
  end

  is_valid_param = (ARGV[0] =~ DATETIME_REGEXP)
  is_valid_param = (ARGV[1] =~ DATETIME_REGEXP)
  unless is_valid_param
    log.warn "Invalid date format parameter #{ARGV[0]} / #{ARGV[1]}."
    log.warn "Parameter format is 'YYYY-mm-DDThh:MM:ss'"
    exit 1
  end
  extract_from = ARGV[0]
  extract_to = ARGV[1]
  log.info "Parallel test log extraction start."
  log.info "  * Query log extraction started."
  log.info "  * Query will extract from #{extract_from} to #{extract_to}."

  # クエリのログを出力
  extract_result = "query-log-#{extract_from}_#{extract_to}.log"
  File.open(extract_result, 'w'){|f|
    f.puts "\"query_id\",\"started_time\",\"elapsed_microsec\",\"query_text\""
    query_logs = extract_query_logs(DateTime.parse(extract_from), DateTime.parse(extract_to))
    query_logs.each{|query_log|
      f.puts "\"#{query_log[:id]}\",\"#{query_log[:started_time]}\",\"#{query_log[:elapsed_microsec]}\",\"#{query_log[:query_text]}\""
    }
  }
  log.info "Query log extraction successfully finished."
  log.info "Query extracted to #{extract_result}"
end
