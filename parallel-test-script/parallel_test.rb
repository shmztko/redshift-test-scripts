#!/usr/bin/ruby
require 'date'
require 'json'
require 'logger'

# 設定ファイルのパス
DB_CONFIG = 'conf/db-config.json'
QUERY_CONFIG = 'conf/query-config.json'
REQUEST_PARAMS = 'conf/request-params.json'

# ロガー設定
log = Logger.new(STDOUT)
log.level = Logger::INFO

# DB接続情報を取得します。
def get_db_config
  JSON.parse(File.read(DB_CONFIG), symbolize_names: true)
end

# 設定ファイルからテスト用クエリの実行情報を取得します。
def get_quereis total_request
  query_conf = JSON.parse(File.read(QUERY_CONFIG), symbolize_names: true)
  params_conf = JSON.parse(File.read(REQUEST_PARAMS), symbolize_names: true)

  queries = []
  query_conf.each{|query_conf|
    # レートから算出した数分クエリ実行情報を格納する。
    exec_count = (query_conf[:rate] * total_request).round
    exec_count.times{|i|
      queries.push({
        type: query_conf[:type],
        scripts: query_conf[:scripts],
        params: params_conf[:params][query_conf[:type]]
      })
    }
  }
  queries
end

# PostgreSQLのクエリを非同期で発行します。
# PostgreSQLのクエリを非同期で発行します。
def exec_query query, db_config, async=false
  query[:scripts].each{|script|
    command = "PGPASSWORD=#{db_config[:db_pass]} "
    command += "psql -U #{db_config[:db_user]} -h #{db_config[:host]} -p #{db_config[:port]} -d #{db_config[:db_name]} "
    command += "-f sql/#{script} "
    
    # パラメータの付与
    query[:params].each{|key, value|
      command += " -v #{key}=#{value}"
    }
    # psql コマンドによる出力は表示しない
    command += " > /dev/null"
    command += " &" if async
    system command
  }
end

# このスクリプトが直接実行された時に行われる処理です。
# 並列クエリ実行のテストを行います。
# ARGV[0] : 実行するリクエスト総数
# ARGV[1] : 何分かけて指定されたリクエストを実行するか
if __FILE__ == $0
  if ARGV.length < 1
    log.warn """
      Invalid arguments.
      0 : total_request (*required)
      1 : execution_period(min)
    """
    exit 0
  end

  log.info "Parallel test command start."

  total_request = ARGV[0].to_f
  # 引数が指定されていない場合は、デフォルトで60min実行する。
  execution_period_min = ARGV.size >= 2 ? ARGV[1].to_f : 60
  request_period_sec = (execution_period_min * 60) / total_request

  log.info "  * Command run for #{execution_period_min} [min]."
  log.info "  * Total #{total_request} request is planned to be executed."
  log.info "  * Wait #{request_period_sec} [sec] between each request."
  log.info "  * Press CTRL+C to cancel this script."

  queries = get_quereis(total_request)  
  db_config = get_db_config
  count = 0;
  while queries.size > 0
    query = queries.delete_at(rand(queries.size))

    exec_query(query, db_config, true)
    log.info "[#{count}] Query Requested. #{query}"
    count+=1

    sleep request_period_sec
  end
end
