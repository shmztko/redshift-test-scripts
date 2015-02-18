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
def get_queries
  query_conf = JSON.parse(File.read(QUERY_CONFIG), symbolize_names: true)
  params_conf = JSON.parse(File.read(REQUEST_PARAMS), symbolize_names: true)

  query_conf.map{|query_conf|
    {
      type: query_conf[:type],
      trial_count: query_conf[:trial_count],
      scripts: query_conf[:scripts],
      params: params_conf[:params][query_conf[:type].intern]
    }
  }
end

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
  if ARGV.size == 1 and ARGV[0] == '--help'
    log.warn """
      Invalid arguments.
      0 : interval sec between each trial (default 10sec)
    """
    exit 0
  end

  log.info "Query test command start."

  # 引数が指定されていない場合は、デフォルトで10秒待つ.
  interval_sec = ARGV.size >= 1 ? ARGV[0].to_f : 10


  db_config = get_db_config

  get_queries.each{|query|
    log.info "Query test start. -> #{query}"
    query[:trial_count].times{|i|
      log.info "  Trial-#{i} started."
      exec_query(query, db_config)
      log.info "  Trial-#{i} finished."
      sleep interval_sec
    }
  }
  log.info "Query test command finished."
end
