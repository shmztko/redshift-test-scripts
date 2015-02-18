#!/usr/bin/ruby
$LOAD_PATH << File.expand_path('../lib', __FILE__)
require 'logger'
require 'date'
require 'json'
require 'fileutils'
require 'parallel'
require 'aws-sdk'
require 'redshift_shutdown_runner'
require 'redshift_startup_runner'

# Script constants
CONFIG_FILE = 'conf/config.json'
LOG_FILE = 'logs/redshift_ctl.log'
STARTUP_CMD = 'startup'
SHUTDOWN_CMD = 'shutdown'
COMMANDS = [STARTUP_CMD, SHUTDOWN_CMD]

# Main
if __FILE__ == $0
  # Logger setting
  logger = Logger.new(LOG_FILE)

  if ARGV.length < 1
    logger.info """ One argument required.
      ARGV[0] : control action. (value shuold be '#{STARTUP_CMD}' or '#{SHUTDOWN_CMD}'.)
                #{STARTUP_CMD}  : Restore redshift cluster from a latest snapshot.
                #{SHUTDOWN_CMD} : Delete redshift cluster and take a final snapshot.
    """
    exit 1
  end

  cmd = ARGV[0]
  if COMMANDS.index(cmd).nil?
    logger.warn "Invalid action -> '#{cmd}. action shuold be '#{STARTUP_CMD}' or '#{SHUTDOWN_CMD}'."
    exit 1
  end

  config = JSON.parse(File.read(CONFIG_FILE), symbolize_names: true)
  redshift_client = AWS::Redshift.new(
    access_key_id: config[:access_key_id],
    secret_access_key: config[:secret_access_key],
    region: config[:region]
  ).client

  # Control multiple clusters at the same time.
  Parallel.each(config[:clusters], in_threads: config[:concurrent_execution_count]) {|cluster_conf|
    if cmd == STARTUP_CMD
      StartupRunner.new(redshift_client, cluster_conf, logger).run
    elsif cmd == SHUTDOWN_CMD
      ShutdownRunner.new(redshift_client, cluster_conf, logger).run
    else
      logger.warn "Invalid action was given."
    end
  }
end