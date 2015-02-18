# Command runner for redshift boot script.
require File.expand_path('../redshift_cluster.rb', __FILE__)
require File.expand_path('../redshift_snapshot.rb', __FILE__)

class StartupRunner
  
  WAIT_TIME = 10

  def initialize(client, cluster_conf, logger)
    @cluster_conf = cluster_conf
    @logger = logger
    @cluster = Cluster.new(client, cluster_conf[:cluster_identifier])
  end

  def run
    startup
  end

  def startup
    if @cluster.exists? == false
      snapshot_id = get_latest_snapshot_id
      @cluster_conf[:restore_options][:snapshot_identifier] = snapshot_id
      @logger.info "Restoring cluster from snapshot. (cluster_id=#{@cluster.id}, snapshot_id=#{snapshot_id})"
      @cluster.restore @cluster_conf[:restore_options]

      availability_checked = false
      while true do
        if availability_checked == false and @cluster.available?
          @logger.info "Cluster is available now. (cluster_id=#{@cluster.id}, snapshot_id=#{snapshot_id})"
          @logger.info "Restoring started. (cluster_id=#{@cluster.id}, snapshot_id=#{snapshot_id})"
          availability_checked = true
        end

        if @cluster.restored?
          @logger.info "Cluster was successfully restored from snapshot. (cluster_id=#{@cluster.id}, snapshot_id=#{snapshot_id})"
          break
        else
          sleep WAIT_TIME
        end
      end
    else
      @logger.warn "Cluster '#{@cluster.id}' already exists."
    end
  end

  def get_latest_snapshot_id
    log_path = @cluster_conf[:snapshot_options][:log_file_path]
      if File.exists? log_path
        snapshot_log = JSON.parse(File.read(log_path), symbolize_names: true)
        latest_snapshot = snapshot_log[:snapshots].sort_by{|s| s[:age] }.reverse[0]
        latest_snapshot[:identifier]
    else
      raise "Snapshot log (#{log_path}) was not found for cluster '#{@cluster_conf[:cluster_identifier]}'."
    end
  end

  private :startup
end