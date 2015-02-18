# Command runner for redshift boot script.
require File.expand_path('../redshift_cluster.rb', __FILE__)
require File.expand_path('../redshift_snapshot.rb', __FILE__)

class ShutdownRunner 
  
  WAIT_TIME = 10

  def initialize(client, cluster_conf, logger)
    @cluster_conf = cluster_conf
    @logger = logger
    @client = client
    @cluster = Cluster.new(client, cluster_conf[:cluster_identifier])
  end

  def run
    shutdown
  end

  def shutdown
    if @cluster.exists?
      @logger.info "Deleting cluster. (cluster_id=#{@cluster.id})"

      snapshot_id = get_final_cluster_snapshot_id
      @cluster_conf[:delete_options][:final_cluster_snapshot_identifier] = snapshot_id      
      @cluster.delete @cluster_conf[:delete_options]

      final_snapshot_checked = false
      deleting_start_checked = false
      while true do
        if final_snapshot_checked == false and @cluster.creating_final_snapshot?
          @logger.info "Creating final snapshot. (cluster_id=#{@cluster.id}, snapshot_id=#{snapshot_id})"
          final_snapshot_checked = true
        end

        if deleting_start_checked == false and @cluster.deleting?
          if @cluster_conf[:delete_options][:skip_final_cluster_snapshot]
            @logger.info "Final snapshot was not created."
          else
            @logger.info "Final snapshot was created with id '#{snapshot_id}'"
            save_snapshot_log snapshot_id
            delete_old_snapshot
          end
          deleting_start_checked = true
        end

        if @cluster.exists? == false
          @logger.info "Cluster was successfully deleted. (cluster_id=#{@cluster.id})"
          break
        else
          sleep WAIT_TIME
        end
      end
    else
      @logger.warn "Cluster '#{@cluster.id}' does not exists.'"
    end
  end

  def get_final_cluster_snapshot_id
    'snapshot-' + @cluster_conf[:cluster_identifier] + '-' + Time.now.strftime('%Y%m%d%H%M%S')
  end

  def create_or_get_snapshot_log log_path
    if File.exist?(log_path) == false
      FileUtils.mkdir_p(Pathname.new(log_path).parent)
      File.open(log_path,"w") do |f|
        f.puts("{ \"snapshots\": []}")
      end
    end
    JSON.parse(File.read(log_path), symbolize_names: true)
  end

  def save_snapshot_log snapshot_id
    log_path = @cluster_conf[:snapshot_options][:log_file_path]
    snapshot_log = create_or_get_snapshot_log log_path

    created_snapshot = Snapshot.new(@client, snapshot_id).describe
    snapshot_log[:snapshots].push({
      identifier: snapshot_id,
      created_at: created_snapshot[:snapshot_create_time],
      age: snapshot_log[:snapshots].length
    })
    File.write(log_path, JSON.pretty_generate(snapshot_log))
  end

  def delete_old_snapshot
    log_path = @cluster_conf[:snapshot_options][:log_file_path]
    snapshot_log = create_or_get_snapshot_log(log_path)
    snapshots = snapshot_log[:snapshots]
    while snapshots.length > @cluster_conf[:snapshot_options][:retention_ages] do
      old_snapshot = snapshots.sort_by!{|s| s[:age] }.delete_at(0)
      Snapshot.new(@client, old_snapshot[:identifier]).delete
      snapshot_log[:snapshots] = snapshots.map.with_index {|s, i|
        s[:age] = i
        s
      }
      File.write(log_path, JSON.pretty_generate(snapshot_log))
      @logger.info "Old snapshot was purged. (snapshot_id=#{old_snapshot[:identifier]})"
    end
  end

  private :shutdown, :save_snapshot_log, :get_final_cluster_snapshot_id

  # def delete_snapshot
  #   if @cluster.snapshot_exists?
  #     @logger.info "Deleting old snapshot. (snapshot_id=#{@cluster.snapshot_id})"
  #     @cluster.delete_snapshot
  #     while true do
  #       if @cluster.snapshot_exists? == false
  #         print "\n"
  #         @logger.info "Snapshot successfully deleted. (snapshot_id=#{@cluster.snapshot_id})"
  #         break
  #       else
  #         sleep WAIT_TIME
  #         print "."
  #       end
  #     end
  #   end
  # end
end