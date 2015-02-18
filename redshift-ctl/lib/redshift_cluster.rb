# Wwrapper class of Redshift Cluster.
class Cluster

  attr_reader :id

  def initialize client, cluster_id
    @client = client
    @id = cluster_id
  end

  def describe
    begin
      result = @client.describe_clusters({cluster_identifier: self.id})
      if result[:clusters].length == 1
        result[:clusters][0]
      else
        raise "More than one clusters found. (cluster_id=#{self.id})"
      end
    rescue AWS::Redshift::Errors::ClusterNotFound => e
      nil
    end
  end

  def exists?
    self.describe.nil? == false
  end 

  def available?
    cluster_info = self.describe
    if cluster_info.nil?
      false
    else
      cluster_info[:cluster_status] == 'available'
    end
  end

  def deleting?
    cluster_info = self.describe
    if cluster_info.nil?
      false
    else
      cluster_info[:cluster_status] == 'deleting'
    end
  end

  def creating_final_snapshot?
    cluster_info = self.describe
    if cluster_info.nil?
      false
    else
      cluster_info[:cluster_status] == 'final-snapshot'
    end
  end

  def restored?
    cluster_info = self.describe
    if cluster_info.nil?
      false
    else
      cluster_info[:restore_status][:status] == 'completed'
    end
  end

  def restore options
    if options.has_key?(:cluster_identifier) == false
      options[:cluster_identifier] = self.id
    end

    if options[:publicly_accessible] == false
      options.delete(:elastic_ip)
    end

    @client.restore_from_cluster_snapshot(options)
  end

  def delete options
    if options.has_key?(:cluster_identifier) == false
      options[:cluster_identifier] = self.id
    end

    if options[:skip_final_cluster_snapshot] == true
      options.delete(:final_cluster_snapshot_identifier)
    end

    @client.delete_cluster(options)
  end
end