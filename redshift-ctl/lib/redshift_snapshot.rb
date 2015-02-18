# Wwrapper class of Redshift Snapshot.
class Snapshot
  attr_reader :id

  def initialize client, snapshot_id
    @client = client
    @id = snapshot_id
  end

  def describe
    begin
      result = @client.describe_cluster_snapshots({snapshot_identifier: self.id})
      if result[:snapshots].length == 1
        result[:snapshots][0]
      else
        raise "More than one snapshots found. (snapshot_id=#{self.id})"
      end
    rescue AWS::Redshift::Errors::ClusterSnapshotNotFound => e
      nil
    end
  end

  def exists?
    self.describe.nil? == false
  end

  def delete
    @client.delete_cluster_snapshot({snapshot_identifier: self.id})
  end
end