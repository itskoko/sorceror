require 'poseidon'
require 'zk'

module KafkaHelper
  def advance_offsets_forward!
    zk = ZK.new(Sorceror::Config.zookeeper_hosts.join(','))

    begin
      kafka = nil
      topics_offsets = {}

      zk.children('/brokers/topics').each do |topic|
        partitions = zk.children("/brokers/topics/#{topic}/partitions")
        broker = JSON.load(zk.get('/brokers/ids/0')[0])
        kafka  = Poseidon::Connection.new(broker["host"], broker["port"], "newrelic-conumser-lag", 1000)

        partition_requests = partitions.map { |partition| Poseidon::Protocol::PartitionOffsetRequest.new(partition.to_i, -1, 1) }
        offset_res = kafka.offset([Poseidon::Protocol::TopicOffsetRequest.new(topic, partition_requests)])

        offset_res[0].partition_offsets.each do |offset|
          topics_offsets[topic] ||= {}
          topics_offsets[topic][offset.partition] = offset.offsets.first.offset
        end
      end
    ensure
      kafka.close
    end

    consumers = Sorceror::Observer.observer_groups.keys.map { |k| "#{Sorceror::Config.app}.#{k}"}
    consumers << Sorceror::Config.app
    consumers.each do |consumer|
      topics_offsets.each do |topic, partitions|
        partitions.each do |partition, offset|
          path = "/consumers/#{consumer}/offsets/#{topic}/#{partition}"
          zk.mkdir_p(path)
          zk.set(path, offset.to_s)
        end
      end
    end
    true
  ensure
    zk.close
    zk.close! # otherwise connections lay around
  end

  def kafka_hosts
    ["localhost:#{KafkaHarness::KAFKA_PORT}"]
  end

  def zookeeper_hosts
    ["localhost:#{KafkaHarness::ZOOKP_PORT}"]
  end

  def create_topics!(prefix)
    $tc.create_topics(prefix)
  end
end

if ENV['POSEIDON_LOGGER_LEVEL']
  require 'poseidon'
  Poseidon.logger = Logger.new(STDOUT).tap { |l| l.level = ENV['POSEIDON_LOGGER_LEVEL'].to_i }
end

RSpec.configure do |config|
  if config.files_to_run.grep(/integration/).present?
    config.before(:suite) do
      $tc ||= KafkaHarness.new
      $tc.start
    end

    config.after(:suite) do
      $tc.stop
    end
  end
end
