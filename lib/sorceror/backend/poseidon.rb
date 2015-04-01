require 'poseidon'
require 'poseidon_cluster'

class Sorceror::Backend::Poseidon
  attr_accessor :connection, :connection_lock

  def initialize
    # The poseidon socket doesn't like when multiple threads access to it apparently
    @connection_lock = Mutex.new
  end

  def new_connection
    client_id = ['sorceror', Sorceror::Config.app, Poseidon::Cluster.guid].join('.')
    @connection = ::Poseidon::Producer.new(Sorceror::Config.kafka_hosts, client_id,
                                          :type => :sync,
                                          :compression_codec => :none, # TODO Make configurable
                                          :metadata_refresh_interval_ms => 600_000,
                                          :max_send_retries => 10,
                                          :retry_backoff_ms => 100,
                                          :required_acks => 1,
                                          :ack_timeout_ms => 1000,
                                          :socket_timeout_ms => Sorceror::Config.socket_timeout)
  end

  def connect
    @connection = new_connection
  end

  def disconnect
    @connection_lock.synchronize do
      return unless connected?
      @connection.shutdown
      @connection = nil
    end
  end

  # TODO: extend Poseidon with a connected? method
  def connected?
    @connection.present?
  end

  def raw_publish(options)
    tries ||= 5
    if @connection.send_messages([Poseidon::MessageToSend.new(options[:topic], options[:payload], options[:topic_key])])
      Sorceror.debug "[publish] [kafka] #{options[:topic]}/#{options[:topic_key]} #{options[:payload]}"
    else
      raise Sorceror::Error::Publisher.new(Exception.new('There were no messages to publish?'), :payload => options[:payload])
    end
  rescue Poseidon::Errors::UnableToFetchMetadata => e
    Sorceror.error "[publish] [kafka] Unable to fetch metadata from the cluster (#{tries} tries left)"
    if (tries -= 1) > 0
      retry
    else
      raise e
    end
  rescue StandardError => e
    raise Sorceror::Error::Publisher.new(e, :payload => options[:payload])
  end

  def publish(options={})
    Sorceror.ensure_connected

    @connection_lock.synchronize do
      raw_publish(options)
      options[:on_confirm].call if options[:on_confirm]
    end
  rescue StandardError => e
    Sorceror.warn("[publish] Failure publishing to kafka #{e}\n#{e.backtrace.join("\n")}")
    e = Sorceror::Error::Publisher.new(e, :payload => options[:payload])

    if options[:async]
      Sorceror::Config.error_notifier.call(e)
    else
      raise e
    end
  end
end
