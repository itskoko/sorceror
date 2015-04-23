require 'poseidon'
require 'poseidon_cluster'

class Sorceror::Backend::Poseidon
  attr_accessor :connection, :connection_lock

  def initialize
    # The poseidon socket doesn't like when multiple threads access to it apparently
    @connection_lock = Mutex.new
    @distributor_threads = []
  end

  def new_connection
    client_id = ['sorceror', Sorceror::Config.app, Poseidon::Cluster.guid].join('.')
    @connection = ::Poseidon::Producer.new(Sorceror::Config.kafka_hosts, client_id,
                                          :type => :sync,
                                          :compression_codec => :none, # TODO Make configurable
                                          :metadata_refresh_interval_ms => 600_000,
                                          :max_send_retries => 10,
                                          :retry_backoff_ms => 100,
                                          :required_acks => -1,
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

  def publish(options={})
    Sorceror.ensure_connected

    @connection_lock.synchronize do
      raw_publish(options)
      options[:on_confirm].call if options[:on_confirm]
    end
  rescue StandardError => e
    Sorceror.warn("[publish] Failure publishing to kafka #{e}\n#{e.backtrace.join("\n")}")
    raise Sorceror::Error::Publisher.new(e, :payload => options[:payload])
  end

  def start_subscriber(consumer)
    num_threads = Sorceror::Config.subscriber_threads

    if consumer.in?([:all, :operation])
      Sorceror::Config.operation_topic.tap do |topic|
        @distributor_threads += num_threads.times.map { DistributorThread::Operation.new(topic: topic) }
        Sorceror.info "[distributor:operation] Starting #{num_threads} thread#{'s' if num_threads>1} topic:#{topic}"
      end
    end

    if consumer.in?([:all, :event])
      Sorceror::Observer.observer_groups.each do |group, options|
        @distributor_threads += num_threads.times.map { DistributorThread::Event.new(topic: Sorceror::Config.event_topic, group: group, options: options) }
        Sorceror.info "[distributor:event] Starting #{num_threads} thread#{'s' if num_threads>1} topic:#{Sorceror::Config.event_topic} and group:#{group}"
      end
    end
  end

  def stop_subscriber
    return if @distributor_threads.empty?
    Sorceror.info "[distributor] Stopping #{@distributor_threads.count} threads"

    @distributor_threads.each { |distributor_thread| distributor_thread.stop }
    @distributor_threads = nil
  end

  def subscriber_stopped?
    @distributor_threads.nil?
  end

  def show_stop_status(num_requests)
    @distributor_threads.to_a.each { |distributor_thread| distributor_thread.show_stop_status(num_requests) }
  end

  private

  def raw_publish(options)
    tries ||= 5
    if @connection.send_messages([Poseidon::MessageToSend.new(options[:topic], options[:payload], options[:topic_key])])
      Sorceror.info "[publish] [kafka] #{options[:topic]}/#{options[:topic_key]} #{options[:payload]}"
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

  class DistributorThread
    def initialize(options)
      @stop = false
      @thread = Thread.new(options) {|opt| main_loop(opt) }
    end

    def subscribe(options)
      # Override
    end

    def disconnect
      @consumer.close
    end

    def fetch_and_process_messages
      @consumer.fetch(:commit => false) do |partition, payloads|
        payloads.each do |payload|
          begin
            metadata = MetaData.new(@consumer, @group, partition, payload.offset)
            process(payload, metadata)
          rescue StandardError => e
            Sorceror.warn "[kafka] [receive] cannot process message: #{e}\n#{e.backtrace.join("\n")}"
            Sorceror::Config.error_notifier.call(e)
          end
        end
      end
    end

    def process(message)
      # Override
    end

    def main_loop(options)
      @consumer = subscribe(options)

      while not @stop do
        begin
          fetch_and_process_messages
        rescue Poseidon::Connection::ConnectionFailedError
          Sorceror.info "[kafka] Reconnecting... [#{@thread.object_id}]"
          @consumer = subscribe(options)
        end
        sleep 0.1
      end
    rescue StandardError => e
      Sorceror.warn "[kafka] [distributor] died: #{e}\n#{e.backtrace.join("\n")}"
      Sorceror::Config.error_notifier.call(e)
    ensure
      @consumer.close if @consumer
      @consumer = nil
    end

    def stop
      Sorceror.info "[distributor] stopping status:#{@thread.status} [#{@thread.object_id}]"

      # We wait in case the consumer is responsible for more than one partition
      # see: https://github.com/bsm/poseidon_cluster/blob/master/lib/poseidon/consumer_group.rb#L229
      @stop = true
      @thread.join
      Sorceror.info "[distributor] stopped [#{@thread.object_id}]"
    end

    def show_stop_status(num_requests)
      backtrace = @thread.backtrace

      STDERR.puts "Still processing messages (#{num_requests})"

      if num_requests > 1 && backtrace
        STDERR.puts
        STDERR.puts backtrace.map { |line| "  \e[1;30m#{line}\e[0m\n" }
        STDERR.puts
        STDERR.puts "I'm a little busy, check out my stack trace."
        STDERR.puts "Be patient (or kill me with -9, but that wouldn't be very nice of you)."
      else
        STDERR.puts "Just a second..."
      end
    end

    class MetaData
      attr_reader :partition

      def initialize(consumer, group, partition, offset)
        @consumer = consumer
        @group = group
        @partition = partition
        @offset = offset
      end

      def ack
        Sorceror.info "[kafka] [commit] topic:#{@consumer.topic} group:#{@group} offset:#{@offset+1} partition:#{@partition}"
        @consumer.commit(@partition, @offset+1)
      end
    end

    class Operation < self
      def subscribe(options)
        raise "No topic specified" unless options[:topic]

        @topic = options[:topic]
        @group = Sorceror::Config.app

        Sorceror.info "[distributor] Subscribing to topic:#{@topic} group:#{@group} [#{@thread.object_id}]"

        @consumer = ::Poseidon::ConsumerGroup.new(@group,
                                                  Sorceror::Config.kafka_hosts,
                                                  Sorceror::Config.zookeeper_hosts,
                                                  options[:topic],
                                                  :trail => Sorceror::Config.trail,
                                                  :max_wait_ms => 100)
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] #{payload.value} topic:#{@consumer.topic} group:#{@group} offset:#{payload.offset} parition:#{metadata.partition} #{Thread.current.object_id}"

        message = Sorceror::Message::Operation.new(payload.value, :metadata => metadata)
        Sorceror::Operation.process(message)
        message.ack
      end
    end

    class Event < self
      def subscribe(options)
        raise "No topic specified" unless options[:topic]
        raise "No group specified" unless options[:group]

        @topic      = options[:topic]
        @group_name = options[:group]
        @group      = "#{Sorceror::Config.app}.#{@group_name}"

        trail = options[:options].fetch(:trail, false)

        Sorceror.info "[distributor] Subscribing to topic:#{@topic} group:#{@group} [#{@thread.object_id}]"

        @consumer = ::Poseidon::ConsumerGroup.new(@group,
                                                  Sorceror::Config.kafka_hosts,
                                                  Sorceror::Config.zookeeper_hosts,
                                                  @topic,
                                                  :trail => trail,
                                                  :max_wait_ms => 10)
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] #{payload.value} topic:#{@consumer.topic} group:#{@group} offset:#{payload.offset} parition:#{metadata.partition} #{Thread.current.object_id}"

        message = Sorceror::Message::Event.new(payload.value, :metadata => metadata)
        Sorceror::Event.process(message, @group_name)
        message.ack
      end
    end
  end
end
