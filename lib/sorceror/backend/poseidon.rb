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

  def start_subscriber
    num_threads = Sorceror::Config.subscriber_threads
    Sorceror::Config.subscriber_topics.each do |topic|
      @distributor_threads ||= num_threads.times.map { DistributorThread.new(topic) }
      Sorceror.debug "[distributor] Starting #{num_threads} thread#{'s' if num_threads>1} topic:#{topic}"
    end
  end

  def stop_subscriber
    return unless @distributor_threads
    Sorceror.debug "[distributor] Stopping #{@distributor_threads.count} threads"

    @distributor_threads.each { |distributor_thread| distributor_thread.stop }
    @distributor_threads = nil
  end

  def show_stop_status(num_requests)
    @distributor_threads.to_a.each { |distributor_thread| distributor_thread.show_stop_status(num_requests) }
  end

  private

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

  class DistributorThread
    def initialize(topic)
      @stop = false
      @thread = Thread.new(topic) {|t| main_loop(t) }

      Sorceror.debug "[distributor] Subscribing to topic:#{topic} [#{@thread.object_id}]"
    end

    def subscribe(options)
      raise "No topic specified" unless options[:topic]

      @consumer = ::Poseidon::ConsumerGroup.new(Sorceror::Config.app,
                                                Sorceror::Config.kafka_hosts,
                                                Sorceror::Config.zookeeper_hosts,
                                                options[:topic],
                                                :trail => Sorceror::Config.trail,
                                                :max_wait_ms => 10)
    end

    def disconnect
      @consumer.close
    end

    def fetch_and_process_messages
      @consumer.fetch(:commit => false) do |partition, payloads|
        payloads.each do |payload|
          Sorceror.debug "[kafka] [receive] #{payload.value} topic:#{@consumer.topic} offset:#{payload.offset} parition:#{partition} #{Thread.current.object_id}"
          begin
            metadata = MetaData.new(@consumer, partition, payload.offset)
            message = Sorceror::Message.new(payload.value, :metadata => metadata)
            Sorceror::Operation.process(message)
            message.ack
          rescue StandardError => e
            Sorceror.warn "[kafka] [receive] cannot process message: #{e}\n#{e.backtrace.join("\n")}"
            Sorceror::Config.error_notifier.call(e)
          end
        end
      end
    end

    def main_loop(topic)
      @consumer = subscribe(:topic => topic)

      while not @stop do
        begin
          fetch_and_process_messages
        rescue Poseidon::Connection::ConnectionFailedError
          Sorceror.debug "[kafka] Reconnecting... [#{@thread.object_id}]"
          @consumer = subscribe(@topic)
        end
        sleep 0.1
      end
      @consumer.close if @consumer
      @consumer = nil
    end

    def stop
      Sorceror.debug "[distributor] stopping status:#{@thread.status} [#{@thread.object_id}]"

      # We wait in case the consumer is responsible for more than one partition
      # see: https://github.com/bsm/poseidon_cluster/blob/master/lib/poseidon/consumer_group.rb#L229
      @stop = true
      @thread.join
      Sorceror.debug "[distributor] stopped [#{@thread.object_id}]"
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
      def initialize(consumer, partition, offset)
        @consumer = consumer
        @partition = partition
        @offset = offset

        Sorceror.debug "[kafka] [metadata] topic:#{@consumer.topic} offset:#{offset} partition:#{partition}"
      end

      def ack
        Sorceror.debug "[kafka] [commit] topic:#{@consumer.topic} offset:#{@offset+1} partition:#{@partition}"
        @consumer.commit(@partition, @offset+1)
      end
    end
  end
end
