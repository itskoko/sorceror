require 'jruby-kafka'

class Sorceror::Backend::JrubyKafka
  attr_accessor :connection

  def initialize
    @threads = Sorceror::Config.subscriber_threads
  end

  def is_real?
    true
  end

  def new_connection
    # TODO Check all settings. Needs to be sync to all ISR.
    @connection = Kafka::Producer.new(:broker_list       => Sorceror::Config.kafka_hosts.join(','),
                                      "serializer.class" => "kafka.serializer.StringEncoder")
    @connection.tap(&:connect)
  end

  def connect
    @connection = new_connection
  end

  def disconnect
    @connection.close
  end

  def connected?
    @connection.present?
  end

  def publish(options={})
    Sorceror.ensure_connected

    raw_publish(options)
  rescue StandardError => e
    Sorceror.warn("[publish] Failure publishing to kafka #{e}\n#{e.backtrace.join("\n")}")
    raise Sorceror::Error::Publisher.new(e, :payload => options[:payload])
  end

  def start_subscriber(consumer)
    @distributors = []

    if consumer.in?([:all, :operation])
      Sorceror::Config.operation_topic.tap do |topic|
        @distributors << Distributor::Operation.new(topic: topic, threads: @threads)
        Sorceror.info "[distributor:operation] Starting #{@threads} topic:#{topic}"
      end
    end

    if consumer.in?([:all, :event])
      Sorceror::Observer.observer_groups.each do |group, options|
        @distributors << Distributor::Event.new(topic: Sorceror::Config.event_topic, group: group, threads: @threads, options: options)
        Sorceror.info "[distributor:event] Starting #{@threads} topic:#{Sorceror::Config.event_topic} and group:#{group}"
      end
    end
  end

  def stop_subscriber
    return unless @distributors

    Sorceror.info "[distributor] Stopping #{@distributors.count} threads"

    @distributors.each { |distributor| distributor.stop }
    @distributors = nil
  end

  def subscriber_stopped?
    @distributor.nil?
  end

  def show_stop_status(num_requests)
    @distributor.to_a.each { |distributor| distributor.show_stop_status(num_requests) }
  end

  private

  def raw_publish(options)
    if @connection.send_msg(options[:topic], options[:topic_key], options[:payload])
      Sorceror.info "[publish] [kafka] #{options[:topic]}/#{options[:topic_key]} #{options[:payload]}"
    end
  rescue StandardError => e
    raise Sorceror::Error::Publisher.new(e, :payload => options[:payload])
  end

  class Distributor
    def initialize(options)
      @threads = options.fetch(:threads)
      subscribe(options)
      run
    end

    def subscribe(options)
      # Override
    end

    def connect(options)
      @topic = options.fetch(:topic)
      @group = options.fetch(:group)
      @trail = options.fetch(:trail)

      @consumer = Kafka::Group.new(:topic_id => @topic,
                                   :zk_connect => Sorceror::Config.zookeeper_hosts.join(','),
                                   :group_id => @group,
                                   :auto_commit_enable => "false",
                                   :auto_offset_reset => @trail ? 'largest' : 'smallest',
                                   :consumer_restart_on_error => "false")

    end

    def run
      @consumer.run(@threads) do |message, metadata|
        begin
          process(message, metadata)
          commit(metadata)
        rescue StandardError => e
          Sorceror.warn "[kafka] [distributor] [skipped] partition: #{message.partition}, offset: #{message.offset}, payload: #{message.message.to_s}"
          Sorceror::Config.error_notifier.call(e)
        end
      end
    end

    def process(payload, metadata)
      # Override
    end

    def commit(metadata)
      @consumer.commit(metadata)
      Sorceror.info "[kafka] [commit] topic:#{metadata.topic} group:#{@group} offset:#{metadata.offset} partition:#{metadata.partition}"
    end

    def stop
      @consumer.shutdown if @consumer
      @consumer = nil
    end

    def show_stop_status(num_requests)
      if @consumer.running?
        STDERR.puts "Still processing messages (#{num_requests})"
      else
        STDERR.puts "Stopped"
      end
    end

    class Operation < self
      def subscribe(options)
        connect(topic: options.fetch(:topic),
                group: Sorceror::Config.app,
                trail: Sorceror::Config.trail)

        Sorceror.info "[distributor] Subscribed to topic:#{@topic} group:#{@group}"
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] #{payload} topic:#{@consumer.topic} group:#{@group} offset:#{metadata.offset} partition:#{metadata.partition}"
        Sorceror::Operation.process(Sorceror::Message::Operation.new(payload))
      end
    end

    class Event < self
      def subscribe(options)
        @group_name = options.fetch(:group)

        connect(topic: options.fetch(:topic),
                group: "#{Sorceror::Config.app}.#{@group_name}",
                trail: options.fetch(:trail, false))

        Sorceror.info "[distributor] Subscribed to topic:#{@topic} group:#{@group}"
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] #{payload} topic:#{@consumer.topic} group:#{@group} offset:#{metadata.offset} parition:#{metadata.partition}"
        Sorceror::Event.process(Sorceror::Message::Event.new(payload, :metadata => metadata), @group_name)
      end
    end
  end
end
