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
    options = Sorceror::Config.publisher_options.merge(:broker_list       => Sorceror::Config.kafka_hosts.join(','),
                                                       "serializer.class" => "kafka.serializer.StringEncoder")
    @connection = Kafka::Producer.new(options)
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

  def publish(message)
    Sorceror.ensure_connected

    raw_publish(message)
  rescue StandardError => e
    Sorceror.warn("[publish] Failure publishing to kafka #{e}\n#{e.backtrace.join("\n")}")
    raise Sorceror::Error::Publisher.new(e, :payload => message.payload)
  end

  def start_subscriber(consumer)
    @distributors = []

    if consumer.in?([:all, :operation])
      Sorceror::Config.operation_topic.tap do |topic|
        @distributors << Distributor::Operation.new(self, topic: topic, threads: @threads)
        Sorceror.info "[distributor:operation] Starting #{@threads} topic:#{topic}"
      end
    end

    if consumer.in?([:all, :event])
      Sorceror::Observer.observer_groups.each do |group, options|
        next unless options[:event]

        @distributors << Distributor::Event.new(self, options.merge(topic: Sorceror::Config.event_topic, group: group, threads: @threads))
        Sorceror.info "[distributor:event] Starting #{@threads} threads: topic:#{Sorceror::Config.event_topic} and group:#{group}"
      end
    end

    if consumer.in?([:all, :snapshot])
      Sorceror::Observer.observer_groups.each do |group, options|
        next unless options[:snapshot]

        @distributors << Distributor::Snapshot.new(self, options.merge(topic: Sorceror::Config.snapshot_topic, group: group, threads: @threads))
        Sorceror.info "[distributor:event] Starting #{@threads} threads: topic:#{Sorceror::Config.snapshot_topic} and group:#{group}"
      end
    end
  end

  def stop_subscriber
    return unless @distributors

    @distributors.each { |distributor| distributor.stop }
    @distributors = nil
  end

  def subscriber_stopped?
    @distributors.nil?
  end

  def show_stop_status(num_requests)
    @distributors.to_a.each { |distributor| distributor.show_stop_status(num_requests) }
  end

  private

  def raw_publish(message)
    @connection.send_msg(message.topic, message.key, message.partition_key, message.to_s)
  rescue StandardError => e
    raise Sorceror::Error::Publisher.new(e, :payload => message.payload)
  end

  class Distributor
    def initialize(supervisor, options)
      @supervisor = supervisor
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

      options = Sorceror::Config.subscriber_options.merge(:topic_id => @topic,
                                                          :zk_connect => Sorceror::Config.zookeeper_hosts.join(','),
                                                          :group_id => @group,
                                                          :auto_commit_enable => "false",
                                                          :auto_offset_reset => @trail ? 'largest' : 'smallest',
                                                          :consumer_restart_on_error => "false")

      @consumer = Kafka::Group.new(options)

    end

    def run
      @consumer.run(@threads) do |message, metadata|
        begin
          process(message, metadata)
          commit(metadata)
        rescue StandardError => e
          Sorceror.warn "[kafka] [distributor] died: #{e.message}\n#{e.backtrace.join("\n")}"
          @supervisor.stop_subscriber
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
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] topic:#{@consumer.topic} group:#{@group} offset:#{metadata.offset} partition:#{metadata.partition}"
        Sorceror::MessageProcessor.process(Sorceror::Message::OperationBatch.new(payload: payload))
      end
    end

    class Event < self
      def subscribe(options)
        @group_name = options.fetch(:group)

        connect(topic: options.fetch(:topic),
                group: "#{Sorceror::Config.app}.#{@group_name}",
                trail: options.fetch(:trail, false))
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] topic:#{@consumer.topic} group:#{@group} offset:#{metadata.offset} parition:#{metadata.partition}"
        Sorceror::MessageProcessor.process(Sorceror::Message::Event.new(payload: payload), @group_name)
      end
    end

    class Snapshot < self
      def subscribe(options)
        @group_name = options.fetch(:group)

        connect(topic: options.fetch(:topic),
                group: "#{Sorceror::Config.app}.#{@group_name}",
                trail: options.fetch(:trail, false))
      end

      def process(payload, metadata)
        Sorceror.info "[kafka] [receive] topic:#{@consumer.topic} group:#{@group} offset:#{metadata.offset} parition:#{metadata.partition}"
        Sorceror::MessageProcessor.process(Sorceror::Message::Snapshot.new(payload: payload), @group_name)
      end
    end
  end
end
