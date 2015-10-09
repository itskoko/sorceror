module Sorceror::Backend
  extend Sorceror::Autoload
  autoload :Poseidon, :Null, :Inline, :Fake, :JrubyKafka

  class << self
    attr_accessor :driver
    attr_accessor :driver_class
    attr_accessor :subscriber_class
    attr_accessor :subscriber_methods

    def driver=(value)
      disconnect
      @driver_class = value.try { |v| "Sorceror::Backend::#{v.to_s.camelize.gsub(/backend/, 'Backend')}".constantize }
    end

    def lost_connection_exception(options={})
      backends = {
        :kafka_hosts     => Sorceror::Config.kafka_hosts,
        :zookeeper_hosts => Sorceror::Config.zookeeper_hosts
      }
      Sorceror::Error::Connection.new(backends, options)
    end

    def ensure_connected
      Sorceror.ensure_connected

      raise lost_connection_exception unless connected?
    end

    def connect
      return if @driver
      @driver = driver_class.new
      @driver.connect
    end

    def disconnect
      return unless @driver
      @driver.stop_subscriber
      @driver.disconnect
      @driver = nil
    end

    def new_connection(*args)
      ensure_connected
      driver.new_connection(*args)
    end

    def subscriber_stopped?
      return true unless @driver
      driver.subscriber_stopped?
    end

    delegate :connected?, :stop_subscriber, :to => :driver

    def publish(*args)
      if context = Thread.current[:sorceror_context]
        context.queue(*args)
      else
        ensure_connected
        driver.publish(*args)
      end
    end

    def start_subscriber(consumer)
      consumer ||= :all
      raise "Unknown operation #{consumer}. Must be one of all, operation or event." unless consumer.in? [:all, :operation, :event]

      ensure_connected
      driver.start_subscriber(consumer)
    end
  end
end
