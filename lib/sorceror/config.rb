module Sorceror::Config
  mattr_accessor :app, :backend, :kafka_backend, :kafka_hosts, :zookeeper_hosts, :publisher_topic,
                 :subscriber_topics, :socket_timeout, :logger, :subscriber_threads,
                 :error_notifier, :retry, :trail

  def self.backend=(value)
    @@backend = value
    Sorceror::Backend.driver = value
  end

  def self.reset
    Sorceror::Backend.driver = nil
    class_variables.each { |var| class_variable_set(var, nil) }
  end

  def self._configure(&block)
    block.call(self) if block

    self.app                  ||= Rails.application.class.parent_name.underscore rescue nil if defined?(Rails)
    self.backend              ||= :poseidon
    self.kafka_hosts          ||= ['localhost:9092']
    self.zookeeper_hosts      ||= ['localhost:2181']
    self.publisher_topic      ||= self.app
    self.subscriber_topics    ||= [self.publisher_topic]

    self.socket_timeout       ||= 50
    self.logger               ||= defined?(Rails) ? Rails.logger : Logger.new(STDERR).tap { |l| l.level = Logger::WARN }
    self.subscriber_threads   ||= 10
    self.error_notifier       ||= proc {}
    self.retry                = nil
    self.trail                = false
  end

  def self.configure(&block)
    reconnect_if_connected do
      self._configure(&block)

      unless self.app
        raise "Promiscuous.configure: please give a name to your app with \"config.app = 'your_app_name'\""
      end
    end

    hook_fork
  end

  def self.hook_fork
    return if @fork_hooked

    Kernel.module_eval do
      alias_method :fork_without_sorceror, :fork

      def fork(&block)
        return fork_without_sorceror(&block) unless Sorceror.should_be_connected?

        Sorceror.disconnect
        pid = if block
          fork_without_sorceror do
            Sorceror.connect
            block.call
          end
        else
          fork_without_sorceror
        end
        Sorceror.connect
        pid
      rescue StandardError => e
        puts e
        puts e.backtrace.join("\n")
        raise e
      end

      module_function :fork
    end

    @fork_hooked = true
  end

  def self.configured?
    self.app != nil
  end

  private

  def self.reconnect_if_connected(&block)
    if Sorceror.should_be_connected?
      Sorceror.disconnect
      yield
      Sorceror.connect
    else
      yield
    end
  end
end
