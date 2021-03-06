module BackendHelper
  def reconfigure_backend(&block)
    Sorceror.configure do |config|
      config.reset
      config.app = 'test'
      config.logger = Logger.new(STDERR); STDERR.sync = true
      config.logger.level = ENV["LOGGER_LEVEL"] ? ENV["LOGGER_LEVEL"].to_i : Logger::ERROR
      config.retry = false
      config.kafka_hosts = kafka_hosts
      config.zookeeper_hosts = zookeeper_hosts
      config.subscriber_threads = 1
      config.trail = false
      config.error_notifier = -> e { Sorceror.info "#{e.message}\n#{e.backtrace.join("\n")}" }
      config.skip_missing_instances = true
      block.call(config) if block
    end
    Sorceror.connect
  end

  def use_backend(backend, &block)
    reconfigure_backend do |config|
      config.backend = backend
      block.call(config) if block
    end
  end

  def run_subscriber_worker!
    Sorceror::Backend.stop_subscriber
    advance_offsets_forward! if Sorceror::Backend.driver.is_real?
    Sorceror::Backend.start_subscriber(:all)
  end

  def process!
    process_operations!
    process_events!
  end

  def process_operations!
    Sorceror::Backend.driver.process_operations
  end

  def process_events!
    Sorceror::Backend.driver.process_events
  end
end

RSpec.configure do |config|
  config.after do
    Sorceror::Backend.stop_subscriber if Sorceror::Backend.driver
    Sorceror::Observer.reset!
  end
end
