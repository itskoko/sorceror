module Sorceror::MessageProcessor
  extend Sorceror::Autoload
  autoload :Operation, :Event

  mattr_accessor :middleware_chain

  def self.process(message, *args)
    "Sorceror::MessageProcessor::#{message.class.to_s.demodulize}".constantize.process(message, *args)
  end

  def retrying(&block)
    retries = 0
    retry_max = 50 # TODO Make constants

    begin
      block.call
    rescue StandardError => e
      Sorceror::Config.error_notifier.call(e)
      raise e unless Sorceror::Config.retry
      if retries < retry_max
        retries += 1
        sleep 0.1 * 3**retries
        retry
      end
    end
  end
end
