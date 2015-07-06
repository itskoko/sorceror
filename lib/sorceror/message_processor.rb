module Sorceror::MessageProcessor
  extend Sorceror::Autoload
  autoload :Operation, :Event

  def self.process(message, *args)
    Sorceror::Middleware.run(message) do
      "Sorceror::MessageProcessor::#{message.class.to_s.demodulize}".constantize.process(message, *args)
    end
  end
end
