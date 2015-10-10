module Sorceror::MessageProcessor
  extend Sorceror::Autoload
  autoload :OperationBatch, :Event, :Snapshot

  def self.process(message, *args)
    Sorceror::Middleware.run(message) do
      "Sorceror::MessageProcessor::#{message.class.to_s.demodulize}".constantize.new(message, *args).run
    end
  end
end
