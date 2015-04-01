module Sorceror::Operation
  extend Promiscuous::Autoload
  autoload :Persistence

  def self.process(message)
    retries = 0   # TODO Make constants
    retry_max = 50

    begin
      if model = Sorceror::Model.models[message.type]
        operation = model.operations[message.operation_name.to_sym]

        unless operation
          raise "Operation #{message.operation_name} not defined for #{message.type}" # TODO Use Error class
        end

        operation.call(message)
      end
    rescue StandardError => e
      Sorceror::Config.error_notifier.call(e)
      raise e unless Sorceror::Config.retry

      if retries < retry_max
        retries += 1
        sleep 5
        retry
      end
    end
  end
end
