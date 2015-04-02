module Sorceror::Operation
  def self.process(message)
    retries = 0   # TODO Make constants
    retry_max = 50

    begin
      if model = Sorceror::Model.models[message.type]
        instance = nil
        message.operations.each do |operation|
          operation_proc = model.operations[operation.name]

          unless operation_proc
            raise "Operation #{operation.name} not defined for #{message.type}" # TODO Use Error class
          end

          instance ||= if operation.name == :__create__
            model.new(operation.attributes)
          else
            model.find(operation.id)
          end

          operation_proc.call(instance)
        end

        begin
          raise "Unable to save" unless instance.mongoid_save
        rescue StandardError => e
          if e.message =~ /E11000/ # Duplicate key
            Promiscuous.warn "[#{message.type}][#{message.attributes['id']}] ignoring already created record"
          else
            raise e
          end
        end
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
