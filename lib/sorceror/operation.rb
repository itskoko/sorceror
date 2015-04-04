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

          instance ||= begin
                         instance = model.where(id: operation.id).first
                         if instance.nil? && operation.name == :__create__
                           instance = model.new(operation.attributes)
                         end
                         instance
                       end

          unless instance
            raise "[#{message.type}][#{operation.name}][#{operation.id}] unable to find instance. Something is wrong!"
          end

          args = [instance, operation.attributes][0...operation_proc.arity]
          operation_proc.call(*args)

          unless instance.__lk__
            instance.__ob__ ||= []
            if model_observers = Sorceror::Observer.observers_by_model[model]
              observers = model_observers.select { |ob| ob[:operation] == operation.name.to_s.gsub('__', '').to_sym }.map { |ob| ob[:name] }
              instance.__ob__ += observers
            end
          end
        end

        instance.__lk__ = true

        begin
          raise "Unable to save" unless instance.mongoid_save
        rescue StandardError => e
          if e.message =~ /E11000/ # Duplicate key
            Promiscuous.warn "[#{message.type}][#{instance.id}] ignoring already created record"
          else
            raise e
          end
        end

        instance.__ob__.each do |observer_name|
          observer = Sorceror::Observer.observers_by_name[observer_name]
          observer[:proc].call(instance)

          instance.__ob__ -= [observer_name]
          raise "Unable to save" unless instance.mongoid_save
        end

        instance.__lk__ = false
        raise "Unable to save" unless instance.mongoid_save
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
