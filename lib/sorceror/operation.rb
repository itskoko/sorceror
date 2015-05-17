module Sorceror::Operation
  # TODO  DRY up with Event using Processor super class perhaps

  def self.process(message)
    retries = 0
    retry_max = 50 # TODO Make constants

    begin
      if model = Sorceror::Model.models[message.type]

        events = {}

        message.operations.each do |operation|
          operation_proc  = model.operations[operation.name][:proc]
          operation_event = model.operations[operation.name][:event]

          unless operation_proc
            raise "Operation #{operation.name} not defined for #{message.type}" # TODO Use Error class
          end

          instance = model.where(id: operation.id).first

          if operation.name == :create
            if instance.nil?
              instance = model.new(operation.attributes)
            else
              if instance[:__op__]
                Sorceror.warn "[#{message.type}][#{operation.name}][#{operation.id}] skipping as instance already created"
                return
              end
            end
          end

          unless instance
            raise "[#{message.type}][#{operation.name}][#{operation.id}] unable to find instance. Something is wrong!"
          end

          events[instance] ||= []
          events[instance] << operation_event

          args = [instance, operation.attributes][0...operation_proc.arity]
          operation_proc.call(*args)

          raise "Unable to save" unless instance.mongoid_save
        end

        events.each do |instance, event_names|
          payload_opts = { :topic      => Sorceror::Config.event_topic,
                           :topic_key  => instance.topic_key,
                           :payload    => MultiJson.dump({
                             :id         => instance.id,
                             :events     => event_names,
                             :attributes => instance.as_json,
                             :type       => instance.class.to_s,
                           })
          }

          # XXX Not idempotent (multiple instances so multiple publishes, so if
          # a publish fails and there are subsequent publishes, the publish will
          # be repeated. This MAY NOT BE A PROBLEM.
          Sorceror::Backend.publish(payload_opts)

          instance[:__op__] ||= true
          raise "Unable to save" unless instance.mongoid_save
        end
      end
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
