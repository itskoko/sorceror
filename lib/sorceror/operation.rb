module Sorceror::Operation
  # TODO  DRY up with Event using Processor super class perhaps

  def self.process(message)
    retries = 0
    retry_max = 50 # TODO Make constants

    begin
      if model = Sorceror::Model.models[message.type]

        events = {}
        instance = nil

        message.operations.each do |operation|
          operation_proc  = model.operations[operation.name][:proc]
          operation_event = model.operations[operation.name][:event]

          unless operation_proc
            raise "Operation #{operation.name} not defined for #{message.type}" # TODO Use Error class
          end

          instance ||= begin
                         instance = model.where(id: operation.id).first
                         if instance.nil? && operation.name == :create # XXX Is there a way to generalize?
                           instance = model.new(operation.attributes)
                         end
                         instance
                       end

          unless instance
            raise "[#{message.type}][#{operation.name}][#{operation.id}] unable to find instance. Something is wrong!"
          end

          events[instance] ||= []
          events[instance] << operation_event

          args = [instance, operation.attributes][0...operation_proc.arity]
          operation_proc.call(*args)

          begin
            raise "Unable to save" unless instance.mongoid_save
          rescue StandardError => e
            if e.message =~ /E11000/ # Duplicate key
              Promiscuous.warn "[#{message.type}][#{instance.id}] ignoring already created record"
            else
              raise e
            end
          end
        end

        events.each do |instance, event_names|
          payload_opts = { :topic      => Sorceror::Config.event_topic,
                           :topic_key  => instance.topic_key,
                           :payload    => MultiJson.dump({
                             :id         => instance.id,
                             :events     => event_names,
                             :attributes => instance.attributes,
                             :type       => instance.class.to_s,
                           })
          }

          # XXX Not idempotent (multiple instances so multiple publishes, so if
          # a publish fails and there are subsequent publishes, the publish will
          # be repeated. This MAY NOT BE A PROBLEM.
          Sorceror::Backend.publish(payload_opts)
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
