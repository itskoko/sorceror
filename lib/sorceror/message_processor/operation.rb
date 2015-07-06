module Sorceror::MessageProcessor::Operation
  def self.process(message)
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

        args = [instance, operation.attributes][0...operation_proc.arity]

        context = Context.new(operation_proc, args)
        context.execute

        unless context.skipped
          events[instance] ||= []
          events[instance] << operation_event
        end

        raise "Unable to save: #{instance.errors.full_messages.join('. ')}" unless instance.mongoid_save
      end

      events.each do |instance, event_names|
        message = Sorceror::Message::Event.new(:partition_key => instance.partition_key,
                                               :payload       => {
          :id          => instance.id,
          :events      => event_names,
          :attributes  => instance.as_json,
          :type        => instance.class.to_s,
        })

        # XXX Not idempotent (multiple instances so multiple publishes, so if
        # a publish fails and there are subsequent publishes, the publish will
        # be repeated. This MAY NOT BE A PROBLEM.
        Sorceror::Backend.publish(message)

        # TODO Use a offset/version number (perhaps stored on doc). Use to
        # ignore already processed messages and protect against another
        # processing during a rebalance.

        instance[:__op__] ||= true

        raise "Unable to save: #{instance.errors.full_messages.join('. ')}" unless instance.mongoid_save
      end
    end
  end

  class Context
    attr_reader :skipped

    def initialize(block, args)
      @block = block
      @args = args
      @skipped = false
    end

    def execute
      self.instance_exec(*@args, &@block)
    rescue Skipped
    end

    def skip
      @skipped = true
      raise Skipped
    end

    class Skipped < Exception
    end
  end
end
