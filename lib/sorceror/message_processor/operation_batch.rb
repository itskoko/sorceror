class Sorceror::MessageProcessor::OperationBatch
  def initialize(message)
    @message = message
  end

  def run
    # Only allow one instance per message (but with multiple events)

    # ALGO
    #
    # PERSISTENT:
    # - For each operation
    #   - Find or instantiate instance
    #   - UNLESS queued events
    #   - Queue publishing event: [model+event+seq] id, event attributes
    #   - Execute operation (don't save)
    #     - Queue published operations
    # - Save instance (including queued events + queued operations to be published)
    # - Publish snapshot: [model+id] id, instance attributes, seq
    # - Mark instance as snapshot processed
    # - For each queued event:
    #   - Publish event
    #   - Update instance remove published event
    # - For each queued operation:
    #   - Publish operation
    #

    @model = Sorceror::Model.models[@message.type]
    raise "Model not defined #{@message.type}" unless @model
    @instance = @model.where(id: @message.id).first

    if @instance.nil?
      @instance = @model.new(@message.operations.first.attributes)
    end

    if @instance.context.last_hash == @message.hash
      Sorceror.warn "[#{@message.type}][#{@message.id}] skipping as "
      return
    end

    @instance.context.current_hash = @message.hash

    # raise_or_warn_if_instance_missing
    unless @instance
      message = "[#{message.type}][#{message.id}] unable to find instance. Something is wrong!"
      if Sorceror::Config.skip_missing_instances
        Sorceror.warn message
      else
        raise message
      end
    end

    unless @instance.context.operation.pending?
      @message.operations.each do |operation|
        context = Context.new(instance: @instance, operation: operation)
        context.execute
      end

      @instance.persist
    end

    @instance.context.operation.publish!
  end

  def old
    events.each do |id, event_names|
      instance = instances[id]
      message = Sorceror::Message::Event.new(:partition_key => [instance.partition_key, event_names].join(':'),
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
      instance.collection.find(instance.atomic_selector).update('$set' => { :__op__ => event_names })
    end

    instances.each do |instance|
      message = Sorceror::Message::Snapshot.new(:partition_key => instance.id,
                                                :payload       => {
        :id          => instance.id,
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

      instance.collection.find(instance.atomic_selector).update('$set' => { :__op__ => true })
    end
  end

  class Context
    def initialize(instance:, operation:)
      @instance = instance
      @operation = operation
      @queued = []
    end

    def execute
      Thread.current[:sorceror_context] = self

      self.instance_exec(*args, &@operation.proc)

      @instance.context.operation.events << @operation.event
    rescue Skipped
    ensure
      Thread.current[:sorceror_context] = nil
    end

    def skip
      raise Skipped
    end

    def queue(operation)
      @instance.metadata.queued << operation
    end

    private

    def args
      [@instance, @operation.attributes][0...@operation.proc.arity]
    end

    class Skipped < Exception
    end
  end
end
