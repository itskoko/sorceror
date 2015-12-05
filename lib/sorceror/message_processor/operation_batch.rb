class Sorceror::MessageProcessor::OperationBatch
  def initialize(message)
    @message = message
  end

  def run
    @model = Sorceror::Model.models[@message.type]
    raise "Model not defined #{@message.type}" unless @model
    @instance = @model.where(id: @message.id).first

    if @instance.nil? && @message.operations.first.name == :create
      @instance = @model.new(@message.operations.first.attributes)
    end

    # raise_or_warn_if_instance_missing
    # XXX This should incorporated into safe error handling protocol
    unless @instance
      error = RuntimeError.new("[#{@message.type}][#{@message.id}] unable to find instance, skipping. Something is wrong!")
      Sorceror::Config.error_notifier.call(error)
      return
    end

    if @instance.context.operation.last_hash == @message.hash
      Sorceror.warn "[#{@message.type}][#{@message.id}] skipping as duplicate"
      return
    end

    @instance.context.operation.current_hash = @message.hash

    unless @instance.context.operation.pending?
      @message.operations.each do |operation|
        context = Context.new(instance: @instance, operation: operation)
        context.execute
      end

      @instance.persist
    end

    @instance.context.operation.publish!
  end

  class Context
    def initialize(instance:, operation:)
      @instance = instance
      @operation = operation
    end

    def execute
      Thread.current[:sorceror_context] = self

      self.instance_exec(*args, &@operation.proc)

      @instance.context.operation.events << [@operation.event, @operation.attributes]
    rescue Skipped
    ensure
      Thread.current[:sorceror_context] = nil
    end

    def skip
      raise Skipped
    end

    def queue(message)
      @instance.context.operation.queue(message)
    end

    private

    def args
      [@instance, @operation.attributes.clone][0...@operation.proc.arity]
    end

    class Skipped < Exception
    end
  end
end
