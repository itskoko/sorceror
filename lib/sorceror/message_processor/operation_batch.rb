class Sorceror::MessageProcessor::OperationBatch
  def initialize(message)
    @message = message
  end

  def run
    @model = Sorceror::Model.models[@message.type]
    raise "Model not defined #{@message.type}" unless @model
    @instance = @model.where(id: @message.id).first

    if @instance.nil?
      @instance = @model.new(@message.operations.first.attributes)
    end

    if @instance.context.operation.last_hash == @message.hash
      Sorceror.warn "[#{@message.type}][#{@message.id}] skipping as "
      return
    end

    @instance.context.operation.current_hash = @message.hash

    # raise_or_warn_if_instance_missing
    unless @instance
      error_message = "[#{message.type}][#{message.id}] unable to find instance. Something is wrong!"
      if Sorceror::Config.skip_missing_instances
        Sorceror.warn error_message
      else
        raise error_message
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
