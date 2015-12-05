class Sorceror::MessageProcessor::Snapshot
  def initialize(message, group_name, filter=[])
    @message = message
    @group_name = group_name
    @filter = filter
  end

  def run
    @model = Sorceror::Model.models[@message.type]
    return unless @model

    @instance = @model.where(id: @message.id).first

    # raise_or_warn_if_instance_missing  DRY with operation_batch
    unless @instance
      error = RuntimeError.new("[#{@message.type}][#{@message.id}] unable to find instance, skipping. Something is wrong!")
      Sorceror::Config.error_notifier.call(error)
      return
    end

    if model_observer_groups = Sorceror::Observer.observer_groups_by_model[@model]
      model_observer_groups = model_observer_groups.select { |group, _| group == @group_name }

      model_observer_groups.each do |group, model_observers|
        # TODO: Protect against multiple threads working on same instance (small
        # possibility during rebalancing)

        unless @instance.context.observer(group).pending?
          observers = model_observers.select { |ob| ob.is_a?(Sorceror::Observer::Definition::Snapshot) }
          observers.each { |ob| @instance.context.observer(group).queue(ob.to_s) }
          @instance.context.observer(group).persist
        end
      end

      model_observer_groups.each do |group, model_observers|
        while observer_name = @instance.context.observer(group).shift_queued
          observer = Sorceror::Observer.observers_by_name[observer_name]
          raise "[#{@message.type}][#{@message.id}] observer #{observer_name} no longer defined" unless observer

          keys = @model.fields.keys + ['id']
          observer.callback.call(@model.new(@message.attributes.slice(*keys)))

          @instance.context.observer(group).persist
        end
      end
    end
  end
end
