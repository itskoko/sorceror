module Sorceror::MessageProcessor::Snapshot
  def self.process(message, group_name, filter)
    if model = Sorceror::Model.models[message.type]
      instance = model.where(id: message.id).first

      unless instance
        error_message = "[#{message.type}][#{message.id}] unable to find instance. Something is wrong!"
        if Sorceror::Config.skip_missing_instances
          Sorceror.warn error_message
          return
        else
          raise error_message
        end
      end

      message.events.each do |event|
        if model_observer_groups = Sorceror::Observer.observer_groups_by_model[model]
          model_observer_groups = model_observer_groups.select { |group, _| group == group_name }

          model_observer_groups.each do |group, model_observers|
            unless instance["__#{group}lk__"]
              observers = model_observers.select { |ob| ob[:event] == event.to_sym && ob[:name] =~ filter }.map { |ob| ob[:name] }
              instance["__#{group}ob__"] ||= []
              instance["__#{group}ob__"] += observers
            end
            # TODO 1) Lock using the ID + Timestamp + Sequence Number
            #      2) Steal lock if Now - Timestamp > Expriation Time (30s) AND Sequence Number in Message > Sequence Number
            instance["__#{group}lk__"] = true
          end

          instance.collection.find(instance.atomic_selector).update(instance.as_document)

          model_observer_groups.each do |group, model_observers|
            instance["__#{group}ob__"].each do |observer_name|
              observer = Sorceror::Observer.observers_by_name[observer_name]
              keys = model.fields.keys + ['id']

              observer[:proc].call(model.new(message.attributes.slice(*keys)))

              instance["__#{group}ob__"] -= [observer_name]
              instance.collection.find(instance.atomic_selector).update(instance.as_document)
            end
          end

          model_observer_groups.each do |group, _|
            instance["__#{group}lk__"] = false
          end
          instance.collection.find(instance.atomic_selector).update(instance.as_document)
        end
      end
    end
  end
end
