module Sorceror::MessageProcessor::Event
  extend Sorceror::MessageProcessor

  def self.process(message, group_name, filter)
    retrying do
      if model = Sorceror::Model.models[message.type]
        instance = model.where(id: message.id).first

        unless instance
          raise "[#{message.type}][#{message.id}] unable to find instance. Something is wrong!"
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

            raise "Unable to save: #{instance.errors.full_messages.join('. ')}" unless instance.mongoid_save

            model_observer_groups.each do |group, model_observers|
              instance["__#{group}ob__"].each do |observer_name|
                observer = Sorceror::Observer.observers_by_name[observer_name]
                observer[:proc].call(instance)

                instance["__#{group}ob__"] -= [observer_name]
                raise "Unable to save: #{instance.errors.full_messages.join('. ')}" unless instance.mongoid_save
              end
            end

            model_observer_groups.each do |group, _|
              instance["__#{group}lk__"] = false
            end
            raise "Unable to save: #{instance.errors.full_messages.join('. ')}" unless instance.mongoid_save
          end
        end
      end
    end
  end
end
