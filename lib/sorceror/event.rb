module Sorceror::Event
  def self.process(message, group_name, filter=//)
    retries = 0
    retry_max = 50 # TODO Make constants

    begin
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

            raise "Unable to save" unless instance.mongoid_save

            model_observer_groups.each do |group, model_observers|
              instance["__#{group}ob__"].each do |observer_name|
                observer = Sorceror::Observer.observers_by_name[observer_name]
                observer[:proc].call(instance)

                instance["__#{group}ob__"] -= [observer_name]
                raise "Unable to save" unless instance.mongoid_save
              end
            end

            model_observer_groups.each do |group, _|
              instance["__#{group}lk__"] = false
            end
            raise "Unable to save" unless instance.mongoid_save
          end
        end
      end
    rescue StandardError => e
      Sorceror::Config.error_notifier.call(e)
      raise e unless Sorceror::Config.retry

      # TODO XXX Need a way to exit if the subscriber has been stopped
      if retries < retry_max
        retries += 1
        sleep 0.1 * 3**retries
        retry
      else
        # TODO What happens??? Just block indefinitely? Perhaps a CLI mechanism
        # to manually clear an offset?
        # to clear this?
      end
    end
  end
end
