module Sorceror::Observer
  extend ActiveSupport::Concern

  module ClassMethods
    def group(name, options={})
      Sorceror::Observer.observer_groups[name] = options # XXX This can be defined multiple times. Need to protect against being overriden?
      @observer_group = name
    end

    def observer(name, options, &block)
      name = "#{@observer_group}:#{name}"

      raise "group must be defined" unless @observer_group
      raise "#{name} observer already defined" if Sorceror::Observer.observers_by_name[name]

      model      = options.first[0]
      event_name = options.first[1]

      defn = { event: event_name, proc: block }

      Sorceror::Observer.observer_groups_by_model[model] ||= {}
      Sorceror::Observer.observer_groups_by_model[model][@observer_group] ||= []
      Sorceror::Observer.observer_groups_by_model[model][@observer_group] << defn.merge(name: name)

      Sorceror::Observer.observers_by_name[name] = defn.merge(model: model)
    end
  end

  class << self
    attr_accessor :observer_groups_by_model
    attr_accessor :observers_by_name
    attr_accessor :observer_groups
  end
  self.observer_groups_by_model = {}
  self.observer_groups = {}
  self.observers_by_name = {}
end
