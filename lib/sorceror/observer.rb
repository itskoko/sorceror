module Sorceror::Observer
  extend ActiveSupport::Concern

  module ClassMethods
    def group(name, options={})
      Sorceror::Observer.observer_groups[name] = options # XXX This can be defined multiple times. Need to protect against being overriden?
      @observer_group = name
    end

    def observer(name, options, &block)
      defn = case options
      when Hash
        Definition::Event.new(*options.first)
      else
        Definition::Snapshot.new(options)
      end
      defn.group = @observer_group
      defn.name = name
      defn.callback = block

      raise "group must be defined" unless @observer_group
      raise "#{defn} observer already defined" if Sorceror::Observer.observers_by_name[defn.to_s]

      Sorceror::Observer.observer_groups_by_model[defn.model] ||= {}
      Sorceror::Observer.observer_groups_by_model[defn.model][@observer_group] ||= []
      Sorceror::Observer.observer_groups_by_model[defn.model][@observer_group] << defn

      Sorceror::Observer.observers_by_name[defn.to_s] = defn
    end
  end

  def self.reset!
    self.observer_groups_by_model = {}
    self.observer_groups = {}
    self.observers_by_name = {}
  end

  class << self
    attr_accessor :observer_groups_by_model
    attr_accessor :observers_by_name
    attr_accessor :observer_groups
  end

  reset!

  class Definition
    attr_accessor :callback
    attr_accessor :name
    attr_accessor :group

    def to_s
      "#{@group}:#{@name}"
    end

    class Event < self
      attr_accessor :model
      attr_accessor :event_name

      def initialize(model, event_name)
        @model = model
        @event_name = event_name
      end
    end

    class Snapshot < self
      attr_accessor :model

      def initialize(model)
        @model = model
      end
    end
  end
end
