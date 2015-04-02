module Sorceror::Observer
  extend ActiveSupport::Concern

  module ClassMethods
    def observer(name, options, &block)
      raise "#{name} observer already defined" if Sorceror::Observer.observers_by_name[name]

      model          = options.first[0]
      operation_name = options.first[1]

      defn = { operation: operation_name,
               proc: block }

      Sorceror::Observer.observers_by_model[model] ||= []
      Sorceror::Observer.observers_by_model[model] << defn.merge(name: name)
      Sorceror::Observer.observers_by_name[name] = defn.merge(model: model)
    end
  end

  class << self
    attr_accessor :observers_by_model
    attr_accessor :observers_by_name
  end
  self.observers_by_model = {}
  self.observers_by_name = {}
end
