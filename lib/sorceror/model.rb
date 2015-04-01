module Sorceror::Model
  extend ActiveSupport::Concern

  included do
    class << self
      [:create, :create!].each do |method|
        alias_method "mongoid_#{method}", method
        define_method(method) do |*args, &block|
          raise "Direct persistence not supported with Sorceror"
        end
      end
    end

    mattr_accessor :partition_key
    mattr_accessor :operations
    self.operations = {}

    [:save, :save!, :update_attributes!, :update_attributes].each do |method|
      alias_method "mongoid_#{method}", method
      define_method(method) do |*args, &block|
        raise "Direct persistence not supported with Sorceror"
      end
    end

    field :__cb__ # Callbacks
    field :__pv__ # Previous Vesion (used to diff and detect changes)

    Sorceror::Model.models[self.model_name.name] = self
  end

  class << self
    attr_accessor :models
  end
  self.models = {}

  module ClassMethods
    def publish(attributes)
      self.new(attributes).publish
    end

    def key(partition_key)
      self.partition_key = partition_key
    end

    def operation(name, &block)
      self.operations[name.to_sym] = block
    end
  end

  def payload
    payload = {
      operation: :__create__,
      type: self.class.to_s,
      id: self.id
    }
    payload.merge(attributes: self.as_json)
  end

  def publish
    return false unless valid?

    raise "Already published" if @published # TODO Use error classes
    raise "Already persisted" if persisted?

    payload_opts = { :topic      => Sorceror::Config.publisher_topic,
                     :topic_key  => topic_key,
                     :payload    => MultiJson.dump(payload),
                     :async      => false }

    Sorceror::Backend.publish(payload_opts)

    @published = true
  end

  def topic_key
    "#{model_name.plural}/#{self.send(partition_key)}"
  end

  def as_json(options={})
    attrs = super
    attrs.reject! { |k,v| k.to_s.match(/^__.*__$/) }
    id = attrs.delete('_id')
    attrs.merge('id' => id)
  end
end
