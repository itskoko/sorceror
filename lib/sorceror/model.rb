module Sorceror::Model
  extend ActiveSupport::Concern

  included do
    class << self
      [:create!].each do |method|
        alias_method "mongoid_#{method}", method
        define_method(method) do |*args, &block|
          raise "Direct persistence not supported with Sorceror"
        end
      end
    end

    define_model_callbacks :publish, only: [:before]

    mattr_accessor :partition_key
    mattr_accessor :operations
    self.operations = { __create__: -> _ {} }

    [:save, :save!, :update, :update!, :update_attributes!, :update_attributes].each do |method|
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
    def create(attributes)
      self.new(attributes).create
    end

    def key(partition_key)
      self.partition_key = partition_key
    end

    def operation(name, &block)
      self.operations[name.to_sym] = block

      define_method(name) do |*args|
        attributes = args[0] || {}
        self.publish(name, attributes)
      end
    end
  end

  def create
    return false unless valid?

    self.publish(:__create__, self.as_json)
  end

  def payload(operation, attributes)
    {
      name: operation,
      id: self.id,
      attributes: attributes
    }
  end

  def publish(operation, attributes)
    raise "Already published" if @published # TODO Use error classes
    raise "Already persisted" if persisted?

    @payloads ||= []

    unless @running_callbacks
      @running_callbacks = true
      run_callbacks :publish
      @running_callbacks = false
    end

    @payloads << payload(operation, attributes)

    unless @running_callbacks
      payload_opts = { :topic      => Sorceror::Config.publisher_topic,
                       :topic_key  => topic_key,
                       :payload    => MultiJson.dump({
                         :operations => @payloads.reverse,
                         :type       => self.class.to_s,
                       })
      }

      Sorceror::Backend.publish(payload_opts)
    end

    @published = true
  end

  def topic_key
    "#{model_name.plural}/#{self.send(partition_key)}"
  end

  def as_json(options={})
    attrs = super
    attrs.reject! { |k,v| k.to_s.match(/^__.*__$/) }
    attrs.reject! { |k,v| v.nil? }
    id = attrs.delete('_id')
    attrs.merge('id' => id)
  end
end
