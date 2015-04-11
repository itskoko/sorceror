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
    self.operations = { create: -> _ {} }

    [:save, :save!, :update, :update!, :update_attributes!, :update_attributes].each do |method|
      alias_method "mongoid_#{method}", method
      define_method(method) do |*args, &block|
        raise "Direct persistence not supported with Sorceror"
      end
    end

    field :__ob__ # Observers
    field :__lk__ # Instance lock

    Sorceror::Model.models[self.model_name.name] = self

    include Sorceror::Serializer
  end

  class << self
    attr_accessor :models
  end
  self.models = {}

  module ClassMethods
    def create(attributes)
      self.new(attributes).tap(&:create)
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

    self.publish(:create, -> { self.as_json })

    self
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

    attributes = attributes.call if attributes.is_a? Proc
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
end
