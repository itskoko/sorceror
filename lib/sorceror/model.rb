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

    define_model_callbacks :operation, only: [:before]

    mattr_accessor :partition_key
    mattr_accessor :operations
    self.operations = { create: { proc: -> _ {}, event: :created  } }

    [:save, :save!, :update, :update!, :update_attributes!, :update_attributes].each do |method|
      alias_method "mongoid_#{method}", method
      define_method(method) do |*args, &block|
        raise "Direct persistence not supported with Sorceror"
      end
    end

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

    def operation(defn, &block)
      name, event_name = defn.first

      self.operations[name.to_sym] = { proc: block, event: event_name }

      define_method(name) do |*args|
        attributes = args[0] || {}
        publish_operation(name, attributes)
      end
    end
  end

  def create
    return false unless valid?

    self.publish_operation(:create, -> { self.as_json })

    begin
      self.mongoid_save
    rescue => e
      if e.message =~ /E11000/
        Sorceror.warn "[#{self.class}][#{self.id}] ignoring already created instance"
      else
        raise e
      end
    end

    self
  end

  def payload(name, attributes)
    {
      name: name,
      id: self.id,
      attributes: attributes
    }
  end

  def publish_operation(name, attributes)
    raise "Already published" if @published # TODO Use error classes
    raise "Already persisted" if persisted?

    @payloads ||= []

    unless @running_callbacks
      @running_callbacks = true
      run_callbacks :operation
      @running_callbacks = false
    end

    attributes = attributes.call if attributes.is_a? Proc
    @payloads << payload(name, attributes)

    unless @running_callbacks
      payload_opts = { :topic      => Sorceror::Config.operation_topic,
                       :topic_key  => topic_key,
                       :payload    => MultiJson.dump({
                         :operations => @payloads[-1..-1] + @payloads[0..-2],
                         :type       => self.class.to_s,
                       })
      }

      Sorceror::Backend.publish(payload_opts)

      @published = true
    end
  end

  def topic_key
    "#{model_name.plural}/#{self.send(partition_key)}"
  end
end
