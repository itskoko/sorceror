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

    mattr_accessor :_partition_with
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

    partition_with :self
  end

  class << self
    attr_accessor :models
  end
  self.models = {}

  module ClassMethods
    def create(attributes)
      self.new(attributes).tap(&:create)
    end

    def partition_with(partition_with)
      raise ArgumentError.new("#{partition_with} is not a relation on #{self}") unless (relations.keys << 'self').include?(partition_with.to_s)

      self._partition_with = partition_with
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
      message = Sorceror::Message::Operation.new(:partition_key => partition_key,
                                                 :payload       => {
                                                   :operations  => @payloads[-1..-1] + @payloads[0..-2],
                                                   :type        => self.class.to_s,
                                                 })

      Sorceror::Backend.publish(message)

      @published = true
    end
  end

  def partition_key
    if self._partition_with == :self
      "#{model_name.plural}/#{self.id}"
    else
      # TODO If the partition field is nil reload. This is expensive as it loads
      # the entire object. Consider:
      #   1) Only loading the required fields
      #   2) Caching the key on the object (PREFERRED)
      (self.send(_partition_with) || self.reload.send(_partition_with)).partition_key
    end
  end
end
