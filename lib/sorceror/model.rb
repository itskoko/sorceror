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

    field :__c__

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
    def create(attributes={})
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

  def persist
    self.collection.find(self.atomic_selector).update(self.as_document, [ :upsert ])
  end

  def context
    @context ||= Context.new(self)
  end

  def create
    return false unless valid?

    self.publish_operation(:create, -> { self.as_json })

    begin
      self.send(:insert_as_root)
    rescue => e
      Sorceror.warn "[#{self.class}][#{self.id}] ignoring #{e.message}\n#{e.backtrace.join("\n")}"
    end

    self
  end

  def payload(name, attributes)
    {
      name: name,
      attributes: attributes
    }
  end

  def publish_operation(name, attributes)
    @payloads ||= []

    unless @running_callbacks
      @running_callbacks = true
      run_before_callbacks :create if name == :create
      run_before_callbacks :operation
      @running_callbacks = false
    end

    attributes = attributes.call if attributes.is_a? Proc
    @payloads << payload(name, attributes)

    unless @running_callbacks
      message = Sorceror::Message::OperationBatch.new(:partition_key => partition_key,
                                                      :payload       => {
        :id          => self.id,
        :operations  => @payloads,
        :type        => self.class.to_s
      })

      if context = Thread.current[:sorceror_context]
        context.queue(message)
      else
        Sorceror::Backend.publish(message)
      end
    end
  end

  def partition_key
    "#{model_name.plural}/#{self.id}"
  end

  class Context
    attr_accessor :current_hash

    attr_reader :attrs
    attr_reader :instance

    def initialize(instance)
      @instance = instance
      @attrs = @instance.__c__ || { operation: { events: [] }, event: {} }.with_indifferent_access
      @instance.__c__ = @attrs
    end

    def persist(last: false)
      @attrs.merge!(last_hash: current_hash) if last
      @instance.collection.find(@instance.atomic_selector).update('$set' => { :__c__ => @attrs })
    end

    def operation
      Operation.new(self)
    end

    def last_hash
      @attrs[:last_hash]
    end

    class Operation
      def initialize(context)
        @context = context
      end

      def events
        @context.attrs[:operation][:events]
      end

      def pending?
        !events.empty?
      end

      def publish!
        publish_events!
      end

      def publish_events!
        until events.empty? do
          event = events.shift
          message = Sorceror::Message::Event.new(:partition_key => [@context.instance.partition_key, event].join(':'),
                                                 :payload       => {
            :id    => @context.instance.id,
            :type  => @context.instance.class.to_s,
            :name  => event
          })

          Sorceror::Backend.publish(message)

          @context.persist(last: events.empty?)
        end
      end

      def publish_snapshot!
      end

      def publish_operations!
      end
    end
  end
end
