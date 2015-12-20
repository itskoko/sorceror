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
    def create(attributes={})
      self.new(attributes).tap(&:create)
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
    self.collection.find(self.atomic_selector).update_one(self.as_document, upsert: true)
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
      Sorceror.warn "[#{self.class}][#{self.id}] ignoring #{e.message}"
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
      message = Sorceror::Message::OperationBatch.new(:payload => {
        :id          => self.id,
        :operations  => @payloads.reverse,
        :type        => self.class.to_s
      })

      Sorceror::Backend.publish(message)
    end
  end

  class Context
    attr_reader :instance

    def initialize(instance)
      @instance = instance
    end

    def operation
      @operation ||= Operation.new(self)
    end

    def observer(observer_group)
      @observers ||= {}
      @observers[observer_group] ||= Observer.new(observer_group, self)
    end

    class Operation
      attr_reader :attrs
      attr_accessor :current_hash

      FIELD = :__co__

      def initialize(context)
        @context = context
        @attrs = @context.instance[FIELD] || { operation: { queued: [], events: [] }, event: {} }
        @context.instance[FIELD] = @attrs
      end

      def last_hash
        @attrs[:last_hash]
      end

      def persist(last: false)
        @attrs.merge!(last_hash: current_hash) if last
        @context.instance.collection.find(@context.instance.atomic_selector).update_one('$set' => { FIELD => @attrs })
      end

      def events
        @attrs[:operation][:events]
      end

      def queue(message)
        @attrs[:operation][:queued] << YAML.dump(message)
      end

      def queued
        @attrs[:operation][:queued]
      end

      def pending?
        !events.empty?
      end

      def publish!
        publish_events!
        publish_snapshot!
        publish_operations!
        persist(last: true)
      end

      def publish_events!
        until events.empty? do
          event = events.shift
          message = Sorceror::Message::Event.new(:payload       => {
            :id         => @context.instance.id,
            :type       => @context.instance.class.to_s,
            :name       => event[0],
            :attributes => event[1]
          })

          Sorceror::Backend.publish(message)

          persist
        end
      end

      def publish_snapshot!
        message = Sorceror::Message::Snapshot.new(:payload       => {
          :id         => @context.instance.id,
          :type       => @context.instance.class.to_s,
          :attributes => @context.instance.as_json
        })

        Sorceror::Backend.publish(message)
      end

      def publish_operations!
        until queued.empty?
          Sorceror::Backend.publish(YAML.load(queued.shift))
          persist
        end
      end
    end

    class Observer
      FIELD = :__ce__

      def initialize(observer_group, context)
        @context = context
        @observer_group = observer_group
        @context.instance[FIELD] ||= {}
        @queued = @context.instance[FIELD][@observer_group] || []
      end

      def persist
        @context.instance.collection.find(@context.instance.atomic_selector).update_one('$set' => { "#{FIELD}.#{@observer_group}" => @queued }) if @queued
      end

      def queue(observer)
        @queued << observer
      end

      def shift_queued
        @queued.shift
      end

      def pending?
        !@queued.empty?
      end
    end
  end
end
