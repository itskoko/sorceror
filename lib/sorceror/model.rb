module Sorceror
  module Model
    extend ActiveSupport::Concern

    included do
      class << self
        [:create, :create!].each do |method|
          alias_method "orig_#{method}", method
          define_method(method) do |*args, &block|
            raise "Direct persistence not supported with Sorceror"
          end
        end
      end

      mattr_accessor :partition_key

      [:save, :save!, :update_attributes!, :update_attributes].each do |method|
        alias_method "mongoid_#{method}", method
        define_method(method) do |*args, &block|
          raise "Direct persistence not supported with Sorceror"
        end
      end
    end

    def initialize(attributes)
    end

    module ClassMethods
      def publish(attributes)
        self.new(attributes).publish
      end

      def key(partition_key)
        self.partition_key = partition_key
      end
    end

    def payload
      payload = {
        operation: :created,
        id: self.id
      }
      payload.merge(attributes: self.as_json)
    end

    def publish
      return false unless valid?

      raise "Already published" if @published # TODO Use error classes
      raise "Already persisted" if persisted?

      payload_opts = { :topic      => model_name.plural,
                       :topic_key  => self.send(partition_key).to_s,
                       :payload    => MultiJson.dump(payload),
                       :async      => false }

      Sorceror::Backend.publish(payload_opts)

      @published = true
    end

    def initialize(*args)
      super
    end

    def as_json(options={})
      attrs = super
      id = attrs.delete('_id')
      attrs.merge('id' => id)
    end
  end
end

