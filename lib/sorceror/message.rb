class Sorceror::Message
  attr_accessor :payload, :parsed_payload

  def initialize(payload, options={})
    self.payload = payload
    @metadata = options[:metadata]
  end

  def parsed_payload
    @parsed_payload ||= if payload.is_a?(Hash)
      payload.with_indifferent_access
    else
      MultiJson.load(payload)
    end
  end

  def type
    parsed_payload['type']
  end

  def to_s
    "#{app} -> #{types}"
  end

  def ack
    Sorceror.debug "[receive] #{payload}"

    @metadata.ack
  rescue StandardError => e
    # We don't care if we fail, the message will be redelivered at some point
    Sorceror.warn "[receive] Some exception happened, but it's okay: #{e}\n#{e.backtrace.join("\n")}"
    Sorceror::Config.error_notifier.call(e)
  end

  class Operation < self
    def operations
      parsed_payload['operations'].map { |op| Payload.new(op) }
    end

    class Payload
      def initialize(payload)
        @payload = payload
      end

      def id
        @payload['id']
      end

      def name
        @payload['name'].to_sym
      end

      def attributes
        @payload['attributes']
      end
    end
  end

  class Event < self
    def id
      parsed_payload['id']
    end

    def events
      parsed_payload['events']
    end

    def attributes
      parsed_payload['attributes']
    end
  end
end
