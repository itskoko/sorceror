class Sorceror::Message
  attr_accessor :partition_key
  attr_accessor :payload

  def initialize(options)
    @payload       = options.fetch(:payload)
    @partition_key = options.fetch(:partition_key)
  end

  def parsed_payload
    @parsed_payload ||= if payload.is_a?(Hash)
      payload.with_indifferent_access
    else
      MultiJson.load(payload)
    end
  end

  def to_s
    @to_s ||= MultiJson.dump(parsed_payload)
  end

  def type
    parsed_payload['type']
  end

  def topic
    raise NotImplementedError
  end

  class Operation < self
    def topic
      Sorceror::Config.operation_topic
    end

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
    def topic
      Sorceror::Config.event_topic
    end

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

  def Snapshot
    def topic
      Sorceror::Config.event_topic
    end

    def id
      parsed_payload['id']
    end

    def attributes
      parsed_payload['attributes']
    end
  end
end
