class Sorceror::Backend::Inline
  cattr_accessor :filter

  def connect
  end

  def disconnect
  end

  def connected?
    true
  end

  def publish(options={})
    if options[:topic] == Sorceror::Config.operation_topic
      message = Sorceror::Message::Operation.new(options[:payload], :metadata => MetaData)
      Sorceror::Operation.process(message)
    elsif options[:topic] == Sorceror::Config.event_topic
      message = Sorceror::Message::Event.new(options[:payload], :metadata => MetaData)

      Sorceror::Observer.observer_groups.each do |group, _|
        Sorceror::Event.process(message, group, filter)
      end
    else
      raise "Invalid payload attributes to publish #{options}"
    end
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
  end

  class MetaData
    def self.ack
    end
  end
end
