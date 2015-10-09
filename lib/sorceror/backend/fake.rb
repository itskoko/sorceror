class Sorceror::Backend::Fake
  cattr_accessor :filter

  attr_reader :operations
  attr_reader :events
  attr_reader :snapshots

  def initialize
    @operations = []
    @events = []
    @snapshots = []
  end

  def is_real?
    false
  end

  def connect
  end

  def disconnect
  end

  def connected?
    true
  end

  def publish(message)
    @operations << message if message.is_a? Sorceror::Message::OperationBatch
    @events << message     if message.is_a? Sorceror::Message::Event
     @snapshots << message if message.is_a? Sorceror::Message::Snapshot
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
    @operations.clear
    @events.clear
  end

  def process_operations
    while message = @operations.shift do
      _publish(message)
    end
  rescue => e
    @operations.unshift(message) if message
    raise e
  end

  def process_events
    while message = @events.shift do
      _publish(message)
    end
  rescue => e
    @operations.unshift(message) if message
    raise e
  end

  def _publish(message)
    marshalled_message = message.class.new(payload: message.to_s,
                                           partition_key: message.partition_key)

    if message.class == Sorceror::Message::OperationBatch
      Sorceror::MessageProcessor.process(marshalled_message)
    elsif message.class == Sorceror::Message::Event
      Sorceror::Observer.observer_groups.keys.each do |group|
        Sorceror::MessageProcessor.process(marshalled_message, group, filter || //)
      end
    else
      raise "Unknown message class #{message.class}"
    end
  end
end
