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
    @snapshots << message  if message.is_a? Sorceror::Message::Snapshot
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
    @operations.clear
    @events.clear
    @snapshots.clear
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
    @events.unshift(message) if message
    raise e
  end

  def process_snapshots
    while message = @snapshots.shift do
      _publish(message)
    end
  rescue => e
    @snapshots.unshift(message) if message
    raise e
  end

  def _publish(message)
    marshalled_message = message.class.new(payload: message.to_s,
                                           key: message.key,
                                           partition_key: message.partition_key)

    if message.class == Sorceror::Message::OperationBatch
      Sorceror::MessageProcessor.process(marshalled_message)
    else
      Sorceror::Observer.observer_groups.keys.each do |group|
        Sorceror::MessageProcessor.process(marshalled_message, group, filter || [])
      end
    end
  end
end
