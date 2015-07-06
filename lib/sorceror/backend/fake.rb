class Sorceror::Backend::Fake
  attr_reader :operations
  attr_reader :events

  def initialize
    @operations = []
    @events = []
    @inline = Sorceror::Backend::Inline.new
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
    @operations << message if message.is_a? Sorceror::Message::Operation
    @events << message     if message.is_a? Sorceror::Message::Event
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
    @operations.clear
    @events.clear
  end

  def process_operations
    while message = @operations.first do
      @inline.publish(message)
      @operations.shift
    end
  end

  def process_events
    while message = @events.first do
      @inline.publish(message)
      @events.shift
    end
  end
end
