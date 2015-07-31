class Sorceror::Backend::Inline
  cattr_accessor :filter

  def initialize
    @fake = Sorceror::Backend::Fake.new
    @running = false
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
    @fake.publish(message)

    unless @running
      @fake.filter = filter
      @running = true
      until @fake.operations.empty? && @fake.events.empty?
        @fake.process_operations
        @fake.process_events
      end
      @running = false
      @fake.filter = nil
    end
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
  end

  def operations
    @fake.operations
  end
end
