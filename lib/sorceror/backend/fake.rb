class Sorceror::Backend::Fake
  def initialize
    @messages = []
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
    @messages << message
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
    @messages.clear
  end

  def process
    while message = @messages.first do
      @inline.publish(message)
      @messages.shift
    end
  end
end
