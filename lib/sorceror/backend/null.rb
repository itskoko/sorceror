class Sorceror::Backend::Null
  def connect
  end

  def is_real?
    false
  end

  def disconnect
  end

  def connected?
    true
  end

  def publish(message)
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
  end
end
