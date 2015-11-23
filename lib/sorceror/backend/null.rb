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
    Sorceror.debug "[publish] [null] #{message.topic}/#{message.partition_key}/#{message.key} #{message.to_s}"
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
  end
end
