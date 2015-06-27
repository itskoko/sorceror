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

  def publish(options={})
    Sorceror.debug "[publish] [null] #{options[:topic]}/#{options[:partition_key]} #{options[:payload]}"
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
  end
end
