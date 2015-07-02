class Sorceror::Backend::Inline
  cattr_accessor :filter

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
    # Serialize -> Deserialize: ensures real backend is mimicked
    marshalled_message = message.class.new(payload: message.to_s,
                                           partition_key: message.partition_key)

    if message.class == Sorceror::Message::Operation
      Sorceror::MessageProcessor.process(marshalled_message)
    elsif message.class == Sorceror::Message::Event
      Sorceror::Observer.observer_groups.keys.each do |group|
        Sorceror::MessageProcessor.process(marshalled_message, group, filter || //)
      end
    else
      raise "Unknown message class #{message.class}"
    end
  end

  def start_subscriber(consumer)
  end

  def stop_subscriber
  end
end
