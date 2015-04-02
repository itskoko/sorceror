class Sorceror::Backend::Inline
  def connect
  end

  def disconnect
  end

  def connected?
    true
  end

  def publish(options={})
    message = Sorceror::Message.new(options[:payload], :metadata => MetaData)
    Sorceror::Operation.process(message)
    Sorceror.debug "[publish] [fake] #{options[:topic]}/#{options[:topic_key]} #{options[:payload]}"
  end

  def start_subscriber
  end

  def stop_subscriber
  end

  class MetaData
    def self.ack
    end
  end
end
