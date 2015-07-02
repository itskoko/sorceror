class Sorceror::Payload
  attr_accessor :type
  attr_accessor :partition_key
  attr_accessor :body

  def initialize(options)
    @type = options.fetch(:type)
    @partition_key = options.fetch(:partition_key)
    @body = options.fetch(:body)
  end

  def topic
    Sorceror::Config.send("#{self.type}_topic")
  end
end
