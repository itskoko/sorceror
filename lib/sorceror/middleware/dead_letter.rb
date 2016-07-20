class Sorceror::Middleware::DeadLetter
  def self.queue
    Queue
  end

  def run(message)
    begin
      yield
    rescue StandardError => e
      raise e unless Sorceror::Config.dead_letter
      Sorceror::Config.error_notifier.call(e)
      dead_letter(message, e)
    end
  end

  def dead_letter(message, exception)
    Sorceror::Middleware::DeadLetter.queue.create(type: message.class.to_s.demodulize,
                                                  payload: message.to_s,
                                                  operation: exception.operation_name,
                                                  exception: exception.message)
  end

  class Queue
    include Mongoid::Document

    field :type
    field :payload
    field :exception
    field :operation
  end
end
