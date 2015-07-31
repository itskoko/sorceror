class Sorceror::Middleware::Retrying
  def run(message)
    retries = 0

    begin
      yield
    rescue StandardError => e
      Sorceror::Config.error_notifier.call(e)
      raise e unless Sorceror::Config.retry
      if retries < Sorceror::Config.max_retries
        retries += 1
        sleep 0.1 * 3**retries
        retry
      else
        raise e
      end
    end
  end
end
