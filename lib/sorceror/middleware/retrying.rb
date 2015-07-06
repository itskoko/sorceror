class Sorceror::Middleware::Retrying
  # TODO Make configurable
  MAX = 50

  def run(message)
    retries = 0

    begin
      yield
    rescue StandardError => e
      Sorceror::Config.error_notifier.call(e)
      raise e unless Sorceror::Config.retry
      if retries < MAX
        retries += 1
        sleep 0.1 * 3**retries
        retry
      end
    end
  end
end
