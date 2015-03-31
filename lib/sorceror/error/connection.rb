class Sorceror::Error::Connection < Sorceror::Error::Base
  attr_accessor :url, :inner

  def initialize(url, options={})
    super(nil)
    @url = url
    @inner = options[:inner]
    set_backtrace(@inner.backtrace) if @inner
  end

  def message
    if @inner
      "Lost connection with #{@url}: #{@inner.class}: #{@inner.message}"
    else
      "Lost connection with #{@url}"
    end
  end

  alias to_s message
end
