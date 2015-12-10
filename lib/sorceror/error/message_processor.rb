class Sorceror::Error::MessageProcessor < Sorceror::Error::Base
  attr_accessor :inner, :operation_name

  def initialize(inner, operation_name, options={})
    super(nil)
    self.inner = inner
    set_backtrace(inner.backtrace)
    self.operation_name = operation_name
  end

  def message
    "#{self.operation_name}: #{self.inner.class} #{self.inner.message}"
  end

  alias to_s message
end
