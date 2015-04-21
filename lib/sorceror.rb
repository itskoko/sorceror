require 'active_support/dependencies/autoload'
require 'active_support/deprecation'
require 'active_support/core_ext'
require 'active_model/callbacks'

require 'multi_json'

module Sorceror
  def self.require_for(gem, file)
    only_for(gem) { require file }
  end

  def self.only_for(gem, &block)
    require gem
    block.call
  rescue LoadError
  end

  require 'sorceror/autoload'
  require_for 'rails', 'sorceror/railtie'

  extend Sorceror::Autoload
  autoload :Model, :Error, :Backend, :Config, :Message, :Worker,
           :Operation, :Message, :Observer, :CLI, :Serializer, :Event

  class << self
    def configure(&block)
      Config.configure(&block)
    end

    [:debug, :info, :error, :warn, :fatal].each do |level|
      define_method(level) do |msg|
        Sorceror::Config.logger.__send__(level, "[sorceror] #{msg}")
      end
    end

    def connect
      Backend.connect
      @should_be_connected = true
    end

    def disconnect
      Backend.disconnect
      @should_be_connected = false
    end

    def should_be_connected?
      !!@should_be_connected
    end

    def health_check
      health = { :backend => true }

      begin
        Backend.ensure_connected
      rescue StandardError
        health[:backend] = false
      end

      health[:status]  = health.all?{|key, value| value == true} ?  :ok : :service_unavailable

      health
    end

    def healthy?
      health_check[:status] == :ok
    end

    def ensure_connected
      unless should_be_connected?
        connect
      end
    end
  end

  at_exit { self.disconnect rescue nil }
end
