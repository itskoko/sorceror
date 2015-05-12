DATABASE='sorceror_test'

Mongoid.configure do |config|
  uri = "mongodb://localhost:27017/"
  uri += "#{DATABASE}"

  config.sessions = { :default => { :uri => uri } }

  if ENV['MOPED_LOGGER_LEVEL']
    Moped.logger = Logger.new(STDOUT).tap { |l| l.level = ENV['MOPED_LOGGER_LEVEL'].to_i }
  end
end

RSpec.configure do |config|
  config.before(:each) do
    Mongoid::Sessions.disconnect
    Mongoid.purge!
  end
end
