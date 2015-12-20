DATABASE='sorceror_test'

Mongoid.configure do |config|
  config.load_configuration(:clients => { :default => { :database => DATABASE, :hosts => ["localhost:27017"] }})
end

log_level = ENV['MONGO_LOGGER_LEVEL'] ? ENV['MONGO_LOGGER_LEVEL'] : 2
Mongoid.logger = Logger.new(STDOUT).tap { |l| l.level = log_level }
Mongo::Logger.logger = Logger.new(STDOUT).tap { |l| l.level = log_level }

RSpec.configure do |config|
  config.before(:each) do
    Mongoid::Config.purge!
    Mongoid::Clients.disconnect
  end
end
