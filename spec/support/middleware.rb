RSpec.configure do |config|
  config.before do
    Sorceror::Middleware.reset!
  end
end
