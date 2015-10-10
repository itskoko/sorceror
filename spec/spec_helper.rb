require "codeclimate-test-reporter"
if ENV['CODECLIMATE_REPO_TOKEN']
  CodeClimate::TestReporter.start
elsif ENV["COVERAGE"]
  SimpleCov.start do
    add_filter "/spec"
  end
end

require 'rubygems'
require 'bundler'
Bundler.require

Dir["./spec/support/**/*.rb"].each {|f| require f}

if ENV['CI']
  require 'rspec/retry'
  RSpec.configure do |config|
    config.verbose_retry = true
    config.default_retry_count = 5
  end
end

RSpec.configure do |config|

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.disable_monkey_patching!

  if config.files_to_run.one?
    config.default_formatter = 'doc'
  else
   config.order = :random
  end

  config.color = true

  Kernel.srand config.seed

  config.include AsyncHelper
  config.include KafkaHelper
  config.include BackendHelper
end
