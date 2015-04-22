class Sorceror::Railtie < Rails::Railtie
  initializer 'load sorceror' do |app|
    config.before_initialize do
      Mongoid.preload_models = true

      if Sorceror::Config.retry.nil?
        Sorceror::Config.retry = Rails.env.test? ? false : true
      end
    end

    config.to_prepare do
      ::Rails::Sorceror.load_observers(app)
    end
  end
end

module Rails::Sorceror
  extend self

  # From Mongoid lib/rails/mongoid.rb
  def load_observers(app)
    path = Rails.root.join('app', 'observers')
    files = Dir.glob("#{path}/**/*.rb")

    files.sort.each do |file|
      load_observer(file.gsub("#{path}/" , "").gsub(".rb", ""))
    end
  end

  def preload_obs(app)
    load_observer(app) if true
  end

  private

  def load_observer(file)
    begin
      require_dependency(file)
    rescue Exception => e
      Logger.new($stdout).warn(e.message)
    end
  end
end
