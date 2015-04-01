class Sorceror::Railtie < Rails::Railtie
  initializer 'load sorceror' do
    config.before_initialize do
      Mongoid.preload_models = true

      if Sorceror::Config.retry.nil?
        Sorceror::Config.retry = Rails.env.test? ? true : false
      end
    end
  end
end
