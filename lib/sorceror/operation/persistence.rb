module Sorceror::Operation::Persistence
  extend ActiveSupport::Concern

  included do
    operation :__create__ do |message|
      begin
        self.mongoid_create!(message.attributes)
      rescue StandardError => e
        if e.message =~ /E11000/ # Duplicate key
          Promiscuous.warn "[#{message.type}][#{message.attributes['id']}] ignoring already created record"
        end
      end
    end
  end
end
