require 'spec_helper'

RSpec.describe Sorceror, 'Topic' do
  before do
    $observer_fired = 0
  end

  before do
    define_constant :TopicModel do
      include Mongoid::Document
      include Sorceror::Model

      topic :a_topic

      field :field_1, type: String
      field :fired,   type: Boolean

      operation :fire => :fired do |model|
        model.fired = true
      end
    end

    define_constant :BasicObserver do
      include Sorceror::Observer

      group :basic, event: true, topic: :a_topic

      observer :fired, TopicModel => :fired do |instance, attributes|
        $observer_fired += 1
      end
    end
  end

  let(:id) { BSON::ObjectId.new }

  before do
    create_topics!(:a_topic)
    use_backend(:real)
    run_subscriber_worker!
  end

  it 'can use a different topic to publish operations and events' do
    TopicModel.new(id: id).create
    TopicModel.new(id: id).fire

    eventually { expect($observer_fired).to eq(1) }
  end
end
