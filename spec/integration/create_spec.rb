require 'spec_helper'

RSpec.describe Sorceror do
  before do
    $observer_fired_count = 0
  end

  before do
    define_constant :CreateModel do
      include Mongoid::Document
      include Sorceror::Model

      key :id

      field :field_1, type: String
      field :field_2, type: Integer
    end

    define_constant :CreateObserver do
      include Sorceror::Observer

      group :consistency

      observer :created, CreateModel => :created do |model|
        $observer_fired_count += 1
      end
    end
  end

  before { use_backend(:inline) }
  before { run_subscriber_worker! }

  context 'when two identical create messages are published' do
    before do
      id = BSON::ObjectId.new
      CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create
      CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create
    end

    it "only published one event" do
      expect($observer_fired_count).to eq(1)
    end
  end
end
