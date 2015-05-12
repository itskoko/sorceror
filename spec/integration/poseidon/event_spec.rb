require 'spec_helper'

RSpec.describe Sorceror do
  before do
    $observer_fired   = false
    $observer_raises  = false
    $observer_started = false
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      key :id

      field :field_1, type: String
      field :fired,   type: Boolean

      operation :fire => :fired do |model|
        model.fired = true
      end
    end

    define_constant :BasicObserver do
      include Sorceror::Observer

      group :basic

      observer :fired, BasicModel => :fired do |model|
        $observer_started = true
        raise if $observer_raises
        $observer_fired = true
      end
    end
  end

  before { use_backend(:poseidon) { |config| config.retry = retry_on_error } }
  before { run_subscriber_worker! }

  describe 'observer to an event' do
    let(:retry_on_error) { false }

    context 'when the observer runs successfully' do
      it 'runs the observer' do
        id = BSON::ObjectId.new
        BasicModel.new(id: id).create
        BasicModel.new(id: id).fire

        eventually do
          expect($observer_started).to eq(true)
        end

        eventually do
          expect($observer_fired).to eq(true)
        end
      end
    end

    context 'when the observer raises' do
      before { $observer_raises = true }

      context 'without retrying' do
        let(:retry_on_error) { false }

        it "doesn't run the observer" do
          id = BSON::ObjectId.new
          BasicModel.new(id: id).create
          BasicModel.new(id: id).fire

          eventually do
            expect($observer_started).to eq(true)
          end

          eventually do
            expect($observer_fired).to eq(false)
          end
        end
      end

      context 'with retrying' do
        let(:retry_on_error) { true }

        it "retries until the operation succeeds" do
          id = BSON::ObjectId.new
          BasicModel.new(id: id).create
          BasicModel.new(id: id).fire

          eventually do
            expect($observer_started).to eq(true)
          end

          $observer_raises = false

          eventually do
            expect($observer_fired).to eq(true)
          end
        end
      end
    end
  end
end
