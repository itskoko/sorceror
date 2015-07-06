require 'spec_helper'

RSpec.describe Sorceror::Backend, 'Event' do
  before do
    $observer_fired   = 0
    $observer_raises  = false
    $observer_starts  = 0
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

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
        $observer_starts += 1
        raise if $observer_raises
        $observer_fired += 1
      end
    end
  end

  before { use_backend(:real) { |config| config.retry = retry_on_error } }
  before { run_subscriber_worker! }

  describe 'observer to an event' do
    let(:retry_on_error) { false }
    let(:id)             { BSON::ObjectId.new }

    before do
      BasicModel.new(id: id).create
      BasicModel.new(id: id).fire
    end

    context 'when the observer runs successfully' do
      it 'runs the observer' do
        eventually do
          expect($observer_starts).to eq(1)
        end

        eventually do
          expect($observer_fired).to eq(1)
        end
      end

      context 'when the subscriber is restarted' do
        before do
          wait_for { expect($observer_starts).to eq(1) }
          Sorceror::Backend.stop_subscriber
          Sorceror::Backend.start_subscriber(:all)
        end

        it 'does not reprocess the message' do
          sleep 1

          expect($observer_starts).to eq(1)
        end
      end

      context 'when the observer raises' do
        before { $observer_raises = true }

        context 'when the subscriber is restarted' do
          before do
            wait_for { expect($observer_starts).to eq(1) }
            Sorceror::Backend.stop_subscriber
            Sorceror::Backend.start_subscriber(:all)
          end

          it 'reprocesses the message' do
            sleep 1

            expect($observer_starts).to eq(2)
          end
        end
      end
    end
  end
end
