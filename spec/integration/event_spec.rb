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

      group :basic, event: true

      observer :fired, BasicModel => :fired do |instance, attributes|
        $observer_starts += 1
        raise if $observer_raises
        $observer_fired += 1
      end
    end
  end

  before do
    use_backend(:real) { |config|
      config.retry = retry_on_error
      config.max_retries = max_retries
    }
    run_subscriber_worker!
  end

  describe 'observer to an event' do
    let(:retry_on_error) { false }
    let(:max_retries)    { 100 }
    let(:id)             { BSON::ObjectId.new }

    let(:fire) do
      BasicModel.new(id: id).create
      BasicModel.new(id: id).fire
    end

    context 'when the observer runs successfully' do
      before { fire }

      it 'runs the observer' do
        eventually do
          expect($observer_starts).to eq(1)
        end

        eventually do
          expect($observer_fired).to eq(1)
        end
      end
    end

    context 'when the subscriber is restarted' do
      before do
        fire
        wait_for { expect($observer_starts).to eq(1) }
        Sorceror::Backend.stop_subscriber
        Sorceror::Backend.start_subscriber(:all)
      end

      it 'does not reprocess the message' do
        sleep 1

        expect($observer_starts).to eq(1)
      end
    end

    context 'with existing events' do
      before { fire }

      context 'with an observer that is not trailing' do
        before do
          $not_trailing_fired = 0
          define_constant :NotTrailingObserver do
            include Sorceror::Observer

            group :not_trailing, event: true, trail: false

            observer :fired, BasicModel => :fired do |model, attributes|
              $not_trailing_fired += 1
            end
          end

          sleep 1

          Sorceror::Backend.stop_subscriber
          Sorceror::Backend.start_subscriber(:all)
        end

        it 'processes any existing events' do
          pending "A safe way to test trailing"
          eventually { expect($not_trailing_fired).to eq(1) }
        end
      end

      context 'with an observer that is trailing' do
        before do
          $trailing_fired = 0
          define_constant :TrailingObserver do
            include Sorceror::Observer

            group :trailing, event: true, trail: true

            observer :fired, BasicModel => :fired do |model, attributes|
              $trailing_fired += 1
            end
          end

          sleep 1

          Sorceror::Backend.stop_subscriber
          Sorceror::Backend.start_subscriber(:all)
        end

        it 'does not process any existing events' do
          sleep 1

          expect($trailing_fired).to eq(0)
        end
      end
    end

    context 'when the observer raises' do
      before { $observer_raises = true }

      context 'when the subscriber is restarted' do
        before do
          fire
          wait_for { expect($observer_starts).to eq(1) }
          Sorceror::Backend.stop_subscriber
          Sorceror::Backend.start_subscriber(:all)
        end

        it 'reprocesses the message' do
          sleep 1

          expect($observer_starts).to eq(2)
        end
      end

      context 'when the max number of retries is exceeded' do
        let(:max_retries) { 0 }

        before do
          fire
          wait_for { expect($observer_starts).to eq(1) }
        end

        it 'stops all subscribers' do
          eventually { expect(Sorceror::Backend.subscriber_stopped?).to eq(true) }
        end
      end
    end
  end
end
