require 'spec_helper'

RSpec.describe Sorceror::Middleware, 'Dead Letter' do
  before do
    $operation_raises = false
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer

      operation :fire => :fired do |model, attributes|
        raise 'fail!' if $operation_raises
      end
    end
  end

  let(:dead_letter)    { false }
  let(:max_retries)    { 100 }

  before { use_backend(:inline) { |config|
    config.retry = false
    config.dead_letter = dead_letter
  } }

  let!(:instance) { BasicModel.create(field_1: 'field_1') }

  let(:fire) do
    BasicModel.new(id: instance.id).fire
  end

  context 'when the operation raises' do
    before { $operation_raises = true }

    context 'without dead lettering' do
      it "doesn't process the message" do
        expect { fire }.to raise_error(RuntimeError)

        expect(Sorceror::Backend.driver.operations.count).to    eq(1)
        expect(Sorceror::Middleware::DeadLetter.queue.count).to eq(0)
      end
    end

    context 'with dead lettering' do
      let(:dead_letter) { true }

      it "skips the operation and adds the message to the dead letter queue" do
        fire

        expect(Sorceror::Backend.driver.operations.count).to    eq(0)
        expect(Sorceror::Middleware::DeadLetter.queue.count).to eq(1)
      end

      it 'persists sufficient information' do
        fire

        dead_message = Sorceror::Middleware::DeadLetter.queue.first

        expect(dead_message.type).to      eq('OperationBatch')
        expect(dead_message.payload).to   eq(JSON.dump(id: instance.id, operations: [{ name: 'fire', attributes: {} }], type: 'BasicModel'))
        expect(dead_message.exception).to match(/fail!$/)
        expect(dead_message.operation).to eq('operation-batch')
      end
    end
  end

  context 'when an event observer raises' do
    before do
      define_constant :BasicEventObserver do
        include Sorceror::Observer

        group :event_basic, event: true

        observer :didfire, BasicModel => :fired do |model|
          raise "event fail!"
        end
      end
    end

    context 'without dead lettering' do
      it "doesn't process the message" do
        expect { fire }.to raise_error(RuntimeError)

        expect(Sorceror::Backend.driver.events.count).to    eq(1)
        expect(Sorceror::Middleware::DeadLetter.queue.count).to eq(0)
      end
    end

    context 'with dead lettering' do
      let(:dead_letter) { true }

      it "skips the operation and adds the corrent message to the dead letter queue" do
        fire

        expect(Sorceror::Backend.driver.events.count).to    eq(0)
        expect(Sorceror::Middleware::DeadLetter.queue.count).to eq(1)

        dead_message = Sorceror::Middleware::DeadLetter.queue.first
        expect(dead_message.type).to      eq('Event')
        expect(dead_message.payload).to   eq(JSON.dump(id: instance.id, type: 'BasicModel', name: 'fired', attributes: {}))
        expect(dead_message.exception).to match(/RuntimeError.*event fail!$/)
        expect(dead_message.operation).to eq('event_basic:didfire')
      end
    end
  end

  context 'when a snapshot observer raises' do
    before do
      define_constant :BasicSnapshotObserver do
        include Sorceror::Observer

        group :snapshot_basic, snapshot: true

        observer :did, BasicModel do |model|
          raise "event fail!"
        end
      end
    end

    context 'without dead lettering' do
      it "doesn't process the message" do
        expect { fire }.to raise_error(RuntimeError)

        expect(Sorceror::Backend.driver.snapshots.count).to     eq(1)
        expect(Sorceror::Middleware::DeadLetter.queue.count).to eq(0)
      end
    end

    context 'with dead lettering' do
      let(:dead_letter) { true }

      it "skips the operation and adds the message to the dead letter queue" do
        fire

        expect(Sorceror::Backend.driver.snapshots.count).to     eq(0)
        expect(Sorceror::Middleware::DeadLetter.queue.count).to eq(1)

        dead_message = Sorceror::Middleware::DeadLetter.queue.first
        expect(dead_message.type).to      eq('Snapshot')
        expect(dead_message.payload).to   eq(JSON.dump(id: instance.id, type: 'BasicModel', attributes: instance.as_json))
        expect(dead_message.exception).to match(/RuntimeError.*event fail!$/)
        expect(dead_message.operation).to eq('snapshot_basic:did')
      end
    end
  end
end
