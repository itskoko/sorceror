require 'spec_helper'

RSpec.describe Sorceror, 'create' do
  before do
    $observer_model = nil
    $observer_fired_count = 0
  end

  before do
    define_constant :CreateModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer
    end

    define_constant :CreateObserver do
      include Sorceror::Observer

      group :consistency

      observer :created, CreateModel => :created do |model|
        $observer_model = model
        $observer_fired_count += 1
      end
    end
  end

  before { use_backend(:fake) }

  context 'when two identical create operations are posted' do
    let(:id) { BSON::ObjectId.new }

    before do
      CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create
      CreateModel.new(id: id, field_1: 'another_field_1', field_2: 1).create
    end

    it "processes the first operation" do
      process!

      expect($observer_fired_count).to        eq(1)
      expect(CreateModel.count).to            eq(1)
      expect(CreateModel.find(id).field_1).to eq('field_1')
    end
  end

  it 'persists synchronously (does not require the subscriber running)' do
    CreateModel.new(field_1: 'field_1', field_2: 1).create

    expect(CreateModel.unscoped.count).to eq(1)
  end

  context 'when persistence fails' do
    before { allow_any_instance_of(Moped::Operation::Write).to receive(:execute).and_raise("DB DOWN!!!") }

    it 'the operation fails' do
      expect { CreateModel.new(id: BSON::ObjectId.new, field_1: 'field_1', field_2: 1).create }.to raise_error(RuntimeError)
    end

    context 'and the operation is posted again' do
      it 'succeeds but the initial operation is processed' do
        id = BSON::ObjectId.new

        expect { CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create }.to raise_error(RuntimeError)

        allow_any_instance_of(Moped::Operation::Write).to receive(:execute).and_call_original

        process!

        expect { CreateModel.new(id: id, field_1: 'another_field_1', field_2: 1).create }.to_not raise_error

        process!

        expect($observer_fired_count).to        eq(1)
        expect(CreateModel.count).to            eq(1)
        expect(CreateModel.find(id).field_1).to eq('field_1')
      end
    end
  end

  context 'when publishing fails' do
    before { allow(Sorceror::Backend).to receive(:publish).and_raise("Backend down!!!") }

    it 'the operation fails, an event is not published and an instance is not created' do
      id = BSON::ObjectId.new
      expect { CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create }.to raise_error(RuntimeError)

      process!

      expect($observer_fired_count).to           eq(0)
      expect(CreateModel.where(id: id).count).to eq(0)
    end

    context 'and the operation is posted again with different attributes but publishing succeeds' do
      it 'the instance is created with the new attributes and the event is published' do
        id = BSON::ObjectId.new
        expect { CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create }.to raise_error(RuntimeError)

        allow(Sorceror::Backend).to receive(:publish).and_call_original

        CreateModel.new(id: id, field_1: 'another_field_1', field_2: 1).create

        process!

        expect($observer_fired_count).to        eq(1)
        expect(CreateModel.find(id).field_1).to eq('another_field_1')
      end
    end
  end

  context 'when publishing of the event fails' do
    let(:id) { BSON::ObjectId.new }

    it 'retries until the event is published' do
      CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create

      allow(Sorceror::Backend).to receive(:publish).and_raise("Backend down!!!")

      expect { process! }.to raise_error(RuntimeError)

      allow(Sorceror::Backend).to receive(:publish).and_call_original

      process!

      expect($observer_fired_count).to eq(1)
    end
  end

  context 'when the underlying model changes while processing an event' do
    let(:id) { BSON::ObjectId.new }

    it 'does not use the pass the latest state of the model to the observer' do
      CreateModel.new(id: id, field_1: 'field_1').create

      process_operations!
      CreateModel.collection.find(_id: id).update('$set' => { field_1: 'field_1_updated' })
      process_events!

      expect($observer_model.field_1).to eq('field_1')
    end
  end
end
