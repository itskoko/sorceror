require 'spec_helper'

RSpec.describe Sorceror, 'create' do
  before do
    define_constant :CreateModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer
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
      process_operations!

      expect(Sorceror::Backend.driver.events.count).to  eq(1)
      expect(CreateModel.count).to                      eq(1)
      expect(CreateModel.find(id).field_1).to           eq('field_1')
    end
  end

  it 'persists synchronously (does not require the subscriber running)' do
    CreateModel.new(field_1: 'field_1', field_2: 1).create

    expect(CreateModel.unscoped.count).to eq(1)
  end

  context 'when persistence fails' do
    before { allow_any_instance_of(Moped::Operation::Write).to receive(:execute).and_raise("DB DOWN!!!") }

    it 'the operation does not fail' do
      expect { CreateModel.new(id: BSON::ObjectId.new, field_1: 'field_1', field_2: 1).create }.to_not raise_error
    end

    context 'and the operation is posted again' do
      it 'succeeds but the initial operation is processed' do
        id = BSON::ObjectId.new

        CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create

        allow_any_instance_of(Moped::Operation::Write).to receive(:execute).and_call_original

        process_operations!

        CreateModel.new(id: id, field_1: 'another_field_1', field_2: 1).create

        process_operations!

        expect(Sorceror::Backend.driver.events.count).to eq(1)
        expect(CreateModel.count).to                     eq(1)
        expect(CreateModel.find(id).field_1).to          eq('field_1')
      end
    end
  end

  context 'when publishing fails' do
    before { allow(Sorceror::Backend).to receive(:publish).and_raise("Backend down!!!") }

    it 'the operation fails, an event is not published and an instance is not created' do
      id = BSON::ObjectId.new
      expect { CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create }.to raise_error(RuntimeError)

      process_operations!

      expect(Sorceror::Backend.driver.events.count).to eq(0)
      expect(CreateModel.where(id: id).count).to       eq(0)
    end

    context 'and the operation is posted again with different attributes but publishing succeeds' do
      it 'the instance is created with the new attributes and the event is published' do
        id = BSON::ObjectId.new
        expect { CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create }.to raise_error(RuntimeError)

        allow(Sorceror::Backend).to receive(:publish).and_call_original

        CreateModel.new(id: id, field_1: 'another_field_1', field_2: 1).create

        process_operations!

        expect(Sorceror::Backend.driver.events.count).to eq(1)
        expect(CreateModel.find(id).field_1).to          eq('another_field_1')
      end
    end
  end

  context 'when publishing of the event fails' do
    let(:id) { BSON::ObjectId.new }

    it 'retries until the event is published' do
      CreateModel.new(id: id, field_1: 'field_1', field_2: 1).create

      allow(Sorceror::Backend).to receive(:publish).and_raise("Backend down!!!")

      expect { process_operations! }.to raise_error(RuntimeError)

      allow(Sorceror::Backend).to receive(:publish).and_call_original

      process_operations!

      expect(Sorceror::Backend.driver.events.count).to eq(1)
    end
  end
end
