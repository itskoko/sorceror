require 'spec_helper'

RSpec.describe Sorceror do
  before do
    $operation_raises    = false
    $operation_started   = false
    $operation_completed = false
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      key :id

      field :field_1, type: String
      field :field_2, type: Integer

      operation :update => :updated do |model, attributes|
        $operation_started = true
        raise if $operation_raises
        model.assign_attributes(attributes)
        $operation_completed = true
      end
    end
  end

  let(:retry_on_error) { false }

  before { use_backend(:poseidon) { |config| config.retry = retry_on_error } }
  before { run_subscriber_worker! }

  describe 'create' do
    it 'creates the instance' do
      id = BSON::ObjectId.new
      BasicModel.new(id: id, field_1: 'field_1', field_2: 1).create

      eventually do
        instance = BasicModel.first
        expect(instance.id).to eq(id)
        expect(instance.field_1).to eq('field_1')
        expect(instance.field_2).to eq(1)
      end
    end
  end

  describe 'update operation' do
    it 'updates the instance' do
      id = BSON::ObjectId.new
      BasicModel.new(id: id, field_1: 'field_1', field_2: 1).create
      BasicModel.new(id: id).update(field_1: 'field_1_updated', field_2: 2)

      eventually do
        instance = BasicModel.first
        expect(instance.id).to eq(id)
        expect(instance.field_1).to eq('field_1_updated')
        expect(instance.field_2).to eq(2)
      end
    end

    context 'when the operation raises' do
      before { $operation_raises = true }

      context 'without retrying' do
        it "doesn't run the observer" do
          id = BSON::ObjectId.new
          BasicModel.new(id: id, field_1: 'field_1', field_2: 1).create
          BasicModel.new(id: id).update(field_1: 'field_1_updated', field_2: 2)

          eventually do
            expect($operation_started).to eq(true)
          end

          eventually do
            expect($operation_completed).to eq(false)
          end
        end
      end

      context 'with retrying' do
        let(:retry_on_error) { true }

        it "retries until the operation succeeds" do
          id = BSON::ObjectId.new
          BasicModel.new(id: id, field_1: 'field_1', field_2: 1).create
          BasicModel.new(id: id).update(field_1: 'field_1_updated', field_2: 2)

          eventually do
            expect($operation_started).to eq(true)
          end

          $operation_raises = false

          eventually do
            expect($operation_completed).to eq(true)
          end
        end
      end
    end
  end
end
