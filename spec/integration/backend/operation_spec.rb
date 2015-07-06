require 'spec_helper'

RSpec.describe Sorceror::Backend, 'Operation' do
  before do
    $operation_starts = 0
    $operation_completions = 0
    $operation_raises = false
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer

      operation :update => :updated do |model, attributes|
        $operation_starts += 1
        model.assign_attributes(attributes)
        raise if $operation_raises
        $operation_completions += 1
      end
    end
  end

  let(:retry_on_error) { false }

  before { use_backend(:real) { |config| config.retry = retry_on_error } }
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
    let(:id) { BSON::ObjectId.new }

    before do
      BasicModel.new(id: id, field_1: 'field_1', field_2: 1).create
      BasicModel.new(id: id).update(field_1: 'field_1_updated', field_2: 2)
    end

    it 'updates the instance' do
      eventually do
        instance = BasicModel.first
        expect(instance.id).to eq(id)
        expect(instance.field_1).to eq('field_1_updated')
        expect(instance.field_2).to eq(2)
      end
    end

    context 'when the subscriber is restarted' do
      before do
        wait_for { expect(BasicModel.first.field_1).to eq('field_1_updated') }
        Sorceror::Backend.stop_subscriber
        Sorceror::Backend.start_subscriber(:all)
      end

      it 'does not reprocess the message' do
        sleep 1

        expect($operation_starts).to eq(1)
      end
    end

    context 'when the operation raises' do
      before { $operation_raises = true }

      context 'when the subscriber is restarted' do
        before do
          wait_for { $operation_starts == 1 }
          Sorceror::Backend.stop_subscriber
          Sorceror::Backend.start_subscriber(:all)
        end

        it 'reprocesses the message' do
          sleep 1

          expect($operation_starts).to eq(2)
        end
      end
    end
  end
end
