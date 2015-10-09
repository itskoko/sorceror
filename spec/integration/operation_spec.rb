require 'spec_helper'

RSpec.describe Sorceror do
  before do
    $skip = false
    $operation_fired_count = 0
    $operation_raises      = false
    $operation_proc        = -> {}
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer

      operation :fire => :fired do |model, attributes|
        skip if $skip
        $operation_fired_count += 1
        $operation_proc.call
      end
    end
  end

  before { use_backend(:fake) }

  describe 'Operation execution' do
    let(:instance) { BasicModel.new(field_1: 'field_1') }
    let(:fire) do
      instance.create
      BasicModel.new(id: instance.id).fire
    end

    it 'publishes the correct events' do
      fire

      process_operations!

      expect(Sorceror::Backend.driver.events.count).to eq(2)

      expect(Sorceror::Backend.driver.events[0].id).to         eq(instance.id)
      expect(Sorceror::Backend.driver.events[0].name).to       eq(:created)
      expect(Sorceror::Backend.driver.events[0].attributes).to eq({ 'id' => instance.id.to_s, 'field_1' => 'field_1' })

      expect(Sorceror::Backend.driver.events[1].id).to         eq(instance.id)
      expect(Sorceror::Backend.driver.events[1].name).to       eq(:fired)
      expect(Sorceror::Backend.driver.events[1].attributes).to eq({})
    end

    it 'publishes the correct snapshot' do
      fire

      process_operations!

      expect(Sorceror::Backend.driver.snapshots.count).to eq(2)

      expect(Sorceror::Backend.driver.snapshots[0].id).to         eq(instance.id)
      expect(Sorceror::Backend.driver.snapshots[0].attributes).to eq({ 'id'      => instance.id,
                                                                       'field_1' => 'field_1' })

      expect(Sorceror::Backend.driver.snapshots[1].id).to         eq(instance.id)
      expect(Sorceror::Backend.driver.snapshots[1].attributes).to eq({ 'id'      => instance.id,
                                                                       'field_1' => 'field_1' })
    end

    context 'when the operation creates another operation' do
      let(:another_instance) { BasicModel.new(field_2: 'field_2') }

      before do
        $operation_proc = -> { another_instance.create }
      end

      it 'publishes the other operation after the first operation has been processed' do
        fire

        expect(Sorceror::Backend.driver.operations.count).to eq(2)

        process_operations!

        expect(BasicModel.find(another_instance.id)).to_not eq(nil)
      end
    end


    describe 'skipping' do
      context 'when not skipping' do
        before { $skip = false }

        it "publishes the operation" do
          fire

          process_operations!

          expect($operation_fired_count).to eq(1)
        end
      end

      context 'when skipping' do
        before { $skip = true }

        it "does not publish the operation" do
          fire

          process_operations!

          expect($operation_fired_count).to                       eq(0)
          expect(Sorceror::Backend.driver.events.count).to        eq(1)
          expect(Sorceror::Backend.driver.events.first.name).to   eq(:created)
        end
      end
    end
  end
end
