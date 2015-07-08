require 'spec_helper'

RSpec.describe Sorceror do
  before do
    $operation_fired_count = 0
    $operation_raises      = false
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
      end
    end
  end

  before { use_backend(:fake) }

  describe 'Operation execution' do
    let(:fire) do
      instance = BasicModel.create(field_1: 'field_1')
      BasicModel.new(id: instance.id).fire
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
          expect(Sorceror::Backend.driver.events.first.events).to eq([:created])
        end
      end
    end
  end
end
