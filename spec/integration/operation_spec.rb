require 'spec_helper'

RSpec.describe Sorceror do
  before do
    $observer_fired_count = 0
    $operation_fired_count = 0
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

    define_constant :BasicObserver do
      include Sorceror::Observer

      group :basic

      observer :fired, BasicModel => :fired do |model|
        $observer_fired_count += 1
      end
    end
  end

  before { use_backend(:inline) }

  describe 'Operation execution' do
    let(:fire) do
      instance = BasicModel.create(field_1: 'field_1')
      BasicModel.new(id: instance.id).fire
    end

    context 'when not skipping' do
      before { $skip = false }

      it "publishes the operation" do
        fire

        expect($observer_fired_count).to  eq(1)
        expect($operation_fired_count).to eq(1)
      end
    end

    context 'when skipping' do
      before { $skip = true }

      it "does not publish the operation" do
        fire

        expect($observer_fired_count).to  eq(0)
        expect($operation_fired_count).to eq(0)
      end
    end
  end
end
