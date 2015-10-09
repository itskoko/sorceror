require 'spec_helper'

RSpec.describe Sorceror, 'callbacks' do
  before do
    define_constant :CallbackModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: Array, default: []
      field :field_2, type: Array, default: []

      operation :operation_1 => :operation_1d do |instance|
        instance.field_1 << 1
      end

      operation :operation_2 => :operation_2d do |instance|
        instance.field_1 << 2
      end

      before_operation :callback_1
      before_create    :callback_2

      def callback_1
        self.field_2 << 1
      end

      def callback_2
        operation_1
        self.field_2 << 2
      end
    end
  end

  before { use_backend(:fake) }

  it 'runs callbacks in order and publishes the final state of the instance' do
    CallbackModel.create

    process_operations!

    payload_attributes = Sorceror::Backend.driver.snapshots.first.payload[:attributes]
    expect(payload_attributes["field_1"]).to eq([1])
    expect(payload_attributes["field_2"]).to eq([2,1])
  end
end
