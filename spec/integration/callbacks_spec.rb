require 'spec_helper'

RSpec.describe Sorceror, 'callbacks' do
  before do
    define_constant :CallbackModel do
      include Mongoid::Document
      include Sorceror::Model

      key :id

      field :field_1, type: Integer

      operation :operation_1 => :operation_1d do |instance|
        instance.field_1 = 1
      end

      operation :operation_2 => :operation_2d do |instance|
        instance.field_1 = 2
      end

      before_operation :callback_1
      before_operation :callback_2

      def callback_1
        operation_1
      end

      def callback_2
        operation_2
      end
    end
  end

  before { use_backend(:inline) }

  it 'runs both callbacks in order' do
    CallbackModel.create(field_1: 0)

    expect(CallbackModel.first.field_1).to eq(2)
  end
end
