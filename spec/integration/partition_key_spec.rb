require 'spec_helper'

RSpec.describe Sorceror, 'Parition Key' do
  before { use_backend(:null) }

  context 'without a key defined' do
    before do
      define_constant :KeyModel do
        include Mongoid::Document
        include Sorceror::Model

        field :field_1, type: String
        field :field_2, type: Integer
      end
    end

    it 'uses the id of the object' do
      instance = KeyModel.new

      expect(instance.partition_key).to eq("key_models/#{instance.id}")
    end
  end
end
