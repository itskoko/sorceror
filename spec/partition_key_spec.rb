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

  context 'with a key defined on a related model' do
      let(:instance) { KeyModel.new(related_model: relation).create }
      let(:relation) { RelatedModel.new.create }

      before do
        define_constant :KeyModel do
          include Mongoid::Document
          include Sorceror::Model

          field :field_1, type: String
          field :field_2, type: Integer

          belongs_to :related_model

          partition_with :related_model
        end

        define_constant :RelatedModel do
          include Mongoid::Document
          include Sorceror::Model

          field :field_1, type: String
        end
      end

    it 'uses the key of the related model' do
      expect(instance.partition_key).to eq(relation.partition_key)
    end

    it "does't require the instance have the relation instantiated" do
      expect(KeyModel.new(id: instance.id).partition_key).to eq(relation.partition_key)
    end
  end

  context 'with a key invalid key defined' do
    it 'raises' do
      expect {
        define_constant :InvalidKeyModel do
          include Mongoid::Document
          include Sorceror::Model

          partition_with :xxx

          field :field_1, type: String
        end
      }.to raise_error(ArgumentError)
    end
  end
end
