require 'spec_helper'

RSpec.describe Sorceror::Model do
  before do
    define_constant :SomeModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
    end
  end

  describe 'key' do
    let(:instance) { SomeModel.new }

    it 'defaults to the class name / id' do
      expect(instance.key).to eq("SomeModel/#{instance.id}")
    end
  end
end
