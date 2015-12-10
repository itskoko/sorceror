require 'spec_helper'

RSpec.describe Sorceror, 'Middleware Spec' do
  before { use_backend(:inline) }

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
    end
  end

  before do
    $messages = []
    $counts   = []
  end

  context 'with multiple middleware defined' do
    let(:middleware) do
      define_constant :BasicMiddleware do
        def run(message)
          $messages << message
          $counts << 1
          yield
          $counts << 2
        end
      end
    end

    let(:another_middleware) do
      define_constant :AnotherBasicMiddleware do
        def run(message)
          $counts << 3
          yield
          $counts << 4
        end
      end
    end

    before do
      Sorceror::Middleware.add(middleware)
      Sorceror::Middleware.add(another_middleware)
    end

    it 'executes the middleware in order' do
      BasicModel.new(field_1: 'field_1').create

      expect($counts).to eq([1, 3, 4, 2])

      expect($messages.count).to                                        eq(1)
      expect($messages.first.class).to                                  eq(Sorceror::Message::OperationBatch)
      expect($messages.first.operations.first.attributes['field_1']).to eq('field_1')
    end
  end

  context 'without middleware defined' do
    it 'executes the operation' do
      BasicModel.new(field_1: 'field_1').create

      expect(BasicModel.count).to eq(1)
      expect($counts.length).to   eq(0)
      expect($messages.length).to eq(0)
    end
  end

  it 'dead letters before retrying' do
    expect(Sorceror::Middleware::DEFAULT).to eq([:DeadLetter, :Retrying])
  end
end
