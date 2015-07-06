require 'spec_helper'

RSpec.describe Sorceror::Middleware, 'Retrying' do
  before do
    $operation_fired_count = 0
    $operation_raises      = false
    $operation_completions = 0
  end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer

      operation :fire => :fired do |model, attributes|
        $operation_fired_count += 1
        raise 'fail!' if $operation_raises
        $operation_completions += 1
      end
    end
  end

  let(:retry_on_error) { false }

  before { use_backend(:inline) { |config| config.retry = retry_on_error } }

  let(:fire) do
    instance = BasicModel.create(field_1: 'field_1')
    BasicModel.new(id: instance.id).fire
  end

  context 'when the operation raises' do
    before { $operation_raises = true }

    context 'without retrying' do
      it "doesn't run the observer" do
        expect { fire }.to raise_error(RuntimeError)

        expect($operation_fired_count).to eq(1)
        expect($operation_completions).to eq(0)
      end
    end

    context 'with retrying' do
      let(:retry_on_error) { true }

      it "retries until the operation succeeds" do
        firing = Thread.new { fire }

        eventually do
          expect($operation_fired_count).to eq(1)
        end

        $operation_raises = false

        firing.join

        eventually do
          expect($operation_completions).to eq(1)
        end
      end
    end
  end

  context 'when the observer raises' do
    before do
      $observer_raises = true
      $observer_starts = 0
      $observer_fired  = 0
    end

    before do
      define_constant :BasicObserver do
        include Sorceror::Observer

        group :basic

        observer :fired, BasicModel => :fired do |model|
          $observer_starts += 1
          raise if $observer_raises
          $observer_fired += 1
        end
      end
    end

    context 'without retrying' do
      let(:retry_on_error) { false }

      it "doesn't run the observer" do
        expect { fire }.to raise_error(RuntimeError)

        expect($observer_starts).to eq(1)
        expect($observer_fired).to  eq(0)
      end
    end

    context 'with retrying' do
      let(:retry_on_error) { true }

      it "retries until the operation succeeds" do
        firing = Thread.new { fire }

        eventually do
          expect($observer_starts).to eq(1)
        end

        $observer_raises = false

        firing.join

        expect($observer_fired).to eq(1)
      end
    end
  end
end
