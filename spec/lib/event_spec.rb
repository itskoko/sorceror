require 'spec_helper'

RSpec.describe Sorceror do
    before do
      $snapshot_observer_starts = 0
      $snapshot_observer_model = nil
      $event_observer_starts = 0
      $event_observer_attrs = nil
    end

  before do
    define_constant :BasicModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer

      operation :fire => :fired do |model, attributes|
      end
    end

    define_constant :BasicEventObserver do
      include Sorceror::Observer

      group :basic

      observer :basic_model_fired, BasicModel => :fired do |attrs|
        $event_observer_starts += 1
        $event_observer_attrs = attrs
      end
    end

    define_constant :BasicSnapshotObserver do
      include Sorceror::Observer

      group :basic

      observer :basic_model_snapshots, BasicModel do |model|
        $snapshot_observer_starts += 1
        $snapshot_observer_model = model
      end
    end
  end

  before { use_backend(:inline) }

  describe 'Event execution' do
    let(:instance) { BasicModel.new(field_1: 'field_1') }
    let(:fire) do
      instance.create
      BasicModel.new(id: instance.id).fire(:something => :changed)
    end

    it 'invokes snapshot and event observers' do
      fire

      expect($event_observer_starts).to eq(1)
      expect($event_observer_attrs).to  eq('something' => 'changed')

      expect($snapshot_observer_starts).to        eq(2)
      expect($snapshot_observer_model.id).to      eq(instance.id)
      expect($snapshot_observer_model.field_1).to eq(instance.field_1)
    end
  end
end
