require 'spec_helper'

RSpec.describe Sorceror, 'create' do
  before do
    $observer_model = nil
    $observer_fired_count = 0
  end

  before do
    define_constant :ObserverModel do
      include Mongoid::Document
      include Sorceror::Model

      field :field_1, type: String
      field :field_2, type: Integer
    end

    define_constant :CreatedObserver do
      include Sorceror::Observer

      group :consistency, snapshot: true

      observer :created, ObserverModel do |model|
        $observer_model = model
        $observer_fired_count += 1
      end
    end
  end

  before { use_backend(:fake) }


  describe 'observers' do
    let(:id) { BSON::ObjectId.new }

    context 'when the underlying model changes while processing an event' do
      it 'does not use pass the latest state of the model to the observer' do
        ObserverModel.new(id: id, field_1: 'field_1').create

        process_operations!
        ObserverModel.collection.find(_id: id).update('$set' => { field_1: 'field_1_updated' })
        process_snapshots!

        expect($observer_model.id).to      eq(id)
        expect($observer_model.field_1).to eq('field_1')
      end
    end
  end
end
