module Sorceror::Serializer
  extend ActiveSupport::Concern

  def as_json(options={})
    attrs = super

    id = attrs.delete('_id') # For Mongoid

    attrs.reject! { |k,v| k.to_s.match(/^__.*__$/) }
    attrs.reject! { |k,v| v.nil? }
    attrs.merge!(id: id)

    attrs.map { |k,v| attrs[k] = v.is_a?(Time) ? v.to_f : v }

    attrs
  end
end
