# encoding: utf-8
$:.unshift File.expand_path("../lib", __FILE__)
$:.unshift File.expand_path("../../lib", __FILE__)

require 'sorceror/version'

Gem::Specification.new do |s|
  s.name        = "sorceror"
  s.version     = Sorceror::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Kareem Kouddous"]
  s.email       = ["kareemknyc@gmail.com"]
  s.homepage    = "http://github.com/kareemk/sorceror"
  s.summary     = "Magical event sourcing"
  s.description = "Magical event sourcing"

  s.executables   = ['sorceror']

  s.add_dependency "activesupport", ">= 4"
  s.add_dependency "activemodel", ">= 4"
  s.add_dependency "poseidon", "~> 0.0.5"
  s.add_dependency "promiscuous-poseidon_cluster", "~> 0.4.1 "
  s.add_dependency "multi_json", "~> 1.8"

  s.files        = Dir["lib/**/*"] + Dir["bin/**/*"]
  s.require_path = 'lib'
  s.has_rdoc     = false
end
