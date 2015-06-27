require 'set'

module DefineConstantMacros
  def define_constant(path, base = Object, &block)
    namespace, class_name = *constant_path(path)
    klass = Class.new(base)
    namespace.const_set(class_name, klass)
    klass.class_eval(&block) if block_given?
    @defined_constants << path
    klass
  end

  def constant_path(constant_name)
    names = constant_name.to_s.split('::')
    class_name = names.pop
    namespace = names.inject(Object) { |result, name| result.const_get(name) }
    [namespace, class_name]
  end
end

RSpec.configure do |config|
  config.before(:each) do
    @defined_constants = []
  end

  config.after(:each) do
    @defined_constants.reverse.each do |path|
      namespace, class_name = *constant_path(path)
      namespace.send(:remove_const, class_name)
    end
  end

  config.include DefineConstantMacros
end
