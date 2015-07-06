class Sorceror::Middleware
  extend Sorceror::Autoload

  DEFAULT = [:Retrying]
  autoload *DEFAULT

  def self.add(middleware)
    Chain << Head.new(middleware)
  end

  def self.reset!
    Chain.reset!
    DEFAULT.each { |default| add("Sorceror::Middleware::#{default}".constantize) }
  end

  def self.run(message, &block)
    chain = Chain.for(Tail.new(block))
    chain.head.run(message, chain.tail)
  end

  class Chain
    mattr_accessor :chain
    @@chain = []

    def initialize(chain)
      @chain = chain
    end

    def self.for(tail)
      Chain.new(@@chain.clone + [tail])
    end

    def self.<<(middleware)
      @@chain << middleware
    end

    def self.reset!
      @@chain.clear
    end

    def head
      @chain[0]
    end

    def tail
      Chain.new(@chain[1..-1])
    end
  end

  class Head
    def initialize(middleware)
      @middleware = middleware
    end

    def run(message, chain)
      @middleware.new.run(message, &-> { chain.head.run(message, chain.tail) } )
    end
  end

  class Tail
    def initialize(block)
      @block = block
    end

    def run(message, chain)
      @block.call(message)
    end
  end

  reset!
end
