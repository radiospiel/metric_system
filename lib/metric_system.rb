require "expectation"

require_relative "metric_system/core_extensions"

module MetricSystem
  extend self

  attr :target

  def target=(target)
    case target
    when nil
      @target = nil
    when String
      require_relative "metric_system/database"
      @target = MetricSystem::Database.new(target)
    else
      require_relative "metric_system/io"
      @target = MetricSystem::IO.new(target)
    end
  end

  extend Forwardable
  delegate [:aggregate, :select, :print, :run, :ask, :register] => :"@target"

  def gauge(name, value, starts_at = nil)
    @target.add_event :gauges, name, value, starts_at
  end

  def count(name, value, starts_at = nil)
    @target.add_event :counters, name, value, starts_at
  end

  def measure(name, starts_at = nil, &block)
    start = Time.now
    yield.tap do
      gauge name, Time.now - start, starts_at
    end
  end
end
