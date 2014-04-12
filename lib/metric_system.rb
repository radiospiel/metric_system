require "expectation"

require_relative "metric_system/core_extensions"

module MetricSystem
  extend self

  attr :target

  def target=(target)
    case target
    when nil

    when String
      require_relative "metric_system/database"
      extend MetricSystem::Database
    else
      require_relative "metric_system/io"
      extend MetricSystem::IO
    end

    open(target)
  end

  def gauge(name, value, starts_at = nil)
    add_event :gauges, name, value, starts_at
  end

  def count(name, value, starts_at = nil)
    add_event :counters, name, value, starts_at
  end

  def measure(name, starts_at = nil, &block)
    start = Time.now
    yield.tap do
      gauge name, Time.now - start, starts_at
    end
  end
end
