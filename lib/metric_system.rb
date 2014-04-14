require "expectation"
require "forwardable"

module MetricSystem
end

require_relative "metric_system/core_extensions"
require_relative "metric_system/version"

module MetricSystem
  extend self

  attr :target, :database

  def target=(target)
    @target = @database = nil

    case target
    when nil
    when String
      require_relative "metric_system/database"
      @target = @database = MetricSystem::Database.new(target)
    else
      require_relative "metric_system/io"
      @target = MetricSystem::IO.new(target)
    end
  end

  extend Forwardable
  delegate [:aggregate, :select, :print, :run, :ask] => :"@target"
  delegate [:transaction] => :"@target"
  delegate [:add_event] => :"@target"
  delegate [:quit_server!] => :"@target"

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
