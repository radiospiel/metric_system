$: << "#{File.dirname(__FILE__)}/lib"

require "metric_system"
require "metric_system/web"

MetricSystem.target = "samples.sqlite"
MetricSystem.target.register :value_by_day,
  "SELECT date(starts_at) AS starts_on, value FROM aggregates WHERE period='day'"

MetricSystem.target.register :value_by_day_name,  
  "SELECT date(starts_at),              value FROM aggregates WHERE period='day'"

run MetricSystem::Web
