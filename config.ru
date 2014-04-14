$: << "#{File.dirname(__FILE__)}/lib"

require "metric_system"
require "metric_system/web"

MetricSystem.target = "samples.sqlite"

class Stats < MetricSystem::Web
  register_query :value_by_day,
    "SELECT date(starts_at) AS starts_on, value FROM aggregates WHERE period=:period"

  register_query :value_by_day_name,  
    "SELECT date(starts_at), value FROM aggregates WHERE period=:period"
end

run Stats #, 1,2,3

#run MetricSystem::Web.new
