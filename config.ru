$: << "#{File.dirname(__FILE__)}/lib"

require "metric_system"
require "metric_system/web"

class Stats < MetricSystem::Web
  set :database, "samples.sqlite"

  register_query :value_by_day,
    "SELECT date(starts_at) AS starts_on, value FROM aggregates WHERE period=:period"

  register_query :value_by_day_name,
    "SELECT date(starts_at), value FROM aggregates WHERE period=:period"
end

run Stats

#.new("samples.sqlite")

#, 1,2,3

#run MetricSystem::Web.new
