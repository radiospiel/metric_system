$: << "#{File.dirname(__FILE__)}/lib"
require "metric_system"
require "metric_system/web"

database = MetricSystem.new("samples.sqlite")
database.register :value_by_day,
  "SELECT date(starts_at) AS starts_on, value FROM aggregates WHERE period='day'"

database.register :value_by_day_name,  
  "SELECT date(starts_at),              value FROM aggregates WHERE period='day'"

MetricSystem::Web.database = database

run MetricSystem::Web
