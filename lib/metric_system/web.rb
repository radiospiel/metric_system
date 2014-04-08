require 'sinatra/base'
require 'to_js'

module GoogleCharts
  extend self
  
  def convert(query)
    # A Google Chart compatible column description; see
    # https://developers.google.com/chart/interactive/docs/reference#dataparam
    cols = query.columns.map do |column|
      type = case column
      when /_at$/   then :datetime     # new Date(2008, 0, 15, 14, 30, 45)
      when /_on$/   then :date         # 
      when /value/  then :number
      else               :string
      end

      { id: column, type: type, label: column }
    end
    
    rows = query.map { |record| convert_record record, cols }
    
    { cols: cols, rows: rows }.to_js
  end
  
  private
  
  def convert_record(record, cols)
    values = cols.map do |col|
      id, type = col.values_at(:id, :type)
      v = record.send(id)

      { v: v }
      
      # case type
      # when :date      then f = "v.inspect"
      # when :datetime  then f = "v.inspect"
      # when :number    then f = "v"
      # else            f = v
      # end
      #   
    end
    
    { c: values }
  end
end

class MetricSystem::Web < Sinatra::Base
  set :environment, :development
  set :raise_errors, true
  set :views, "#{File.dirname(__FILE__)}/web"

  helpers do
    def select(query, *args)
      database = MetricSystem::Web.database
      GoogleCharts.convert database.select(query, *args)
    end
  end
  
  get '/data.js' do
    content_type "application/javascript"
    select "SELECT date(starts_at), value FROM aggregates WHERE period='day'"
  end
  
  get '/' do
    erb :index
  end
end
