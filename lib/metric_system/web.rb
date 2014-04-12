require 'sinatra/base'
require 'to_js'

module GoogleCharts
  extend self

  def convert(results)
    results.description.to_js
  end

  private

end

class MetricSystem::Web < Sinatra::Base
  set :environment, :development
  set :raise_errors, true
  set :views, "#{File.dirname(__FILE__)}/web"
  set :dump_errors, true

  helpers do
    def select(query, *args)
      @result_cache ||= {}
      @result_cache[query] ||= MetricSystem.database.select(query, *args).data_table.to_js
    end
  end

  get '/:query.js' do
    content_type "application/javascript"
    select params[:query].to_sym
  end

  get '/' do
    erb :dashboard
  end
end
