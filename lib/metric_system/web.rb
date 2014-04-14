require 'sinatra/base'
require 'to_js'

class MetricSystem::Web < Sinatra::Base
  set :environment, :development
  set :raise_errors, true
  set :views, "#{File.dirname(__FILE__)}/web"
  set :dump_errors, true

  def self.register_query(name, query)
    registry[name] = query
  end

  def self.registry
    @registry ||= {}
  end

  def self.select(query, *args)
    expect! query => [ String, Symbol ]

    if query.is_a?(Symbol)
      expect! query => registry.keys
      query = registry.fetch(query)
    end

    result = MetricSystem.select query, *args #period: "month"
    result.data_table.to_js
  end

  helpers do
    def select(query, *args)
      @result_cache ||= {}
      @result_cache[[query, args]] ||= self.class.select(query, *args, params)
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
