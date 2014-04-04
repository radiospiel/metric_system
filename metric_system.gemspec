# This file is part of the sinatra-sse ruby gem.
#
# Copyright (c) 2011, 2012 @radiospiel
# Distributed under the terms of the modified BSD license, see LICENSE.BSD

$:.unshift File.expand_path("../lib", __FILE__)
require "metric_system/version"

Gem::Specification.new do |gem|
  gem.name     = "metric_system"
  gem.version  = MetricSystem::VERSION

  gem.author   = "radiospiel"
  gem.email    = "eno@radiospiel.org"
  gem.homepage = "http://github.com/radiospiel/sinatra-sse"
  gem.summary  = "A simple metrics aggregator"
  
  gem.add_dependency "expectation"
  gem.add_dependency "sqlite3"
  
  gem.description = gem.summary

  gem.files = Dir["**/*"].select { |d| d =~ %r{^(README|bin/|data/|ext/|lib/|spec/|test/)} }
end
