# This file is part of the metric_system ruby gem.
#
# Copyright (c) 2011, 2012 @radiospiel
# Distributed under the terms of the modified BSD license, see LICENSE.BSD

$:.unshift File.expand_path("../../lib", __FILE__)
puts File.expand_path("../../lib", __FILE__)

require 'rdoc/task'

RDoc::Task.new do |rdoc|
  require "metric_system/version"
  version = MetricSystem::VERSION

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "MetricSystem #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
