task :default => :test

task :test do
  Dir.glob("test/*_test.rb").each do |path|
    load path
  end
end
