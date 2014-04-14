$: << "#{File.dirname(__FILE__)}/../lib"
require "metric_system"

SOCKET = "performance.socket"
DBPATH = "samples.sqlite"

mode, _ = *ARGV
if mode == nil then
  require "metric_system/server"

  Thread.new do
    sleep 1
    system "ruby #{__FILE__} sender"
  end

  MetricSystem::Server.run DBPATH, SOCKET, :quit_server => true

  puts "server stopped"

  MetricSystem.print "SELECT * FROM aggregates"
  exit
end

# ---------------------------------------------------------------------

def benchmark(msg, &block)
  starts = Time.now

  yield

ensure
  runtime = Time.now - starts
  if runtime > 0.1
    STDERR.puts "%s: %.3f secs" % [ msg, runtime = Time.now - starts ]
  end
end

TICKS = 10

File.unlink(DBPATH) rescue nil

require "time"
require "socket"

benchmark "Sending #{TICKS} events to metric_system" do
  MetricSystem.target = UNIXSocket.new(SOCKET)
  1.upto(TICKS) do |day|
    MetricSystem.count "clicks", 1
  end
end

STDERR.puts "Quitting server"
MetricSystem.quit_server!

__END__

db.print "SELECT COUNT(*) FROM aggregates"

db.print "SELECT * FROM aggregates"
