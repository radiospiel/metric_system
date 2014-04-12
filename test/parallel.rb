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

  MetricSystem::Server.run DBPATH, SOCKET

  puts "server stopped"

  MetricSystem.print "SELECT COUNT(*) FROM aggregates"
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

# TICKS_PER_DAY = 86400
TICKS_PER_DAY = 10

File.unlink(DBPATH) rescue nil

require "time"
require "socket"

benchmark "Sending #{365 * TICKS_PER_DAY} events to metric_system" do
  distance = 24 * 3600.0 / TICKS_PER_DAY
  MetricSystem.target = UNIXSocket.new(SOCKET)
  0.upto(364) do |day|
    midnight = Time.parse("2013-01-01").to_i + day * 24 * 3600

    benchmark "Day ##{day}: add metrics" do
      1.upto(TICKS_PER_DAY) do |step|
        time = midnight + distance * step
        MetricSystem.count "clicks", 1, time
      end
    end
  end
end

STDERR.puts "Quitting server"
MetricSystem.quit_server!

__END__

db.print "SELECT COUNT(*) FROM aggregates"

db.print "SELECT * FROM aggregates"
