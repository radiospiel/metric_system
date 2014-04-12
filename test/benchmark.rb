$: << "#{File.dirname(__FILE__)}/../lib"
require "metric_system"

def benchmark(msg, &block)
  starts = Time.now

  yield

ensure
  runtime = Time.now - starts
  if runtime > 0.1
    STDERR.puts "%s: %.3f secs" % [ msg, runtime = Time.now - starts ]
  end
end

TICKS_PER_DAY = 86400
TICKS_PER_DAY = 10
DBPATH = "samples.sqlite"

File.unlink(DBPATH) rescue nil
MetricSystem.target = DBPATH

benchmark "Building metric_system" do
  distance = 24 * 3600.0 / TICKS_PER_DAY

  0.upto(365) do |day|
    midnight = Time.parse("2013-01-01").to_i + day * 24 * 3600

    benchmark "Day ##{day}: add metrics" do
      MetricSystem.transaction do
        1.upto(TICKS_PER_DAY) do |step|
          time = midnight + distance * step
          STDERR.print "."
          MetricSystem.count "clicks", 1, time
        end
      end
    end

    benchmark "Day ##{day}: aggregate" do
      MetricSystem.aggregate
    end
  end
end

MetricSystem.print "SELECT COUNT(*) FROM aggregates"

__END__
db.print "SELECT * FROM aggregates"
