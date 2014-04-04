def benchmark(msg, &block)
  starts = Time.now

  yield

ensure
  runtime = Time.now - starts
  if runtime > 0.5
    STDERR.puts "%s: %.3f secs" % [ msg, runtime = Time.now - starts ]
  end
end
