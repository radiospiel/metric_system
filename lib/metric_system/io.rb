class MetricSystem::IO
  def initialize(io)
    expect! io => ::IO
    @io = io
  end

  def add_event(table, name, value, starts_at)
    starts_at = Time.now unless starts_at
    @io.puts "#{table} #{name} #{value} #{starts_at}"
  end

  def quit_server!
    @io.puts "QUIT:SERVER"
  end
end
