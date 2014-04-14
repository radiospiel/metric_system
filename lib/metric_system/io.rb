class MetricSystem::IO
  def initialize(io)
    expect! io => ::IO
    @io = io
  end

  def add_event(table, name, value, starts_at)
    if starts_at
      starts_at = Time.parse(starts_at) if starts_at.is_a?(String)
      starts_at = starts_at.to_i
    end

    @io.puts "#{table} #{name} #{value} #{starts_at}"
  end

  def quit_server!
    @io.puts "SHUTDOWN:SERVER"
  end
end
