require 'eventmachine'
#require 'eventmachine/timer'

module MetricSystem::Server
  include EM::P::LineProtocol
  extend self

  module Buffer
    extend self

    def buffer
      @buffer ||= []
    end

    def take
      taken, @buffer = @buffer, []
      taken
    end

    def push(event)
      buffer << event
    end

    def length
      buffer.length
    end
  end

  class Event < Struct.new(:table, :name, :value, :time)
    def self.parse(line)
      table, name, value, time, remainder = line.split(" ", 5)
      value = value.to_f
      time = time ? time.to_i : Time.now.to_i
      new(table, name, value, time)
    end
  end

  def receive_line(line)
    if line == "SHUTDOWN:SERVER"
      MetricSystem::Server.shutdown
      return
    end

    return if MetricSystem::Server.shutting_down?

    return unless event = Event.parse(line)

    Buffer.push event

    MetricSystem::Server.flush if Buffer.length % 1000 == 0
  rescue
    STDERR.puts "#{$!}, from\n\t" + $!.backtrace.join("\t")
  end

  def self.flush
    return unless Buffer.length > 0

    if @busy
      STDERR.puts "    Waiting for writer: backlog is #{Buffer.length} entries"
      return
    end

    @busy = true

    events = Buffer.take

    operation = proc {
      flush_events events
    }
    callback = proc {
      @busy = nil

      if shutting_down?
        flush_events(Buffer.take)
        EM.stop
      end
    }
    EventMachine.defer operation, callback
  rescue
    STDERR.puts "#{$!}, from\n\t" + $!.backtrace.join("\t")
  end

  def self.flush_events(events)
    return if events.empty?

    starts_at = Time.now
    MetricSystem.transaction do
      events.each do |event|
        MetricSystem.add_event event.table, event.name, event.value, event.time
      end
    end
    STDERR.puts "    Writing #{events.count} events: %.3f secs" % (Time.now - starts_at)

    starts_at = Time.now
    MetricSystem.aggregate
    STDERR.puts "    Merging #{events.count} events: %.3f secs" % (Time.now - starts_at)
  end

  # Note that this will block current thread.
  def self.run(db, socket_path, options = {})
    @options = options || {}
    MetricSystem.target = db

    STDERR.puts "Starting server at socket: #{socket_path}"

    EventMachine.run {
      EventMachine::PeriodicTimer.new(1) do
        MetricSystem::Server.flush
      end

      EventMachine.start_server socket_path, MetricSystem::Server
    }
  end

  def self.shutting_down?
    @shutting_down
  end

  def self.shutdown
    return if shutting_down?
    return unless @options[:quit_server]

    @shutting_down = true
    MetricSystem::Server.flush
  end
end
