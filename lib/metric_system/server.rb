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
    return unless event = Event.parse(line)

    Buffer.push event

    return unless Buffer.length % 1000 == 0

    MetricSystem::Server.flush
  rescue
    STDERR.puts "#{$!}, from\n\t" + $!.backtrace.join("\t")
  end

  def self.db
    @db
  end
  def self.db=(db)
    @db = db
  end

  def self.flush
    return unless Buffer.length > 0

    unless @db
      STDERR.puts "    Waiting for writer: backlog is #{Buffer.length} entries"
      return
    end
    
    db, @db = @db, nil

    events = Buffer.take

    operation = proc {
      starts_at = Time.now
      db.transaction do
        events.each do |event|
          db.add_event event.table, event.name, event.value, event.time
        end
      end

      starts_at = Time.now
      db.aggregate

      STDERR.puts "        Merging #{events.count} events: %.3f secs" % (Time.now - starts_at)
      
      db
    }
    callback = proc { |db| 
      @db = db
    }
  
    EventMachine.defer operation, callback    
  rescue
    STDERR.puts "#{$!}, from\n\t" + $!.backtrace.join("\t")
  end
  
  # Note that this will block current thread.
  def self.run(db, socket_path)
    @db = MetricSystem.new db

    STDERR.puts "Starting server at socket: #{socket_path}"
    
    EventMachine.run {
      EventMachine::PeriodicTimer.new(1) do
        MetricSystem::Server.flush
      end

      EventMachine.start_server socket_path, MetricSystem::Server
    }
  end
end
