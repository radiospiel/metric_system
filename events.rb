# -- sqlite3 + improvements ---------------------------------------------------

require "sqlite3"

# manage SQLite3::Records
#
# The SQLite3::Record module is able to generate classes that are optimized
# for a specific set of columns. It is build on top of Struct, which is way
# faster than Hashes, for example.
module SQLite3::Record
  attr :columns, true
  
  def build(*attrs)
    attrs = columns.zip(attrs).map do |key, value|
      case key
      when /_at$/ then value = Time.parse(value)
      when /_on$/ then value = Date.parse(value)
      else value
      end
    end
    
    new *attrs
  end
  
  def self.for_columns(columns)
    columns = columns.map(&:to_sym)

    @@classes ||= {}
    @@classes[columns] ||= begin
      struct = Struct.new(*columns)
      struct.extend SQLite3::Record
      struct.columns = columns
      struct
    end
  end
end

class SQLite3::Query
  def initialize(statement)
    # expect! statement => SQLite3::Statement
    @statement = statement
  end
  
  def run(*args)
    @statement.execute *args
  end
  
  def select(*args)
    @klass ||= SQLite3::Record.for_columns(@statement.columns)

    run(*args).map do |rec|
      @klass.build *rec
    end
  end
end

class SQLite3::Database
  # execute multiple SQL statements at once.
  def exec(sql, *args)
    while sql =~ /\S/ do
      statement = prepare(sql)

      sql = statement.remainder
      if statement.active?
        statement.execute!(*args)
      end
    end
  end

  # -- cached queries ---------------------------------------------------------
  
  private
  
  def query(sql)
    @queries ||= {}
    @queries[sql] ||= SQLite3::Query.new prepare(sql)
  end

  public
  
  def run(sql, *args)
    query(sql).run *args
  end

  # run a select like query. Returns an array of records.
  def select(sql, *args)
    query(sql).select *args
  end
end


# -- The Events module --------------------------------------------------------

module Events
end

require "forwardable"
class Events::DB
  SCHEMA=<<-SQL
  PRAGMA synchronous = NORMAL;

  CREATE TABLE IF NOT EXISTS gauges(
    id INTEGER PRIMARY KEY,
  
    name NOT NULL,                                                -- the event name
    value NOT NULL,                                               -- the value
    starts_at TIMESTAMP NOT NULL DEFAULT (strftime('%s','now'))   -- the timestamp
  );

  CREATE TABLE IF NOT EXISTS counters(
    id INTEGER PRIMARY KEY,
  
    name NOT NULL,                                                -- the event name
    value NOT NULL,                                               -- the value
    starts_at TIMESTAMP NOT NULL DEFAULT (strftime('%s','now'))   -- the timestamp
  );

  CREATE TABLE IF NOT EXISTS aggregates(
      id INTEGER PRIMARY KEY,

      name NOT NULL,                                      -- the event name
      starts_at TIMESTAMP NOT NULL,                       -- the start-at timestamp
      period,                                             -- the length of the period
      value                                               -- number of name events in periods
  );

  CREATE INDEX IF NOT EXISTS aggregates_idx1 ON aggregates(name);
  CREATE INDEX IF NOT EXISTS aggregates_idx2 ON aggregates(starts_at);

  SQL


  extend Forwardable
  delegate [:select, :transaction, :rollback] => :@db

  def initialize(path)
    @db = SQLite3::Database.new(path)
    @db.exec SCHEMA
  end

  # def gauge(name, value, starts_at = nil)
  #   add_event :gauges, name, value, starts_at
  # end

  def count(name, value, starts_at = nil)
    add_event :counters, name, value, starts_at
  end
  
  private
  
  def add_event(table, name, value, starts_at)
    # get names of all related events. An event "a.b.c" is actually
    # 3 events: "a", "a.b", "a.b.c"
    names = begin
      parts = name.split(".")
      parts.length.downto(1).map do |cnt|
        parts[0,cnt].join(".")
      end
    end

    if starts_at
      starts_at = Time.parse(starts_at) if starts_at.is_a?(String)
      
      names.each do |name|
        @db.run "INSERT INTO #{table}(name, value, starts_at) VALUES(?, ?, ?)", name, value, starts_at.to_i
      end
    else
      names.each do |name|
        @db.run "INSERT INTO #{table}(name, value) VALUES(?, ?)", name, value
      end
    end
  end
  
  public
  
  PERIOD_START_SQL_FRAGMENT = {
    year:     "strftime('%Y-01-01',          starts_at, 'unixepoch')",
    month:    "strftime('%Y-%m-01',          starts_at, 'unixepoch')",
    week:     "strftime('%Y-%m-%d',          starts_at, 'unixepoch', 'weekday 1', '-7 days')",
    day:      "strftime('%Y-%m-%d',          starts_at, 'unixepoch')",
    hour:     "strftime('%Y-%m-%d %H:00:00', starts_at, 'unixepoch')",
    minute:   "strftime('%Y-%m-%d %H:%M:00', starts_at, 'unixepoch')",
    second:   "strftime('%Y-%m-%d %H:%M:%S', starts_at, 'unixepoch')",
  }
  
  def aggregate
    aggregate_counters
  end
  
  def aggregate_counters
    transaction do

      PERIOD_START_SQL_FRAGMENT.each do |key, starts_at|
        benchmark "aggregate #{key.inspect} values" do
          aggregate_for_period key
        end
      end

      @db.exec "DELETE FROM counters"
    end
  end
  
  def aggregate_for_period(key)
    starts_at = PERIOD_START_SQL_FRAGMENT[key]

    @db.exec <<-SQL
      -- preaggregate counters from aggregates into batch_for_#{key} 
      CREATE TEMPORARY TABLE batch_for_#{key} AS
        SELECT name, starts_at, SUM(value) AS value FROM
        (
          SELECT name     AS name, 
            #{starts_at}  AS starts_at,
            value         AS value
          FROM counters
        
          UNION
        
          SELECT name     AS name, 
            starts_at     AS starts_at,
            value         AS value
          FROM aggregates
          WHERE period='#{key}'
        );
      
      --
      DELETE FROM aggregates
      WHERE period='#{key}';
      
      INSERT INTO aggregates(name, starts_at, period, value) 
      SELECT name, starts_at, '#{key}', value FROM batch_for_#{key};
    SQL
  end
end

def benchmark(msg, &block)
  starts = Time.now

  yield

ensure
  runtime = Time.now - starts
  if runtime > 0.5
    STDERR.puts "%s: %.3f secs" % [ msg, runtime = Time.now - starts ]
  end
end


require "pp"

require "test/unit"

class Array
  def by(key = nil, &block)
    ary = []

    if key
      each do |rec|
        ary << rec[key] << rec
      end
    else
      each do |value|
        ary << yield(value) << value
      end
    end

    Hash[*ary]
  end
end

class Events::Test < Test::Unit::TestCase
  def test_success
    assert true
  end
  
  def test_single_aggregate
    db = Events::DB.new ":memory:"
    db.count "foo", 1, "2014-04-02 12:10:11"
    db.aggregate
    
    r = db.select "SELECT name, value, period, starts_at FROM aggregates"
    pp r.by(&:period)
  end
end

__END__

# Open a database
require "fileutils"
db = Events::DB.new "events.db"
db = Events::DB.new ":memory:"

NAMES = %w(foo bar baz)
SUBNAMES = %w(left right top bottom)

COUNT=250 # 000

benchmark  "Added #{2*COUNT} entries" do
  db.transaction do

    COUNT.times do 
      name, subname = NAMES.sample, SUBNAMES.sample
      value = rand(1000)
      value *= value

      db.count "#{name}.#{subname}", value
      db.count "#{name}.#{subname}", value, Time.now - 100 * 3600 * 24 + rand(80000)
    end 
  end
end

benchmark  "aggregated" do
  db.aggregate
end
__END__

#
events.aggregate(:year)
events.aggregate(:month)
events.aggregate(:week)
events.aggregate(:day)
events.aggregate(:hour)
events.aggregate(:minute)
events.aggregate(:second)
