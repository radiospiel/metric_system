require "expectation"

# -- sqlite3 + improvements ---------------------------------------------------

require "sqlite3"

# manage SQLite3::Records
#
# The SQLite3::Record module is able to generate classes that are optimized
# for a specific set of columns. It is build on top of Struct, which is way
# faster than Hashes, for example.
module SQLite3::Record
  module ClassMethods
    attr :columns, true

    private

    def to_time(s)
      case s
      when String then Time.parse(s)
      when Fixnum then Time.at(s)
      else s
      end
    end

    def to_date(s)
      return unless time = to_time(s)
      time.to_date
    end

    public

    def build(*attrs)
      attrs = columns.zip(attrs).map do |key, value|
        case key
        when /_at$/ then to_time(value)
        when /_on$/ then to_date(value)
        else value
        end
      end

      new *attrs
    end
  end

  def to_a
    self.class.columns.map do |column| send(column) end
  end

  def self.for_columns(columns)
    columns = columns.map(&:to_sym)

    @@classes ||= {}
    @@classes[columns] ||= begin
      struct = Struct.new(*columns)
      struct.extend SQLite3::Record::ClassMethods
      struct.include SQLite3::Record

      struct.columns = columns
      struct
    end
  end
end

class SQLite3::Query
  def initialize(sql, statement)
    expect! statement => SQLite3::Statement

    @sql, @statement = sql, statement
  end

  def run(*args)
    # STDERR.puts "Q: #{@sql} #{args.map(&:inspect).join(", ")}"
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
    args = prepare_arguments(args)

    while sql =~ /\S/ do
      statement = prepare(sql)

      sql = statement.remainder
      if statement.active?
        statement.execute!(*args)
      end
    end

  rescue
    STDERR.puts "#{sql}: #{$!}"
    raise
  end

  # -- cached queries ---------------------------------------------------------

  private

  def query(sql)
    @queries ||= {}
    @queries[sql] ||= SQLite3::Query.new sql, prepare(sql)
  end

  def prepare_arguments(args)
    args.map do |arg|
      case arg
      when Time then arg.to_i
      when Date then arg.to_time.to_i
      else arg
      end
    end
  end

  public

  def run(sql, *args)
    query(sql).run *prepare_arguments(args)
  end

  # run a select like query. Returns an array of records.
  def select(sql, *args)
    query(sql).select *prepare_arguments(args)
  end

  def print(sql, *args)
    results = select sql, *args
    log_sql = sql.gsub(/\n/, " ").gsub(/\s+/, " ")
    puts "=" * log_sql.length
    puts log_sql
    puts "-" * log_sql.length

    results.each do |result|
      pp result.to_a
    end
    puts "=" * log_sql.length
  end
end


# -- The Events module --------------------------------------------------------

module Events
end

require "forwardable"
class Events::DB
  extend Forwardable
  delegate [:exec, :select, :transaction, :rollback] => :@db

  def initialize(path)
    @db = SQLite3::Database.new(path)

    [ :counters, :gauges ].each do |name|
      exec <<-SQL
      CREATE TABLE IF NOT EXISTS #{name}(
        id INTEGER PRIMARY KEY,

        name NOT NULL,                                                -- the event name
        value NOT NULL,                                               -- the value
        starts_at TIMESTAMP NOT NULL DEFAULT (strftime('%s','now'))   -- the timestamp
      );

      CREATE TABLE IF NOT EXISTS aggregated_#{name}(
          id INTEGER PRIMARY KEY,

          name NOT NULL,                                      -- the event name
          starts_at TIMESTAMP NOT NULL,                       -- the start-at timestamp
          duration,                                           -- the duration (estimate, in secs.)
          period,                                             -- the name of the period (year, day, etc.)
          sum,                                                -- the sum of event values
          count,                                              -- the count of events
          value                                               -- the aggregated value
      );

      CREATE INDEX IF NOT EXISTS aggregated_#{name}_idx1 ON aggregated_#{name}(name);
      CREATE INDEX IF NOT EXISTS aggregated_#{name}_idx2 ON aggregated_#{name}(starts_at);
      CREATE INDEX IF NOT EXISTS aggregated_#{name}_idx2 ON aggregated_#{name}(period);
      CREATE INDEX IF NOT EXISTS aggregated_#{name}_idx2 ON aggregated_#{name}(duration);
      SQL
    end

    exec <<-SQL
    PRAGMA synchronous = NORMAL;

    CREATE VIEW IF NOT EXISTS aggregates AS
      SELECT * FROM aggregated_gauges
        UNION
      SELECT * FROM aggregated_counters
    SQL
  end

  def gauge(name, value, starts_at = nil)
    add_event :gauges, name, value, starts_at
  end

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
    year:     [ 31536000, "strftime('%Y-01-01',          starts_at, 'unixepoch')" ],
    month:    [  2592000, "strftime('%Y-%m-01',          starts_at, 'unixepoch')" ],
    week:     [   604800, "strftime('%Y-%m-%d',          starts_at, 'unixepoch',  'weekday 1', '-7 days')" ],
    day:      [    86400, "strftime('%Y-%m-%d',          starts_at, 'unixepoch')" ],
    hour:     [     3600, "strftime('%Y-%m-%d %H:00:00', starts_at, 'unixepoch')" ],
    minute:   [       60, "strftime('%Y-%m-%d %H:%M:00', starts_at, 'unixepoch')" ],
    second:   [        1, "strftime('%Y-%m-%d %H:%M:%S', starts_at, 'unixepoch')" ],
  }

  def aggregate(*keys)
    if keys.empty?
      keys = PERIOD_START_SQL_FRAGMENT.keys
    end

    transaction do
      keys.each do |key|
        aggregate_for_period key, :counters
      end

      @db.exec "DELETE FROM counters"
    end

    transaction do
      keys.each do |key|
        aggregate_for_period key, :gauges
      end

      @db.exec "DELETE FROM gauges"
    end
  end

  def aggregate_for_period(key, events_table)
    key = key.to_sym

    expect! key => PERIOD_START_SQL_FRAGMENT.keys
    expect! events_table => [ :counters, :gauges ]

    aggregated_table = "aggregated_#{events_table}"

    duration, starts_at = PERIOD_START_SQL_FRAGMENT[key]

    # sql expression to calculate value from sum and count of event values
    aggregate = events_table == :gauges ? "CAST(sum AS FLOAT) / count" : "sum"

    @db.exec <<-SQL
      CREATE TEMPORARY TABLE batch AS
        SELECT name, starts_at, SUM(sum) AS sum, SUM(count) AS count FROM
        (
          SELECT name     AS name,
            #{starts_at}  AS starts_at,
            SUM(value)    AS sum,
            COUNT(value)  AS count
            FROM #{events_table}
          GROUP BY name, starts_at

          UNION

          SELECT name     AS name,
            starts_at     AS starts_at,
            sum           AS sum,
            count         AS count
          FROM #{aggregated_table}
          WHERE duration=#{duration}
        )
        GROUP BY name, starts_at;

      DELETE FROM #{aggregated_table}
      WHERE duration=#{duration};

      INSERT INTO #{aggregated_table}(name, starts_at, period, duration, sum, count, value)
                      SELECT name, starts_at, '#{key}', #{duration}, sum, count, #{aggregate}
                      FROM batch;
    SQL

    @db.exec <<-SQL
      DROP TABLE batch;
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

class PP
  class << self
    alias_method :old_pp, :pp
    def pp(obj, out = $>, width = 140)
      old_pp(obj, out, width)
    end
  end
end

require "test/unit"

class Hash
  def to_ostruct
    require "ostruct"
    OpenStruct.new self
  end
end

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

module Events::TestCases
end

class Events::TestCases::Counters < Test::Unit::TestCase
  def db
    @db ||= Events::DB.new ":memory:"
  end

  def test_two_events
    db.count "foo",     1, "2014-03-02 12:10:11"
    db.count "foo",     1, "2014-03-02 14:10:11"
    db.aggregate :minute, :hour, :day

    r = db.select("SELECT name, value, period, starts_at FROM aggregates ORDER BY duration, name, starts_at")
    r = r.map(&:to_a)

    assert_equal(r, [
      ["foo", 1, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo", 1, "minute",  Time.parse("2014-03-02 13:10:00 +0100")],
      ["foo", 1, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo", 1, "hour",    Time.parse("2014-03-02 13:00:00 +0100")],
      ["foo", 2, "day",     Time.parse("2014-03-02 00:00:00 +0100")],
    ])
  end

  def test_conversion
    db.exec "CREATE TABLE tmp(value, starts_at, starts_on)"
    now = Time.parse("2014-03-02 11:10:11 +0100")
    day = Date.parse("2014-03-02")

    db.exec "INSERT INTO tmp (value, starts_at, starts_on) VALUES(?, ?, ?)", "one", now, now
    rows = db.select("SELECT value, starts_at, starts_on FROM tmp")
    assert_equal rows.map(&:to_a), [
      [ "one", now, day ]
    ]
  end

  def test_single_aggregate
    db.count "foo", 1, "2014-03-02 12:10:11"
    db.aggregate

    r = db.select("SELECT name, value, period, starts_at FROM aggregates ORDER BY duration, name")
    assert_equal(r.map(&:to_a), [
      ["foo", 1, "second",  Time.parse("2014-03-02 11:10:11 +0100")],
      ["foo", 1, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo", 1, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo", 1, "day",     Time.parse("2014-03-02 00:00:00 +0100")],
      ["foo", 1, "week",    Time.parse("2014-02-24 00:00:00 +0100")],
      ["foo", 1, "month",   Time.parse("2014-03-01 00:00:00 +0100")],
      ["foo", 1, "year",    Time.parse("2014-01-01 00:00:00 +0100")]
    ])
  end

  def test_store_combined_name
    db.count "foo",     1, "2014-03-02 12:10:11"
    db.count "foo.bar", 2, "2014-03-02 12:10:11"
    r = db.select("SELECT name, value, starts_at FROM counters ORDER BY name")
    assert_equal(r.map(&:to_a), [
      ["foo"    , 1, Time.parse("2014-03-02 12:10:11 +0100")],
      ["foo"    , 2, Time.parse("2014-03-02 12:10:11 +0100")],
      ["foo.bar", 2, Time.parse("2014-03-02 12:10:11 +0100")]
      ])
  end

  def test_combined_name
    db.count "foo",     3, "2014-03-02 12:10:11"
    db.count "foo.bar", 2, "2014-03-02 12:10:11"
    db.aggregate :second, :minute, :hour

    r = db.select <<-SQL
      SELECT name, value, period, starts_at
      FROM aggregates
      ORDER BY duration, name
    SQL

    assert_equal(r.map(&:to_a), [
      ["foo"    , 5, "second",  Time.parse("2014-03-02 11:10:11 +0100")],
      ["foo.bar", 2, "second",  Time.parse("2014-03-02 11:10:11 +0100")],
      ["foo"    , 5, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo.bar", 2, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo"    , 5, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo.bar", 2, "hour",    Time.parse("2014-03-02 11:00:00 +0100")]
    ])
  end

  def test_double_aggregate
    db.count "foo",     3, "2014-03-02 12:10:11"
    db.count "foo.bar", 2, "2014-03-02 12:10:11"
    db.aggregate :second, :minute, :hour
    db.aggregate :second, :minute, :hour

    r = db.select <<-SQL
      SELECT name, value, period, starts_at
      FROM aggregates
      ORDER BY duration, name
    SQL

    assert_equal(r.map(&:to_a), [
      ["foo"    , 5, "second",  Time.parse("2014-03-02 11:10:11 +0100")],
      ["foo.bar", 2, "second",  Time.parse("2014-03-02 11:10:11 +0100")],
      ["foo"    , 5, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo.bar", 2, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo"    , 5, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo.bar", 2, "hour",    Time.parse("2014-03-02 11:00:00 +0100")]
    ])
  end
end

class Events::TestCases::Gauging < Test::Unit::TestCase
  def db
    @db ||= Events::DB.new ":memory:"
  end

  def test_two_events
    db.gauge "foo", 1, "2014-03-02 12:10:11"
    db.gauge "foo", 2, "2014-03-02 14:10:11"
    db.aggregate :minute, :hour, :day

    r = db.select("SELECT name, value, period, starts_at FROM aggregates ORDER BY duration, name, starts_at")
    r = r.map(&:to_a)

    assert_equal(r, [
      ["foo", 1, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo", 2, "minute",  Time.parse("2014-03-02 13:10:00 +0100")],
      ["foo", 1, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo", 2, "hour",    Time.parse("2014-03-02 13:00:00 +0100")],
      ["foo", 1.5, "day",   Time.parse("2014-03-02 00:00:00 +0100")],
    ])
  end
end

class Events::TestCases::Mixed < Test::Unit::TestCase
  def db
    @db ||= Events::DB.new ":memory:"
  end

  def xtest_two_events
    db.gauge "foo", 1, "2014-03-02 12:10:11"
    db.gauge "foo", 2, "2014-03-02 14:10:11"
    db.count "bar", 1, "2014-03-02 12:10:11"
    db.count "bar", 2, "2014-03-02 14:10:11"
    db.aggregate :minute, :hour, :day

    r = db.select("SELECT name, value, period, starts_at FROM aggregates ORDER BY name, duration, starts_at")
    r = r.map(&:to_a)

    assert_equal(r, [
      ["bar", 1,    "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["bar", 2,    "minute",  Time.parse("2014-03-02 13:10:00 +0100")],
      ["bar", 1,    "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["bar", 2,    "hour",    Time.parse("2014-03-02 13:00:00 +0100")],
      ["bar", 3,    "day",     Time.parse("2014-03-02 00:00:00 +0100")],
      ["foo", 1,    "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo", 2,    "minute",  Time.parse("2014-03-02 13:10:00 +0100")],
      ["foo", 1,    "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo", 2,    "hour",    Time.parse("2014-03-02 13:00:00 +0100")],
      ["foo", 1.5,  "day",     Time.parse("2014-03-02 00:00:00 +0100")],
    ])
  end

  def test_two_events_x
    db.count "bar", 1, "2014-03-02 12:10:11"
    db.count "bar", 2, "2014-03-02 14:10:11"
    db.gauge "foo", 1, "2014-03-02 12:10:11"

    db.aggregate :minute, :hour, :day

    r = db.select("SELECT name, value, period, starts_at FROM aggregates WHERE name='bar' ORDER BY name, duration, starts_at")
    r = r.map(&:to_a)

    assert_equal(r, [
      ["bar", 1,    "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["bar", 2,    "minute",  Time.parse("2014-03-02 13:10:00 +0100")],
      ["bar", 1,    "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["bar", 2,    "hour",    Time.parse("2014-03-02 13:00:00 +0100")],
      ["bar", 3,    "day",     Time.parse("2014-03-02 00:00:00 +0100")],
    ])
  end
end
