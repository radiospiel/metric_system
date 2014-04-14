class MetricSystem::Database
  def initialize(path, readonly = false)
    require_relative "./sqlite3_extensions"
    open path

    exec "PRAGMA query_only = 1" if readonly # This might or might not work.
    @db.readonly = !!readonly
  end

  extend Forwardable

  delegate [:exec, :select, :transaction, :rollback, :print, :run, :ask, :register, :readonly?] => :"@db"

  PERIODS = [
    [ :year,   31536000, "strftime('%Y-01-01',          starts_at, 'unixepoch')" ],
    [ :month,   2592000, "strftime('%Y-%m-01',          starts_at, 'unixepoch')" ],
    [ :week,     604800, "strftime('%Y-%m-%d',          starts_at, 'unixepoch',  'weekday 1', '-7 days')" ],
    [ :day,       86400, "strftime('%Y-%m-%d',          starts_at, 'unixepoch')" ],
    [ :hour,       3600, "strftime('%Y-%m-%d %H:00:00', starts_at, 'unixepoch')" ],
    [ :minute,       60, "strftime('%Y-%m-%d %H:%M:00', starts_at, 'unixepoch')" ],
    # [ :second,        1, "strftime('%Y-%m-%d %H:%M:%S', starts_at, 'unixepoch')" ],
  ]

  private

  def open(path)
    @db = SQLite3::Database.new(path)

    [ :counters, :gauges ].each do |name|
      exec <<-SQL
      CREATE TABLE IF NOT EXISTS #{name}(
        id INTEGER PRIMARY KEY,

        name NOT NULL,                                                -- the event name
        value NOT NULL,                                               -- the value
        starts_at TIMESTAMP NOT NULL DEFAULT (strftime('%s','now'))   -- the timestamp
      );

      CREATE INDEX IF NOT EXISTS #{name}_idx1 ON #{name}(name, starts_at);

      CREATE TABLE IF NOT EXISTS aggregated_#{name}(
          id INTEGER PRIMARY KEY,

          name NOT NULL,                                      -- the event name
          starts_at TIMESTAMP NOT NULL,                       -- the start-at timestamp
          duration NOT NULL,                                  -- the duration (estimate, in secs.)
          period,                                             -- the name of the period (year, day, etc.)
          sum,                                                -- the sum of event values
          count,                                              -- the count of events
          value                                               -- the aggregated value
      );

      CREATE UNIQUE INDEX IF NOT EXISTS aggregated_#{name}_uidx1 ON aggregated_#{name}(name, starts_at, duration);
      CREATE INDEX IF NOT EXISTS aggregated_#{name}_idx2 ON aggregated_#{name}(starts_at);
      CREATE INDEX IF NOT EXISTS aggregated_#{name}_idx3 ON aggregated_#{name}(duration);
      SQL
    end

    exec <<-SQL
    PRAGMA synchronous = NORMAL;
    PRAGMA journal_mode = WAL;

    CREATE VIEW IF NOT EXISTS aggregates AS
      SELECT * FROM aggregated_gauges
        UNION
      SELECT * FROM aggregated_counters
    SQL
  end

  public

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
        run "INSERT INTO #{table}(name, value, starts_at) VALUES(?, ?, ?)", name, value, starts_at.to_i
      end
    else
      names.each do |name|
        run "INSERT INTO #{table}(name, value) VALUES(?, ?)", name, value
      end
    end
  rescue
    STDERR.puts "#{$!}: #{table} #{name.inspect}, #{value.inspect}"
  end

  PERIODS_BY_KEY = PERIODS.by(&:first)

  def aggregate(*keys)
    if keys.empty?
      keys = PERIODS.map(&:first)
    end

    transaction do
      keys.each do |period|
        aggregate_for_period :period => period, :source => :counters, :dest => :aggregated_counters, :aggregate => "sum"
      end

      exec "DELETE FROM counters"
    end

    transaction do
      keys.each do |period|
        aggregate_for_period :period => period, :source => :gauges, :dest => :aggregated_gauges, :aggregate => "CAST(sum AS FLOAT) / count"
      end

      exec "DELETE FROM gauges"
    end
  end

  private

  def aggregate_for_period(options)
    expect! options => {
      :period => PERIODS.map(&:first)
    }
    period, source, dest, aggregate = options.values_at :period, :source, :dest, :aggregate

    _, duration, starts_at = PERIODS_BY_KEY[period]

    # sql expression to calculate value from sum and count of event values
    aggregate = source == :gauges ? "CAST(sum AS FLOAT) / count" : "sum"

    exec <<-SQL
      CREATE TEMPORARY TABLE batch AS
        SELECT name, starts_at, SUM(sum) AS sum, SUM(count) AS count FROM
        (
          SELECT name     AS name,
            #{starts_at}  AS starts_at,
            SUM(value)    AS sum,
            COUNT(value)  AS count
            FROM #{source}
          GROUP BY name, starts_at

          UNION

          SELECT #{dest}.name     AS name,
            #{dest}.starts_at     AS starts_at,
            sum                   AS sum,
            count                 AS count
          FROM #{dest}
          -- INNER JOIN #{source} ON #{dest}.name=#{source}.name
          WHERE duration=#{duration}
            AND #{dest}.starts_at >= (SELECT MIN(starts_at) FROM #{source})
        )
        GROUP BY name, starts_at;

      INSERT OR REPLACE INTO #{dest}(name, starts_at, period, duration, sum, count, value)
                      SELECT name, starts_at, '#{period}', #{duration}, sum, count, #{aggregate}
                      FROM batch;
    SQL

    exec <<-SQL
      DROP TABLE batch;
    SQL
  end
end
