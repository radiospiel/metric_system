$: << "#{File.dirname(__FILE__)}/../lib"
require "metric_system"
require "test/unit"

class MetricSystem::TestCounters < Test::Unit::TestCase
  def setup
    MetricSystem.target = ":memory:"
  end

  def teardown
    MetricSystem.target = nil
  end

  def db
    MetricSystem
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
    db = MetricSystem.target
    expect! db => MetricSystem::Database
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
    db.aggregate :minute, :hour

    r = db.select <<-SQL
      SELECT name, value, period, starts_at
      FROM aggregates
      ORDER BY duration, name
    SQL

    assert_equal(r.map(&:to_a), [
      ["foo"    , 5, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo.bar", 2, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo"    , 5, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo.bar", 2, "hour",    Time.parse("2014-03-02 11:00:00 +0100")]
    ])
  end

  def test_double_aggregate
    db.count "foo",     3, "2014-03-02 12:10:11"
    db.count "foo.bar", 2, "2014-03-02 12:10:11"
    db.aggregate :minute, :hour
    db.aggregate :minute, :hour

    r = db.select <<-SQL
      SELECT name, value, period, starts_at
      FROM aggregates
      ORDER BY duration, name
    SQL

    assert_equal(r.map(&:to_a), [
      ["foo"    , 5, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo.bar", 2, "minute",  Time.parse("2014-03-02 11:10:00 +0100")],
      ["foo"    , 5, "hour",    Time.parse("2014-03-02 11:00:00 +0100")],
      ["foo.bar", 2, "hour",    Time.parse("2014-03-02 11:00:00 +0100")]
    ])
  end
end

