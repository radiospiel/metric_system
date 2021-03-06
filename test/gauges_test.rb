$: << "#{File.dirname(__FILE__)}/../lib"
require "metric_system"
require "test/unit"

class MetricSystem::TestGauging < Test::Unit::TestCase
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
