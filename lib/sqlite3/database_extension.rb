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
    expect! sql => String

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

  def ask(sql, *args)
    query(sql).ask *prepare_arguments(args)
  end

  # run a select like query. Returns an array of records.
  def select(sql, *args)
    query(sql).select *prepare_arguments(args)
  end

  def print(sql, *args)
    require "pp"

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
