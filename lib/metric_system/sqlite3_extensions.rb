require "sqlite3"
require "pp"

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

  def ask(*args)
    results = run(*args)
    row = results.first
    results.reset

    if !row               then  nil
    elsif row.length == 1 then  row.first
    else                        row
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

  def ask(sql, *args)
    query(sql).ask *prepare_arguments(args)
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
